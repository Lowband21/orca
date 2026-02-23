use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::{PgPool, Row};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::job::{DependencyKey, Job, JobHandle, JobId, JobKind, JobPriority};
use crate::lease::{DequeueRequest, JobLease, LeaseId, LeaseRenewal};
use crate::queue::{LeaseExpiryScanner, QueueService};

/// PostgreSQL-backed implementation of the queue service.
///
/// Provides durable job storage with deduplication, leasing, and
/// lifecycle management using PostgreSQL as the backend.
#[derive(Debug)]
pub struct PostgresQueueService<J: Job> {
    pool: PgPool,
    entity_scope: String,
    _phantom: std::marker::PhantomData<J>,
}

impl<J: Job> PostgresQueueService<J> {
    /// Create a new PostgreSQL queue service.
    pub fn new(pool: PgPool, entity_scope: impl Into<String>) -> Self {
        Self {
            pool,
            entity_scope: entity_scope.into(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get a reference to the connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn kind_to_str(kind: &J::Kind) -> String {
        kind.as_str().to_string()
    }

    fn priority_to_i16(priority: JobPriority) -> i16 {
        match priority {
            JobPriority::P0 => 0,
            JobPriority::P1 => 1,
            JobPriority::P2 => 2,
            JobPriority::P3 => 3,
        }
    }

    fn i16_to_priority(val: i16) -> anyhow::Result<JobPriority> {
        match val {
            0 => Ok(JobPriority::P0),
            1 => Ok(JobPriority::P1),
            2 => Ok(JobPriority::P2),
            3 => Ok(JobPriority::P3),
            other => Err(anyhow::anyhow!("invalid priority value: {}", other)),
        }
    }
}

#[async_trait]
impl<J: Job + 'static> QueueService<J> for PostgresQueueService<J> {
    async fn enqueue(
        &self,
        entity_id: J::EntityId,
        job: J,
        priority: JobPriority,
    ) -> anyhow::Result<JobHandle> {
        let job_id = JobId::new();
        let payload_json = serde_json::to_value(&job)?;
        let entity_id_str = entity_id.to_string();
        let kind_str = Self::kind_to_str(&job.kind());
        let dedupe_key = job.dedupe_key();
        let priority_val = Self::priority_to_i16(priority);
        let dependency_key: Option<String> = None;
        let state = if dependency_key.is_some() {
            "deferred"
        } else {
            "ready"
        };

        let existing = sqlx::query(
            r#"
            SELECT id, priority
            FROM orca_jobs
            WHERE dedupe_key = $1
              AND entity_scope = $2
              AND state IN ('ready','deferred','leased')
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .bind(&dedupe_key)
        .bind(&self.entity_scope)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = existing {
            let existing_id = JobId(row.try_get("id")?);
            let existing_priority: i16 = row.try_get("priority")?;

            if priority_val < existing_priority {
                sqlx::query(
                    r#"
                    UPDATE orca_jobs
                    SET priority = $1,
                        available_at = LEAST(available_at, NOW()),
                        updated_at = NOW()
                    WHERE id = $2
                      AND entity_scope = $3
                      AND state IN ('ready','deferred')
                    "#,
                )
                .bind(priority_val)
                .bind(existing_id.0)
                .bind(&self.entity_scope)
                .execute(&self.pool)
                .await?;
            }

            return Ok(JobHandle {
                id: existing_id,
                priority,
                dedupe_key: dedupe_key.clone(),
                accepted: false,
            });
        }

        let insert_res = sqlx::query(
            r#"
            INSERT INTO orca_jobs (
                id, entity_id, entity_scope, job_kind, payload, priority, state,
                attempts, available_at, lease_owner, lease_id, lease_expires_at,
                dedupe_key, dependency_key, last_error, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, 0, NOW(), NULL, NULL, NULL, $8, $9, NULL, NOW(), NOW())
            "#,
        )
        .bind(job_id.0)
        .bind(&entity_id_str)
        .bind(&self.entity_scope)
        .bind(&kind_str)
        .bind(&payload_json)
        .bind(priority_val)
        .bind(state)
        .bind(&dedupe_key)
        .bind(&dependency_key)
        .execute(&self.pool)
        .await;

        match insert_res {
            Ok(_) => Ok(JobHandle {
                id: job_id,
                priority,
                dedupe_key: dedupe_key.clone(),
                accepted: true,
            }),
            Err(sqlx::Error::Database(db_err)) => {
                let code = db_err.code().map(|c| c.to_string());
                if code.as_deref() == Some("23505") {
                    let existing = sqlx::query(
                        r#"
                        SELECT id, priority, available_at, state
                        FROM orca_jobs
                        WHERE dedupe_key = $1
                          AND entity_scope = $2
                          AND state IN ('ready','deferred','leased')
                        ORDER BY created_at ASC
                        LIMIT 1
                        "#,
                    )
                    .bind(&dedupe_key)
                    .bind(&self.entity_scope)
                    .fetch_optional(&self.pool)
                    .await?;

                    if let Some(row) = existing {
                        let existing_pri: i16 = row.try_get("priority")?;
                        if priority_val < existing_pri {
                            let row_id: Uuid = row.try_get("id")?;
                            sqlx::query(
                                r#"
                                UPDATE orca_jobs
                                SET priority = $1,
                                    available_at = LEAST(available_at, NOW()),
                                    updated_at = NOW()
                                WHERE id = $2
                                  AND entity_scope = $3
                                  AND state IN ('ready','deferred')
                                "#,
                            )
                            .bind(priority_val)
                            .bind(row_id)
                            .bind(&self.entity_scope)
                            .execute(&self.pool)
                            .await?;
                        }

                        let row_id: Uuid = row.try_get("id")?;
                        return Ok(JobHandle {
                            id: JobId(row_id),
                            priority,
                            dedupe_key: dedupe_key.clone(),
                            accepted: false,
                        });
                    } else {
                        let job_id2 = JobId::new();
                        let retry = sqlx::query(
                            r#"
                            INSERT INTO orca_jobs (
                                id, entity_id, entity_scope, job_kind, payload, priority, state,
                                attempts, available_at, lease_owner, lease_id, lease_expires_at,
                                dedupe_key, dependency_key, last_error, created_at, updated_at
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, 'ready', 0, NOW(), NULL, NULL, NULL, $7, NULL, NULL, NOW(), NOW())
                            "#,
                        )
                        .bind(job_id2.0)
                        .bind(&entity_id_str)
                        .bind(&self.entity_scope)
                        .bind(&kind_str)
                        .bind(&payload_json)
                        .bind(priority_val)
                        .bind(&dedupe_key)
                        .execute(&self.pool)
                        .await;

                        match retry {
                            Ok(_) => Ok(JobHandle {
                                id: job_id2,
                                priority,
                                dedupe_key: dedupe_key.clone(),
                                accepted: true,
                            }),
                            Err(sqlx::Error::Database(db_err2))
                                if db_err2
                                    .code()
                                    .map(|c| c.to_string())
                                    .as_deref()
                                    == Some("23505") =>
                            {
                                let winner = sqlx::query(
                                    r#"
                                    SELECT id
                                    FROM orca_jobs
                                    WHERE dedupe_key = $1
                                      AND entity_scope = $2
                                      AND state IN ('ready','deferred','leased')
                                    ORDER BY created_at ASC
                                    LIMIT 1
                                    "#,
                                )
                                .bind(&dedupe_key)
                                .bind(&self.entity_scope)
                                .fetch_optional(&self.pool)
                                .await?;

                                if let Some(w) = winner {
                                    let w_id: Uuid = w.try_get("id")?;
                                    Ok(JobHandle {
                                        id: JobId(w_id),
                                        priority,
                                        dedupe_key: dedupe_key.clone(),
                                        accepted: false,
                                    })
                                } else {
                                    Err(anyhow::anyhow!(
                                        "enqueue conflict retry: could not resolve existing row"
                                    ))
                                }
                            }
                            Err(e) => Err(e.into()),
                        }
                    }
                } else {
                    Err(anyhow::anyhow!("enqueue insert failed: {}", db_err))
                }
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn enqueue_many(
        &self,
        requests: Vec<(J::EntityId, J, JobPriority)>,
    ) -> anyhow::Result<Vec<JobHandle>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self.pool.begin().await?;
        let mut out: Vec<JobHandle> = Vec::with_capacity(requests.len());

        for (entity_id, job, priority) in requests {
            let job_id = JobId::new();
            let payload_json = serde_json::to_value(&job)?;
            let entity_id_str = entity_id.to_string();
            let kind_str = Self::kind_to_str(&job.kind());
            let dedupe_key = job.dedupe_key();
            let priority_val = Self::priority_to_i16(priority);
            let dependency_key: Option<String> = None;
            let state = if dependency_key.is_some() {
                "deferred"
            } else {
                "ready"
            };

            let existing = sqlx::query(
                r#"
                SELECT id, priority
                FROM orca_jobs
                WHERE dedupe_key = $1
                  AND entity_scope = $2
                  AND state IN ('ready','deferred','leased')
                ORDER BY created_at ASC
                LIMIT 1
                "#,
            )
            .bind(&dedupe_key)
            .bind(&self.entity_scope)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some(row) = existing {
                let existing_priority: i16 = row.try_get("priority")?;
                if priority_val < existing_priority {
                    let existing_id: Uuid = row.try_get("id")?;
                    let _ = sqlx::query(
                        r#"
                        UPDATE orca_jobs
                        SET priority = $1,
                            available_at = LEAST(available_at, NOW()),
                            updated_at = NOW()
                        WHERE id = $2
                          AND entity_scope = $3
                          AND state IN ('ready','deferred')
                        "#,
                    )
                    .bind(priority_val)
                    .bind(existing_id)
                    .bind(&self.entity_scope)
                    .execute(&mut *tx)
                    .await;
                }
                let existing_id: Uuid = row.try_get("id")?;
                out.push(JobHandle {
                    id: JobId(existing_id),
                    priority,
                    dedupe_key: dedupe_key.clone(),
                    accepted: false,
                });
                continue;
            }

            let insert_res = sqlx::query(
                r#"
                INSERT INTO orca_jobs (
                    id, entity_id, entity_scope, job_kind, payload, priority, state,
                    attempts, available_at, lease_owner, lease_id, lease_expires_at,
                    dedupe_key, dependency_key, last_error, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, 0, NOW(), NULL, NULL, NULL, $8, $9, NULL, NOW(), NOW())
                "#,
            )
            .bind(job_id.0)
            .bind(&entity_id_str)
            .bind(&self.entity_scope)
            .bind(&kind_str)
            .bind(&payload_json)
            .bind(priority_val)
            .bind(state)
            .bind(&dedupe_key)
            .bind(&dependency_key)
            .execute(&mut *tx)
            .await;

            match insert_res {
                Ok(_) => {
                    out.push(JobHandle {
                        id: job_id,
                        priority,
                        dedupe_key: dedupe_key.clone(),
                        accepted: true,
                    });
                }
                Err(sqlx::Error::Database(db_err)) => {
                    let code = db_err.code().map(|c| c.to_string());
                    if code.as_deref() == Some("23505") {
                        let existing = sqlx::query(
                            r#"
                            SELECT id, priority, available_at, state
                            FROM orca_jobs
                            WHERE dedupe_key = $1
                              AND entity_scope = $2
                              AND state IN ('ready','deferred','leased')
                            ORDER BY created_at ASC
                            LIMIT 1
                            "#,
                        )
                        .bind(&dedupe_key)
                        .bind(&self.entity_scope)
                        .fetch_optional(&mut *tx)
                        .await?;

                        if let Some(row) = existing {
                            let existing_pri: i16 = row.try_get("priority")?;
                            if priority_val < existing_pri {
                                let row_id: Uuid = row.try_get("id")?;
                                sqlx::query(
                                    r#"
                                    UPDATE orca_jobs
                                    SET priority = $1,
                                        available_at = LEAST(available_at, NOW()),
                                        updated_at = NOW()
                                    WHERE id = $2
                                      AND entity_scope = $3
                                      AND state IN ('ready','deferred')
                                    "#,
                                )
                                .bind(priority_val)
                                .bind(row_id)
                                .bind(&self.entity_scope)
                                .execute(&mut *tx)
                                .await?;
                            }
                            let row_id: Uuid = row.try_get("id")?;
                            out.push(JobHandle {
                                id: JobId(row_id),
                                priority,
                                dedupe_key: dedupe_key.clone(),
                                accepted: false,
                            });
                        } else {
                            drop(tx.rollback().await);
                            return Err(anyhow::anyhow!(
                                "enqueue_many conflict but no existing job found"
                            ));
                        }
                    } else {
                        drop(tx.rollback().await);
                        return Err(anyhow::anyhow!(
                            "enqueue_many insert failed: {}",
                            db_err
                        ));
                    }
                }
                Err(e) => {
                    drop(tx.rollback().await);
                    return Err(e.into());
                }
            }
        }

        tx.commit().await?;
        Ok(out)
    }

    async fn dequeue(
        &self,
        request: DequeueRequest<J::Kind, J::EntityId>,
    ) -> anyhow::Result<Option<JobLease<J>>> {
        let mut tx = self.pool.begin().await?;

        let kind_str = Self::kind_to_str(&request.kind);

        struct SelectedRow {
            id: Uuid,
            payload: serde_json::Value,
            priority: i16,
            attempts: i32,
            available_at: DateTime<Utc>,
            dedupe_key: String,
            dependency_key: Option<String>,
            created_at: DateTime<Utc>,
        }

        let row: Option<SelectedRow> = if let Some(selector) = request.selector
        {
            let entity_id_str = selector.entity_id.to_string();
            let priority_val = Self::priority_to_i16(selector.priority);

            sqlx::query(
                r#"
                WITH next AS (
                    SELECT id
                    FROM orca_jobs
                    WHERE state = 'ready'
                      AND entity_scope = $1
                      AND job_kind = $2
                      AND entity_id = $3
                      AND priority = $4
                      AND available_at <= NOW()
                    ORDER BY available_at, attempts, created_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                SELECT j.id, j.payload, j.priority, j.attempts,
                       j.available_at, j.dedupe_key, j.dependency_key, j.created_at
                FROM orca_jobs j
                JOIN next ON next.id = j.id
                "#,
            )
            .bind(&self.entity_scope)
            .bind(&kind_str)
            .bind(&entity_id_str)
            .bind(priority_val)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| SelectedRow {
                id: row.try_get("id").unwrap_or_default(),
                payload: row.try_get("payload").unwrap_or(serde_json::Value::Null),
                priority: row.try_get("priority").unwrap_or(2),
                attempts: row.try_get("attempts").unwrap_or(0),
                available_at: row.try_get("available_at").unwrap_or(Utc::now()),
                dedupe_key: row.try_get("dedupe_key").unwrap_or_default(),
                dependency_key: row.try_get("dependency_key").ok(),
                created_at: row.try_get("created_at").unwrap_or(Utc::now()),
            })
        } else {
            sqlx::query(
                r#"
                SELECT id, payload, priority, attempts, available_at,
                       dedupe_key, dependency_key, created_at
                FROM orca_jobs
                WHERE entity_scope = $1
                  AND job_kind = $2
                  AND state = 'ready'
                  AND available_at <= NOW()
                ORDER BY priority ASC, available_at ASC, attempts ASC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                "#,
            )
            .bind(&self.entity_scope)
            .bind(&kind_str)
            .fetch_optional(&mut *tx)
            .await?
            .map(|row| SelectedRow {
                id: row.try_get("id").unwrap_or_default(),
                payload: row.try_get("payload").unwrap_or(serde_json::Value::Null),
                priority: row.try_get("priority").unwrap_or(2),
                attempts: row.try_get("attempts").unwrap_or(0),
                available_at: row.try_get("available_at").unwrap_or(Utc::now()),
                dedupe_key: row.try_get("dedupe_key").unwrap_or_default(),
                dependency_key: row.try_get("dependency_key").ok(),
                created_at: row.try_get("created_at").unwrap_or(Utc::now()),
            })
        };

        let Some(row) = row else {
            drop(tx);
            return Ok(None);
        };

        let lease_id = LeaseId::new();
        let expires_at = Utc::now() + request.lease_ttl;

        let updated = sqlx::query(
            r#"
            UPDATE orca_jobs
            SET state='leased',
                lease_owner=$1,
                lease_id=$2,
                lease_expires_at=$3,
                attempts = COALESCE(attempts, 0),
                renewals = 0,
                updated_at=NOW()
            WHERE id = $4
              AND entity_scope = $5
              AND state = 'ready'
            RETURNING lease_id
            "#,
        )
        .bind(&request.worker_id)
        .bind(lease_id.0)
        .bind(expires_at)
        .bind(row.id)
        .bind(&self.entity_scope)
        .fetch_optional(&mut *tx)
        .await?;

        if updated.is_none() {
            drop(tx);
            return Ok(None);
        }

        let job: J = serde_json::from_value(row.payload)?;

        let lease = JobLease {
            job_id: JobId(row.id),
            lease_id,
            job,
            worker_id: request.worker_id,
            expires_at,
            renewals: 0,
        };

        tx.commit().await?;
        Ok(Some(lease))
    }

    async fn complete(&self, lease_id: LeaseId) -> anyhow::Result<()> {
        let res = sqlx::query(
            r#"
            UPDATE orca_jobs
            SET state='completed',
                lease_owner=NULL,
                lease_id=NULL,
                lease_expires_at=NULL,
                completed_at=NOW(),
                duration_ms=EXTRACT(EPOCH FROM (NOW() - created_at))::integer * 1000
            WHERE lease_id = $1
              AND entity_scope = $2
              AND state='leased'
            "#,
        )
        .bind(lease_id.0)
        .bind(&self.entity_scope)
        .execute(&self.pool)
        .await?;

        if res.rows_affected() > 0 {
            debug!("completed job with lease {:?}", lease_id.0);
        }
        Ok(())
    }

    async fn fail(
        &self,
        lease_id: LeaseId,
        retryable: bool,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let mut tx = self.pool.begin().await?;

        let row = sqlx::query(
            r#"
            SELECT id, attempts, entity_id, payload, max_attempts
            FROM orca_jobs
            WHERE lease_id = $1
              AND entity_scope = $2
              AND state = 'leased'
            FOR UPDATE
            "#,
        )
        .bind(lease_id.0)
        .bind(&self.entity_scope)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(row) = row else {
            drop(tx);
            return Ok(());
        };

        let attempts_before: i32 = row.try_get("attempts")?;
        let max_attempts: i32 = row.try_get("max_attempts")?;
        let attempt_next = attempts_before.saturating_add(1) as u32;

        if retryable && attempts_before < max_attempts {
            let base_delay_ms = 1000u64;
            let delay_ms = base_delay_ms
                * 2u64.pow((attempt_next as u32).saturating_sub(1));
            let capped_delay_ms = delay_ms.min(300000u64);

            sqlx::query(
                r#"
                UPDATE orca_jobs
                SET attempts = attempts + 1,
                    state = 'ready',
                    lease_owner = NULL,
                    lease_id = NULL,
                    lease_expires_at = NULL,
                    last_error = $2,
                    backoff_until = NOW() + ($3::bigint) * INTERVAL '1 millisecond',
                    updated_at = NOW()
                WHERE id = $1
                  AND entity_scope = $4
                "#,
            )
            .bind(row.try_get::<Uuid, _>("id")?)
            .bind(&error)
            .bind(capped_delay_ms as i64)
            .bind(&self.entity_scope)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;

            warn!(
                "job {} failed retryable; attempts now {}; scheduled retry in {}ms",
                row.try_get::<Uuid, _>("id")?,
                attempts_before + 1,
                capped_delay_ms
            );
            Ok(())
        } else {
            let new_state = if retryable { "dead_letter" } else { "failed" };

            sqlx::query(
                r#"
                UPDATE orca_jobs
                SET state = $2,
                    lease_owner = NULL,
                    lease_id = NULL,
                    lease_expires_at = NULL,
                    last_error = $3,
                    updated_at = NOW()
                WHERE id = $1
                  AND entity_scope = $4
                "#,
            )
            .bind(row.try_get::<Uuid, _>("id")?)
            .bind(new_state)
            .bind(&error)
            .bind(&self.entity_scope)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;

            warn!(
                "job {} moved to {} after attempts {}",
                row.try_get::<Uuid, _>("id")?,
                new_state,
                attempts_before
            );
            Ok(())
        }
    }

    async fn dead_letter(
        &self,
        lease_id: LeaseId,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let res = sqlx::query(
            r#"
            UPDATE orca_jobs
            SET state='dead_letter',
                lease_owner=NULL,
                lease_id=NULL,
                lease_expires_at=NULL,
                last_error=$3,
                updated_at=NOW()
            WHERE lease_id = $1
              AND entity_scope = $2
              AND state = 'leased'
            "#,
        )
        .bind(lease_id.0)
        .bind(&self.entity_scope)
        .bind(&error)
        .execute(&self.pool)
        .await?;

        if res.rows_affected() > 0 {
            warn!("job with lease {:?} moved to dead_letter", lease_id.0);
        }
        Ok(())
    }

    async fn renew(
        &self,
        renewal: LeaseRenewal,
    ) -> anyhow::Result<JobLease<J>> {
        let extend_ms: i64 = renewal.extend_by.num_milliseconds();

        let row = sqlx::query(
            r#"
            UPDATE orca_jobs
            SET lease_expires_at = lease_expires_at + ($1::bigint) * INTERVAL '1 millisecond',
                renewals = COALESCE(renewals, 0) + 1
            WHERE lease_id = $2
              AND entity_scope = $3
              AND state = 'leased'
              AND lease_expires_at > NOW()
            RETURNING
                id, entity_id, job_kind, payload, priority, attempts, available_at,
                dedupe_key, dependency_key, created_at, updated_at,
                lease_owner, lease_expires_at,
                renewals
            "#,
        )
        .bind(extend_ms)
        .bind(renewal.lease_id.0)
        .bind(&self.entity_scope)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            warn!(
                "renewal failed: lease {:?} not found or expired",
                renewal.lease_id.0
            );
            return Err(anyhow::anyhow!("lease not found or expired"));
        };

        let job: J = serde_json::from_value(row.try_get("payload")?)?;

        let expires_at: DateTime<Utc> = row.try_get("lease_expires_at")?;
        let renewals: i32 = row.try_get("renewals")?;

        let job_id_val: Uuid = row.try_get("id")?;
        let lease = JobLease {
            job_id: JobId(job_id_val),
            lease_id: renewal.lease_id,
            job,
            worker_id: row.try_get("lease_owner").unwrap_or_default(),
            expires_at,
            renewals: renewals as u32,
        };

        debug!(
            "renewed lease {:?} until {}",
            lease.lease_id.0, lease.expires_at
        );
        Ok(lease)
    }

    async fn queue_depth(&self, kind: J::Kind) -> anyhow::Result<usize> {
        let kind_str = Self::kind_to_str(&kind);

        let row = sqlx::query(
            r#"
            SELECT COUNT(*)::bigint AS count
            FROM orca_jobs
            WHERE entity_scope = $1
              AND job_kind = $2
              AND state = 'ready'
            "#,
        )
        .bind(&self.entity_scope)
        .bind(&kind_str)
        .fetch_one(&self.pool)
        .await?;

        let count: i64 = row.try_get("count")?;
        Ok(count as usize)
    }

    async fn cancel_job(&self, job_id: JobId) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE orca_jobs
            SET state = 'cancelled',
                updated_at = NOW()
            WHERE id = $1
              AND entity_scope = $2
              AND state IN ('ready','deferred')
            "#,
        )
        .bind(job_id.0)
        .bind(&self.entity_scope)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn release_dependency(
        &self,
        entity_id: J::EntityId,
        dependency_key: &DependencyKey,
    ) -> anyhow::Result<u64> {
        let entity_id_str = entity_id.to_string();

        let updated = sqlx::query(
            r#"
            UPDATE orca_jobs
            SET state = 'ready',
                dependency_key = NULL,
                available_at = NOW(),
                updated_at = NOW()
            WHERE entity_id = $1
              AND entity_scope = $2
              AND state = 'deferred'
              AND dependency_key = $3
            "#,
        )
        .bind(&entity_id_str)
        .bind(&self.entity_scope)
        .bind(dependency_key.as_str())
        .execute(&self.pool)
        .await?;

        Ok(updated.rows_affected())
    }
}

#[async_trait]
impl<J: Job + 'static> LeaseExpiryScanner for PostgresQueueService<J> {
    async fn scan_expired_leases(&self) -> anyhow::Result<u64> {
        let expired = sqlx::query(
            r#"
            SELECT id, attempts, entity_id, payload, max_attempts
            FROM orca_jobs
            WHERE entity_scope = $1
              AND state = 'leased'
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at < NOW()
            "#,
        )
        .bind(&self.entity_scope)
        .fetch_all(&self.pool)
        .await?;

        let mut resurrected = 0u64;

        for row in expired {
            let attempts_before: i32 = row.try_get("attempts")?;
            let max_attempts: i32 = row.try_get("max_attempts")?;

            if attempts_before < max_attempts {
                let attempt_next = attempts_before.saturating_add(1) as u32;

                let base_delay_ms = 1000u64;
                let delay_ms = base_delay_ms
                    * 2u64.pow((attempt_next as u32).saturating_sub(1));
                let capped_delay_ms = delay_ms.min(300000u64);

                sqlx::query(
                    r#"
                    UPDATE orca_jobs
                    SET attempts = attempts + 1,
                        state = 'ready',
                        lease_owner = NULL,
                        lease_id = NULL,
                        lease_expires_at = NULL,
                        backoff_until = NOW() + ($2::bigint) * INTERVAL '1 millisecond',
                        last_error = COALESCE(last_error, 'lease expired'),
                        updated_at = NOW()
                    WHERE id = $1
                      AND entity_scope = $3
                      AND state = 'leased'
                    "#,
                )
                .bind(row.try_get::<Uuid, _>("id")?)
                .bind(capped_delay_ms as i64)
                .bind(&self.entity_scope)
                .execute(&self.pool)
                .await?;

                resurrected += 1;
            } else {
                sqlx::query(
                    r#"
                    UPDATE orca_jobs
                    SET
                        state = 'dead_letter',
                        lease_owner = NULL,
                        lease_id = NULL,
                        lease_expires_at = NULL,
                        updated_at = NOW(),
                        last_error = COALESCE(last_error, 'lease expired (max attempts)')
                    WHERE id = $1
                      AND entity_scope = $2
                      AND state = 'leased'
                    "#,
                )
                .bind(row.try_get::<Uuid, _>("id")?)
                .bind(&self.entity_scope)
                .execute(&self.pool)
                .await?;
            }
        }

        Ok(resurrected)
    }
}
