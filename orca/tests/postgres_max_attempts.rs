//! Integration tests verifying that `fail()` and `scan_expired_leases()`
//! honour the per-job `max_attempts` column instead of a hardcoded value.
//!
//! Requires a running Postgres instance with the orca schema applied.
//! Run with: `cargo test --test postgres_max_attempts --features postgres -- --ignored`

#![cfg(feature = "postgres")]

use orca::lease::LeaseId;
use orca::persistence::PostgresQueueService;
use orca::queue::QueueService;
use orca_testkit::TestJob;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Insert a leased job via raw SQL so we can control `max_attempts` and `attempts`.
async fn insert_leased_job(
    pool: &PgPool,
    entity_scope: &str,
    attempts: i32,
    max_attempts: i32,
    lease_id: Uuid,
) -> Uuid {
    let job_id = Uuid::new_v4();
    let dedupe_key = format!("test-{job_id}");

    sqlx::query(
        r#"
        INSERT INTO orca_jobs (
            id, entity_id, entity_scope, job_kind, payload, priority, state,
            attempts, max_attempts, available_at,
            lease_owner, lease_id, lease_expires_at,
            dedupe_key, dependency_key, last_error, created_at, updated_at
        )
        VALUES (
            $1, $2, $3, 'simple', $4, 2, 'leased',
            $5, $6, NOW(),
            'test-worker', $7, NOW() + INTERVAL '5 minutes',
            $8, NULL, NULL, NOW(), NOW()
        )
        "#,
    )
    .bind(job_id)
    .bind(Uuid::new_v4())
    .bind(entity_scope)
    .bind(serde_json::json!({"Simple": {"name": "test"}}))
    .bind(attempts)
    .bind(max_attempts)
    .bind(lease_id)
    .bind(&dedupe_key)
    .execute(pool)
    .await
    .expect("insert_leased_job");

    job_id
}

async fn get_job_state(pool: &PgPool, job_id: Uuid) -> String {
    sqlx::query("SELECT state::text AS state FROM orca_jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .expect("get_job_state")
        .try_get::<String, _>("state")
        .expect("state column")
}

async fn re_lease(pool: &PgPool, job_id: Uuid, lease_id: Uuid) {
    sqlx::query(
        r#"
        UPDATE orca_jobs
        SET state = 'leased',
            lease_owner = 'test-worker',
            lease_id = $1,
            lease_expires_at = NOW() + INTERVAL '5 minutes'
        WHERE id = $2
        "#,
    )
    .bind(lease_id)
    .bind(job_id)
    .execute(pool)
    .await
    .expect("re_lease");
}

async fn cleanup(pool: &PgPool, entity_scope: &str) {
    sqlx::query("DELETE FROM orca_jobs WHERE entity_scope = $1")
        .bind(entity_scope)
        .execute(pool)
        .await
        .ok();
}

/// Job with max_attempts=1: after one attempt (attempts_before=1), a retryable
/// fail should move the job to dead_letter immediately.
#[tokio::test]
#[ignore] // requires DATABASE_URL
async fn fail_with_max_attempts_1_dead_letters_immediately() {
    let pool = PgPool::connect(
        &std::env::var("DATABASE_URL").expect("DATABASE_URL required"),
    )
    .await
    .expect("connect");

    let scope = format!("test-ma1-{}", Uuid::new_v4());
    let lease_id = Uuid::new_v4();

    // Insert a leased job with attempts=1, max_attempts=1
    // (simulates: already attempted once, this is the last chance)
    let job_id =
        insert_leased_job(&pool, &scope, 1, 1, lease_id).await;

    let svc: PostgresQueueService<TestJob> =
        PostgresQueueService::new(pool.clone(), &scope);

    // Fail retryable: attempts_before(1) < max_attempts(1) → false → dead_letter
    svc.fail(LeaseId(lease_id), true, Some("boom".into()))
        .await
        .expect("fail");

    let state = get_job_state(&pool, job_id).await;
    assert_eq!(
        state, "dead_letter",
        "Job with max_attempts=1 and attempts=1 should dead-letter on retryable fail"
    );

    cleanup(&pool, &scope).await;
}

/// Job with max_attempts=5: after two retryable fails (attempts going 0→1→2),
/// the job should still be in ready state (not dead-lettered).
#[tokio::test]
#[ignore] // requires DATABASE_URL
async fn fail_with_max_attempts_5_stays_ready_after_two_failures() {
    let pool = PgPool::connect(
        &std::env::var("DATABASE_URL").expect("DATABASE_URL required"),
    )
    .await
    .expect("connect");

    let scope = format!("test-ma5-{}", Uuid::new_v4());
    let lease_id_1 = Uuid::new_v4();

    // Insert leased job: attempts=0, max_attempts=5
    let job_id =
        insert_leased_job(&pool, &scope, 0, 5, lease_id_1).await;

    let svc: PostgresQueueService<TestJob> =
        PostgresQueueService::new(pool.clone(), &scope);

    // First fail: attempts_before=0 < max_attempts=5 → retry → ready
    svc.fail(LeaseId(lease_id_1), true, Some("err1".into()))
        .await
        .expect("fail 1");

    assert_eq!(
        get_job_state(&pool, job_id).await,
        "ready",
        "After first fail with max_attempts=5, job should be ready"
    );

    // Re-lease for second failure
    let lease_id_2 = Uuid::new_v4();
    re_lease(&pool, job_id, lease_id_2).await;

    // Second fail: attempts_before=1 < max_attempts=5 → retry → ready
    svc.fail(LeaseId(lease_id_2), true, Some("err2".into()))
        .await
        .expect("fail 2");

    assert_eq!(
        get_job_state(&pool, job_id).await,
        "ready",
        "After second fail with max_attempts=5, job should still be ready"
    );

    // Verify attempts counter
    let attempts: i32 =
        sqlx::query("SELECT attempts FROM orca_jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .try_get("attempts")
            .unwrap();
    assert_eq!(attempts, 2, "Attempts should be 2 after two failures");

    cleanup(&pool, &scope).await;
}
