//! Runtime integration tests for orca orchestration system.
//!
//! Tests multi-entity fairness, priority ordering, lease renewal,
//! dead-letter handling, and concurrent worker behavior.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use orca::budget::{BudgetConfig, InMemoryBudget};
use orca::correlation::CorrelationCache;
use orca::events::{EventMeta, InProcEventBus, JobEvent, JobEventPayload};
use orca::job::{Job, JobHandle, JobId, JobPriority};
use orca::lease::{JobLease, LeaseId};
use orca::queue::{LeaseExpiryScanner, QueueService};
use orca::runtime::{
    DispatchStatus, JobDispatcher, JobEventPublisher, OrchestratorRuntime,
    OrchestratorRuntimeConfig, StandardOrchestratorRuntimeBuilder,
    WorkloadToKindMapper,
};
use orca_testkit::{
    InMemoryQueueService, TestEntityId, TestJob, TestJobKind, TestWorkloadKind,
};
use tokio::sync::broadcast;
use tokio::time::timeout;

#[derive(Clone)]
struct TestEventBus {
    bus: Arc<InProcEventBus<TestJob>>,
}

impl TestEventBus {
    fn new(capacity: usize) -> Self {
        Self {
            bus: Arc::new(InProcEventBus::new(capacity)),
        }
    }
}

#[async_trait]
impl JobEventPublisher<TestJob> for TestEventBus {
    async fn publish(&self, event: JobEvent<TestJob>) -> anyhow::Result<()> {
        self.bus.publish_job(event)
    }

    fn subscribe_jobs(&self) -> broadcast::Receiver<JobEvent<TestJob>> {
        self.bus.subscribe_job_events()
    }
}

#[derive(Clone, Copy)]
struct TestMapper;

impl WorkloadToKindMapper<TestJob> for TestMapper {
    fn map(&self, _kind: TestJobKind) -> TestWorkloadKind {
        TestWorkloadKind::General
    }
}

#[derive(Clone, Copy)]
enum FailureMode {
    None,
    Retryable,
    Permanent,
}

#[derive(Clone)]
struct TestDispatcher {
    failure_mode: FailureMode,
}

impl TestDispatcher {
    fn new(failure_mode: FailureMode) -> Self {
        Self { failure_mode }
    }
}

#[async_trait]
impl JobDispatcher<TestJob> for TestDispatcher {
    async fn dispatch(&self, lease: &JobLease<TestJob>) -> DispatchStatus {
        match &lease.job {
            TestJob::Slow { duration_ms, .. } => {
                tokio::time::sleep(Duration::from_millis(*duration_ms)).await;
                DispatchStatus::Success
            }
            TestJob::Failing { error, .. } => match self.failure_mode {
                FailureMode::Retryable => DispatchStatus::RetryableFailure {
                    error: Some(error.clone()),
                },
                FailureMode::Permanent => DispatchStatus::PermanentFailure {
                    error: Some(error.clone()),
                },
                FailureMode::None => DispatchStatus::Success,
            },
            TestJob::Simple { .. } => DispatchStatus::Success,
        }
    }
}

#[derive(Clone)]
struct TestInMemoryQueue {
    inner: InMemoryQueueService,
    leases: Arc<Mutex<HashMap<LeaseId, LeaseRecord>>>,
}

impl TestInMemoryQueue {
    fn new() -> Self {
        Self {
            inner: InMemoryQueueService::new(),
            leases: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Clone)]
struct LeaseRecord {
    job_id: JobId,
    job: TestJob,
    worker_id: String,
    expires_at: chrono::DateTime<Utc>,
    renewals: u32,
}

#[async_trait]
impl QueueService<TestJob> for TestInMemoryQueue {
    async fn enqueue(
        &self,
        entity_id: TestEntityId,
        job: TestJob,
        priority: JobPriority,
    ) -> anyhow::Result<JobHandle> {
        self.inner.enqueue(entity_id, job, priority).await
    }

    async fn enqueue_many(
        &self,
        requests: Vec<(TestEntityId, TestJob, JobPriority)>,
    ) -> anyhow::Result<Vec<JobHandle>> {
        self.inner.enqueue_many(requests).await
    }

    async fn dequeue(
        &self,
        request: orca::lease::DequeueRequest<TestJobKind, TestEntityId>,
    ) -> anyhow::Result<Option<JobLease<TestJob>>> {
        let lease = self.inner.dequeue(request).await?;
        if let Some(lease) = lease.as_ref() {
            let mut leases = self.leases.lock().expect("lease lock");
            leases.insert(
                lease.lease_id,
                LeaseRecord {
                    job_id: lease.job_id,
                    job: lease.job.clone(),
                    worker_id: lease.worker_id.clone(),
                    expires_at: lease.expires_at,
                    renewals: lease.renewals,
                },
            );
        }
        Ok(lease)
    }

    async fn complete(&self, lease_id: LeaseId) -> anyhow::Result<()> {
        let result = self.inner.complete(lease_id).await;
        self.leases.lock().expect("lease lock").remove(&lease_id);
        result
    }

    async fn fail(
        &self,
        lease_id: LeaseId,
        retryable: bool,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let result = self.inner.fail(lease_id, retryable, error).await;
        self.leases.lock().expect("lease lock").remove(&lease_id);
        result
    }

    async fn dead_letter(
        &self,
        lease_id: LeaseId,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let result = self.inner.dead_letter(lease_id, error).await;
        self.leases.lock().expect("lease lock").remove(&lease_id);
        result
    }

    async fn renew(
        &self,
        renewal: orca::lease::LeaseRenewal,
    ) -> anyhow::Result<JobLease<TestJob>> {
        let mut leases = self.leases.lock().expect("lease lock");
        let record = leases
            .get_mut(&renewal.lease_id)
            .ok_or_else(|| anyhow::anyhow!("Lease not found"))?;
        if record.worker_id != renewal.worker_id {
            anyhow::bail!("Lease owner mismatch");
        }

        record.renewals += 1;
        record.expires_at = Utc::now() + renewal.extend_by;

        Ok(JobLease {
            job_id: record.job_id,
            lease_id: renewal.lease_id,
            job: record.job.clone(),
            worker_id: record.worker_id.clone(),
            expires_at: record.expires_at,
            renewals: record.renewals,
        })
    }

    async fn queue_depth(&self, kind: TestJobKind) -> anyhow::Result<usize> {
        self.inner.queue_depth(kind).await
    }

    async fn cancel_job(&self, job_id: JobId) -> anyhow::Result<()> {
        self.inner.cancel_job(job_id).await
    }

    async fn release_dependency(
        &self,
        entity_id: TestEntityId,
        dependency_key: &orca::job::DependencyKey,
    ) -> anyhow::Result<u64> {
        self.inner.release_dependency(entity_id, dependency_key).await
    }
}

#[async_trait]
impl LeaseExpiryScanner for TestInMemoryQueue {
    async fn scan_expired_leases(&self) -> anyhow::Result<u64> {
        Ok(0)
    }
}

fn default_config() -> OrchestratorRuntimeConfig {
    OrchestratorRuntimeConfig::default()
}

fn renewal_config() -> OrchestratorRuntimeConfig {
    let mut config = OrchestratorRuntimeConfig::default();
    config.default_lease_ttl_secs = 1;
    config.renew_min_margin_secs = 0;
    config.renew_at_fraction = 0.5;
    config.max_lease_renewals = 5;
    config
}

fn build_runtime<Q, D>(
    config: OrchestratorRuntimeConfig,
    queue: Arc<Q>,
    dispatcher: Arc<D>,
    events: Arc<TestEventBus>,
) -> OrchestratorRuntime<
    TestJob,
    Q,
    InMemoryBudget<TestWorkloadKind, TestEntityId>,
    orca::scheduler::WeightedFairScheduler<TestEntityId>,
    D,
    TestMapper,
>
where
    Q: QueueService<TestJob> + LeaseExpiryScanner + 'static,
    D: JobDispatcher<TestJob> + 'static,
{
    let budget = Arc::new(InMemoryBudget::new(BudgetConfig::with_default(10)));
    let events: Arc<dyn JobEventPublisher<TestJob>> = events;
    let mapper = Arc::new(TestMapper);

    StandardOrchestratorRuntimeBuilder::new(config)
        .with_queue(queue)
        .with_budget(budget)
        .with_dispatcher(dispatcher)
        .with_events(events)
        .with_correlations(CorrelationCache::new())
        .with_workload_mapper(mapper)
        .build()
        .expect("build runtime")
}

async fn publish_enqueued(
    events: &TestEventBus,
    entity_id: TestEntityId,
    handle: &JobHandle,
    job: &TestJob,
    priority: JobPriority,
) -> anyhow::Result<()> {
    let event = JobEvent {
        meta: EventMeta::new(
            entity_id,
            None,
            format!("enqueue-{}", handle.id),
        ),
        payload: JobEventPayload::Enqueued {
            job_id: handle.id,
            kind: job.kind(),
            priority,
        },
    };
    events.publish(event).await
}

async fn next_event(
    rx: &mut broadcast::Receiver<JobEvent<TestJob>>,
) -> Option<JobEvent<TestJob>> {
    loop {
        match rx.recv().await {
            Ok(event) => return Some(event),
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => return None,
        }
    }
}

#[tokio::test]
async fn inmemory_lease_renewal_emits_events() {
    let queue = Arc::new(TestInMemoryQueue::new());
    let events = Arc::new(TestEventBus::new(256));
    let dispatcher = Arc::new(TestDispatcher::new(FailureMode::None));
    let runtime =
        build_runtime(renewal_config(), queue.clone(), dispatcher, events.clone());

    runtime.start().await.expect("start runtime");
    runtime.spawn_worker_pool(TestJobKind::Slow, 2).await;

    let entity_id = TestEntityId::new("renewal-entity");
    let job = TestJob::Slow {
        duration_ms: 1500,
        name: "slow-job".into(),
    };
    let handle =
        queue.enqueue(entity_id, job.clone(), JobPriority::P2).await.unwrap();
    publish_enqueued(&events, entity_id, &handle, &job, JobPriority::P2)
        .await
        .unwrap();

    let mut rx = events.subscribe_jobs();
    let mut renewals = 0;
    let wait = timeout(Duration::from_secs(5), async {
        while renewals < 1 {
            if let Some(event) = next_event(&mut rx).await {
                if let JobEventPayload::LeaseRenewed { job_id, .. } =
                    event.payload
                {
                    if job_id == handle.id {
                        renewals += 1;
                    }
                }
            }
        }
    })
    .await;

    assert!(wait.is_ok(), "timed out waiting for lease renewal");
    runtime.shutdown().await.expect("shutdown runtime");
}

#[tokio::test]
async fn inmemory_dead_letter_emits_event() {
    let queue = Arc::new(TestInMemoryQueue::new());
    let events = Arc::new(TestEventBus::new(128));
    let dispatcher = Arc::new(TestDispatcher::new(FailureMode::Permanent));
    let runtime =
        build_runtime(default_config(), queue.clone(), dispatcher, events.clone());

    runtime.start().await.expect("start runtime");
    runtime.spawn_worker_pool(TestJobKind::Failing, 1).await;

    let entity_id = TestEntityId::new("dead-letter-entity");
    let job = TestJob::Failing {
        error: "boom".into(),
        name: "dead-letter".into(),
    };
    let handle =
        queue.enqueue(entity_id, job.clone(), JobPriority::P1).await.unwrap();
    publish_enqueued(&events, entity_id, &handle, &job, JobPriority::P1)
        .await
        .unwrap();

    let mut rx = events.subscribe_jobs();
    let wait = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = next_event(&mut rx).await {
                if let JobEventPayload::DeadLettered { job_id, .. } =
                    event.payload
                {
                    if job_id == handle.id {
                        break;
                    }
                }
            }
        }
    })
    .await;

    assert!(wait.is_ok(), "timed out waiting for dead-letter event");
    runtime.shutdown().await.expect("shutdown runtime");
}

#[tokio::test]
async fn inmemory_concurrent_workers_process_jobs() {
    let queue = Arc::new(TestInMemoryQueue::new());
    let events = Arc::new(TestEventBus::new(2048));
    let dispatcher = Arc::new(TestDispatcher::new(FailureMode::None));
    let runtime =
        build_runtime(default_config(), queue.clone(), dispatcher, events.clone());

    runtime.start().await.expect("start runtime");
    runtime.spawn_worker_pool(TestJobKind::Simple, 4).await;

    let mut expected = HashSet::new();
    for idx in 0..50 {
        let entity_id = TestEntityId::new(&format!("entity-{idx}"));
        let job = TestJob::Simple {
            name: format!("job-{idx}"),
        };
        let handle =
            queue.enqueue(entity_id, job.clone(), JobPriority::P2).await.unwrap();
        publish_enqueued(&events, entity_id, &handle, &job, JobPriority::P2)
            .await
            .unwrap();
        expected.insert(handle.id);
    }

    let mut rx = events.subscribe_jobs();
    let mut completed = HashSet::new();
    let wait = timeout(Duration::from_secs(10), async {
        while completed.len() < expected.len() {
            if let Some(event) = next_event(&mut rx).await {
                if let JobEventPayload::Completed { job_id, .. } = event.payload {
                    completed.insert(job_id);
                }
            }
        }
    })
    .await;

    assert!(wait.is_ok(), "timed out waiting for completions");
    assert_eq!(completed, expected);
    runtime.shutdown().await.expect("shutdown runtime");
}

#[cfg(feature = "postgres")]
mod postgres_tests {
    use super::*;
    use orca::persistence::PostgresQueueService;
    use orca::scheduler::PriorityWeights;
    use sqlx::PgPool;
    use uuid::Uuid;

    async fn connect() -> Option<PgPool> {
        let url = match std::env::var("DATABASE_URL") {
            Ok(url) => url,
            Err(_) => return None,
        };
        PgPool::connect(&url).await.ok()
    }

    async fn cleanup(pool: &PgPool, scope: &str) {
        let _ = sqlx::query("DELETE FROM orca_jobs WHERE entity_scope = $1")
            .bind(scope)
            .execute(pool)
            .await;
    }

    #[tokio::test]
    async fn postgres_multi_entity_fairness() {
        let pool = match connect().await {
            Some(pool) => pool,
            None => return,
        };
        let scope = format!("rt-fairness-{}", Uuid::new_v4());
        let queue = Arc::new(PostgresQueueService::new(pool.clone(), &scope));
        let events = Arc::new(TestEventBus::new(512));
        let dispatcher = Arc::new(TestDispatcher::new(FailureMode::None));
        let runtime =
            build_runtime(default_config(), queue.clone(), dispatcher, events.clone());

        runtime.start().await.expect("start runtime");
        runtime.spawn_worker_pool(TestJobKind::Simple, 1).await;

        let mut entities = Vec::new();
        for idx in 0..5 {
            let entity_id = TestEntityId::new(&format!("entity-{idx}"));
            let job = TestJob::Simple {
                name: format!("job-{idx}"),
            };
            let handle =
                queue.enqueue(entity_id, job.clone(), JobPriority::P2).await.unwrap();
            publish_enqueued(&events, entity_id, &handle, &job, JobPriority::P2)
                .await
                .unwrap();
            entities.push(entity_id);
        }

        let mut rx = events.subscribe_jobs();
        let mut seen = HashSet::new();
        let wait = timeout(Duration::from_secs(10), async {
            while seen.len() < entities.len() {
                if let Some(event) = next_event(&mut rx).await {
                    if let JobEventPayload::Dequeued { .. } = event.payload {
                        seen.insert(event.meta.entity_id);
                    }
                }
            }
        })
        .await;

        assert!(wait.is_ok(), "timed out waiting for dequeues");
        assert_eq!(seen.len(), entities.len());
        runtime.shutdown().await.expect("shutdown runtime");
        cleanup(&pool, &scope).await;
    }

    #[tokio::test]
    async fn postgres_priority_ordering() {
        let pool = match connect().await {
            Some(pool) => pool,
            None => return,
        };
        let scope = format!("rt-priority-{}", Uuid::new_v4());
        let queue = Arc::new(PostgresQueueService::new(pool.clone(), &scope));
        let events = Arc::new(TestEventBus::new(512));
        let dispatcher = Arc::new(TestDispatcher::new(FailureMode::None));

        let mut config = default_config();
        config.priority_weights = PriorityWeights::equal();
        let runtime = build_runtime(config, queue.clone(), dispatcher, events.clone());

        runtime.start().await.expect("start runtime");
        runtime.spawn_worker_pool(TestJobKind::Simple, 1).await;

        let entity_id = TestEntityId::new("priority-entity");
        let priorities = [
            JobPriority::P0,
            JobPriority::P1,
            JobPriority::P2,
            JobPriority::P3,
        ];
        for (idx, priority) in priorities.iter().enumerate() {
            let job = TestJob::Simple {
                name: format!("p{idx}"),
            };
            let handle =
                queue.enqueue(entity_id, job.clone(), *priority).await.unwrap();
            publish_enqueued(&events, entity_id, &handle, &job, *priority)
                .await
                .unwrap();
        }

        let mut rx = events.subscribe_jobs();
        let mut dequeued = Vec::new();
        let wait = timeout(Duration::from_secs(10), async {
            while dequeued.len() < priorities.len() {
                if let Some(event) = next_event(&mut rx).await {
                    if let JobEventPayload::Dequeued { priority, .. } =
                        event.payload
                    {
                        dequeued.push(priority);
                    }
                }
            }
        })
        .await;

        assert!(wait.is_ok(), "timed out waiting for dequeues");
        assert_eq!(dequeued, priorities);
        runtime.shutdown().await.expect("shutdown runtime");
        cleanup(&pool, &scope).await;
    }

    #[tokio::test]
    async fn postgres_lease_renewal_under_load() {
        let pool = match connect().await {
            Some(pool) => pool,
            None => return,
        };
        let scope = format!("rt-renewal-{}", Uuid::new_v4());
        let queue = Arc::new(PostgresQueueService::new(pool.clone(), &scope));
        let events = Arc::new(TestEventBus::new(1024));
        let dispatcher = Arc::new(TestDispatcher::new(FailureMode::None));
        let runtime =
            build_runtime(renewal_config(), queue.clone(), dispatcher, events.clone());

        runtime.start().await.expect("start runtime");
        runtime.spawn_worker_pool(TestJobKind::Slow, 2).await;

        let mut expected = HashSet::new();
        for idx in 0..3 {
            let entity_id = TestEntityId::new(&format!("renewal-{idx}"));
            let job = TestJob::Slow {
                duration_ms: 1500,
                name: format!("slow-{idx}"),
            };
            let handle =
                queue.enqueue(entity_id, job.clone(), JobPriority::P2).await.unwrap();
            publish_enqueued(&events, entity_id, &handle, &job, JobPriority::P2)
                .await
                .unwrap();
            expected.insert(handle.id);
        }

        let mut rx = events.subscribe_jobs();
        let mut renewed = HashSet::new();
        let wait = timeout(Duration::from_secs(10), async {
            while renewed.len() < expected.len() {
                if let Some(event) = next_event(&mut rx).await {
                    if let JobEventPayload::LeaseRenewed { job_id, .. } =
                        event.payload
                    {
                        if expected.contains(&job_id) {
                            renewed.insert(job_id);
                        }
                    }
                }
            }
        })
        .await;

        assert!(wait.is_ok(), "timed out waiting for renewals");
        runtime.shutdown().await.expect("shutdown runtime");
        cleanup(&pool, &scope).await;
    }

    #[tokio::test]
    async fn postgres_dead_letters_after_max_attempts() {
        let pool = match connect().await {
            Some(pool) => pool,
            None => return,
        };
        let scope = format!("rt-dead-letter-{}", Uuid::new_v4());
        let queue = Arc::new(PostgresQueueService::new(pool.clone(), &scope));
        let events = Arc::new(TestEventBus::new(512));
        let dispatcher = Arc::new(TestDispatcher::new(FailureMode::Retryable));
        let runtime =
            build_runtime(default_config(), queue.clone(), dispatcher, events.clone());

        runtime.start().await.expect("start runtime");
        runtime.spawn_worker_pool(TestJobKind::Failing, 1).await;

        let entity_id = TestEntityId::new("dead-letter");
        let job = TestJob::Failing {
            error: "boom".into(),
            name: "dead-letter".into(),
        };
        let handle =
            queue.enqueue(entity_id, job.clone(), JobPriority::P1).await.unwrap();
        publish_enqueued(&events, entity_id, &handle, &job, JobPriority::P1)
            .await
            .unwrap();

        sqlx::query("UPDATE orca_jobs SET max_attempts = 1 WHERE id = $1")
            .bind(handle.id.0)
            .execute(&pool)
            .await
            .unwrap();

        let mut rx = events.subscribe_jobs();
        let wait = timeout(Duration::from_secs(10), async {
            loop {
                if let Some(event) = next_event(&mut rx).await {
                    if let JobEventPayload::DeadLettered { job_id, .. } =
                        event.payload
                    {
                        if job_id == handle.id {
                            break;
                        }
                    }
                }
            }
        })
        .await;

        assert!(wait.is_ok(), "timed out waiting for dead-letter");
        runtime.shutdown().await.expect("shutdown runtime");
        cleanup(&pool, &scope).await;
    }
}
