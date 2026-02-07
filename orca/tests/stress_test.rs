//! Stress tests for orca orchestration system.
//!
//! Tests high-throughput scenarios with 1000 jobs and 10 workers.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orca::budget::{BudgetConfig, InMemoryBudget};
use orca::correlation::CorrelationCache;
use orca::events::{EventMeta, InProcEventBus, JobEvent, JobEventPayload};
use orca::job::{Job, JobHandle, JobId, JobPriority};
use orca::lease::JobLease;
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

#[derive(Clone)]
struct SuccessDispatcher;

#[async_trait]
impl JobDispatcher<TestJob> for SuccessDispatcher {
    async fn dispatch(&self, _lease: &JobLease<TestJob>) -> DispatchStatus {
        DispatchStatus::Success
    }
}

#[derive(Clone)]
struct TestInMemoryQueue {
    inner: InMemoryQueueService,
}

impl TestInMemoryQueue {
    fn new() -> Self {
        Self {
            inner: InMemoryQueueService::new(),
        }
    }
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
        self.inner.dequeue(request).await
    }

    async fn complete(&self, lease_id: orca::lease::LeaseId) -> anyhow::Result<()> {
        self.inner.complete(lease_id).await
    }

    async fn fail(
        &self,
        lease_id: orca::lease::LeaseId,
        retryable: bool,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        self.inner.fail(lease_id, retryable, error).await
    }

    async fn dead_letter(
        &self,
        lease_id: orca::lease::LeaseId,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        self.inner.dead_letter(lease_id, error).await
    }

    async fn renew(
        &self,
        renewal: orca::lease::LeaseRenewal,
    ) -> anyhow::Result<JobLease<TestJob>> {
        self.inner.renew(renewal).await
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
    let budget = Arc::new(InMemoryBudget::new(BudgetConfig::with_default(20)));
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
async fn stress_1000_jobs_with_10_workers() {
    let queue = Arc::new(TestInMemoryQueue::new());
    let events = Arc::new(TestEventBus::new(10_000));
    let dispatcher = Arc::new(SuccessDispatcher);
    let runtime = build_runtime(
        OrchestratorRuntimeConfig::default(),
        queue.clone(),
        dispatcher,
        events.clone(),
    );

    runtime.start().await.expect("start runtime");
    runtime.spawn_worker_pool(TestJobKind::Simple, 10).await;

    let mut expected = HashSet::new();
    for idx in 0..1000 {
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
    let wait = timeout(Duration::from_secs(30), async {
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
