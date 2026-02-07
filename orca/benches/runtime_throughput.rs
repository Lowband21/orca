//! Benchmarks for runtime throughput using criterion.
//!
//! These benchmarks measure end-to-end job processing throughput:
//! - Jobs per second with 1 worker
//! - Jobs per second with 5 workers
//! - Jobs per second with 10 workers
//!
//! Note: These are integration-style benchmarks that exercise the full runtime,
//! including queue, budget, scheduler, and dispatcher.

#![allow(missing_docs)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use orca::budget::{BudgetConfig, InMemoryBudget};
use orca::correlation::CorrelationCache;
use orca::events::{EventMeta, InProcEventBus, JobEvent, JobEventPayload};
use orca::job::{Job, JobHandle, JobId, JobPriority};
use orca::lease::JobLease;
use orca::queue::{LeaseExpiryScanner, QueueService};
use orca::runtime::{
    DispatchStatus, JobDispatcher, JobEventPublisher, OrchestratorRuntime,
    OrchestratorRuntimeConfig, StandardOrchestratorRuntimeBuilder, WorkloadToKindMapper,
};
use orca_testkit::{
    InMemoryQueueService, TestEntityId, TestJob, TestJobKind, TestWorkloadKind,
};
use tokio::sync::broadcast;

/// A success-only dispatcher that completes jobs immediately.
/// This isolates runtime overhead from job execution time.
#[derive(Clone)]
struct ImmediateSuccessDispatcher;

#[async_trait]
impl JobDispatcher<TestJob> for ImmediateSuccessDispatcher {
    async fn dispatch(&self, _lease: &JobLease<TestJob>) -> DispatchStatus {
        // No work - just return success immediately
        DispatchStatus::Success
    }
}

/// Event bus that publishes job lifecycle events.
#[derive(Clone)]
struct BenchEventBus {
    bus: Arc<InProcEventBus<TestJob>>,
}

impl BenchEventBus {
    fn new(capacity: usize) -> Self {
        Self {
            bus: Arc::new(InProcEventBus::new(capacity)),
        }
    }
}

#[async_trait]
impl JobEventPublisher<TestJob> for BenchEventBus {
    async fn publish(&self, event: JobEvent<TestJob>) -> anyhow::Result<()> {
        self.bus.publish_job(event)
    }

    fn subscribe_jobs(&self) -> broadcast::Receiver<JobEvent<TestJob>> {
        self.bus.subscribe_job_events()
    }
}

/// Workload mapper that maps all job kinds to General workload.
#[derive(Clone, Copy)]
struct BenchMapper;

impl WorkloadToKindMapper<TestJob> for BenchMapper {
    fn map(&self, _kind: TestJobKind) -> TestWorkloadKind {
        TestWorkloadKind::General
    }
}

/// Test queue wrapper that implements LeaseExpiryScanner.
#[derive(Clone)]
struct BenchQueue {
    inner: InMemoryQueueService,
}

impl BenchQueue {
    fn new() -> Self {
        Self {
            inner: InMemoryQueueService::new(),
        }
    }
}

impl Default for BenchQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueueService<TestJob> for BenchQueue {
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
impl LeaseExpiryScanner for BenchQueue {
    async fn scan_expired_leases(&self) -> anyhow::Result<u64> {
        Ok(0)
    }
}

/// Builds a runtime with the specified configuration.
fn build_runtime(
    config: OrchestratorRuntimeConfig,
    queue: Arc<BenchQueue>,
    dispatcher: Arc<ImmediateSuccessDispatcher>,
    events: Arc<BenchEventBus>,
) -> OrchestratorRuntime<
    TestJob,
    BenchQueue,
    InMemoryBudget<TestWorkloadKind, TestEntityId>,
    orca::scheduler::WeightedFairScheduler<TestEntityId>,
    ImmediateSuccessDispatcher,
    BenchMapper,
> {
    let budget = Arc::new(InMemoryBudget::new(BudgetConfig::with_default(100)));
    let events: Arc<dyn JobEventPublisher<TestJob>> = events;
    let mapper = Arc::new(BenchMapper);

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

/// Helper to publish an enqueued event.
async fn publish_enqueued(
    events: &BenchEventBus,
    entity_id: TestEntityId,
    handle: &JobHandle,
    job: &TestJob,
    priority: JobPriority,
) -> anyhow::Result<()> {
    let event = JobEvent {
        meta: EventMeta::new(entity_id, None, format!("enqueue-{}", handle.id)),
        payload: JobEventPayload::Enqueued {
            job_id: handle.id,
            kind: job.kind(),
            priority,
        },
    };
    events.publish(event).await
}

/// Helper to receive next event with lag handling.
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

/// Runtime configuration optimized for benchmarks.
fn bench_runtime_config() -> OrchestratorRuntimeConfig {
    let mut config = OrchestratorRuntimeConfig::default();
    // Short lease TTL for faster benchmarks
    config.default_lease_ttl_secs = 10;
    config.renew_min_margin_secs = 1;
    config.renew_at_fraction = 0.5;
    config.max_lease_renewals = 5;
    config.housekeeper_interval_ms = 1000;
    config
}

/// Benchmark: Jobs per second with varying worker counts.
///
/// Measures throughput of the complete job lifecycle:
/// enqueue → dequeue → dispatch → complete
fn bench_runtime_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    let worker_counts = vec![1, 5, 10];
    let job_counts = vec![100, 500];

    let mut group = c.benchmark_group("runtime_throughput");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for worker_count in &worker_counts {
        for job_count in &job_counts {
            let bench_id = BenchmarkId::new(
                format!("{}_workers", worker_count),
                format!("{}_jobs", job_count),
            );

            group.throughput(Throughput::Elements(*job_count as u64));
            group.bench_with_input(bench_id, &(*worker_count, *job_count), |b, (workers, jobs)| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let queue = Arc::new(BenchQueue::new());
                        let events = Arc::new(BenchEventBus::new(10_000));
                        let dispatcher = Arc::new(ImmediateSuccessDispatcher);
                        let runtime = build_runtime(
                            bench_runtime_config(),
                            queue.clone(),
                            dispatcher,
                            events.clone(),
                        );

                        // Start runtime
                        runtime.start().await.expect("start runtime");
                        runtime.spawn_worker_pool(TestJobKind::Simple, *workers).await;

                        // Pre-enqueue all jobs
                        let mut handles = Vec::with_capacity(*jobs);
                        let start_instant = Instant::now();

                        for idx in 0..*jobs {
                            let entity_id = TestEntityId::new(&format!("entity-{idx}"));
                            let job = TestJob::Simple {
                                name: format!("job-{idx}"),
                            };
                            let handle = queue
                                .enqueue(entity_id, job.clone(), JobPriority::P2)
                                .await
                                .expect("enqueue should succeed");
                            publish_enqueued(&events, entity_id, &handle, &job, JobPriority::P2)
                                .await
                                .expect("publish should succeed");
                            handles.push(handle.id);
                        }

                        // Wait for all jobs to complete
                        let expected: HashSet<JobId> = handles.into_iter().collect();
                        let mut rx = events.subscribe_jobs();
                        let mut completed = HashSet::new();

                        loop {
                            if let Some(event) = next_event(&mut rx).await {
                                if let JobEventPayload::Completed { job_id, .. } = event.payload {
                                    completed.insert(job_id);
                                    if completed.len() >= expected.len() {
                                        break;
                                    }
                                }
                            }
                        }

                        let elapsed = start_instant.elapsed();
                        total_duration += elapsed;

                        // Cleanup
                        runtime.shutdown().await.expect("shutdown runtime");
                    }

                    total_duration
                });
            });
        }
    }

    group.finish();
}

/// Benchmark: Parallel job processing scaling.
///
/// Measures how throughput scales with more workers (fixed job count).
fn bench_scaling(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    let worker_counts = vec![1, 2, 5, 10, 20];
    const JOB_COUNT: usize = 500;

    let mut group = c.benchmark_group("scaling");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));

    for worker_count in &worker_counts {
        group.throughput(Throughput::Elements(JOB_COUNT as u64));
        group.bench_with_input(
            BenchmarkId::new("workers", worker_count),
            worker_count,
            |b, &workers| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let mut total_duration = Duration::ZERO;

                    for _ in 0..iters {
                        let queue = Arc::new(BenchQueue::new());
                        let events = Arc::new(BenchEventBus::new(10_000));
                        let dispatcher = Arc::new(ImmediateSuccessDispatcher);
                        let runtime = build_runtime(
                            bench_runtime_config(),
                            queue.clone(),
                            dispatcher,
                            events.clone(),
                        );

                        runtime.start().await.expect("start runtime");
                        runtime.spawn_worker_pool(TestJobKind::Simple, workers).await;

                        // Enqueue jobs
                        let mut handles = Vec::with_capacity(JOB_COUNT);
                        let start_instant = Instant::now();

                        for idx in 0..JOB_COUNT {
                            let entity_id = TestEntityId::new(&format!("entity-{idx}"));
                            let job = TestJob::Simple {
                                name: format!("job-{idx}"),
                            };
                            let handle = queue
                                .enqueue(entity_id, job.clone(), JobPriority::P2)
                                .await
                                .expect("enqueue should succeed");
                            publish_enqueued(&events, entity_id, &handle, &job, JobPriority::P2)
                                .await
                                .expect("publish should succeed");
                            handles.push(handle.id);
                        }

                        // Wait for completion
                        let expected: HashSet<JobId> = handles.into_iter().collect();
                        let mut rx = events.subscribe_jobs();
                        let mut completed = HashSet::new();

                        loop {
                            if let Some(event) = next_event(&mut rx).await {
                                if let JobEventPayload::Completed { job_id, .. } = event.payload {
                                    completed.insert(job_id);
                                    if completed.len() >= expected.len() {
                                        break;
                                    }
                                }
                            }
                        }

                        let elapsed = start_instant.elapsed();
                        total_duration += elapsed;

                        runtime.shutdown().await.expect("shutdown runtime");
                    }

                    total_duration
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_runtime_throughput, bench_scaling);
criterion_main!(benches);
