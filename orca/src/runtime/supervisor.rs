use std::any::type_name;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::Duration;
use tokio::sync::{Mutex, Notify};

use crate::budget::{Budget, EntityId};
use crate::correlation::CorrelationCache;
use crate::events::{EventMeta, JobEvent, JobEventPayload};
use crate::job::{Job, JobId, JobPriority};
use crate::lease::{DequeueRequest, JobLease, LeaseRenewal, QueueSelector};
use crate::queue::{LeaseExpiryScanner, QueueService};
use crate::scheduler::{
    PriorityWeights, SchedulerConfig, SchedulingReservation,
    WeightedFairScheduler,
};

/// Maps job kinds to their workload kinds for budget management.
///
/// Implementors define how job types are mapped to workload categories
/// for concurrency limiting purposes.
pub trait WorkloadToKindMapper<J: Job>: Send + Sync {
    /// Map a job kind to its corresponding workload kind.
    fn map(&self, kind: J::Kind) -> J::WorkloadKind;
}

/// Configuration for the orchestrator runtime.
#[derive(Clone, Debug)]
pub struct OrchestratorRuntimeConfig {
    /// Default lease time-to-live in seconds.
    pub default_lease_ttl_secs: i64,
    /// Minimum margin before lease expiry for renewal attempts.
    pub renew_min_margin_secs: i64,
    /// Fraction of lease TTL at which to attempt renewal.
    pub renew_at_fraction: f32,
    /// Maximum number of lease renewals allowed.
    pub max_lease_renewals: u32,
    /// Interval between housekeeper runs in milliseconds.
    pub housekeeper_interval_ms: u64,
    /// Scheduler configuration.
    pub scheduler: SchedulerConfig,
    /// Priority weights for scheduling.
    pub priority_weights: PriorityWeights,
}

impl Default for OrchestratorRuntimeConfig {
    fn default() -> Self {
        Self {
            default_lease_ttl_secs: 30,
            renew_min_margin_secs: 2,
            renew_at_fraction: 0.5,
            max_lease_renewals: 10,
            housekeeper_interval_ms: 5000,
            scheduler: SchedulerConfig::default(),
            priority_weights: PriorityWeights::default(),
        }
    }
}

/// Token for signaling graceful shutdown to workers.
#[derive(Clone, Debug)]
pub struct ShutdownToken {
    inner: Arc<ShutdownTokenInner>,
}

#[derive(Debug)]
struct ShutdownTokenInner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl ShutdownToken {
    /// Create a new shutdown token.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ShutdownTokenInner {
                cancelled: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    /// Signal cancellation.
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    /// Check if cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }

    /// Wait until cancelled.
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        self.inner.notify.notified().await;
    }
}

impl Default for ShutdownToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Main orchestrator runtime for job processing.
pub struct OrchestratorRuntime<J, Q, B, S, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner + 'static,
    B: Budget<J::WorkloadKind, J::EntityId> + 'static,
    S: Scheduler<J::EntityId> + 'static,
    D: JobDispatcher<J> + 'static,
    M: WorkloadToKindMapper<J> + 'static,
{
    config: OrchestratorRuntimeConfig,
    queue: Arc<Q>,
    budget: Arc<B>,
    scheduler: Arc<S>,
    dispatcher: Arc<D>,
    correlations: CorrelationCache,
    events: Arc<dyn JobEventPublisher<J> + 'static>,
    workload_mapper: Arc<M>,
    shutdown_token: ShutdownToken,
    worker_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl<J, Q, B, S, D, M> fmt::Debug for OrchestratorRuntime<J, Q, B, S, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner + 'static,
    B: Budget<J::WorkloadKind, J::EntityId> + 'static,
    S: Scheduler<J::EntityId> + 'static,
    D: JobDispatcher<J> + 'static,
    M: WorkloadToKindMapper<J> + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let queue_type = type_name::<Q>();
        let budget_type = type_name::<B>();
        let scheduler_type = type_name::<S>();
        let dispatcher_type = type_name::<D>();
        let mapper_type = type_name::<M>();

        let worker_count = self
            .worker_handles
            .try_lock()
            .map(|handles| handles.len())
            .unwrap_or_default();

        f.debug_struct("OrchestratorRuntime")
            .field("config", &self.config)
            .field("queue_type", &queue_type)
            .field("budget_type", &budget_type)
            .field("scheduler_type", &scheduler_type)
            .field("dispatcher_type", &dispatcher_type)
            .field("mapper_type", &mapper_type)
            .field("worker_count", &worker_count)
            .field("shutdown_cancelled", &self.shutdown_token.is_cancelled())
            .finish()
    }
}

impl<J, Q, B, S, D, M> OrchestratorRuntime<J, Q, B, S, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner + 'static,
    B: Budget<J::WorkloadKind, J::EntityId> + 'static,
    S: Scheduler<J::EntityId> + 'static,
    D: JobDispatcher<J> + 'static,
    M: WorkloadToKindMapper<J> + 'static,
{
    /// Create a new orchestrator runtime with the given components.
    pub fn new(
        config: OrchestratorRuntimeConfig,
        queue: Arc<Q>,
        budget: Arc<B>,
        scheduler: Arc<S>,
        dispatcher: Arc<D>,
        events: Arc<dyn JobEventPublisher<J> + 'static>,
        correlations: CorrelationCache,
        workload_mapper: Arc<M>,
    ) -> Self {
        Self {
            config,
            queue,
            budget,
            scheduler,
            dispatcher,
            correlations,
            events,
            workload_mapper,
            shutdown_token: ShutdownToken::new(),
            worker_handles: Mutex::new(Vec::new()),
        }
    }

    /// Get the runtime configuration.
    pub fn config(&self) -> &OrchestratorRuntimeConfig {
        &self.config
    }

    /// Get a clone of the queue service.
    pub fn queue(&self) -> Arc<Q> {
        Arc::clone(&self.queue)
    }

    /// Get a clone of the budget manager.
    pub fn budget(&self) -> Arc<B> {
        Arc::clone(&self.budget)
    }

    /// Get a clone of the scheduler.
    pub fn scheduler(&self) -> Arc<S> {
        Arc::clone(&self.scheduler)
    }

    /// Get a clone of the dispatcher.
    pub fn dispatcher(&self) -> Arc<D> {
        Arc::clone(&self.dispatcher)
    }

    /// Get a clone of the event publisher.
    pub fn events(&self) -> Arc<dyn JobEventPublisher<J>> {
        Arc::clone(&self.events)
    }

    /// Get a clone of the correlation cache.
    pub fn correlations(&self) -> CorrelationCache {
        self.correlations.clone()
    }

    /// Start the runtime scheduler observer.
    pub async fn start(&self) -> anyhow::Result<()> {
        self.spawn_scheduler_observer();
        Ok(())
    }

    /// Gracefully shut down the runtime and all workers.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        tracing::info!("Initiating graceful shutdown of orchestrator runtime");

        self.shutdown_token.cancel();

        let handles = {
            let mut guard = self.worker_handles.lock().await;
            std::mem::take(&mut *guard)
        };

        for handle in handles {
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(30),
                handle,
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!("Worker task failed: {:?}", e),
                Err(_) => {
                    tracing::warn!("Worker task timed out during shutdown")
                }
            }
        }

        tracing::info!("Orchestrator runtime shutdown complete");
        Ok(())
    }

    /// Spawn a pool of workers for a specific job kind.
    pub async fn spawn_worker_pool(&self, kind: J::Kind, parallelism: usize) {
        let worker_group = format!("{}-{}", kind, std::process::id());
        let config = self.config.clone();
        let queue = self.queue();
        let budget = self.budget();
        let scheduler = self.scheduler();
        let dispatcher = self.dispatcher();
        let events = self.events();
        let correlations = self.correlations.clone();
        let workload_mapper = self.workload_mapper.clone();

        for i in 0..parallelism {
            let worker_id = format!("{}-w{}", worker_group, i);
            let q = Arc::clone(&queue);
            let b = Arc::clone(&budget);
            let s = Arc::clone(&scheduler);
            let d = Arc::clone(&dispatcher);
            let e = Arc::clone(&events);
            let correlation_cache = correlations.clone();
            let mapper = Arc::clone(&workload_mapper);
            let shutdown = self.shutdown_token.clone();
            let lease_config = config.clone();
            let worker_kind = kind;

            let handle = tokio::spawn(async move {
                Self::worker_loop(
                    worker_id,
                    worker_kind,
                    q,
                    b,
                    s,
                    d,
                    e,
                    correlation_cache,
                    mapper,
                    shutdown,
                    lease_config,
                )
                .await;
            });

            let mut handles = self.worker_handles.lock().await;
            handles.push(handle);
        }
    }

    /// Spawn a housekeeper task for scanning expired leases.
    pub fn spawn_housekeeper(&self) {
        let q = self.queue();
        let interval = tokio::time::Duration::from_millis(
            self.config.housekeeper_interval_ms,
        );
        let shutdown = self.shutdown_token.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::info!("Housekeeper shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(interval) => {
                        if let Err(err) = q.scan_expired_leases().await {
                            tracing::warn!("housekeeper scan_expired_leases error: {err}");
                        }
                    }
                }
            }
        });
    }

    fn spawn_scheduler_observer(&self) {
        let mut job_rx = self.events.subscribe_jobs();
        let scheduler = self.scheduler.clone();
        let correlations = self.correlations.clone();
        let shutdown = self.shutdown_token.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => {
                        tracing::info!("Scheduler observer shutting down");
                        break;
                    }
                    event = job_rx.recv() => match event {
                        Ok(event) => {
                            match event.payload {
                                JobEventPayload::Enqueued { job_id, priority, .. } => {
                                    correlations.remember(job_id.0, event.meta.correlation_id).await;
                                    scheduler.record_enqueued(event.meta.entity_id, priority).await;
                                }
                                JobEventPayload::Completed { .. } => {
                                    scheduler.record_completed(event.meta.entity_id).await;
                                }
                                JobEventPayload::Failed { .. } => {
                                    scheduler.record_completed(event.meta.entity_id).await;
                                }
                                JobEventPayload::DeadLettered { .. } => {
                                    scheduler.record_completed(event.meta.entity_id).await;
                                }
                                _ => {}
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!("scheduler observer lagged, skipped {skipped} events");
                        }
                    }
                }
            }
        });
    }

    async fn worker_loop(
        worker_id: String,
        kind: J::Kind,
        queue: Arc<Q>,
        budget: Arc<B>,
        scheduler: Arc<S>,
        dispatcher: Arc<D>,
        events: Arc<dyn JobEventPublisher<J> + 'static>,
        correlations: CorrelationCache,
        workload_mapper: Arc<M>,
        shutdown: ShutdownToken,
        config: OrchestratorRuntimeConfig,
    ) {
        loop {
            if shutdown.is_cancelled() {
                tracing::info!("Worker {} shutting down", worker_id);
                break;
            }

            let workload = workload_mapper.map(kind);
            if let Ok(false) = budget.has_budget(workload).await {
                tokio::time::sleep(tokio::time::Duration::from_millis(50))
                    .await;
                continue;
            }

            let reservation = match scheduler.reserve().await {
                Some(reservation) => reservation,
                None => {
                    tokio::time::sleep(tokio::time::Duration::from_millis(50))
                        .await;
                    continue;
                }
            };

            let dequeue = DequeueRequest {
                kind,
                worker_id: worker_id.clone(),
                lease_ttl: Duration::seconds(config.default_lease_ttl_secs),
                selector: Some(QueueSelector::<J::EntityId> {
                    entity_id: reservation.entity_id,
                    priority: reservation.priority,
                }),
            };

            match queue.dequeue(dequeue).await {
                Ok(Some(lease)) => {
                    let _ = scheduler.confirm(reservation.id).await;

                    let job_id = lease.job_id.0;
                    let job_kind = kind;
                    let job_priority = reservation.priority;
                    let lease_id = lease.lease_id;
                    let entity_id = reservation.entity_id;
                    let current_expires_at = lease.expires_at;

                    let correlation_id =
                        correlations.fetch_or_generate(job_id).await;

                    let dequeue_event = JobEvent {
                        meta: EventMeta::new(
                            entity_id,
                            Some(correlation_id),
                            format!("dequeue-{}", job_id),
                        ),
                        payload: JobEventPayload::Dequeued {
                            job_id: JobId(job_id),
                            kind: job_kind,
                            priority: job_priority,
                            lease_id,
                        },
                    };
                    let _ = events.publish(dequeue_event).await;

                    let token = match budget.acquire(workload, entity_id).await
                    {
                        Ok(t) => t,
                        Err(err) => {
                            tracing::error!("budget acquire error: {err}");
                            let _ = queue
                                .fail(
                                    lease_id,
                                    true,
                                    Some("budget acquire failed".into()),
                                )
                                .await;
                            scheduler.release(entity_id).await;
                            scheduler
                                .record_enqueued(entity_id, job_priority)
                                .await;
                            continue;
                        }
                    };

                    let ttl = Duration::seconds(config.default_lease_ttl_secs);
                    let renew_margin = tokio::time::Duration::from_secs(
                        config.renew_min_margin_secs as u64,
                    );
                    let renew_fraction = config.renew_at_fraction;

                    let (cancel_tx, mut cancel_rx) =
                        tokio::sync::mpsc::channel::<()>(1);

                    let mut local_expires_at = current_expires_at;
                    let renewer_q = Arc::clone(&queue);
                    let renewer_e = Arc::clone(&events);
                    let worker_id_clone = worker_id.clone();
                    let renew_correlations = correlations.clone();
                    let renew_handle = tokio::spawn(async move {
                        loop {
                            let now = chrono::Utc::now();
                            let mut sleep_dur =
                                tokio::time::Duration::from_millis(500);
                            if local_expires_at > now {
                                let ttl_total = ttl.to_std().unwrap_or(
                                    tokio::time::Duration::from_secs(30),
                                );
                                let target =
                                    ttl_total.mul_f32(1.0 - renew_fraction);
                                let remaining = (local_expires_at - now)
                                    .to_std()
                                    .unwrap_or(
                                        tokio::time::Duration::from_millis(0),
                                    );
                                sleep_dur = if remaining > target {
                                    remaining - target
                                } else if remaining > renew_margin {
                                    remaining - renew_margin
                                } else {
                                    tokio::time::Duration::from_millis(0)
                                };
                            }

                            tokio::select! {
                                _ = tokio::time::sleep(sleep_dur) => {}
                                _ = cancel_rx.recv() => { break; }
                            }

                            match renewer_q
                                .renew(LeaseRenewal {
                                    lease_id,
                                    worker_id: worker_id_clone.clone(),
                                    extend_by: ttl,
                                })
                                .await
                            {
                                Ok(updated) => {
                                    local_expires_at = updated.expires_at;
                                    let correlation_id = renew_correlations
                                        .fetch_or_generate(updated.lease_id.0)
                                        .await;
                                    let renew_event = JobEvent {
                                        meta: EventMeta::new(
                                            entity_id,
                                            Some(correlation_id),
                                            format!(
                                                "renew-{}",
                                                updated.lease_id.0
                                            ),
                                        ),
                                        payload:
                                            JobEventPayload::LeaseRenewed {
                                                job_id: JobId(
                                                    updated.lease_id.0,
                                                ),
                                                lease_id: updated.lease_id,
                                                renewals: updated.renewals,
                                            },
                                    };
                                    let _ =
                                        renewer_e.publish(renew_event).await;
                                }
                                Err(_) => {
                                    tracing::trace!(lease = ?lease_id, "lease renew skipped");
                                    break;
                                }
                            }
                        }
                    });

                    let dispatch_status = dispatcher.dispatch(&lease).await;

                    let _ = cancel_tx.try_send(());
                    let _ = renew_handle.await;

                    match dispatch_status {
                        DispatchStatus::Success => {
                            if let Err(err) = queue.complete(lease_id).await {
                                tracing::error!("queue complete error: {err}");
                            }
                            let correlation_id =
                                correlations.take_or_generate(job_id).await;
                            let event = JobEvent {
                                meta: EventMeta::new(
                                    entity_id,
                                    Some(correlation_id),
                                    format!("complete-{}", job_id),
                                ),
                                payload: JobEventPayload::Completed {
                                    job_id: JobId(job_id),
                                    kind: job_kind,
                                    priority: job_priority,
                                },
                            };
                            if let Err(err) = events.publish(event).await {
                                tracing::error!(
                                    "publish complete event failed: {err}"
                                );
                            }
                            scheduler.record_completed(entity_id).await;
                        }
                        DispatchStatus::RetryableFailure { error } => {
                            if let Err(err) =
                                queue.fail(lease_id, true, error).await
                            {
                                tracing::error!("queue fail error: {err}");
                            }
                            let correlation_id =
                                correlations.take_or_generate(job_id).await;
                            let event = JobEvent {
                                meta: EventMeta::new(
                                    entity_id,
                                    Some(correlation_id),
                                    format!("fail-{}", job_id),
                                ),
                                payload: JobEventPayload::Failed {
                                    job_id: JobId(job_id),
                                    kind: job_kind,
                                    priority: job_priority,
                                    retryable: true,
                                },
                            };
                            if let Err(err) = events.publish(event).await {
                                tracing::error!(
                                    "publish fail event failed: {err}"
                                );
                            }
                            scheduler
                                .record_enqueued(entity_id, job_priority)
                                .await;
                        }
                        DispatchStatus::PermanentFailure { error } => {
                            if let Err(err) =
                                queue.fail(lease_id, false, error.clone()).await
                            {
                                tracing::error!("queue fail error: {err}");
                            }
                            let correlation_id =
                                correlations.take_or_generate(job_id).await;
                            let event = JobEvent {
                                meta: EventMeta::new(
                                    entity_id,
                                    Some(correlation_id),
                                    format!("dead-letter-{}", job_id),
                                ),
                                payload: JobEventPayload::DeadLettered {
                                    job_id: JobId(job_id),
                                    kind: job_kind,
                                    priority: job_priority,
                                },
                            };
                            if let Err(err) = events.publish(event).await {
                                tracing::error!(
                                    "publish dead-letter event failed: {err}"
                                );
                            }
                            scheduler.record_completed(entity_id).await;
                        }
                    }

                    if let Err(err) = budget.release(token).await {
                        tracing::error!("budget release error: {err}");
                    }
                }
                Ok(None) => {
                    scheduler.cancel(reservation.id).await;
                }
                Err(err) => {
                    tracing::warn!("dequeue error: {err}");
                    scheduler.cancel(reservation.id).await;
                    tokio::time::sleep(tokio::time::Duration::from_millis(100))
                        .await;
                }
            }
        }
    }
}

/// Trait for publishing job lifecycle events.
#[async_trait]
pub trait JobEventPublisher<J: Job>: Send + Sync {
    /// Publish a job event.
    async fn publish(&self, event: JobEvent<J>) -> anyhow::Result<()>;
    /// Subscribe to job events.
    fn subscribe_jobs(&self) -> tokio::sync::broadcast::Receiver<JobEvent<J>>;
}

/// Trait for dispatching jobs to handlers.
#[async_trait]
pub trait JobDispatcher<J: Job>: Send + Sync {
    /// Dispatch a job for execution.
    async fn dispatch(&self, lease: &JobLease<J>) -> DispatchStatus;
}

/// Result of job dispatch execution.
#[derive(Clone, Debug)]
pub enum DispatchStatus {
    /// Job completed successfully.
    Success,
    /// Job failed but can be retried.
    RetryableFailure {
        /// Error message.
        error: Option<String>,
    },
    /// Job failed permanently.
    PermanentFailure {
        /// Error message.
        error: Option<String>,
    },
}

/// Trait for scheduling job reservations.
#[async_trait]
pub trait Scheduler<E: EntityId>: Send + Sync {
    /// Reserve a scheduling slot.
    async fn reserve(&self) -> Option<SchedulingReservation<E>>;
    /// Confirm a reservation.
    async fn confirm(
        &self,
        reservation_id: uuid::Uuid,
    ) -> Option<SchedulingReservation<E>>;
    /// Cancel a reservation.
    async fn cancel(&self, reservation_id: uuid::Uuid);
    /// Release capacity for an entity.
    async fn release(&self, entity_id: E);
    /// Record a job as enqueued.
    async fn record_enqueued(&self, entity_id: E, priority: JobPriority);
    /// Record a job as completed.
    async fn record_completed(&self, entity_id: E);
}

#[async_trait]
impl<E: EntityId> Scheduler<E> for WeightedFairScheduler<E> {
    async fn reserve(&self) -> Option<SchedulingReservation<E>> {
        self.reserve().await
    }

    async fn confirm(
        &self,
        reservation_id: uuid::Uuid,
    ) -> Option<SchedulingReservation<E>> {
        self.confirm(reservation_id).await
    }

    async fn cancel(&self, reservation_id: uuid::Uuid) {
        self.cancel(reservation_id).await;
    }

    async fn release(&self, entity_id: E) {
        self.release(entity_id).await;
    }

    async fn record_enqueued(&self, entity_id: E, priority: JobPriority) {
        self.record_enqueued(entity_id, priority).await;
    }

    async fn record_completed(&self, entity_id: E) {
        self.record_completed(entity_id).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use std::fmt;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;
    use tokio::time::timeout;

    use crate::budget::{BudgetConfig, InMemoryBudget};
    use crate::events::InProcEventBus;
    use crate::job::{DependencyKey, JobHandle, JobId, JobPriority};
    use crate::lease::{DequeueRequest, JobLease, LeaseId, LeaseRenewal};

    #[derive(
        Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize,
    )]
    enum TestJobKind {
        Test,
    }

    impl crate::job::JobKind for TestJobKind {
        fn as_str(&self) -> &'static str {
            "test"
        }
    }

    impl fmt::Display for TestJobKind {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test")
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
    enum TestWorkloadKind {
        Test,
    }

    impl crate::job::WorkloadKind for TestWorkloadKind {}

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestJob {
        id: JobId,
        kind: TestJobKind,
        priority: JobPriority,
    }

    impl Job for TestJob {
        type EntityId = JobId;
        type Kind = TestJobKind;
        type DomainEvent = ();
        type WorkloadKind = TestWorkloadKind;

        fn entity_id(&self) -> Self::EntityId {
            self.id
        }

        fn kind(&self) -> Self::Kind {
            self.kind
        }

        fn dedupe_key(&self) -> String {
            self.id.to_string()
        }

        fn priority(&self) -> JobPriority {
            self.priority
        }
    }

    struct TestMapper;

    impl WorkloadToKindMapper<TestJob> for TestMapper {
        fn map(&self, _kind: TestJobKind) -> TestWorkloadKind {
            TestWorkloadKind::Test
        }
    }

    struct NoopQueue;

    #[async_trait]
    impl QueueService<TestJob> for NoopQueue {
        async fn enqueue(
            &self,
            _entity_id: JobId,
            _job: TestJob,
            _priority: JobPriority,
        ) -> anyhow::Result<JobHandle> {
            bail!("queue not used in shutdown test")
        }

        async fn enqueue_many(
            &self,
            _requests: Vec<(JobId, TestJob, JobPriority)>,
        ) -> anyhow::Result<Vec<JobHandle>> {
            bail!("queue not used in shutdown test")
        }

        async fn dequeue(
            &self,
            _request: DequeueRequest<TestJobKind, JobId>,
        ) -> anyhow::Result<Option<JobLease<TestJob>>> {
            bail!("queue not used in shutdown test")
        }

        async fn complete(&self, _lease_id: LeaseId) -> anyhow::Result<()> {
            bail!("queue not used in shutdown test")
        }

        async fn fail(
            &self,
            _lease_id: LeaseId,
            _retryable: bool,
            _error: Option<String>,
        ) -> anyhow::Result<()> {
            bail!("queue not used in shutdown test")
        }

        async fn dead_letter(
            &self,
            _lease_id: LeaseId,
            _error: Option<String>,
        ) -> anyhow::Result<()> {
            bail!("queue not used in shutdown test")
        }

        async fn renew(
            &self,
            _renewal: LeaseRenewal,
        ) -> anyhow::Result<JobLease<TestJob>> {
            bail!("queue not used in shutdown test")
        }

        async fn queue_depth(
            &self,
            _kind: TestJobKind,
        ) -> anyhow::Result<usize> {
            bail!("queue not used in shutdown test")
        }

        async fn cancel_job(&self, _job_id: JobId) -> anyhow::Result<()> {
            bail!("queue not used in shutdown test")
        }

        async fn release_dependency(
            &self,
            _entity_id: JobId,
            _dependency_key: &DependencyKey,
        ) -> anyhow::Result<u64> {
            bail!("queue not used in shutdown test")
        }
    }

    #[async_trait]
    impl LeaseExpiryScanner for NoopQueue {
        async fn scan_expired_leases(&self) -> anyhow::Result<u64> {
            Ok(0)
        }
    }

    struct NoopDispatcher;

    #[async_trait]
    impl JobDispatcher<TestJob> for NoopDispatcher {
        async fn dispatch(&self, _lease: &JobLease<TestJob>) -> DispatchStatus {
            DispatchStatus::Success
        }
    }

    struct TestEventBus {
        bus: InProcEventBus<TestJob>,
    }

    impl TestEventBus {
        fn new() -> Self {
            Self {
                bus: InProcEventBus::new(16),
            }
        }
    }

    #[async_trait]
    impl JobEventPublisher<TestJob> for TestEventBus {
        async fn publish(
            &self,
            event: JobEvent<TestJob>,
        ) -> anyhow::Result<()> {
            self.bus.publish_job(event)
        }

        fn subscribe_jobs(&self) -> broadcast::Receiver<JobEvent<TestJob>> {
            self.bus.subscribe_job_events()
        }
    }

    fn build_runtime() -> OrchestratorRuntime<
        TestJob,
        NoopQueue,
        InMemoryBudget<TestWorkloadKind, JobId>,
        WeightedFairScheduler<JobId>,
        NoopDispatcher,
        TestMapper,
    > {
        let config = OrchestratorRuntimeConfig::default();
        let queue = Arc::new(NoopQueue);
        let budget = Arc::new(InMemoryBudget::new(BudgetConfig::<
            TestWorkloadKind,
        >::default()));
        let scheduler = Arc::new(WeightedFairScheduler::new(
            &config.scheduler,
            &config.priority_weights,
        ));
        let dispatcher = Arc::new(NoopDispatcher);
        let events: Arc<dyn JobEventPublisher<TestJob>> =
            Arc::new(TestEventBus::new());
        let correlations = CorrelationCache::new();
        let mapper = Arc::new(TestMapper);

        OrchestratorRuntime::new(
            config,
            queue,
            budget,
            scheduler,
            dispatcher,
            events,
            correlations,
            mapper,
        )
    }

    #[tokio::test]
    async fn test_shutdown_token_shared_state() {
        let token = ShutdownToken::new();
        let clone1 = token.clone();
        let clone2 = token.clone();

        // Cancel original
        token.cancel();

        // All clones should see cancellation immediately
        assert!(clone1.is_cancelled());
        assert!(clone2.is_cancelled());

        // cancelled() should return immediately (not hang)
        timeout(Duration::from_secs(1), clone1.cancelled())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_shutdown_token_cancelled_wakes_clones() {
        let token = ShutdownToken::new();
        let clone1 = token.clone();
        let clone2 = token.clone();
        let clone3 = token.clone();

        // Spawn 3 workers that wait on their cloned tokens
        let h1 = tokio::spawn(async move { clone1.cancelled().await });
        let h2 = tokio::spawn(async move { clone2.cancelled().await });
        let h3 = tokio::spawn(async move { clone3.cancelled().await });

        // Give workers time to enter the wait
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Cancel from original token
        token.cancel();

        // All workers should complete within 5 seconds
        let results = timeout(
            Duration::from_secs(5),
            futures::future::join_all(vec![h1, h2, h3]),
        )
        .await
        .expect("workers did not observe cancellation within 5 seconds");

        for r in results {
            r.expect("worker task panicked");
        }
    }

    #[tokio::test]
    async fn test_shutdown_token_default_not_cancelled() {
        let token = ShutdownToken::default();
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn test_runtime_shutdown_wakes_workers() {
        let runtime = build_runtime();
        let shutdown = runtime.shutdown_token.clone();

        let h1 = tokio::spawn({
            let token = shutdown.clone();
            async move { token.cancelled().await }
        });
        let h2 = tokio::spawn({
            let token = shutdown.clone();
            async move { token.cancelled().await }
        });
        let h3 = tokio::spawn({
            let token = shutdown.clone();
            async move { token.cancelled().await }
        });

        {
            let mut guard = runtime.worker_handles.lock().await;
            guard.extend(vec![h1, h2, h3]);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = timeout(Duration::from_secs(5), runtime.shutdown()).await;
        assert!(result.is_ok(), "shutdown did not complete within 5 seconds");
        result.unwrap().expect("shutdown returned error");
    }
}
