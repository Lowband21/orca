use std::any::type_name;
use std::fmt;
use std::sync::Arc;

use crate::budget::Budget;
use crate::correlation::CorrelationCache;
use crate::job::Job;
use crate::queue::{LeaseExpiryScanner, QueueService};
use crate::scheduler::WeightedFairScheduler;

use super::supervisor::{
    JobDispatcher, JobEventPublisher, OrchestratorRuntime, OrchestratorRuntimeConfig, Scheduler,
    WorkloadToKindMapper,
};

/// Builder for constructing an `OrchestratorRuntime` with explicit dependencies.
///
/// The builder validates that all required dependencies are provided before
/// constructing the runtime. Each dependency is configured via a `with_*` method.
///
/// # Example
///
/// ```ignore
/// use orca::*;
///
/// let runtime = OrchestratorRuntimeBuilder::<MyJob, _, _, _, _, _>::new(config)
///     .with_queue(queue)
///     .with_budget(budget)
///     .with_scheduler(scheduler)
///     .with_dispatcher(dispatcher)
///     .with_events(events)
///     .with_correlations(correlations)
///     .with_workload_mapper(mapper)
///     .build()?;
/// ```
pub struct OrchestratorRuntimeBuilder<J, Q, B, S, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner + 'static,
    B: Budget<J::WorkloadKind, J::EntityId> + 'static,
    S: Scheduler<J::EntityId> + 'static,
    D: JobDispatcher<J> + 'static,
    M: WorkloadToKindMapper<J> + 'static,
{
    config: OrchestratorRuntimeConfig,
    queue: Option<Arc<Q>>,
    budget: Option<Arc<B>>,
    scheduler: Option<Arc<S>>,
    dispatcher: Option<Arc<D>>,
    events: Option<Arc<dyn JobEventPublisher<J> + 'static>>,
    correlations: Option<CorrelationCache>,
    workload_mapper: Option<Arc<M>>,
}

impl<J, Q, B, S, D, M> fmt::Debug for OrchestratorRuntimeBuilder<J, Q, B, S, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner + 'static,
    B: Budget<J::WorkloadKind, J::EntityId> + 'static,
    S: Scheduler<J::EntityId> + 'static,
    D: JobDispatcher<J> + 'static,
    M: WorkloadToKindMapper<J> + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("OrchestratorRuntimeBuilder");
        debug.field("config", &self.config);
        debug.field("queue_set", &self.queue.is_some());
        debug.field("budget_set", &self.budget.is_some());
        debug.field("scheduler_set", &self.scheduler.is_some());
        debug.field("dispatcher_set", &self.dispatcher.is_some());
        debug.field("events_set", &self.events.is_some());
        debug.field("correlations_set", &self.correlations.is_some());
        debug.field("workload_mapper_set", &self.workload_mapper.is_some());

        if self.queue.is_some() {
            debug.field("queue_type", &type_name::<Q>());
        }
        if self.budget.is_some() {
            debug.field("budget_type", &type_name::<B>());
        }
        if self.scheduler.is_some() {
            debug.field("scheduler_type", &type_name::<S>());
        }
        if self.dispatcher.is_some() {
            debug.field("dispatcher_type", &type_name::<D>());
        }
        if self.workload_mapper.is_some() {
            debug.field("mapper_type", &type_name::<M>());
        }

        debug.finish()
    }
}

impl<J, Q, B, S, D, M> OrchestratorRuntimeBuilder<J, Q, B, S, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner + 'static,
    B: Budget<J::WorkloadKind, J::EntityId> + 'static,
    S: Scheduler<J::EntityId> + 'static,
    D: JobDispatcher<J> + 'static,
    M: WorkloadToKindMapper<J> + 'static,
{
    /// Create a new builder with the given runtime configuration.
    pub fn new(config: OrchestratorRuntimeConfig) -> Self {
        Self {
            config,
            queue: None,
            budget: None,
            scheduler: None,
            dispatcher: None,
            events: None,
            correlations: None,
            workload_mapper: None,
        }
    }

    /// Set the queue service.
    pub fn with_queue(mut self, queue: Arc<Q>) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Set the budget manager.
    pub fn with_budget(mut self, budget: Arc<B>) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Set the scheduler.
    pub fn with_scheduler(mut self, scheduler: Arc<S>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// Set the job dispatcher.
    pub fn with_dispatcher(mut self, dispatcher: Arc<D>) -> Self {
        self.dispatcher = Some(dispatcher);
        self
    }

    /// Set the event publisher.
    pub fn with_events(mut self, events: Arc<dyn JobEventPublisher<J> + 'static>) -> Self {
        self.events = Some(events);
        self
    }

    /// Set the correlation cache.
    pub fn with_correlations(mut self, correlations: CorrelationCache) -> Self {
        self.correlations = Some(correlations);
        self
    }

    /// Set the workload-to-kind mapper.
    pub fn with_workload_mapper(mut self, mapper: Arc<M>) -> Self {
        self.workload_mapper = Some(mapper);
        self
    }

    /// Build the `OrchestratorRuntime` with all configured dependencies.
    ///
    /// # Errors
    ///
    /// Returns an error if any required dependency is missing.
    pub fn build(self) -> anyhow::Result<OrchestratorRuntime<J, Q, B, S, D, M>> {
        let queue = self
            .queue
            .ok_or_else(|| anyhow::anyhow!("queue dependency missing"))?;
        let budget = self
            .budget
            .ok_or_else(|| anyhow::anyhow!("budget dependency missing"))?;
        let scheduler = self
            .scheduler
            .ok_or_else(|| anyhow::anyhow!("scheduler dependency missing"))?;
        let dispatcher = self
            .dispatcher
            .ok_or_else(|| anyhow::anyhow!("dispatcher dependency missing"))?;
        let events = self
            .events
            .ok_or_else(|| anyhow::anyhow!("events dependency missing"))?;
        let correlations = self.correlations.unwrap_or_default();
        let workload_mapper = self
            .workload_mapper
            .ok_or_else(|| anyhow::anyhow!("workload_mapper dependency missing"))?;

        Ok(OrchestratorRuntime::new(
            self.config,
            queue,
            budget,
            scheduler,
            dispatcher,
            events,
            correlations,
            workload_mapper,
        ))
    }
}

/// Convenience builder for creating a runtime with a `WeightedFairScheduler`.
///
/// This builder simplifies the common case where you want to use the built-in
/// weighted-fair scheduler instead of a custom scheduler implementation.
pub struct StandardOrchestratorRuntimeBuilder<J, Q, B, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner + 'static,
    B: Budget<J::WorkloadKind, J::EntityId> + 'static,
    D: JobDispatcher<J> + 'static,
    M: WorkloadToKindMapper<J> + 'static,
{
    config: OrchestratorRuntimeConfig,
    queue: Option<Arc<Q>>,
    budget: Option<Arc<B>>,
    dispatcher: Option<Arc<D>>,
    events: Option<Arc<dyn JobEventPublisher<J> + 'static>>,
    correlations: Option<CorrelationCache>,
    workload_mapper: Option<Arc<M>>,
}

impl<J, Q, B, D, M> fmt::Debug for StandardOrchestratorRuntimeBuilder<J, Q, B, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner + 'static,
    B: Budget<J::WorkloadKind, J::EntityId> + 'static,
    D: JobDispatcher<J> + 'static,
    M: WorkloadToKindMapper<J> + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StandardOrchestratorRuntimeBuilder")
            .field("config", &self.config)
            .field("queue_set", &self.queue.is_some())
            .field("budget_set", &self.budget.is_some())
            .field("dispatcher_set", &self.dispatcher.is_some())
            .field("events_set", &self.events.is_some())
            .field("correlations_set", &self.correlations.is_some())
            .field("workload_mapper_set", &self.workload_mapper.is_some())
            .finish()
    }
}

impl<J, Q, B, D, M> StandardOrchestratorRuntimeBuilder<J, Q, B, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner + 'static,
    B: Budget<J::WorkloadKind, J::EntityId> + 'static,
    D: JobDispatcher<J> + 'static,
    M: WorkloadToKindMapper<J> + 'static,
{
    /// Create a new builder with the given runtime configuration.
    pub fn new(config: OrchestratorRuntimeConfig) -> Self {
        Self {
            config,
            queue: None,
            budget: None,
            dispatcher: None,
            events: None,
            correlations: None,
            workload_mapper: None,
        }
    }

    /// Set the queue service.
    pub fn with_queue(mut self, queue: Arc<Q>) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Set the budget manager.
    pub fn with_budget(mut self, budget: Arc<B>) -> Self {
        self.budget = Some(budget);
        self
    }

    /// Set the job dispatcher.
    pub fn with_dispatcher(mut self, dispatcher: Arc<D>) -> Self {
        self.dispatcher = Some(dispatcher);
        self
    }

    /// Set the event publisher.
    pub fn with_events(mut self, events: Arc<dyn JobEventPublisher<J> + 'static>) -> Self {
        self.events = Some(events);
        self
    }

    /// Set the correlation cache.
    pub fn with_correlations(mut self, correlations: CorrelationCache) -> Self {
        self.correlations = Some(correlations);
        self
    }

    /// Set the workload-to-kind mapper.
    pub fn with_workload_mapper(mut self, mapper: Arc<M>) -> Self {
        self.workload_mapper = Some(mapper);
        self
    }

    /// Build the runtime with a `WeightedFairScheduler`.
    ///
    /// This creates a runtime using the standard weighted-fair scheduler
    /// configured with the priority weights from the runtime config.
    pub fn build(
        self,
    ) -> anyhow::Result<OrchestratorRuntime<J, Q, B, WeightedFairScheduler<J::EntityId>, D, M>>
    {
        let queue = self
            .queue
            .ok_or_else(|| anyhow::anyhow!("queue dependency missing"))?;
        let budget = self
            .budget
            .ok_or_else(|| anyhow::anyhow!("budget dependency missing"))?;
        let dispatcher = self
            .dispatcher
            .ok_or_else(|| anyhow::anyhow!("dispatcher dependency missing"))?;
        let events = self
            .events
            .ok_or_else(|| anyhow::anyhow!("events dependency missing"))?;
        let correlations = self.correlations.unwrap_or_default();
        let workload_mapper = self
            .workload_mapper
            .ok_or_else(|| anyhow::anyhow!("workload_mapper dependency missing"))?;

        let scheduler = Arc::new(WeightedFairScheduler::new(
            &self.config.scheduler,
            &self.config.priority_weights,
        ));

        Ok(OrchestratorRuntime::new(
            self.config,
            queue,
            budget,
            scheduler,
            dispatcher,
            events,
            correlations,
            workload_mapper,
        ))
    }
}
