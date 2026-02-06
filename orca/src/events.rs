use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::job::{Job, JobId, JobPriority};
use crate::lease::LeaseId;
use crate::queue::QueueService;

/// Metadata envelope attached to every job event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventMeta<E> {
    pub version: u16,
    pub correlation_id: Uuid,
    pub idempotency_key: String,
    pub entity_id: E,
    pub timestamp: DateTime<Utc>,
}

impl<E> EventMeta<E> {
    pub fn new(
        entity_id: E,
        correlation_id: Option<Uuid>,
        idempotency_key: impl Into<String>,
    ) -> Self {
        Self {
            version: 1,
            correlation_id: correlation_id.unwrap_or_else(Uuid::now_v7),
            idempotency_key: idempotency_key.into(),
            entity_id,
            timestamp: Utc::now(),
        }
    }
}

/// Generic job event with metadata and payload.
#[derive(Clone, Debug)]
pub struct JobEvent<J: Job> {
    pub meta: EventMeta<J::EntityId>,
    pub payload: JobEventPayload<J>,
}

/// Event payload emitted for job lifecycle transitions.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum JobEventPayload<J: Job> {
    /// Job was enqueued to the queue.
    Enqueued {
        job_id: JobId,
        kind: J::Kind,
        priority: JobPriority,
    },
    /// Job was merged with an existing job (deduplication).
    Merged {
        existing_job_id: JobId,
        merged_job_id: JobId,
        kind: J::Kind,
        priority: JobPriority,
    },
    /// Job was dequeued and assigned a lease.
    Dequeued {
        job_id: JobId,
        kind: J::Kind,
        priority: JobPriority,
        lease_id: LeaseId,
    },
    /// Job completed successfully.
    Completed {
        job_id: JobId,
        kind: J::Kind,
        priority: JobPriority,
    },
    /// Job failed (may be retryable).
    Failed {
        job_id: JobId,
        kind: J::Kind,
        priority: JobPriority,
        retryable: bool,
    },
    /// Job was moved to dead letter queue after exhausting retries.
    DeadLettered {
        job_id: JobId,
        kind: J::Kind,
        priority: JobPriority,
    },
    /// Job lease was renewed.
    LeaseRenewed {
        job_id: JobId,
        lease_id: LeaseId,
        renewals: u32,
    },
    /// Job lease expired without renewal.
    LeaseExpired { job_id: JobId, lease_id: LeaseId },
}

/// Generic event publisher trait for publishing events of type `E`.
#[async_trait]
pub trait EventPublisher<E>: Send + Sync
where
    E: Clone + Send + Sync + 'static,
{
    /// Publish an event to all subscribers.
    ///
    /// Returns an error if the event cannot be published.
    async fn publish(&self, event: E) -> anyhow::Result<()>;
}

/// Generic event subscriber trait for receiving events of type `E`.
pub trait EventSubscriber<E>: Send + Sync
where
    E: Clone + Send + Sync + 'static,
{
    /// Subscribe to events, returning a broadcast receiver.
    ///
    /// Multiple subscribers can receive the same events (fan-out).
    fn subscribe(&self) -> broadcast::Receiver<E>;
}

/// Trait for routing domain-specific events to jobs.
///
/// This trait allows consumers to define their own domain events and
/// route them to the job queue system. Implementors can subscribe to
/// domain events (e.g., file discovery, user actions) and enqueue
/// appropriate jobs in response.
///
/// # Example
///
/// ```ignore
/// #[derive(Clone)]
/// enum MyDomainEvent {
///     FileDiscovered { path: String },
/// }
///
/// struct MyEventRouter;
///
/// #[async_trait]
/// impl DomainEventRouter<MyJob> for MyEventRouter {
///     type DomainEvent = MyDomainEvent;
///
///     async fn route(
///         &self,
///         event: Self::DomainEvent,
///         queue: &dyn QueueService<MyJob>,
///     ) -> anyhow::Result<()> {
///         match event {
///             MyDomainEvent::FileDiscovered { path } => {
///                 let job = MyJob::ProcessFile { path };
///                 queue.enqueue(job.entity_id(), job, job.priority()).await?;
///             }
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait DomainEventRouter<J: Job>: Send + Sync {
    /// The type of domain events this router handles.
    type DomainEvent: Clone + Send + Sync + 'static;

    /// Route a domain event to the appropriate job(s).
    ///
    /// This method is called when a domain event occurs. The implementation
    /// should decide which jobs (if any) to enqueue based on the event.
    ///
    /// # Arguments
    ///
    /// * `event` - The domain event to route
    /// * `queue` - The queue service for enqueuing jobs
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if routing succeeded, or an error if routing failed.
    async fn route(
        &self,
        event: Self::DomainEvent,
        queue: &dyn QueueService<J>,
    ) -> anyhow::Result<()>;
}

/// In-process event bus using tokio broadcast channels.
///
/// `InProcEventBus` provides a lightweight, fan-out event bus that supports
/// multiple subscribers for both job lifecycle events and consumer-defined
/// domain events. Events are broadcast to all active subscribers; if a
/// subscriber lags behind, it will receive `RecvError::Lagged` but won't
/// block the publisher.
///
/// # Characteristics
///
/// - Non-blocking publish: Publishers never wait for slow subscribers
/// - Fan-out: All subscribers receive all events (within capacity)
/// - Bounded: Events are dropped if capacity is exceeded (Lagged)
/// - In-process: Events don't leave the process (for external pub/sub,
///   implement a bridge using the `EventPublisher` trait)
///
/// # Example
///
/// ```ignore
/// use orca::{InProcEventBus, EventSubscriber};
///
/// // Create event bus with capacity for 1000 events
/// let event_bus = InProcEventBus::<MyJob>::new(1000);
///
/// // Subscribe multiple consumers
/// let mut rx1 = event_bus.subscribe_jobs();
/// let mut rx2 = event_bus.subscribe_jobs();
///
/// // Events will be broadcast to both receivers
/// ```
pub struct InProcEventBus<J: Job> {
    job_sender: broadcast::Sender<JobEvent<J>>,
    domain_sender: broadcast::Sender<J::DomainEvent>,
    job_capacity: usize,
    domain_capacity: usize,
}

impl<J: Job> std::fmt::Debug for InProcEventBus<J> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcEventBus")
            .field("job_capacity", &self.job_capacity)
            .field("job_subscribers", &self.job_sender.receiver_count())
            .field("domain_capacity", &self.domain_capacity)
            .field("domain_subscribers", &self.domain_sender.receiver_count())
            .finish()
    }
}

impl<J: Job> InProcEventBus<J> {
    /// Create a new event bus with the specified capacity for both channels.
    ///
    /// # Arguments
    ///
    /// * `capacity` - The maximum number of events to buffer for each channel.
    ///   When the buffer is full, old events are dropped and subscribers
    ///   will receive `RecvError::Lagged`.
    ///
    /// # Type Parameters
    ///
    /// * `J` - The job type this event bus handles events for. The job type
    ///   must implement the `Job` trait with an associated `DomainEvent` type.
    pub fn new(capacity: usize) -> Self {
        let (job_sender, _) = broadcast::channel(capacity);
        let (domain_sender, _) = broadcast::channel(capacity);
        Self {
            job_sender,
            domain_sender,
            job_capacity: capacity,
            domain_capacity: capacity,
        }
    }

    /// Create a new event bus with separate capacities for job and domain channels.
    ///
    /// # Arguments
    ///
    /// * `job_capacity` - The maximum number of job lifecycle events to buffer.
    /// * `domain_capacity` - The maximum number of domain events to buffer.
    pub fn with_capacities(
        job_capacity: usize,
        domain_capacity: usize,
    ) -> Self {
        let (job_sender, _) = broadcast::channel(job_capacity);
        let (domain_sender, _) = broadcast::channel(domain_capacity);
        Self {
            job_sender,
            domain_sender,
            job_capacity,
            domain_capacity,
        }
    }

    /// Get the number of active job event subscribers.
    pub fn job_subscriber_count(&self) -> usize {
        self.job_sender.receiver_count()
    }

    /// Get the number of active domain event subscribers.
    pub fn domain_subscriber_count(&self) -> usize {
        self.domain_sender.receiver_count()
    }

    /// Get the configured capacity for job event channel.
    pub fn job_capacity(&self) -> usize {
        self.job_capacity
    }

    /// Get the configured capacity for domain event channel.
    pub fn domain_capacity(&self) -> usize {
        self.domain_capacity
    }

    /// Publish a job event to all subscribers.
    ///
    /// This method is non-blocking. If no subscribers exist, the event
    /// is silently dropped.
    pub fn publish_job(&self, event: JobEvent<J>) -> anyhow::Result<()> {
        let _ = self.job_sender.send(event);
        Ok(())
    }

    /// Publish a domain event to all subscribers.
    ///
    /// This method is non-blocking. If no subscribers exist, the event
    /// is silently dropped.
    pub fn publish_domain(&self, event: J::DomainEvent) -> anyhow::Result<()> {
        let _ = self.domain_sender.send(event);
        Ok(())
    }

    /// Subscribe to job lifecycle events.
    ///
    /// Returns a broadcast receiver that will receive all job events
    /// published after subscription. The receiver will get a cloned
    /// copy of each event, allowing independent consumption.
    pub fn subscribe_job_events(&self) -> broadcast::Receiver<JobEvent<J>> {
        self.job_sender.subscribe()
    }

    /// Subscribe to domain events.
    ///
    /// Returns a broadcast receiver for the job type's associated
    /// domain events. Multiple subscribers receive cloned copies.
    pub fn subscribe_domain_events(
        &self,
    ) -> broadcast::Receiver<J::DomainEvent> {
        self.domain_sender.subscribe()
    }
}

/// Trait for subscribing to job lifecycle events.
pub trait JobEventStream<J: Job>: Send + Sync {
    fn subscribe_jobs(&self) -> broadcast::Receiver<JobEvent<J>>;
}

impl<J: Job> JobEventStream<J> for InProcEventBus<J> {
    fn subscribe_jobs(&self) -> broadcast::Receiver<JobEvent<J>> {
        self.subscribe_job_events()
    }
}

/// Trait for subscribing to domain events.
pub trait DomainEventStream<J: Job>: Send + Sync {
    fn subscribe_domain(&self) -> broadcast::Receiver<J::DomainEvent>;
}

impl<J: Job> DomainEventStream<J> for InProcEventBus<J> {
    fn subscribe_domain(&self) -> broadcast::Receiver<J::DomainEvent> {
        self.subscribe_domain_events()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::time::Duration;
    use tokio::time::timeout;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestDomainEvent {
        data: String,
    }

    #[derive(
        Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize,
    )]
    enum TestJobKind {
        Test,
    }

    impl crate::JobKind for TestJobKind {
        fn as_str(&self) -> &'static str {
            "test"
        }
    }

    impl std::fmt::Display for TestJobKind {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "test")
        }
    }

    #[derive(
        Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize,
    )]
    struct TestEntityId(Uuid);

    impl crate::EntityId for TestEntityId {
        fn as_uuid(&self) -> Uuid {
            self.0
        }
    }

    impl std::fmt::Display for TestEntityId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestJob {
        id: TestEntityId,
        kind: TestJobKind,
        priority: JobPriority,
    }

    impl Job for TestJob {
        type EntityId = TestEntityId;
        type Kind = TestJobKind;
        type DomainEvent = TestDomainEvent;

        fn entity_id(&self) -> Self::EntityId {
            self.id.clone()
        }

        fn kind(&self) -> Self::Kind {
            self.kind
        }

        fn dedupe_key(&self) -> String {
            format!("test-{}", self.id.0)
        }

        fn priority(&self) -> JobPriority {
            self.priority
        }
    }

    #[tokio::test]
    async fn test_event_bus_broadcast_to_multiple_subscribers() {
        let bus = InProcEventBus::<TestJob>::new(100);

        // Subscribe 3 receivers
        let mut rx1 = bus.subscribe_jobs();
        let mut rx2 = bus.subscribe_jobs();
        let mut rx3 = bus.subscribe_jobs();

        // Publish 5 events
        for i in 0..5 {
            let event = JobEvent {
                meta: EventMeta::new(
                    TestEntityId(Uuid::now_v7()),
                    None,
                    format!("test-{}", i),
                ),
                payload: JobEventPayload::Enqueued {
                    job_id: JobId::new(),
                    kind: TestJobKind::Test,
                    priority: JobPriority::P0,
                },
            };
            bus.publish_job(event).unwrap();
        }

        // All 3 receivers should get all 5 events
        for _ in 0..5 {
            assert!(
                timeout(Duration::from_millis(100), rx1.recv())
                    .await
                    .is_ok()
            );
            assert!(
                timeout(Duration::from_millis(100), rx2.recv())
                    .await
                    .is_ok()
            );
            assert!(
                timeout(Duration::from_millis(100), rx3.recv())
                    .await
                    .is_ok()
            );
        }
    }

    #[tokio::test]
    async fn test_lagged_subscriber_doesnt_block_publisher() {
        let bus = InProcEventBus::<TestJob>::new(2); // Small capacity

        let mut rx = bus.subscribe_jobs();

        // Publish 5 events without reading
        for i in 0..5 {
            let event = JobEvent {
                meta: EventMeta::new(
                    TestEntityId(Uuid::now_v7()),
                    None,
                    format!("test-{}", i),
                ),
                payload: JobEventPayload::Enqueued {
                    job_id: JobId::new(),
                    kind: TestJobKind::Test,
                    priority: JobPriority::P0,
                },
            };
            // Should not block even with full buffer
            bus.publish_job(event).unwrap();
        }

        // Receiver should get Lagged error
        let result = timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(result.is_ok());

        match result.unwrap() {
            Err(broadcast::error::RecvError::Lagged(_)) => {
                // Expected - subscriber lagged behind
            }
            Ok(_) => {
                // Also acceptable - got an event
            }
            Err(broadcast::error::RecvError::Closed) => {
                panic!("Channel should not be closed");
            }
        }
    }

    #[tokio::test]
    async fn test_domain_event_broadcast() {
        let bus = InProcEventBus::<TestJob>::new(100);

        let mut rx1 = bus.subscribe_domain();
        let mut rx2 = bus.subscribe_domain();

        // Publish domain events
        for i in 0..3 {
            let event = TestDomainEvent {
                data: format!("event-{}", i),
            };
            bus.publish_domain(event).unwrap();
        }

        // Both subscribers should get all events
        for i in 0..3 {
            let evt1 = timeout(Duration::from_millis(100), rx1.recv())
                .await
                .unwrap()
                .unwrap();
            let evt2 = timeout(Duration::from_millis(100), rx2.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(evt1.data, format!("event-{}", i));
            assert_eq!(evt2.data, format!("event-{}", i));
        }
    }

    #[tokio::test]
    async fn test_event_bus_debug_format() {
        let bus = InProcEventBus::<TestJob>::new(100);

        let _rx1 = bus.subscribe_jobs();
        let _rx2 = bus.subscribe_jobs();
        let _rx3 = bus.subscribe_domain();

        let debug_str = format!("{:?}", bus);
        assert!(debug_str.contains("InProcEventBus"));
        assert!(debug_str.contains("job_subscribers: 2"));
        assert!(debug_str.contains("domain_subscribers: 1"));
        assert!(debug_str.contains("job_capacity: 100"));
        assert!(debug_str.contains("domain_capacity: 100"));
    }

    #[tokio::test]
    async fn test_event_meta_creation() {
        let entity_id = TestEntityId(Uuid::now_v7());
        let correlation_id = Uuid::now_v7();
        let meta =
            EventMeta::new(entity_id.clone(), Some(correlation_id), "test-key");

        assert_eq!(meta.version, 1);
        assert_eq!(meta.correlation_id, correlation_id);
        assert_eq!(meta.idempotency_key, "test-key");
        assert_eq!(meta.entity_id.0, entity_id.0);
        assert!(
            meta.timestamp <= Utc::now(),
            "Timestamp should be in the past or present"
        );
    }

    #[tokio::test]
    async fn test_event_bus_with_capacities() {
        let bus = InProcEventBus::<TestJob>::with_capacities(50, 75);

        assert_eq!(bus.job_capacity(), 50);
        assert_eq!(bus.domain_capacity(), 75);

        let _job_rx = bus.subscribe_jobs();
        let _domain_rx = bus.subscribe_domain();

        assert_eq!(bus.job_subscriber_count(), 1);
        assert_eq!(bus.domain_subscriber_count(), 1);
    }

    #[tokio::test]
    async fn test_job_event_payload_variants() {
        let job_id = JobId::new();
        let lease_id = LeaseId::new();

        // Test all payload variants implement Clone + Debug
        let payloads = vec![
            JobEventPayload::<TestJob>::Enqueued {
                job_id,
                kind: TestJobKind::Test,
                priority: JobPriority::P0,
            },
            JobEventPayload::<TestJob>::Merged {
                existing_job_id: job_id,
                merged_job_id: JobId::new(),
                kind: TestJobKind::Test,
                priority: JobPriority::P1,
            },
            JobEventPayload::<TestJob>::Dequeued {
                job_id,
                kind: TestJobKind::Test,
                priority: JobPriority::P0,
                lease_id,
            },
            JobEventPayload::<TestJob>::Completed {
                job_id,
                kind: TestJobKind::Test,
                priority: JobPriority::P0,
            },
            JobEventPayload::<TestJob>::Failed {
                job_id,
                kind: TestJobKind::Test,
                priority: JobPriority::P0,
                retryable: true,
            },
            JobEventPayload::<TestJob>::DeadLettered {
                job_id,
                kind: TestJobKind::Test,
                priority: JobPriority::P0,
            },
            JobEventPayload::<TestJob>::LeaseRenewed {
                job_id,
                lease_id,
                renewals: 3,
            },
            JobEventPayload::<TestJob>::LeaseExpired { job_id, lease_id },
        ];

        // Verify Clone + Debug
        for payload in payloads {
            let _cloned = payload.clone();
            let _debug = format!("{:?}", payload);
        }
    }
}
