//! Orca - Generic job orchestration for ForensicsIQ and Ferrex.
//!
//! A foundational crate providing core traits and types for distributed job queue
//! systems with workload budgeting, event publishing, and lease-based job processing.
//!
//! # Core Concepts
//!
//! - **Job**: The fundamental unit of work. Jobs are user-defined types that implement
//!   the [`Job`] trait, allowing them to be enqueued, leased, and executed.
//!
//! - **Queue**: The [`QueueService`] trait abstracts queue backends, providing operations
//!   for enqueueing, dequeuing, and managing job lifecycle.
//!
//! - **Budget**: The [`Budget`] trait provides backpressure and concurrency limiting
//!   for workload types, preventing resource exhaustion.
//!
//! - **Scheduler**: The [`Scheduler`] trait and [`WeightedFairScheduler`] implementation
//!   provide fair allocation of worker capacity across entities and priorities.
//!
//! - **Events**: The event system (via [`EventPublisher`] and [`InProcEventBus`])
//!   enables reactive workflows and observability.
//!
//! - **Runtime**: The [`OrchestratorRuntime`] ties together all components into a
//!   cohesive job processing system.
//!
//! # Feature Flags
//!
//! - `postgres` - PostgreSQL persistence support via sqlx
//! - `metrics` - Prometheus metrics support
//!
//! # Example
//!
//! ```ignore
//! use orca::*;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! enum MyJob {
//!     ProcessFile { path: String },
//! }
//!
//! impl Job for MyJob {
//!     type EntityId = JobId;
//!     type Kind = MyJobKind;
//!     // ... implement required methods
//! }
//! ```

/// Budget management for workload concurrency limiting.
///
/// The `budget` module provides the [`Budget`] trait for managing concurrent
/// workload execution, along with the [`InMemoryBudget`] implementation that
/// tracks per-workload counts in memory.
pub mod budget;

/// Configuration structures for queue and persistence settings.
///
/// The `config` module defines configuration types like [`PersistenceConfig`]
/// and [`QueueConfig`] for tuning system behavior.
pub mod config;

/// Correlation tracking for job lineage.
///
/// The `correlation` module provides [`CorrelationCache`] for tracking
/// job-to-correlation ID mappings, enabling distributed tracing and
/// request lineage across job orchestration workflows.
pub mod correlation;

/// Event publishing and subscription system.
///
/// The `events` module provides traits and types for job lifecycle events:
/// - [`EventPublisher`] and [`EventSubscriber`] for pub/sub patterns
/// - [`JobEvent`] and [`JobEventPayload`] for event data
/// - [`InProcEventBus`] for in-process event broadcasting
/// - [`DomainEventRouter`] for routing domain events to jobs
pub mod events;

/// Core job definitions and traits.
///
/// The `job` module defines the fundamental job abstractions:
/// - [`Job`] trait - the main trait for job types
/// - [`JobKind`] - marker trait for job type enums
/// - [`EntityId`] - trait for entity identifiers
/// - [`WorkloadKind`] - trait for workload categorization
/// - [`JobPriority`] - priority levels (P0-P3)
/// - [`JobState`] - job lifecycle states
/// - [`JobHandle`] - handle returned from enqueue operations
/// - [`JobId`] - unique job identifier
/// - [`DependencyKey`] - for job dependencies
pub mod job;

/// Lease management for job execution.
///
/// The `lease` module provides lease-based job processing:
/// - [`JobLease`] - represents a leased job
/// - [`LeaseId`] - unique lease identifier
/// - [`DequeueRequest`] - request to dequeue a job
/// - [`LeaseRenewal`] - request to extend a lease
/// - [`QueueSelector`] - for targeted dequeuing
/// - [`LeaseRetryConfig`] - retry and backoff configuration
/// - [`CompletionOutcome`] and [`ResurrectionOutcome`] - lease processing results
pub mod lease;

/// Queue operations and snapshotting.
///
/// The `queue` module defines the [`QueueService`] trait for queue backends
/// and [`LeaseExpiryScanner`] for managing expired leases. Also provides
/// [`QueueSnapshot`] for monitoring queue depth.
pub mod queue;

/// Fair scheduling for job distribution.
///
/// The `scheduler` module provides [`WeightedFairScheduler`] for fair allocation
/// of worker capacity across entities and job priorities. Includes:
/// - [`SchedulerConfig`] - scheduler configuration
/// - [`PriorityWeights`] - priority distribution weights
/// - [`EntityQueuePolicy`] - per-entity scheduling policies
/// - [`SchedulingReservation`] - reservation handle
/// - [`ReadyCountEntry`] - for bulk ready count updates
pub mod scheduler;

#[cfg(feature = "postgres")]
/// PostgreSQL persistence implementation.
///
/// The `persistence` module provides PostgreSQL-backed implementations
/// of the queue service when the `postgres` feature is enabled.
pub mod persistence;

/// Runtime orchestration and worker management.
///
/// The `runtime` module provides the [`OrchestratorRuntime`] for managing
/// worker pools, lease renewal, and job execution lifecycle. Includes:
/// - [`OrchestratorRuntime`] - main runtime type
/// - [`OrchestratorRuntimeConfig`] - runtime configuration
/// - [`ShutdownToken`] - graceful shutdown signaling
/// - [`JobDispatcher`] - trait for job execution
/// - [`WorkerConfig`] - individual worker configuration
/// - Builders for constructing runtimes
pub mod runtime;

pub use budget::*;
pub use config::*;
pub use correlation::*;
pub use events::*;
pub use job::*;
pub use lease::*;
pub use queue::*;
pub use scheduler::*;
