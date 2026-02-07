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
//! - `postgres` - PostgreSQL persistence support via sqlx (requires database setup)
//! - `metrics` - Prometheus metrics support
//!
//! # Quick Start
//!
//! The simplest way to get started is using the in-memory queue for testing:
//!
//! ```ignore
//! use orca::*;
//! use orca_testkit::InMemoryQueueService;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create queue and enqueue a job
//!     let queue = Arc::new(InMemoryQueueService::new());
//!     
//!     // Define your job type (see examples/ directory for full implementations)
//!     // let job = MyJob::ProcessFile { path: "/tmp/file.txt".to_string() };
//!     // let handle = queue.enqueue(job.entity_id(), job, JobPriority::P1).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! # Examples
//!
//! See the `examples/` directory for complete, runnable examples:
//!
//! - `simple_queue.rs` - Basic enqueue/dequeue operations
//! - `runtime_basic.rs` - Manual worker pools with budget management
//! - `postgres_runtime.rs` - Full production runtime with PostgreSQL
//!
//! Run examples with:
//! ```bash
//! # Simple queue (no external dependencies)
//! cargo run --example simple_queue
//!
//! # With PostgreSQL (requires database setup)
//! cargo run --example postgres_runtime --features postgres
//! ```
//!
//! # Architecture
//!
//! Orca follows a **hybrid trait/enum design** for jobs:
//!
//! 1. Consumers define their own job enums with domain-specific variants
//! 2. They implement the [`Job`] trait on their enum
//! 3. This allows compile-time dispatch while maintaining JSONB round-tripping via serde
//!
//! This design provides:
//! - Type-safe job definitions at compile time
//! - Extensibility without modifying orca
//! - Natural enum-based pattern matching in dispatchers
//! - Clean serialization to JSONB for storage
//!
//! # PostgreSQL Schema
//!
//! When using the `postgres` feature, the schema uses modern PostgreSQL features:
//! - **UUID v7** for time-ordered IDs (efficient indexing)
//! - **JSONB** for job payloads (flexible, indexable)
//! - **SKIP LOCKED** for concurrent dequeue (no contention)
//! - **Partial indexes** for queue depth queries (efficient)
//!
//! See `migrations/001_initial_schema.sql` for the full schema definition.

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

#[cfg(feature = "metrics")]
/// Prometheus metrics instrumentation.
///
/// The `metrics` module provides Prometheus metrics for monitoring job
/// orchestration when the `metrics` feature is enabled.
pub mod metrics;

/// Tracing and telemetry instrumentation.
///
/// The `telemetry` module provides helper functions for creating tracing spans
/// and recording metrics during job lifecycle events.
pub mod telemetry;

pub use budget::*;
pub use config::*;
pub use correlation::*;
pub use events::*;
pub use job::*;
pub use lease::*;
pub use queue::*;
pub use runtime::*;
pub use scheduler::*;

// Re-export telemetry types for convenience
#[doc(inline)]
pub use telemetry::{
    budget_span, instrument_dispatch, job_complete_span, job_dequeue_span,
    job_dispatch_span, job_enqueue_span, job_renew_span, observe_job_duration,
    record_job_completed, record_job_end, record_job_enqueued, record_job_start,
    record_lease_expired, set_budget_utilization, set_queue_depth, JobTimingHandle,
};
