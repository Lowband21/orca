//! Tracing and telemetry instrumentation for orca.
//!
//! This module provides helper functions for creating tracing spans and recording
//! metrics during job lifecycle events. All functions work both with and without
//! the `metrics` feature flag.
//!
//! # Features
//!
//! - Tracing spans for job lifecycle: enqueue, dequeue, dispatch, complete, fail
//! - Integration with the `metrics` module for Prometheus metrics
//! - Helper functions that are no-ops when features are disabled
//!
//! # Example
//!
//! ```ignore
//! use orca::telemetry::{job_dispatch_span, record_job_enqueued};
//!
//! let span = job_dispatch_span(job_id, job_kind);
//! let _enter = span.enter();
//! // ... job execution
//! record_job_enqueued(entity_id, job_kind);
//! ```

use std::future::Future;
use tracing::{info_span, Instrument, Span};

/// Create a tracing span for job dispatch operations.
///
/// The span includes the job_id and job_kind as fields for observability.
///
/// # Arguments
/// * `job_id` - The unique job identifier
/// * `job_kind` - The job kind/type
///
/// # Example
/// ```ignore
/// let span = job_dispatch_span("job-123", "process_file");
/// let _enter = span.enter();
/// // Job execution code here
/// ```
#[must_use]
pub fn job_dispatch_span(job_id: impl AsRef<str>, kind: impl AsRef<str>) -> Span {
    info_span!(
        "orca.dispatch",
        job_id = %job_id.as_ref(),
        job_kind = %kind.as_ref(),
    )
}

/// Create a tracing span for job dequeue operations.
///
/// The span includes the worker_id, entity_id, and priority as fields.
///
/// # Arguments
/// * `worker_id` - The worker identifier
/// * `entity_id` - The entity scope identifier
/// * `priority` - The job priority
#[must_use]
pub fn job_dequeue_span(
    worker_id: impl AsRef<str>,
    entity_id: impl AsRef<str>,
    priority: impl AsRef<str>,
) -> Span {
    info_span!(
        "orca.dequeue",
        worker_id = %worker_id.as_ref(),
        entity_id = %entity_id.as_ref(),
        priority = %priority.as_ref(),
    )
}

/// Create a tracing span for job enqueue operations.
///
/// The span includes the entity_id, job_kind, and priority as fields.
///
/// # Arguments
/// * `entity_id` - The entity scope identifier
/// * `job_kind` - The job kind/type
/// * `priority` - The job priority
#[must_use]
pub fn job_enqueue_span(
    entity_id: impl AsRef<str>,
    job_kind: impl AsRef<str>,
    priority: impl AsRef<str>,
) -> Span {
    info_span!(
        "orca.enqueue",
        entity_id = %entity_id.as_ref(),
        job_kind = %job_kind.as_ref(),
        priority = %priority.as_ref(),
    )
}

/// Create a tracing span for job completion operations.
///
/// The span includes the job_id, job_kind, and status as fields.
///
/// # Arguments
/// * `job_id` - The unique job identifier
/// * `job_kind` - The job kind/type
/// * `status` - The completion status (success, retryable, dead_lettered)
#[must_use]
pub fn job_complete_span(
    job_id: impl AsRef<str>,
    job_kind: impl AsRef<str>,
    status: impl AsRef<str>,
) -> Span {
    info_span!(
        "orca.complete",
        job_id = %job_id.as_ref(),
        job_kind = %job_kind.as_ref(),
        status = %status.as_ref(),
    )
}

/// Create a tracing span for lease renewal operations.
///
/// The span includes the lease_id and job_id as fields.
///
/// # Arguments
/// * `lease_id` - The lease identifier
/// * `job_id` - The job identifier
#[must_use]
pub fn job_renew_span(lease_id: impl AsRef<str>, job_id: impl AsRef<str>) -> Span {
    info_span!(
        "orca.renew",
        lease_id = %lease_id.as_ref(),
        job_id = %job_id.as_ref(),
    )
}

/// Create a tracing span for budget operations.
///
/// The span includes the workload_kind and entity_id as fields.
///
/// # Arguments
/// * `workload_kind` - The workload kind
/// * `entity_id` - The entity scope identifier
#[must_use]
pub fn budget_span(workload_kind: impl AsRef<str>, entity_id: impl AsRef<str>) -> Span {
    info_span!(
        "orca.budget",
        workload_kind = %workload_kind.as_ref(),
        entity_id = %entity_id.as_ref(),
    )
}

/// Instrument a future with a job dispatch span.
///
/// This is a convenience wrapper that attaches a dispatch span to any future.
///
/// # Type Parameters
/// * `F` - The future type
///
/// # Arguments
/// * `job_id` - The unique job identifier
/// * `job_kind` - The job kind/type
/// * `future` - The future to instrument
pub fn instrument_dispatch<F>(
    job_id: impl AsRef<str>,
    job_kind: impl AsRef<str>,
    future: F,
) -> impl Future<Output = F::Output>
where
    F: Future,
{
    let span = job_dispatch_span(job_id, job_kind);
    future.instrument(span)
}

/// Record a job enqueue event in metrics.
///
/// This function records the event both in tracing logs and in Prometheus
/// metrics (when the `metrics` feature is enabled).
///
/// # Arguments
/// * `entity_id` - The entity scope identifier
/// * `job_kind` - The job kind/type
pub fn record_job_enqueued(entity_id: impl AsRef<str>, job_kind: impl AsRef<str>) {
    tracing::info!(
        entity_id = %entity_id.as_ref(),
        job_kind = %job_kind.as_ref(),
        "job enqueued"
    );

    #[cfg(feature = "metrics")]
    crate::metrics::record_job_enqueued(entity_id.as_ref(), job_kind.as_ref());
}

/// Record a job completion event in metrics.
///
/// # Arguments
/// * `entity_id` - The entity scope identifier
/// * `job_kind` - The job kind/type
/// * `status` - The completion status (success, retryable, dead_lettered)
pub fn record_job_completed(
    entity_id: impl AsRef<str>,
    job_kind: impl AsRef<str>,
    status: impl AsRef<str>,
) {
    tracing::info!(
        entity_id = %entity_id.as_ref(),
        job_kind = %job_kind.as_ref(),
        status = %status.as_ref(),
        "job completed"
    );

    #[cfg(feature = "metrics")]
    crate::metrics::record_job_completed(entity_id.as_ref(), job_kind.as_ref(), status.as_ref());
}

/// Record a lease expiration event in metrics.
///
/// # Arguments
/// * `entity_id` - The entity scope identifier
/// * `job_kind` - The job kind/type
pub fn record_lease_expired(entity_id: impl AsRef<str>, job_kind: impl AsRef<str>) {
    tracing::warn!(
        entity_id = %entity_id.as_ref(),
        job_kind = %job_kind.as_ref(),
        "lease expired"
    );

    #[cfg(feature = "metrics")]
    crate::metrics::record_lease_expired(entity_id.as_ref(), job_kind.as_ref());
}

/// Update the queue depth metric.
///
/// # Arguments
/// * `entity_id` - The entity scope identifier
/// * `job_kind` - The job kind/type
/// * `priority` - The job priority level
/// * `depth` - The current queue depth
pub fn set_queue_depth(
    entity_id: impl AsRef<str>,
    job_kind: impl AsRef<str>,
    priority: impl AsRef<str>,
    depth: usize,
) {
    tracing::debug!(
        entity_id = %entity_id.as_ref(),
        job_kind = %job_kind.as_ref(),
        priority = %priority.as_ref(),
        depth = depth,
        "queue depth updated"
    );

    #[cfg(feature = "metrics")]
    crate::metrics::set_queue_depth(
        entity_id.as_ref(),
        job_kind.as_ref(),
        priority.as_ref(),
        depth as f64,
    );
}

/// Update the budget utilization metric.
///
/// # Arguments
/// * `workload_kind` - The workload kind identifier
/// * `entity_id` - The entity scope identifier
/// * `utilization` - The utilization percentage (0-100)
pub fn set_budget_utilization(
    workload_kind: impl AsRef<str>,
    entity_id: impl AsRef<str>,
    utilization: f64,
) {
    tracing::debug!(
        workload_kind = %workload_kind.as_ref(),
        entity_id = %entity_id.as_ref(),
        utilization = utilization,
        "budget utilization updated"
    );

    #[cfg(feature = "metrics")]
    crate::metrics::set_budget_utilization(workload_kind.as_ref(), entity_id.as_ref(), utilization);
}

/// Observe the duration of a job execution.
///
/// # Arguments
/// * `entity_id` - The entity scope identifier
/// * `job_kind` - The job kind/type
/// * `status` - The completion status
/// * `duration_secs` - The duration in seconds
pub fn observe_job_duration(
    entity_id: impl AsRef<str>,
    job_kind: impl AsRef<str>,
    status: impl AsRef<str>,
    duration_secs: f64,
) {
    tracing::info!(
        entity_id = %entity_id.as_ref(),
        job_kind = %job_kind.as_ref(),
        status = %status.as_ref(),
        duration_secs = duration_secs,
        "job duration observed"
    );

    #[cfg(feature = "metrics")]
    crate::metrics::observe_job_duration(
        entity_id.as_ref(),
        job_kind.as_ref(),
        status.as_ref(),
        duration_secs,
    );
}

/// Record the start of job execution for duration tracking.
///
/// Returns an opaque handle that should be passed to `record_job_end`.
///
/// # Arguments
/// * `job_id` - The unique job identifier
///
/// # Returns
/// An opaque handle for tracking the job duration
pub fn record_job_start(job_id: impl AsRef<str>) -> JobTimingHandle {
    JobTimingHandle {
        job_id: job_id.as_ref().to_string(),
        start: std::time::Instant::now(),
    }
}

/// Record the end of job execution and update duration metrics.
///
/// # Arguments
/// * `handle` - The timing handle from `record_job_start`
/// * `entity_id` - The entity scope identifier
/// * `job_kind` - The job kind/type
/// * `status` - The completion status
pub fn record_job_end(
    handle: JobTimingHandle,
    entity_id: impl AsRef<str>,
    job_kind: impl AsRef<str>,
    status: impl AsRef<str>,
) {
    let duration = handle.start.elapsed();
    let duration_secs = duration.as_secs_f64();

    observe_job_duration(entity_id, job_kind, status, duration_secs);
}

/// Handle for tracking job execution duration.
///
/// This is an opaque type returned by `record_job_start` and consumed by `record_job_end`.
#[derive(Debug)]
pub struct JobTimingHandle {
    job_id: String,
    start: std::time::Instant,
}

impl JobTimingHandle {
    /// Get the job ID associated with this timing handle.
    #[must_use]
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Get the elapsed time since the job started.
    #[must_use]
    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_dispatch_span() {
        let span = job_dispatch_span("job-123", "process_file");
        assert_eq!(span.metadata().unwrap().name(), "orca.dispatch");
    }

    #[test]
    fn test_job_dequeue_span() {
        let span = job_dequeue_span("worker-1", "entity-1", "P0");
        assert_eq!(span.metadata().unwrap().name(), "orca.dequeue");
    }

    #[test]
    fn test_job_enqueue_span() {
        let span = job_enqueue_span("entity-1", "process", "P1");
        assert_eq!(span.metadata().unwrap().name(), "orca.enqueue");
    }

    #[test]
    fn test_job_complete_span() {
        let span = job_complete_span("job-123", "process", "success");
        assert_eq!(span.metadata().unwrap().name(), "orca.complete");
    }

    #[test]
    fn test_job_renew_span() {
        let span = job_renew_span("lease-1", "job-123");
        assert_eq!(span.metadata().unwrap().name(), "orca.renew");
    }

    #[test]
    fn test_budget_span() {
        let span = budget_span("cpu_intensive", "entity-1");
        assert_eq!(span.metadata().unwrap().name(), "orca.budget");
    }

    #[test]
    fn test_timing_handle() {
        let handle = record_job_start("job-123");
        assert_eq!(handle.job_id(), "job-123");

        // Sleep a tiny bit to ensure duration > 0
        std::thread::sleep(std::time::Duration::from_millis(1));
        let elapsed = handle.elapsed();
        assert!(elapsed.as_nanos() > 0);

        // record_job_end should not panic
        record_job_end(handle, "entity-1", "process", "success");
    }
}
