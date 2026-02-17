//! Prometheus metrics instrumentation for orca.
//!
//! This module provides Prometheus metrics for monitoring job orchestration.
//! All metrics are conditionally compiled behind the `metrics` feature flag.
//!
//! # Metrics
//!
//! ## Counters
//! - `orca_jobs_enqueued_total` - Total number of jobs enqueued
//! - `orca_jobs_completed_total` - Total number of jobs completed (success + failure)
//! - `orca_lease_expired_total` - Total number of expired leases
//!
//! ## Gauges
//! - `orca_queue_depth` - Current depth of job queues
//! - `orca_budget_utilization` - Current budget utilization percentage
//!
//! ## Histograms
//! - `orca_job_duration_seconds` - Job execution duration in seconds
#![cfg(feature = "metrics")]

use prometheus::{exponential_buckets, CounterVec, GaugeVec, HistogramVec, Opts, Registry};
use std::sync::LazyLock;

/// Global Prometheus registry for orca metrics.
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

/// Counter for total jobs enqueued.
///
/// Labels:
/// - `entity_scope`: The entity scope identifier
/// - `job_kind`: The job kind/type
pub static JOBS_ENQUEUED_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new("orca_jobs_enqueued_total", "Total number of jobs enqueued");
    CounterVec::new(opts, &["entity_scope", "job_kind"])
        .expect("orca_jobs_enqueued_total metric creation failed")
});

/// Counter for total jobs completed (success, failure, dead-lettered).
///
/// Labels:
/// - `entity_scope`: The entity scope identifier
/// - `job_kind`: The job kind/type
/// - `status`: The completion status (success, retryable, dead_lettered)
pub static JOBS_COMPLETED_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new(
        "orca_jobs_completed_total",
        "Total number of jobs completed",
    );
    CounterVec::new(opts, &["entity_scope", "job_kind", "status"])
        .expect("orca_jobs_completed_total metric creation failed")
});

/// Counter for total expired leases.
///
/// Labels:
/// - `entity_scope`: The entity scope identifier
/// - `job_kind`: The job kind/type
pub static LEASE_EXPIRED_TOTAL: LazyLock<CounterVec> = LazyLock::new(|| {
    let opts = Opts::new("orca_lease_expired_total", "Total number of expired leases");
    CounterVec::new(opts, &["entity_scope", "job_kind"])
        .expect("orca_lease_expired_total metric creation failed")
});

/// Gauge for current queue depth.
///
/// Labels:
/// - `entity_scope`: The entity scope identifier
/// - `job_kind`: The job kind/type
/// - `priority`: The job priority level
pub static QUEUE_DEPTH: LazyLock<GaugeVec> = LazyLock::new(|| {
    let opts = Opts::new("orca_queue_depth", "Current depth of job queues");
    GaugeVec::new(opts, &["entity_scope", "job_kind", "priority"])
        .expect("orca_queue_depth metric creation failed")
});

/// Gauge for budget utilization percentage.
///
/// Labels:
/// - `workload_kind`: The workload kind identifier
/// - `entity_scope`: The entity scope identifier (optional, "all" for global)
pub static BUDGET_UTILIZATION: LazyLock<GaugeVec> = LazyLock::new(|| {
    let opts = Opts::new(
        "orca_budget_utilization",
        "Current budget utilization percentage (0-100)",
    );
    GaugeVec::new(opts, &["workload_kind", "entity_scope"])
        .expect("orca_budget_utilization metric creation failed")
});

/// Histogram for job execution duration in seconds.
///
/// Labels:
/// - `entity_scope`: The entity scope identifier
/// - `job_kind`: The job kind/type
/// - `status`: The completion status (success, retryable, dead_lettered)
pub static JOB_DURATION_SECONDS: LazyLock<HistogramVec> = LazyLock::new(|| {
    let buckets = exponential_buckets(0.001, 2.0, 15).expect("bucket creation failed");
    let opts = prometheus::HistogramOpts::new(
        "orca_job_duration_seconds",
        "Job execution duration in seconds",
    )
    .buckets(buckets);
    HistogramVec::new(opts, &["entity_scope", "job_kind", "status"])
        .expect("orca_job_duration_seconds metric creation failed")
});

/// Initialize all metrics by registering them with the global registry.
///
/// This function is idempotent - calling it multiple times is safe.
/// It is automatically called when metrics are first used.
pub fn init_metrics() -> anyhow::Result<()> {
    let registry = &*REGISTRY;

    for metric in [
        Box::new(JOBS_ENQUEUED_TOTAL.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(JOBS_COMPLETED_TOTAL.clone()),
        Box::new(LEASE_EXPIRED_TOTAL.clone()),
        Box::new(QUEUE_DEPTH.clone()),
        Box::new(BUDGET_UTILIZATION.clone()),
        Box::new(JOB_DURATION_SECONDS.clone()),
    ] {
        if let Err(e) = registry.register(metric) {
            let msg = e.to_string();
            if !msg.contains("Duplicate metrics collector registration attempted") {
                return Err(e.into());
            }
        }
    }

    Ok(())
}

/// Helper to record a job enqueue event.
pub fn record_job_enqueued(entity_scope: &str, job_kind: &str) {
    JOBS_ENQUEUED_TOTAL
        .with_label_values(&[entity_scope, job_kind])
        .inc();
}

/// Helper to record a job completion event.
pub fn record_job_completed(entity_scope: &str, job_kind: &str, status: &str) {
    JOBS_COMPLETED_TOTAL
        .with_label_values(&[entity_scope, job_kind, status])
        .inc();
}

/// Helper to record a lease expiration event.
pub fn record_lease_expired(entity_scope: &str, job_kind: &str) {
    LEASE_EXPIRED_TOTAL
        .with_label_values(&[entity_scope, job_kind])
        .inc();
}

/// Helper to update queue depth gauge.
pub fn set_queue_depth(entity_scope: &str, job_kind: &str, priority: &str, depth: f64) {
    QUEUE_DEPTH
        .with_label_values(&[entity_scope, job_kind, priority])
        .set(depth);
}

/// Helper to update budget utilization gauge.
pub fn set_budget_utilization(workload_kind: &str, entity_scope: &str, utilization: f64) {
    BUDGET_UTILIZATION
        .with_label_values(&[workload_kind, entity_scope])
        .set(utilization);
}

/// Helper to observe job duration.
pub fn observe_job_duration(entity_scope: &str, job_kind: &str, status: &str, duration_secs: f64) {
    JOB_DURATION_SECONDS
        .with_label_values(&[entity_scope, job_kind, status])
        .observe(duration_secs);
}

/// Gather all registered metrics in Prometheus text format.
pub fn gather_metrics() -> anyhow::Result<String> {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = REGISTRY.gather();
    encoder
        .encode_to_string(&metric_families)
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initialization() {
        // Should not panic
        init_metrics().expect("metrics initialization should succeed");
    }

    #[test]
    fn test_record_job_enqueued() {
        record_job_enqueued("test_entity", "test_kind");
        // If we get here without panic, the metric was recorded
    }

    #[test]
    fn test_record_job_completed() {
        record_job_completed("test_entity", "test_kind", "success");
        record_job_completed("test_entity", "test_kind", "retryable");
        record_job_completed("test_entity", "test_kind", "dead_lettered");
    }

    #[test]
    fn test_record_lease_expired() {
        record_lease_expired("test_entity", "test_kind");
    }

    #[test]
    fn test_set_queue_depth() {
        set_queue_depth("test_entity", "test_kind", "P0", 42.0);
    }

    #[test]
    fn test_set_budget_utilization() {
        set_budget_utilization("test_workload", "all", 75.5);
    }

    #[test]
    fn test_observe_job_duration() {
        observe_job_duration("test_entity", "test_kind", "success", 1.5);
    }

    #[test]
    fn test_gather_metrics() {
        init_metrics().expect("metrics initialization should succeed");

        record_job_enqueued("test_entity", "test_kind");
        record_job_completed("test_entity", "test_kind", "success");

        let output = gather_metrics().expect("gather should succeed");
        assert!(output.contains("orca_jobs_enqueued_total"));
        assert!(output.contains("orca_jobs_completed_total"));
    }
}
