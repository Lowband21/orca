use std::fmt::Display;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{JobId, JobPriority};

/// Unique identifier for a job lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct LeaseId(pub Uuid);

impl Default for LeaseId {
    fn default() -> Self {
        Self::new()
    }
}

impl LeaseId {
    /// Create a new lease ID using UUID v7.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Display for LeaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Represents a leased job with metadata.
#[derive(Clone, Debug)]
pub struct JobLease<J> {
    /// ID of the leased job.
    pub job_id: JobId,
    /// Unique identifier for this lease.
    pub lease_id: LeaseId,
    /// The job payload.
    pub job: J,
    /// ID of the worker holding this lease.
    pub worker_id: String,
    /// Timestamp when the lease expires.
    pub expires_at: DateTime<Utc>,
    /// Number of times this lease has been renewed.
    pub renewals: u32,
}

impl<J> JobLease<J> {
    /// Creates a new job lease.
    pub fn new(
        job_id: JobId,
        job: J,
        worker_id: String,
        lease_ttl: Duration,
    ) -> Self {
        Self {
            job_id,
            lease_id: LeaseId::new(),
            job,
            worker_id,
            expires_at: Utc::now() + lease_ttl,
            renewals: 0,
        }
    }

    /// Checks if the lease has expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at < Utc::now()
    }

    /// Renews the lease with the given duration.
    pub fn renew(&mut self, extend_by: Duration) {
        self.expires_at = self.expires_at + extend_by;
        self.renewals += 1;
    }
}

impl<J: Serialize> Serialize for JobLease<J> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("JobLease", 6)?;
        state.serialize_field("job_id", &self.job_id)?;
        state.serialize_field("lease_id", &self.lease_id)?;
        state.serialize_field("job", &self.job)?;
        state.serialize_field("worker_id", &self.worker_id)?;
        state.serialize_field("expires_at", &self.expires_at)?;
        state.serialize_field("renewals", &self.renewals)?;
        state.end()
    }
}

impl<'de, J: Deserialize<'de>> Deserialize<'de> for JobLease<J> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct JobLeaseFields<J> {
            job_id: JobId,
            lease_id: LeaseId,
            job: J,
            worker_id: String,
            expires_at: DateTime<Utc>,
            renewals: u32,
        }
        let helper = JobLeaseFields::deserialize(deserializer)?;
        Ok(JobLease {
            job_id: helper.job_id,
            lease_id: helper.lease_id,
            job: helper.job,
            worker_id: helper.worker_id,
            expires_at: helper.expires_at,
            renewals: helper.renewals,
        })
    }
}

/// Request to extend a lease duration.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LeaseRenewal {
    /// ID of the lease to renew.
    pub lease_id: LeaseId,
    /// ID of the worker requesting the renewal.
    pub worker_id: String,
    /// Duration to extend the lease by.
    pub extend_by: Duration,
}

/// Request to dequeue a job from the queue.
#[derive(Clone, Debug)]
pub struct DequeueRequest<K, E = ()> {
    /// Kind of job to dequeue.
    pub kind: K,
    /// ID of the worker requesting the dequeue.
    pub worker_id: String,
    /// Time-to-live for the lease if a job is dequeued.
    pub lease_ttl: Duration,
    /// Optional selector for targeted dequeuing.
    pub selector: Option<QueueSelector<E>>,
}

impl<K: Serialize, E: Serialize> Serialize for DequeueRequest<K, E> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("DequeueRequest", 4)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("worker_id", &self.worker_id)?;
        state.serialize_field("lease_ttl", &self.lease_ttl)?;
        state.serialize_field("selector", &self.selector)?;
        state.end()
    }
}

impl<'de, K: Deserialize<'de>, E: Deserialize<'de>> Deserialize<'de>
    for DequeueRequest<K, E>
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct DequeueRequestFields<K, E> {
            kind: K,
            worker_id: String,
            lease_ttl: Duration,
            selector: Option<QueueSelector<E>>,
        }
        let helper = DequeueRequestFields::deserialize(deserializer)?;
        Ok(DequeueRequest {
            kind: helper.kind,
            worker_id: helper.worker_id,
            lease_ttl: helper.lease_ttl,
            selector: helper.selector,
        })
    }
}

/// Selector for queue filtering by entity and priority.
#[derive(Clone, Copy, Debug)]
pub struct QueueSelector<E> {
    /// Entity ID to filter by.
    pub entity_id: E,
    /// Priority level to filter by.
    pub priority: JobPriority,
}

impl<E: Serialize> Serialize for QueueSelector<E> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("QueueSelector", 2)?;
        state.serialize_field("entity_id", &self.entity_id)?;
        state.serialize_field("priority", &self.priority)?;
        state.end()
    }
}

impl<'de, E: Deserialize<'de>> Deserialize<'de> for QueueSelector<E> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct QueueSelectorFields<E> {
            entity_id: E,
            priority: JobPriority,
        }
        let helper = QueueSelectorFields::deserialize(deserializer)?;
        Ok(QueueSelector {
            entity_id: helper.entity_id,
            priority: helper.priority,
        })
    }
}

/// Result of job completion processing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CompletionOutcome {
    /// Job completed successfully.
    Completed,
    /// Job failed and should be retried.
    Retry {
        /// Whether this retry is retryable or permanent.
        retryable: bool,
        /// Optional error message.
        error: Option<String>,
    },
    /// Job failed permanently and should be dead-lettered.
    DeadLetter {
        /// Optional error message.
        error: Option<String>,
    },
}

/// Configuration for lease retry and backoff behavior.
#[derive(Clone, Debug)]
pub struct LeaseRetryConfig {
    /// Maximum number of retry attempts before dead-lettering.
    pub max_attempts: u16,
    /// Base delay in milliseconds for exponential backoff.
    pub base_delay_ms: u64,
    /// Maximum backoff delay in milliseconds.
    pub max_backoff_ms: u64,
    /// Ratio of jitter to add to delays (0.0 - 1.0).
    pub jitter_ratio: f32,
    /// Minimum jitter in milliseconds.
    pub jitter_min_ms: u64,
    /// Number of attempts that use fast retry factor.
    pub fast_retry_attempts: u16,
    /// Multiplier for fast retries (< 1.0 for faster retries).
    pub fast_retry_factor: f32,
    /// Threshold of attempts before considering entity as heavy.
    pub heavy_entity_attempt_threshold: u16,
    /// Slowdown factor for entities with many attempts.
    pub heavy_entity_slowdown_factor: f32,
}

impl Default for LeaseRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 1000,
            max_backoff_ms: 300000, // 5 minutes
            jitter_ratio: 0.25,
            jitter_min_ms: 500,
            fast_retry_attempts: 2,
            fast_retry_factor: 0.1,
            heavy_entity_attempt_threshold: 10,
            heavy_entity_slowdown_factor: 2.0,
        }
    }
}

/// Computes the resurrection delay for an expired lease using exponential backoff.
///
/// Formula: delay = min(base_delay * 2^attempts, max_backoff)
///
/// # Arguments
/// * `attempts` - The number of previous attempts made on this job
/// * `config` - The lease retry configuration
pub fn compute_resurrection_delay(
    attempts: u16,
    config: &LeaseRetryConfig,
) -> Duration {
    if attempts == 0 {
        return Duration::milliseconds(0);
    }

    // Exponential backoff: base * 2^(attempts-1)
    let exp = (attempts.saturating_sub(1)) as i32;
    let scaled = (config.base_delay_ms as f64) * 2f64.powi(exp);
    let capped = scaled.min(config.max_backoff_ms as f64);
    let clamped_ms = capped.max(0.0) as i64;

    Duration::milliseconds(clamped_ms)
}

/// Determines if a job should be dead-lettered based on attempt count.
///
/// Returns true when attempts >= max_attempts.
pub fn should_dead_letter(attempts: u16, max_attempts: u16) -> bool {
    attempts >= max_attempts
}

/// Result of processing an expired lease during scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResurrectionOutcome {
    /// Job resurrected with delayed availability.
    Resurrected {
        /// Delay in milliseconds before the job becomes available.
        delay_ms: u64,
    },
    /// Job moved to dead-letter queue.
    DeadLettered,
}

/// Processes an expired lease and determines the resurrection outcome.
///
/// This function implements the core logic for lease expiry scanning:
/// - If attempts < max_attempts: Compute backoff and resurrect
/// - If attempts >= max_attempts: Move to dead-letter
///
/// # Arguments
/// * `current_attempts` - Number of attempts already made
/// * `config` - Lease retry configuration
pub fn process_expired_lease(
    current_attempts: u16,
    config: &LeaseRetryConfig,
) -> ResurrectionOutcome {
    let next_attempt = current_attempts.saturating_add(1);

    if should_dead_letter(next_attempt, config.max_attempts) {
        ResurrectionOutcome::DeadLettered
    } else {
        let delay = compute_resurrection_delay(next_attempt, config);
        ResurrectionOutcome::Resurrected {
            delay_ms: delay.num_milliseconds() as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lease_id_display() {
        let id = LeaseId::new();
        let s = id.to_string();
        assert!(!s.is_empty());
    }

    #[test]
    fn test_compute_resurrection_delay_exponential() {
        let config = LeaseRetryConfig::default();

        // First attempt (attempts=1) -> base_delay * 2^0 = base_delay
        let delay1 = compute_resurrection_delay(1, &config);
        assert_eq!(delay1.num_milliseconds() as u64, config.base_delay_ms);

        // Second attempt (attempts=2) -> base_delay * 2^1 = 2*base_delay
        let delay2 = compute_resurrection_delay(2, &config);
        assert_eq!(delay2.num_milliseconds() as u64, config.base_delay_ms * 2);

        // Third attempt (attempts=3) -> base_delay * 2^2 = 4*base_delay
        let delay3 = compute_resurrection_delay(3, &config);
        assert_eq!(delay3.num_milliseconds() as u64, config.base_delay_ms * 4);
    }

    #[test]
    fn test_resurrection_delay_capped() {
        let config = LeaseRetryConfig {
            base_delay_ms: 1000,
            max_backoff_ms: 5000,
            ..Default::default()
        };

        // High attempts should be capped at max_backoff
        let delay = compute_resurrection_delay(10, &config);
        assert_eq!(delay.num_milliseconds() as u64, config.max_backoff_ms);
    }

    #[test]
    fn test_should_dead_letter() {
        assert!(!should_dead_letter(0, 3));
        assert!(!should_dead_letter(1, 3));
        assert!(!should_dead_letter(2, 3));
        assert!(should_dead_letter(3, 3));
        assert!(should_dead_letter(4, 3));
    }

    #[test]
    fn test_process_expired_lease_resurrect() {
        let config = LeaseRetryConfig {
            max_attempts: 3,
            base_delay_ms: 1000,
            ..Default::default()
        };

        // Job with 0 attempts, next attempt is 1 -> resurrect
        let outcome = process_expired_lease(0, &config);
        assert!(matches!(outcome, ResurrectionOutcome::Resurrected { .. }));

        // Job with 1 attempt, next attempt is 2 -> resurrect
        let outcome = process_expired_lease(1, &config);
        assert!(matches!(outcome, ResurrectionOutcome::Resurrected { .. }));
    }

    #[test]
    fn test_process_expired_lease_dead_letter() {
        let config = LeaseRetryConfig {
            max_attempts: 3,
            base_delay_ms: 1000,
            ..Default::default()
        };

        // Job with 2 attempts, next attempt is 3 -> dead-letter
        let outcome = process_expired_lease(2, &config);
        assert_eq!(outcome, ResurrectionOutcome::DeadLettered);
    }
}
