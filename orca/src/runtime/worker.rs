use serde::{Deserialize, Serialize};

/// Configuration for individual workers in the runtime.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Unique identifier for this worker.
    pub worker_id: String,
    /// Polling interval when no work is available, in milliseconds.
    pub poll_interval_ms: u64,
    /// Maximum number of times to poll before backing off.
    pub max_poll_attempts: u32,
    /// Backoff duration when no work is available, in milliseconds.
    pub backoff_ms: u64,
}

impl WorkerConfig {
    /// Create a new worker configuration with the given worker ID.
    pub fn new(worker_id: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            poll_interval_ms: 50,
            max_poll_attempts: 10,
            backoff_ms: 100,
        }
    }

    /// Set the polling interval.
    pub fn with_poll_interval(mut self, ms: u64) -> Self {
        self.poll_interval_ms = ms;
        self
    }

    /// Set the maximum poll attempts.
    pub fn with_max_poll_attempts(mut self, attempts: u32) -> Self {
        self.max_poll_attempts = attempts;
        self
    }

    /// Set the backoff duration.
    pub fn with_backoff(mut self, ms: u64) -> Self {
        self.backoff_ms = ms;
        self
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: "default-worker".to_string(),
            poll_interval_ms: 50,
            max_poll_attempts: 10,
            backoff_ms: 100,
        }
    }
}
