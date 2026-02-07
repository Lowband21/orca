use serde::{Deserialize, Serialize};

/// Configuration for database persistence connections.
///
/// Used to configure connection pool settings for PostgreSQL backends.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// Database connection string (e.g., "postgres://user:pass@host/db").
    pub connection_string: String,
    /// Maximum number of connections in the pool.
    pub max_connections: u32,
    /// Minimum number of connections to maintain in the pool.
    pub min_connections: u32,
    /// Timeout in seconds for acquiring a connection from the pool.
    pub acquire_timeout_seconds: u64,
}

/// Configuration for queue behavior and retry policies.
///
/// Controls batching, retry limits, and dead letter handling.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueConfig {
    /// Number of jobs to process in a single batch operation.
    pub batch_size: usize,
    /// Maximum number of retry attempts before dead-lettering.
    pub max_retries: u16,
    /// Base delay in seconds between retry attempts.
    pub retry_backoff_seconds: u64,
    /// Whether to enable dead letter queue for failed jobs.
    pub dead_letter_enabled: bool,
}
