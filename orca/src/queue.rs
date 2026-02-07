use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::job::{DependencyKey, JobHandle, JobId, JobPriority};
use crate::lease::{DequeueRequest, JobLease, LeaseId, LeaseRenewal};

/// Trait for queue backends that manage job lifecycle.
///
/// Implementors provide persistence and retrieval of jobs, including
/// deduplication, leasing, and completion tracking.
#[async_trait]
pub trait QueueService<J>: Send + Sync
where
    J: crate::Job,
{
    /// Enqueue a single job.
    async fn enqueue(
        &self,
        entity_id: J::EntityId,
        job: J,
        priority: JobPriority,
    ) -> anyhow::Result<JobHandle>;

    /// Enqueue multiple jobs in a batch.
    async fn enqueue_many(
        &self,
        requests: Vec<(J::EntityId, J, JobPriority)>,
    ) -> anyhow::Result<Vec<JobHandle>>;

    /// Dequeue a job for processing.
    async fn dequeue(
        &self,
        request: DequeueRequest<J::Kind, J::EntityId>,
    ) -> anyhow::Result<Option<JobLease<J>>>;

    /// Mark a leased job as completed.
    async fn complete(&self, lease_id: LeaseId) -> anyhow::Result<()>;

    /// Mark a leased job as failed.
    async fn fail(
        &self,
        lease_id: LeaseId,
        retryable: bool,
        error: Option<String>,
    ) -> anyhow::Result<()>;

    /// Move a job to the dead letter queue.
    async fn dead_letter(
        &self,
        lease_id: LeaseId,
        error: Option<String>,
    ) -> anyhow::Result<()>;

    /// Renew a job lease.
    async fn renew(&self, renewal: LeaseRenewal)
    -> anyhow::Result<JobLease<J>>;

    /// Get the current depth of a queue for a job kind.
    async fn queue_depth(&self, kind: J::Kind) -> anyhow::Result<usize>;

    /// Cancel a pending job.
    async fn cancel_job(&self, job_id: JobId) -> anyhow::Result<()>;

    /// Release jobs waiting on a dependency.
    async fn release_dependency(
        &self,
        entity_id: J::EntityId,
        dependency_key: &DependencyKey,
    ) -> anyhow::Result<u64>;
}

/// Trait for scanning and handling expired leases.
///
/// Implementors periodically scan for leases that have expired without
/// completion and either retry or dead-letter them.
#[async_trait]
pub trait LeaseExpiryScanner: Send + Sync {
    /// Scan for and process expired leases.
    async fn scan_expired_leases(&self) -> anyhow::Result<u64>;
}

pub use crate::lease::QueueSelector;

/// Snapshot of queue state at a point in time.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueueSnapshot {
    /// Timestamp when the snapshot was taken.
    pub sampled_at: DateTime<Utc>,
    /// Per-queue entry statistics.
    pub queues: HashMap<String, QueueSnapshotEntry>,
}

impl QueueSnapshot {
    /// Create a new queue snapshot at the given time.
    pub fn new(sampled_at: DateTime<Utc>) -> Self {
        Self {
            sampled_at,
            queues: HashMap::new(),
        }
    }

    /// Get or create an entry for a queue kind.
    pub fn entry_mut(&mut self, kind: &str) -> &mut QueueSnapshotEntry {
        self.queues.entry(kind.to_string()).or_default()
    }
}

/// Statistics for a single queue kind.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueueSnapshotEntry {
    /// Number of jobs ready for processing.
    pub ready_count: usize,
    /// Number of jobs currently leased.
    pub leased_count: usize,
    /// Number of deferred jobs waiting on dependencies.
    pub deferred_count: usize,
    /// Total number of jobs in the queue.
    pub total_count: usize,
}
