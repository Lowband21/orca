use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct CorrelationId(pub Uuid);

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl CorrelationId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for CorrelationId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

/// Generic correlation cache mapping job IDs to correlation IDs.
///
/// Used for job lineage tracing in job orchestration runtimes.
/// Thread-safe via `tokio::sync::Mutex`.
#[derive(Clone, Default, Debug)]
pub struct CorrelationCache {
    inner: Arc<Mutex<HashMap<Uuid, Uuid>>>,
}

impl CorrelationCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Store a mapping from job_id to correlation_id.
    ///
    /// Overwrites any existing mapping for this job.
    pub async fn remember(&self, job_id: Uuid, correlation_id: Uuid) {
        let mut guard = self.inner.lock().await;
        guard.insert(job_id, correlation_id);
    }

    /// Store a mapping from job_id to correlation_id, but only if no mapping exists.
    ///
    /// Preserves existing correlation IDs if already set.
    pub async fn remember_if_absent(&self, job_id: Uuid, correlation_id: Uuid) {
        let mut guard = self.inner.lock().await;
        guard.entry(job_id).or_insert(correlation_id);
    }

    /// Fetch the correlation_id for a job_id, if it exists.
    pub async fn fetch(&self, job_id: Uuid) -> Option<Uuid> {
        let guard = self.inner.lock().await;
        guard.get(&job_id).copied()
    }

    /// Fetch and remove the correlation_id for a job_id, if it exists.
    pub async fn take(&self, job_id: Uuid) -> Option<Uuid> {
        let mut guard = self.inner.lock().await;
        guard.remove(&job_id)
    }

    /// Fetch existing correlation_id or generate a new v7 UUID.
    ///
    /// Storing is idempotent - will return existing mapping if present.
    pub async fn fetch_or_generate(&self, job_id: Uuid) -> Uuid {
        let mut guard = self.inner.lock().await;
        if let Some(existing) = guard.get(&job_id) {
            return *existing;
        }

        let fresh = Uuid::now_v7();
        info!(job_id = %job_id, "missing correlation id; generating new one");
        guard.insert(job_id, fresh);
        fresh
    }

    /// Take existing correlation_id if present, or generate a new v7 UUID.
    ///
    /// Use during cleanup to remove correlation mappings while ensuring
    /// a valid correlation ID is always returned.
    pub async fn take_or_generate(&self, job_id: Uuid) -> Uuid {
        let mut guard = self.inner.lock().await;
        if let Some(existing) = guard.remove(&job_id) {
            return existing;
        }

        let fresh = Uuid::now_v7();
        warn!(job_id = %job_id, "missing correlation id during cleanup; generating new one");
        fresh
    }

    /// Check if a correlation exists for a job.
    pub async fn contains(&self, job_id: Uuid) -> bool {
        let guard = self.inner.lock().await;
        guard.contains_key(&job_id)
    }
}
