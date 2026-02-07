use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::hash::Hash;
use uuid::Uuid;

/// Trait for entity identifiers used in job orchestration.
///
/// Entity IDs must be unique, copyable, serializable, and convertible to UUIDs.
/// They represent the primary identifier for jobs and their associated entities.
pub trait EntityId:
    Copy
    + Eq
    + Hash
    + Serialize
    + DeserializeOwned
    + Display
    + Send
    + Sync
    + 'static
{
    /// Convert the entity ID to a UUID.
    fn as_uuid(&self) -> Uuid;
}

/// Trait for job type identifiers.
///
/// Job kinds categorize different types of work that can be enqueued
/// and processed. Each variant should have a unique string representation.
pub trait JobKind:
    Copy
    + Eq
    + Hash
    + Display
    + std::fmt::Debug
    + Serialize
    + DeserializeOwned
    + Send
    + Sync
    + 'static
{
    /// Get the string representation of this job kind.
    fn as_str(&self) -> &'static str;
}

/// Trait for workload categorization used in budget management.
///
/// Workload kinds allow grouping job types for concurrency limiting
/// and resource allocation purposes.
pub trait WorkloadKind:
    Copy + Eq + Hash + Send + Sync + std::fmt::Debug + 'static
{
}

/// Core trait for job types that can be enqueued and executed.
///
/// Jobs are the fundamental unit of work in the orchestration system.
/// Implementors define their own job types with specific payloads,
/// priorities, and entity associations.
pub trait Job:
    Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// Type of entity identifier associated with this job.
    type EntityId: EntityId;
    /// Job kind type for categorizing this job.
    type Kind: JobKind;
    /// Domain-specific event type for this job.
    type DomainEvent: Clone + Send + Sync + 'static;
    /// Workload kind for budget management.
    type WorkloadKind: WorkloadKind;

    /// Get the entity ID associated with this job.
    fn entity_id(&self) -> Self::EntityId;
    /// Get the job kind for this job.
    fn kind(&self) -> Self::Kind;
    /// Get the deduplication key for this job.
    fn dedupe_key(&self) -> String;
    /// Get the priority of this job.
    fn priority(&self) -> JobPriority;
}

/// Priority levels for job scheduling.
///
/// Lower numbers indicate higher priority. P0 is the highest priority
/// and P3 is the lowest. Used by the scheduler for fair queue allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum JobPriority {
    /// Highest priority (0).
    P0 = 0,
    /// High priority (1).
    P1 = 1,
    /// Medium priority (2).
    P2 = 2,
    /// Lowest priority (3).
    P3 = 3,
}

/// Lifecycle states of a job in the queue.
///
/// Tracks the current state of a job through the orchestration pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum JobState {
    /// Job is ready to be dequeued and processed.
    Ready,
    /// Job is waiting for a dependency to be satisfied.
    Deferred,
    /// Job has been dequeued and assigned a lease.
    Leased,
    /// Job completed successfully.
    Completed,
    /// Job failed processing.
    Failed,
    /// Job failed permanently and was moved to dead letter.
    DeadLetter,
}

/// Handle returned from job enqueue operations.
///
/// Contains information about the enqueued job including whether
/// it was newly created or merged with an existing job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobHandle {
    /// Unique identifier for this job.
    pub id: JobId,
    /// Priority level of the job.
    pub priority: JobPriority,
    /// Deduplication key for this job.
    pub dedupe_key: String,
    /// `true` when the job was newly created, `false` when merged into an existing job.
    pub accepted: bool,
}

/// Unique identifier for a job.
///
/// Uses UUID v7 for time-ordered uniqueness and efficient indexing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct JobId(pub Uuid);

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl JobId {
    /// Create a new job ID using UUID v7.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl EntityId for JobId {
    fn as_uuid(&self) -> Uuid {
        self.0
    }
}

/// Key for establishing job dependencies.
///
/// Jobs with a dependency key are deferred until the dependency
/// is released by another job completing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DependencyKey(String);

impl DependencyKey {
    /// Create a new dependency key.
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    /// Get the key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for DependencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for DependencyKey {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for DependencyKey {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}
