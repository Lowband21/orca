use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::hash::Hash;
use uuid::Uuid;

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
    fn as_uuid(&self) -> Uuid;
}

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
    fn as_str(&self) -> &'static str;
}

pub trait WorkloadKind:
    Copy + Eq + Hash + Send + Sync + std::fmt::Debug + 'static
{
}

pub trait Job:
    Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
    type EntityId: EntityId;
    type Kind: JobKind;
    type DomainEvent: Clone + Send + Sync + 'static;

    fn entity_id(&self) -> Self::EntityId;
    fn kind(&self) -> Self::Kind;
    fn dedupe_key(&self) -> String;
    fn priority(&self) -> JobPriority;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum JobPriority {
    P0 = 0,
    P1 = 1,
    P2 = 2,
    P3 = 3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum JobState {
    Ready,
    Deferred,
    Leased,
    Completed,
    Failed,
    DeadLetter,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobHandle {
    pub id: JobId,
    pub priority: JobPriority,
    pub dedupe_key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct JobId(pub Uuid);

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl JobId {
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DependencyKey(String);

impl DependencyKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

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
