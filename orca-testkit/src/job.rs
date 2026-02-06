use orca::*;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
pub enum TestJob {
    Simple { name: String },
    Slow { duration_ms: u64, name: String },
    Failing { error: String, name: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestDomainEvent {
    pub job_kind: TestJobKind,
    pub message: String,
}

impl Job for TestJob {
    type EntityId = TestEntityId;
    type Kind = TestJobKind;
    type DomainEvent = TestDomainEvent;

    fn entity_id(&self) -> Self::EntityId {
        let name = match self {
            TestJob::Simple { name } => name,
            TestJob::Slow { name, .. } => name,
            TestJob::Failing { name, .. } => name,
        };
        TestEntityId::new(name)
    }

    fn kind(&self) -> Self::Kind {
        match self {
            TestJob::Simple { .. } => TestJobKind::Simple,
            TestJob::Slow { .. } => TestJobKind::Slow,
            TestJob::Failing { .. } => TestJobKind::Failing,
        }
    }

    fn dedupe_key(&self) -> String {
        let entity_id = self.entity_id();
        format!("{}:{}", self.kind(), entity_id)
    }

    fn priority(&self) -> JobPriority {
        JobPriority::P2
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TestEntityId(Uuid);

impl TestEntityId {
    pub fn new(name: &str) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        use std::hash::Hash;
        name.hash(&mut hasher);
        let hash = hasher.finish();
        Self(Uuid::new_v4())
    }
}

impl EntityId for TestEntityId {
    fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Display for TestEntityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TestJobKind {
    Simple,
    Slow,
    Failing,
}

impl JobKind for TestJobKind {
    fn as_str(&self) -> &'static str {
        match self {
            TestJobKind::Simple => "simple",
            TestJobKind::Slow => "slow",
            TestJobKind::Failing => "failing",
        }
    }
}

impl Display for TestJobKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TestWorkloadKind {
    General,
}

impl WorkloadKind for TestWorkloadKind {}
