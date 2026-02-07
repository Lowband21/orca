//! Benchmarks for serialization/deserialization performance using criterion.
//!
//! These benchmarks measure the performance of:
//! - Job serialization to JSONB format
//! - Job deserialization from JSONB

#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use uuid::Uuid;

use orca::job::{EntityId, Job, JobId, JobKind, JobPriority, WorkloadKind};

/// A benchmark job type with realistic payload sizes.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum BenchmarkJob {
    /// Small job - minimal payload
    Small { id: String },
    /// Medium job - typical production payload
    Medium {
        id: String,
        data: Vec<u8>,
        metadata: std::collections::HashMap<String, String>,
    },
    /// Large job - heavy payload for stress testing
    Large {
        id: String,
        content: String,
        attachments: Vec<Attachment>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Attachment {
    name: String,
    size: u64,
    checksum: String,
}

/// Benchmark domain event type.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BenchmarkDomainEvent {
    message: String,
    timestamp: i64,
}

/// Entity ID for benchmark jobs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
struct BenchmarkEntityId(Uuid);

impl EntityId for BenchmarkEntityId {
    fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Display for BenchmarkEntityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Job kind for benchmark jobs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
enum BenchmarkJobKind {
    Small,
    Medium,
    Large,
}

impl JobKind for BenchmarkJobKind {
    fn as_str(&self) -> &'static str {
        match self {
            BenchmarkJobKind::Small => "small",
            BenchmarkJobKind::Medium => "medium",
            BenchmarkJobKind::Large => "large",
        }
    }
}

impl Display for BenchmarkJobKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Workload kind for benchmark jobs.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum BenchmarkWorkloadKind {
    General,
}

impl WorkloadKind for BenchmarkWorkloadKind {}

impl Job for BenchmarkJob {
    type EntityId = BenchmarkEntityId;
    type Kind = BenchmarkJobKind;
    type DomainEvent = BenchmarkDomainEvent;
    type WorkloadKind = BenchmarkWorkloadKind;

    fn entity_id(&self) -> Self::EntityId {
        let id = match self {
            BenchmarkJob::Small { id } => id,
            BenchmarkJob::Medium { id, .. } => id,
            BenchmarkJob::Large { id, .. } => id,
        };
        BenchmarkEntityId(Uuid::new_v4())
    }

    fn kind(&self) -> Self::Kind {
        match self {
            BenchmarkJob::Small { .. } => BenchmarkJobKind::Small,
            BenchmarkJob::Medium { .. } => BenchmarkJobKind::Medium,
            BenchmarkJob::Large { .. } => BenchmarkJobKind::Large,
        }
    }

    fn dedupe_key(&self) -> String {
        format!("{}:{:?}", self.kind(), self.entity_id())
    }

    fn priority(&self) -> JobPriority {
        JobPriority::P2
    }
}

/// Creates a small job for benchmarking.
fn create_small_job() -> BenchmarkJob {
    BenchmarkJob::Small {
        id: "small-job-001".to_string(),
    }
}

/// Creates a medium job (1KB payload) for benchmarking.
fn create_medium_job() -> BenchmarkJob {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("source".to_string(), "benchmark".to_string());
    metadata.insert("version".to_string(), "1.0".to_string());
    metadata.insert("region".to_string(), "us-east-1".to_string());

    BenchmarkJob::Medium {
        id: "medium-job-001".to_string(),
        data: vec![0u8; 512], // 512 bytes
        metadata,
    }
}

/// Creates a large job (10KB payload) for benchmarking.
fn create_large_job() -> BenchmarkJob {
    let attachments: Vec<Attachment> = (0..10)
        .map(|i| Attachment {
            name: format!("file_{}.bin", i),
            size: 1024 * 1024, // 1MB each
            checksum: format!("sha256:{:064x}", i),
        })
        .collect();

    BenchmarkJob::Large {
        id: "large-job-001".to_string(),
        content: "x".repeat(5000),
        attachments,
    }
}

/// Benchmark: Job serialization to JSONB.
///
/// Measures the time to serialize jobs of different sizes to JSON format
/// (simulating JSONB storage in PostgreSQL).
fn bench_job_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("job_serialize");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(10));

    // Small job serialization
    let small_job = create_small_job();
    group.throughput(Throughput::Bytes(
        serde_json::to_vec(&small_job).unwrap().len() as u64,
    ));
    group.bench_function("small", |b| {
        b.iter(|| {
            let json = serde_json::to_vec(black_box(&small_job)).expect("serialize should succeed");
            black_box(json);
        });
    });

    // Medium job serialization
    let medium_job = create_medium_job();
    group.throughput(Throughput::Bytes(
        serde_json::to_vec(&medium_job).unwrap().len() as u64,
    ));
    group.bench_function("medium", |b| {
        b.iter(|| {
            let json =
                serde_json::to_vec(black_box(&medium_job)).expect("serialize should succeed");
            black_box(json);
        });
    });

    // Large job serialization
    let large_job = create_large_job();
    group.throughput(Throughput::Bytes(
        serde_json::to_vec(&large_job).unwrap().len() as u64,
    ));
    group.bench_function("large", |b| {
        b.iter(|| {
            let json = serde_json::to_vec(black_box(&large_job)).expect("serialize should succeed");
            black_box(json);
        });
    });

    group.finish();
}

/// Benchmark: Job deserialization from JSONB.
///
/// Measures the time to deserialize jobs from JSON format.
fn bench_job_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("job_deserialize");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(10));

    // Prepare serialized data
    let small_job = create_small_job();
    let small_json = serde_json::to_vec(&small_job).expect("serialize should succeed");

    let medium_job = create_medium_job();
    let medium_json = serde_json::to_vec(&medium_job).expect("serialize should succeed");

    let large_job = create_large_job();
    let large_json = serde_json::to_vec(&large_job).expect("serialize should succeed");

    // Small job deserialization
    group.throughput(Throughput::Bytes(small_json.len() as u64));
    group.bench_function("small", |b| {
        b.iter(|| {
            let job: BenchmarkJob =
                serde_json::from_slice(black_box(&small_json)).expect("deserialize should succeed");
            black_box(job);
        });
    });

    // Medium job deserialization
    group.throughput(Throughput::Bytes(medium_json.len() as u64));
    group.bench_function("medium", |b| {
        b.iter(|| {
            let job: BenchmarkJob = serde_json::from_slice(black_box(&medium_json))
                .expect("deserialize should succeed");
            black_box(job);
        });
    });

    // Large job deserialization
    group.throughput(Throughput::Bytes(large_json.len() as u64));
    group.bench_function("large", |b| {
        b.iter(|| {
            let job: BenchmarkJob =
                serde_json::from_slice(black_box(&large_json)).expect("deserialize should succeed");
            black_box(job);
        });
    });

    group.finish();
}

/// Benchmark: Round-trip serialization.
///
/// Measures the full cycle of serialize → deserialize for job data.
fn bench_round_trip(c: &mut Criterion) {
    let mut group = c.benchmark_group("round_trip");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(10));

    let job = create_medium_job();
    let serialized = serde_json::to_vec(&job).expect("serialize should succeed");

    group.throughput(Throughput::Bytes(serialized.len() as u64));
    group.bench_function("medium_job", |b| {
        b.iter(|| {
            // Serialize
            let json = serde_json::to_vec(black_box(&job)).expect("serialize should succeed");
            // Deserialize
            let _deserialized: BenchmarkJob =
                serde_json::from_slice(&json).expect("deserialize should succeed");
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_job_serialize,
    bench_job_deserialize,
    bench_round_trip
);
criterion_main!(benches);
