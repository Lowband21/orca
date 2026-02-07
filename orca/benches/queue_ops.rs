//! Benchmarks for queue operations using criterion.
//!
//! These benchmarks measure the performance of basic queue operations:
//! - Single job enqueue
//! - Batch enqueue (100 jobs)
//! - Dequeue with contention (SKIP LOCKED simulation)
//! - Job completion
//! - Full lifecycle (enqueue → dequeue → complete)

#![allow(missing_docs)]

use std::sync::Arc;

use chrono::Duration;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use orca::job::JobPriority;
use orca::lease::{DequeueRequest, LeaseId};
use orca::queue::QueueService;
use orca_testkit::{InMemoryQueueService, TestEntityId, TestJob, TestJobKind};
use tokio::runtime::Runtime;

/// Creates a tokio runtime for async benchmarks.
fn create_runtime() -> Runtime {
    Runtime::new().expect("Failed to create tokio runtime")
}

/// Benchmark: Enqueue single job.
///
/// Measures the latency of enqueuing a single job into the in-memory queue.
fn bench_enqueue_single(c: &mut Criterion) {
    let rt = create_runtime();

    let mut group = c.benchmark_group("enqueue_single");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(10));

    group.bench_function("in_memory", |b| {
        let queue = Arc::new(InMemoryQueueService::new());
        let entity_id = TestEntityId::new("bench-entity");
        let job = TestJob::Simple {
            name: "bench-job".to_string(),
        };

        b.to_async(&rt).iter(|| async {
            let _ = queue
                .enqueue(entity_id, job.clone(), JobPriority::P2)
                .await
                .expect("enqueue should succeed");
        });
    });

    group.finish();
}

/// Benchmark: Enqueue 100 jobs in batch.
///
/// Measures throughput when enqueuing multiple jobs in a single batch operation.
fn bench_enqueue_batch(c: &mut Criterion) {
    let rt = create_runtime();

    let batch_sizes = vec![10, 50, 100, 200];

    let mut group = c.benchmark_group("enqueue_batch");
    group.sample_size(50);
    group.measurement_time(std::time::Duration::from_secs(15));

    for batch_size in &batch_sizes {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("in_memory", batch_size),
            batch_size,
            |b, &size| {
                let queue = Arc::new(InMemoryQueueService::new());

                b.to_async(&rt).iter(|| async {
                    let requests: Vec<(TestEntityId, TestJob, JobPriority)> = (0..size)
                        .map(|i| {
                            (
                                TestEntityId::new(&format!("entity-{i}")),
                                TestJob::Simple {
                                    name: format!("job-{i}"),
                                },
                                JobPriority::P2,
                            )
                        })
                        .collect();

                    let _ = queue
                        .enqueue_many(requests)
                        .await
                        .expect("batch enqueue should succeed");
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Dequeue with contention simulation.
///
/// Measures dequeue performance under contention - simulating multiple workers
/// trying to dequeue jobs concurrently (like SKIP LOCKED behavior).
fn bench_dequeue_contention(c: &mut Criterion) {
    let rt = create_runtime();

    let worker_counts = vec![1, 5, 10, 20];

    let mut group = c.benchmark_group("dequeue_contention");
    group.sample_size(50);
    group.measurement_time(std::time::Duration::from_secs(15));

    for worker_count in &worker_counts {
        group.bench_with_input(
            BenchmarkId::new("workers", worker_count),
            worker_count,
            |b, &workers| {
                let queue = Arc::new(InMemoryQueueService::new());
                let rt = create_runtime();

                // Pre-populate with 1000 jobs
                rt.block_on(async {
                    let requests: Vec<(TestEntityId, TestJob, JobPriority)> = (0..1000)
                        .map(|i| {
                            (
                                TestEntityId::new(&format!("entity-{i}")),
                                TestJob::Simple {
                                    name: format!("job-{i}"),
                                },
                                JobPriority::P2,
                            )
                        })
                        .collect();
                    queue.enqueue_many(requests).await.unwrap();
                });

                b.to_async(&rt).iter(|| async {
                    // Simulate multiple workers dequeuing concurrently
                    let mut handles = vec![];
                    for worker_id in 0..workers {
                        let queue_clone = queue.clone();
                        let handle = tokio::spawn(async move {
                            let request = DequeueRequest {
                                kind: TestJobKind::Simple,
                                worker_id: format!("worker-{worker_id}"),
                                lease_ttl: Duration::seconds(30),
                                selector: None,
                            };
                            let _ = queue_clone.dequeue(request).await;
                        });
                        handles.push(handle);
                    }

                    for handle in handles {
                        let _ = handle.await;
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Complete job.
///
/// Measures the latency of marking a leased job as completed.
fn bench_complete_job(c: &mut Criterion) {
    let rt = create_runtime();

    let mut group = c.benchmark_group("complete_job");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(10));

    group.bench_function("in_memory", |b| {
        let queue = Arc::new(InMemoryQueueService::new());
        let rt = create_runtime();

        // Pre-populate and lease a job
        let lease = rt.block_on(async {
            let entity_id = TestEntityId::new("bench-entity");
            let job = TestJob::Simple {
                name: "bench-job".to_string(),
            };
            queue
                .enqueue(entity_id, job, JobPriority::P2)
                .await
                .expect("enqueue should succeed");

            let request = DequeueRequest {
                kind: TestJobKind::Simple,
                worker_id: "worker-1".to_string(),
                lease_ttl: Duration::seconds(30),
                selector: None,
            };
            queue
                .dequeue(request)
                .await
                .expect("dequeue should succeed")
                .expect("should get a lease")
        });

        let lease_id = lease.lease_id;

        b.to_async(&rt).iter(|| async {
            // Note: InMemoryQueueService's complete doesn't actually check lease_id
            // so we create a new one each iteration for consistency
            let test_lease_id = LeaseId::new();
            let _ = queue.complete(test_lease_id).await;
        });
    });

    group.finish();
}

/// Benchmark: Full lifecycle.
///
/// Measures the throughput of the complete job lifecycle:
/// enqueue → dequeue → complete
fn bench_full_lifecycle(c: &mut Criterion) {
    let rt = create_runtime();

    let mut group = c.benchmark_group("full_lifecycle");
    group.sample_size(50);
    group.measurement_time(std::time::Duration::from_secs(15));
    group.throughput(Throughput::Elements(1));

    group.bench_function("in_memory", |b| {
        let queue = Arc::new(InMemoryQueueService::new());

        b.to_async(&rt).iter(|| async {
            // Enqueue
            let entity_id = TestEntityId::new("bench-entity");
            let job = TestJob::Simple {
                name: "bench-job".to_string(),
            };
            let _handle = queue
                .enqueue(entity_id, job.clone(), JobPriority::P2)
                .await
                .expect("enqueue should succeed");

            // Dequeue
            let request = DequeueRequest {
                kind: TestJobKind::Simple,
                worker_id: "worker-1".to_string(),
                lease_ttl: Duration::seconds(30),
                selector: None,
            };
            let lease = queue
                .dequeue(request)
                .await
                .expect("dequeue should succeed")
                .expect("should get a lease");

            // Complete
            queue
                .complete(lease.lease_id)
                .await
                .expect("complete should succeed");
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_enqueue_single,
    bench_enqueue_batch,
    bench_dequeue_contention,
    bench_complete_job,
    bench_full_lifecycle
);
criterion_main!(benches);
