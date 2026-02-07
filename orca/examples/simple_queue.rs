//! Simple queue example demonstrating basic enqueue/dequeue operations.
//!
//! This example shows how to use the in-memory queue service from orca-testkit
//! to perform basic queue operations without a full runtime.

use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use orca::*;
use orca::runtime::{DispatchStatus, JobDispatcher};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Re-export testkit types for this example
use orca_testkit::{InMemoryQueueService, TestJob, TestJobKind};

/// Workload categories for budget management.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum WorkloadType {
    General,
}

impl WorkloadKind for WorkloadType {}

/// Simple dispatcher that processes jobs directly.
struct SimpleDispatcher;

#[async_trait]
impl JobDispatcher<TestJob> for SimpleDispatcher {
    async fn dispatch(&self, lease: &JobLease<TestJob>) -> DispatchStatus {
        match &lease.job {
            TestJob::Simple { name } => {
                println!("[DISPATCH] Processing simple job: {}", name);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                println!("[DISPATCH] Completed simple job: {}", name);
                DispatchStatus::Success
            }
            TestJob::Slow { name, duration_ms } => {
                println!("[DISPATCH] Processing slow job: {} ({}ms)", name, duration_ms);
                tokio::time::sleep(tokio::time::Duration::from_millis(*duration_ms)).await;
                println!("[DISPATCH] Completed slow job: {}", name);
                DispatchStatus::Success
            }
            TestJob::Failing { name, error } => {
                println!("[DISPATCH] Processing failing job: {}", name);
                println!("[DISPATCH] Job failed: {}", error);
                DispatchStatus::RetryableFailure { error: Some(error.clone()) }
            }
        }
    }
}

/// Manual worker loop for simple demonstration.
/// In production, use OrchestratorRuntime instead.
async fn run_worker(
    worker_id: String,
    queue: Arc<InMemoryQueueService>,
    dispatcher: Arc<SimpleDispatcher>,
    kind: TestJobKind,
) -> anyhow::Result<u64> {
    let mut processed = 0u64;
    let lease_ttl = chrono::Duration::seconds(30);
    
    loop {
        // Try to dequeue a job
        let request = DequeueRequest {
            kind,
            worker_id: worker_id.clone(),
            lease_ttl,
            selector: None,
        };
        
        match queue.dequeue(request).await? {
            Some(lease) => {
                let job_id = lease.job_id;
                let entity_id = lease.job.entity_id();
                let job_priority = lease.job.priority();
                
                println!("[{}] Dequeued job {} (kind: {:?}, priority: {:?})", 
                    worker_id, job_id, kind, job_priority);
                
                // Dispatch the job
                let status = dispatcher.dispatch(&lease).await;
                
                // Handle the result
                match status {
                    DispatchStatus::Success => {
                        queue.complete(lease.lease_id).await?;
                        println!("[{}] Completed job {} successfully", worker_id, job_id);
                        processed += 1;
                    }
                    DispatchStatus::RetryableFailure { error } => {
                        queue.fail(lease.lease_id, true, error).await?;
                        println!("[{}] Job {} failed (will retry)", worker_id, job_id);
                        processed += 1;
                    }
                    DispatchStatus::PermanentFailure { error } => {
                        queue.dead_letter(lease.lease_id, error).await?;
                        println!("[{}] Job {} dead-lettered", worker_id, job_id);
                        processed += 1;
                    }
                }
            }
            None => {
                // No more jobs of this kind
                println!("[{}] No more jobs available for {:?}", worker_id, kind);
                break;
            }
        }
    }
    
    Ok(processed)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Orca Simple Queue Example ===\n");
    
    println!("This example demonstrates basic enqueue/dequeue operations");
    println!("using the in-memory queue service.\n");

    // Create the queue
    let queue = Arc::new(InMemoryQueueService::new());
    let dispatcher = Arc::new(SimpleDispatcher);

    println!("1. Enqueueing jobs...\n");

    // Enqueue some simple jobs
    for i in 0..3 {
        let job = TestJob::Simple {
            name: format!("simple-job-{}", i),
        };
        let handle = queue
            .enqueue(job.entity_id(), job, JobPriority::P1)
            .await?;
        println!("   Enqueued simple job #{} (id={}, priority: {:?})", 
            i, handle.id, handle.priority);
    }

    // Enqueue a slow job
    let slow_job = TestJob::Slow {
        name: "slow-processing-job".to_string(),
        duration_ms: 300,
    };
    let handle = queue
        .enqueue(slow_job.entity_id(), slow_job, JobPriority::P2)
        .await?;
    println!("   Enqueued slow job (id={}, priority: {:?})", handle.id, handle.priority);

    // Enqueue a failing job (to demonstrate error handling)
    let failing_job = TestJob::Failing {
        name: "failing-job".to_string(),
        error: "Simulated processing error".to_string(),
    };
    let handle = queue
        .enqueue(failing_job.entity_id(), failing_job, JobPriority::P3)
        .await?;
    println!("   Enqueued failing job (id={}, priority: {:?})", handle.id, handle.priority);

    // Check queue depths
    let depth_simple = queue.queue_depth(TestJobKind::Simple).await?;
    let depth_slow = queue.queue_depth(TestJobKind::Slow).await?;
    let depth_failing = queue.queue_depth(TestJobKind::Failing).await?;
    println!("\n2. Queue depths:");
    println!("   Simple jobs: {}", depth_simple);
    println!("   Slow jobs: {}", depth_slow);
    println!("   Failing jobs: {}", depth_failing);

    // Process jobs manually
    println!("\n3. Processing jobs...\n");
    
    // Run workers for each job kind
    let simple_count = run_worker(
        "worker-simple".to_string(),
        queue.clone(),
        dispatcher.clone(),
        TestJobKind::Simple,
    ).await?;
    
    let slow_count = run_worker(
        "worker-slow".to_string(),
        queue.clone(),
        dispatcher.clone(),
        TestJobKind::Slow,
    ).await?;
    
    let failing_count = run_worker(
        "worker-failing".to_string(),
        queue.clone(),
        dispatcher.clone(),
        TestJobKind::Failing,
    ).await?;

    println!("\n4. Processing complete:");
    println!("   Simple jobs processed: {}", simple_count);
    println!("   Slow jobs processed: {}", slow_count);
    println!("   Failing jobs processed: {}", failing_count);
    println!("   Total jobs processed: {}\n", simple_count + slow_count + failing_count);

    // Check final queue depths
    let depth_simple = queue.queue_depth(TestJobKind::Simple).await?;
    let depth_slow = queue.queue_depth(TestJobKind::Slow).await?;
    let depth_failing = queue.queue_depth(TestJobKind::Failing).await?;
    println!("5. Final queue depths (should be 0):");
    println!("   Simple jobs: {}", depth_simple);
    println!("   Slow jobs: {}", depth_slow);
    println!("   Failing jobs: {}", depth_failing);

    println!("\n=== Example Complete ===");
    println!("\nKey takeaways:");
    println!("- Jobs can be enqueued with different priorities (P1, P2, P3)");
    println!("- Queue depth can be checked per job kind");
    println!("- Jobs are processed by dequeueing and dispatching");
    println!("- Failing jobs can be retried or dead-lettered based on the error");
    println!("- In production, use OrchestratorRuntime instead of manual worker loops");

    Ok(())
}
