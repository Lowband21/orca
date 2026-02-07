//! Basic runtime example with in-memory queue.
//!
//! This example demonstrates how to use the runtime components with
//! orca-testkit's InMemoryQueueService and TestJob types.
//!
//! For a full production example with PostgreSQL, see `postgres_runtime.rs`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use orca::*;
use orca::runtime::{DispatchStatus, JobDispatcher};
use orca_testkit::{InMemoryQueueService, TestJob, TestJobKind, TestEntityId};

/// Workload categories for budget management.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum WorkloadCategory {
    General,
    Slow,
    Failing,
}

impl WorkloadKind for WorkloadCategory {}

/// Dispatcher that handles the different test job types.
struct TestJobDispatcher;

#[async_trait]
impl JobDispatcher<TestJob> for TestJobDispatcher {
    async fn dispatch(&self, lease: &JobLease<TestJob>) -> DispatchStatus {
        match &lease.job {
            TestJob::Simple { name } => {
                println!("[DISPATCHER] Processing simple job: {}", name);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                println!("[DISPATCHER] Completed simple job: {}", name);
                DispatchStatus::Success
            }
            TestJob::Slow { name, duration_ms } => {
                println!("[DISPATCHER] Processing slow job: {} ({}ms)", name, duration_ms);
                tokio::time::sleep(tokio::time::Duration::from_millis(*duration_ms)).await;
                println!("[DISPATCHER] Completed slow job: {}", name);
                DispatchStatus::Success
            }
            TestJob::Failing { name, error } => {
                println!("[DISPATCHER] Processing failing job: {}", name);
                println!("[DISPATCHER] Job failed: {}", error);
                DispatchStatus::RetryableFailure { error: Some(error.clone()) }
            }
        }
    }
}

/// Maps job kinds to workload categories.
fn workload_for_kind(kind: TestJobKind) -> WorkloadCategory {
    match kind {
        TestJobKind::Simple => WorkloadCategory::General,
        TestJobKind::Slow => WorkloadCategory::Slow,
        TestJobKind::Failing => WorkloadCategory::Failing,
    }
}

/// Manual worker with budget management.
async fn run_worker_with_budget(
    worker_id: String,
    queue: Arc<InMemoryQueueService>,
    budget: Arc<InMemoryBudget<WorkloadCategory, TestEntityId>>,
    dispatcher: Arc<TestJobDispatcher>,
    kind: TestJobKind,
) -> anyhow::Result<u64> {
    let mut processed = 0u64;
    let lease_ttl = chrono::Duration::seconds(30);
    let workload = workload_for_kind(kind);
    
    loop {
        // Try to acquire budget first
        if !budget.has_budget(workload).await? {
            println!("[{}] Budget exhausted for {:?}, waiting...", worker_id, workload);
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        }
        
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
                
                // Acquire budget token
                let token = budget.try_acquire(workload, entity_id).await?;
                if token.is_none() {
                    println!("[{}] Could not acquire budget for job {}", worker_id, job_id);
                    queue.fail(lease.lease_id, true, Some("Budget exhausted".to_string())).await?;
                    continue;
                }
                
                println!("[{}] Dequeued job {} (kind: {:?}, priority: {:?})", 
                    worker_id, job_id, kind, job_priority);
                
                // Dispatch the job
                let status = dispatcher.dispatch(&lease).await;
                
                // Release budget token
                if let Some(token) = token {
                    budget.release(token).await?;
                }
                
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
                println!("[{}] No more jobs available for {:?}", worker_id, kind);
                break;
            }
        }
    }
    
    Ok(processed)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== Orca Runtime Basic Example ===\n");
    println!("This example demonstrates:");
    println!("- In-memory queue (via orca-testkit)");
    println!("- Budget-based concurrency limiting");
    println!("- Manual worker pools\n");

    // Create the queue service
    let queue = Arc::new(InMemoryQueueService::new());

    // Create budget with different limits per workload
    let mut limits = HashMap::new();
    limits.insert(WorkloadCategory::General, 5);  // Max 5 simple jobs
    limits.insert(WorkloadCategory::Slow, 2);   // Max 2 slow jobs
    limits.insert(WorkloadCategory::Failing, 1); // Max 1 failing job
    
    let budget = Arc::new(InMemoryBudget::new(
        BudgetConfig::new(limits, 3)
    ));

    // Create dispatcher
    let dispatcher = Arc::new(TestJobDispatcher);

    // Enqueue simple jobs
    println!("1. Enqueueing simple jobs...");
    for i in 0..10 {
        let job = TestJob::Simple {
            name: format!("simple-job-{}", i),
        };
        let handle = queue
            .enqueue(job.entity_id(), job, JobPriority::P1)
            .await?;
        println!("   Enqueued simple job {} (id={})", i, handle.id);
    }

    // Enqueue slow jobs
    println!("\n2. Enqueueing slow jobs...");
    for i in 0..4 {
        let job = TestJob::Slow {
            name: format!("slow-job-{}", i),
            duration_ms: 200 + (i as u64 * 50),
        };
        let handle = queue
            .enqueue(job.entity_id(), job, JobPriority::P2)
            .await?;
        println!("   Enqueued slow job {} (id={}, {}ms)", i, handle.id, 200 + (i * 50));
    }

    // Enqueue failing jobs
    println!("\n3. Enqueueing failing jobs...");
    for i in 0..2 {
        let job = TestJob::Failing {
            name: format!("failing-job-{}", i),
            error: format!("Simulated error {}", i),
        };
        let handle = queue
            .enqueue(job.entity_id(), job, JobPriority::P3)
            .await?;
        println!("   Enqueued failing job {} (id={})", i, handle.id);
    }

    // Check queue depths
    let simple_depth = queue.queue_depth(TestJobKind::Simple).await?;
    let slow_depth = queue.queue_depth(TestJobKind::Slow).await?;
    let failing_depth = queue.queue_depth(TestJobKind::Failing).await?;
    println!("\n4. Initial queue depths:");
    println!("   Simple jobs: {}", simple_depth);
    println!("   Slow jobs: {}", slow_depth);
    println!("   Failing jobs: {}", failing_depth);

    // Process jobs with concurrent workers
    println!("\n5. Starting workers...\n");
    
    // Spawn workers for each job kind
    let simple_workers: Vec<_> = (0..3).map(|i| {
        tokio::spawn(run_worker_with_budget(
            format!("simple-worker-{}", i),
            queue.clone(),
            budget.clone(),
            dispatcher.clone(),
            TestJobKind::Simple,
        ))
    }).collect();
    
    let slow_workers: Vec<_> = (0..2).map(|i| {
        tokio::spawn(run_worker_with_budget(
            format!("slow-worker-{}", i),
            queue.clone(),
            budget.clone(),
            dispatcher.clone(),
            TestJobKind::Slow,
        ))
    }).collect();
    
    let failing_workers: Vec<_> = (0..1).map(|i| {
        tokio::spawn(run_worker_with_budget(
            format!("failing-worker-{}", i),
            queue.clone(),
            budget.clone(),
            dispatcher.clone(),
            TestJobKind::Failing,
        ))
    }).collect();
    
    // Wait for all workers to complete
    let mut simple_total = 0u64;
    for worker in simple_workers {
        simple_total += worker.await??;
    }
    
    let mut slow_total = 0u64;
    for worker in slow_workers {
        slow_total += worker.await??;
    }
    
    let mut failing_total = 0u64;
    for worker in failing_workers {
        failing_total += worker.await??;
    }

    println!("\n6. Processing complete:");
    println!("   Simple jobs processed: {}", simple_total);
    println!("   Slow jobs processed: {}", slow_total);
    println!("   Failing jobs processed: {}", failing_total);
    println!("   Total jobs processed: {}\n", simple_total + slow_total + failing_total);

    // Check budget utilization
    let (general_current, general_limit) = budget.utilization(WorkloadCategory::General).await?;
    let (slow_current, slow_limit) = budget.utilization(WorkloadCategory::Slow).await?;
    let (failing_current, failing_limit) = budget.utilization(WorkloadCategory::Failing).await?;
    println!("7. Budget utilization:");
    println!("   General: {}/{} (limit: 5)", general_current, general_limit);
    println!("   Slow: {}/{} (limit: 2)", slow_current, slow_limit);
    println!("   Failing: {}/{} (limit: 1)\n", failing_current, failing_limit);

    // Check final queue depths
    let simple_depth = queue.queue_depth(TestJobKind::Simple).await?;
    let slow_depth = queue.queue_depth(TestJobKind::Slow).await?;
    let failing_depth = queue.queue_depth(TestJobKind::Failing).await?;
    println!("8. Final queue depths (should be 0):");
    println!("   Simple jobs: {}", simple_depth);
    println!("   Slow jobs: {}", slow_depth);
    println!("   Failing jobs: {}", failing_depth);

    println!("\n=== Example Complete ===");
    println!("\nKey takeaways:");
    println!("- Budget enforces per-workload concurrency limits");
    println!("- Multiple workers can process jobs concurrently");
    println!("- Budget acquisition happens before job dispatch");
    println!("- Failed jobs are tracked and can be retried");
    println!("- In-memory queue is perfect for testing (via orca-testkit)");
    println!("- For production, use PostgresQueueService with OrchestratorRuntime");
    println!("  (see postgres_runtime.rs example)");

    Ok(())
}
