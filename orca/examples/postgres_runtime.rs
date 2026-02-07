//! PostgreSQL runtime example demonstrating persistent job processing.
//!
//! This example shows how to use PostgresQueueService with a PostgreSQL backend
//! for durable job storage, leasing, and processing.
//!
//! # Prerequisites
//!
//! 1. PostgreSQL server running locally or accessible via network
//! 2. Database created: `createdb orca_example`
//! 3. Schema applied: `psql orca_example -f orca/migrations/001_initial_schema.sql`
//!
//! # Running the Example
//!
//! ```bash
//! # Set the database URL
//! export DATABASE_URL="postgres://username:password@localhost/orca_example"
//!
//! # Or for local development without password:
//! export DATABASE_URL="postgres://localhost/orca_example"
//!
//! # Run the example
//! cargo run --example postgres_runtime --features postgres
//! ```

use std::collections::HashMap;
use std::env;
use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use orca::*;
use orca::persistence::PostgresQueueService;
use orca::runtime::{DispatchStatus, JobDispatcher};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Entity ID type for forensic case identification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
struct CaseId(Uuid);

impl EntityId for CaseId {
    fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Display for CaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Job kind variants for forensic artifact processing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
enum ForensicJobKind {
    ParseArtifact,
    CorrelateTimeline,
    GenerateReport,
}

impl JobKind for ForensicJobKind {
    fn as_str(&self) -> &'static str {
        match self {
            ForensicJobKind::ParseArtifact => "parse_artifact",
            ForensicJobKind::CorrelateTimeline => "correlate_timeline",
            ForensicJobKind::GenerateReport => "generate_report",
        }
    }
}

impl Display for ForensicJobKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Workload categories for budget management in forensic processing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum ForensicWorkload {
    ArtifactParsing,
    TimelineCorrelation,
    ReportGeneration,
}

impl WorkloadKind for ForensicWorkload {}

/// Job enum with different forensic processing tasks.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum ForensicJob {
    ParseArtifact {
        case_id: CaseId,
        evidence_id: Uuid,
        artifact_type: String,
        evidence_path: String,
    },
    CorrelateTimeline {
        case_id: CaseId,
        start_time: String,
        end_time: String,
    },
    GenerateReport {
        case_id: CaseId,
        report_format: String,
    },
}

impl Job for ForensicJob {
    type EntityId = CaseId;
    type Kind = ForensicJobKind;
    type WorkloadKind = ForensicWorkload;
    type DomainEvent = ();

    fn entity_id(&self) -> Self::EntityId {
        match self {
            ForensicJob::ParseArtifact { case_id, .. } => *case_id,
            ForensicJob::CorrelateTimeline { case_id, .. } => *case_id,
            ForensicJob::GenerateReport { case_id, .. } => *case_id,
        }
    }

    fn kind(&self) -> Self::Kind {
        match self {
            ForensicJob::ParseArtifact { .. } => ForensicJobKind::ParseArtifact,
            ForensicJob::CorrelateTimeline { .. } => ForensicJobKind::CorrelateTimeline,
            ForensicJob::GenerateReport { .. } => ForensicJobKind::GenerateReport,
        }
    }

    fn dedupe_key(&self) -> String {
        match self {
            ForensicJob::ParseArtifact { evidence_id, .. } => {
                format!("parse:{}:{}", self.entity_id(), evidence_id)
            }
            ForensicJob::CorrelateTimeline { case_id, start_time, .. } => {
                format!("correlate:{}:{}", case_id, start_time)
            }
            ForensicJob::GenerateReport { case_id, report_format } => {
                format!("report:{}:{}", case_id, report_format)
            }
        }
    }

    fn priority(&self) -> JobPriority {
        match self {
            ForensicJob::ParseArtifact { .. } => JobPriority::P0,
            ForensicJob::CorrelateTimeline { .. } => JobPriority::P1,
            ForensicJob::GenerateReport { .. } => JobPriority::P2,
        }
    }
}

/// Dispatcher that simulates forensic artifact processing.
struct ForensicDispatcher;

#[async_trait]
impl JobDispatcher<ForensicJob> for ForensicDispatcher {
    async fn dispatch(&self, lease: &JobLease<ForensicJob>) -> DispatchStatus {
        match &lease.job {
            ForensicJob::ParseArtifact { case_id, evidence_id, artifact_type, evidence_path } => {
                println!(
                    "[FORENSICS] Parsing artifact '{}' from evidence {} in case {}",
                    artifact_type, evidence_id, case_id
                );
                
                tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
                
                if artifact_type.contains("corrupt") {
                    println!("[FORENSICS] Artifact is corrupted, marking for retry");
                    return DispatchStatus::RetryableFailure {
                        error: Some("Corrupted artifact file".to_string()),
                    };
                }
                
                println!("[FORENSICS] Successfully parsed {}", evidence_path);
                DispatchStatus::Success
            }
            ForensicJob::CorrelateTimeline { case_id, start_time, end_time } => {
                println!(
                    "[FORENSICS] Correlating timeline for case {} from {} to {}",
                    case_id, start_time, end_time
                );
                
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                
                println!("[FORENSICS] Timeline correlation complete for case {}", case_id);
                DispatchStatus::Success
            }
            ForensicJob::GenerateReport { case_id, report_format } => {
                println!(
                    "[FORENSICS] Generating {} report for case {}",
                    report_format, case_id
                );
                
                tokio::time::sleep(tokio::time::Duration::from_millis(800)).await;
                
                println!("[FORENSICS] Report generation complete for case {}", case_id);
                DispatchStatus::Success
            }
        }
    }
}

/// Manual worker with budget and lease management for PostgreSQL queue.
async fn run_worker_with_budget(
    worker_id: String,
    queue: Arc<PostgresQueueService<ForensicJob>>,
    budget: Arc<InMemoryBudget<ForensicWorkload, CaseId>>,
    dispatcher: Arc<ForensicDispatcher>,
    kind: ForensicJobKind,
    workload: ForensicWorkload,
) -> anyhow::Result<u64> {
    let mut processed = 0u64;
    let lease_ttl = chrono::Duration::seconds(60);
    
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
    println!("=== Orca PostgreSQL Runtime Example ===\n");
    println!("This example demonstrates:");
    println!("- PostgreSQL-backed job queue with durable storage");
    println!("- SKIP LOCKED for concurrent job dequeuing");
    println!("- Budget-based concurrency limiting");
    println!("- Manual worker pools with lease management\n");

    // Get database URL from environment
    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/orca_example".to_string());

    println!("Connecting to PostgreSQL...");
    println!("Database URL: {}\n", database_url);

    // Create connection pool
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Create PostgreSQL queue service with entity scope
    let queue: Arc<PostgresQueueService<ForensicJob>> = Arc::new(
        PostgresQueueService::new(pool.clone(), "forensics")
    );

    // Create budget with limits per workload type
    let mut limits = HashMap::new();
    limits.insert(ForensicWorkload::ArtifactParsing, 4);
    limits.insert(ForensicWorkload::TimelineCorrelation, 2);
    limits.insert(ForensicWorkload::ReportGeneration, 1);
    
    let budget = Arc::new(InMemoryBudget::new(
        BudgetConfig::new(limits, 2)
    ));

    // Create dispatcher
    let dispatcher = Arc::new(ForensicDispatcher);

    // Enqueue artifact parsing jobs
    println!("1. Enqueueing artifact parsing jobs...");
    let case_id = CaseId(Uuid::new_v4());
    
    for i in 0..6 {
        let artifact_type = if i == 3 { "corrupt_registry".to_string() } else { 
            format!("user_activity_{}", i) 
        };
        
        let job = ForensicJob::ParseArtifact {
            case_id,
            evidence_id: Uuid::new_v4(),
            artifact_type,
            evidence_path: format!("/evidence/case_{}/artifacts/artifact_{}.evtx", case_id.0, i),
        };
        
        let handle = queue
            .enqueue(job.entity_id(), job, JobPriority::P0)
            .await?;
        println!("   Enqueued parse job for artifact {} (id={})", i, handle.id);
    }

    // Enqueue timeline correlation jobs
    println!("\n2. Enqueueing timeline correlation jobs...");
    for i in 0..2 {
        let job = ForensicJob::CorrelateTimeline {
            case_id,
            start_time: "2024-01-01T00:00:00Z".to_string(),
            end_time: "2024-01-31T23:59:59Z".to_string(),
        };
        
        let handle = queue
            .enqueue(job.entity_id(), job, JobPriority::P1)
            .await?;
        println!("   Enqueued correlation job {} (id={})", i, handle.id);
    }

    // Enqueue report generation jobs
    println!("\n3. Enqueueing report generation jobs...");
    let formats = vec!["pdf", "html", "json"];
    for format in formats {
        let job = ForensicJob::GenerateReport {
            case_id,
            report_format: format.to_string(),
        };
        
        let handle = queue
            .enqueue(job.entity_id(), job, JobPriority::P2)
            .await?;
        println!("   Enqueued {} report generation (id={})", format, handle.id);
    }

    // Check queue depths
    let parse_depth = queue.queue_depth(ForensicJobKind::ParseArtifact).await?;
    let correlate_depth = queue.queue_depth(ForensicJobKind::CorrelateTimeline).await?;
    let report_depth = queue.queue_depth(ForensicJobKind::GenerateReport).await?;
    
    println!("\n4. Initial queue depths:");
    println!("   Artifact parsing jobs: {}", parse_depth);
    println!("   Timeline correlation jobs: {}", correlate_depth);
    println!("   Report generation jobs: {}", report_depth);

    // Process jobs with concurrent workers
    println!("\n5. Starting workers...\n");
    
    // Spawn multiple workers for each job kind
    let parse_workers: Vec<_> = (0..4).map(|i| {
        tokio::spawn(run_worker_with_budget(
            format!("parse-worker-{}", i),
            queue.clone(),
            budget.clone(),
            dispatcher.clone(),
            ForensicJobKind::ParseArtifact,
            ForensicWorkload::ArtifactParsing,
        ))
    }).collect();
    
    let correlate_workers: Vec<_> = (0..2).map(|i| {
        tokio::spawn(run_worker_with_budget(
            format!("correlate-worker-{}", i),
            queue.clone(),
            budget.clone(),
            dispatcher.clone(),
            ForensicJobKind::CorrelateTimeline,
            ForensicWorkload::TimelineCorrelation,
        ))
    }).collect();
    
    let report_workers: Vec<_> = (0..1).map(|i| {
        tokio::spawn(run_worker_with_budget(
            format!("report-worker-{}", i),
            queue.clone(),
            budget.clone(),
            dispatcher.clone(),
            ForensicJobKind::GenerateReport,
            ForensicWorkload::ReportGeneration,
        ))
    }).collect();
    
    // Wait for all workers to complete
    let mut parse_total = 0u64;
    for worker in parse_workers {
        parse_total += worker.await??;
    }
    
    let mut correlate_total = 0u64;
    for worker in correlate_workers {
        correlate_total += worker.await??;
    }
    
    let mut report_total = 0u64;
    for worker in report_workers {
        report_total += worker.await??;
    }

    println!("\n6. Processing complete:");
    println!("   Artifact parsing jobs: {}", parse_total);
    println!("   Timeline correlation jobs: {}", correlate_total);
    println!("   Report generation jobs: {}", report_total);
    println!("   Total jobs processed: {}\n", parse_total + correlate_total + report_total);

    // Check budget utilization
    let (parse_current, parse_limit) = budget.utilization(ForensicWorkload::ArtifactParsing).await?;
    let (correlate_current, correlate_limit) = budget.utilization(ForensicWorkload::TimelineCorrelation).await?;
    let (report_current, report_limit) = budget.utilization(ForensicWorkload::ReportGeneration).await?;
    
    println!("7. Budget utilization:");
    println!("   Artifact parsing: {}/{} (limit: 4)", parse_current, parse_limit);
    println!("   Timeline correlation: {}/{} (limit: 2)", correlate_current, correlate_limit);
    println!("   Report generation: {}/{} (limit: 1)\n", report_current, report_limit);

    // Check final queue depths
    let parse_depth = queue.queue_depth(ForensicJobKind::ParseArtifact).await?;
    let correlate_depth = queue.queue_depth(ForensicJobKind::CorrelateTimeline).await?;
    let report_depth = queue.queue_depth(ForensicJobKind::GenerateReport).await?;
    
    println!("8. Final queue depths:");
    println!("   Artifact parsing jobs: {} (may have retryable jobs)", parse_depth);
    println!("   Timeline correlation jobs: {}", correlate_depth);
    println!("   Report generation jobs: {}", report_depth);

    println!("\n=== Example Complete ===");
    println!("\nKey takeaways:");
    println!("- PostgresQueueService provides durable job storage with SKIP LOCKED");
    println!("- Jobs survive application restarts and are persisted in PostgreSQL");
    println!("- Lease management ensures jobs don't get stuck if workers crash");
    println!("- Failed jobs can be retried based on dispatcher return status");
    println!("- Budget enforces per-workload concurrency limits");
    println!("\nDatabase schema used:");
    println!("  - orca_jobs table with JSONB payloads");
    println!("  - SKIP LOCKED for concurrent dequeue");
    println!("  - Partial indexes for efficient queries");
    println!("  - Lease management with expiry and renewal");

    Ok(())
}
