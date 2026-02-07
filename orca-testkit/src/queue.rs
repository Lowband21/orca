use crate::{TestEntityId, TestJob, TestJobKind};
use async_trait::async_trait;
use chrono::Utc;
use orca::*;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone)]
pub struct InMemoryQueueService {
    queues: Arc<Mutex<HashMap<TestJobKind, VecDeque<QueuedJob>>>>,
    jobs: Arc<Mutex<HashMap<JobId, QueuedJob>>>,
    next_id: Arc<Mutex<u64>>,
}

#[derive(Clone, Debug)]
struct QueuedJob {
    job_id: JobId,
    entity_id: TestEntityId,
    job: TestJob,
    priority: JobPriority,
    enqueued_at: chrono::DateTime<Utc>,
    state: JobState,
    lease_owner: Option<String>,
    lease_expires_at: Option<chrono::DateTime<Utc>>,
    lease_renewals: u32,
    deps_released: bool,
}

impl InMemoryQueueService {
    pub fn new() -> Self {
        Self {
            queues: Arc::new(Mutex::new(HashMap::new())),
            jobs: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
        }
    }

    fn generate_id(&self) -> JobId {
        let id = *self.next_id.lock();
        *self.next_id.lock() = id + 1;
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        id.hash(&mut hasher);
        JobId(Uuid::new_v4())
    }
}

impl Default for InMemoryQueueService {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QueueService<TestJob> for InMemoryQueueService {
    async fn enqueue(
        &self,
        entity_id: TestEntityId,
        job: TestJob,
        priority: JobPriority,
    ) -> anyhow::Result<JobHandle> {
        let job_id = self.generate_id();
        let dedupe_key = job.dedupe_key();
        let kind = job.kind();

        let queued = QueuedJob {
            job_id,
            entity_id,
            job,
            priority,
            enqueued_at: Utc::now(),
            state: JobState::Ready,
            lease_owner: None,
            lease_expires_at: None,
            lease_renewals: 0,
            deps_released: false,
        };

        let mut jobs = self.jobs.lock();
        let mut queues = self.queues.lock();

        queues
            .entry(kind)
            .or_insert_with(VecDeque::new)
            .push_back(queued.clone());
        jobs.insert(job_id, queued);

        Ok(JobHandle {
            id: job_id,
            priority,
            dedupe_key,
            accepted: true,
        })
    }

    async fn enqueue_many(
        &self,
        requests: Vec<(TestEntityId, TestJob, JobPriority)>,
    ) -> anyhow::Result<Vec<JobHandle>> {
        let mut handles = Vec::new();
        for (entity_id, job, priority) in requests {
            handles.push(self.enqueue(entity_id, job, priority).await?);
        }
        Ok(handles)
    }

    async fn dequeue(
        &self,
        request: DequeueRequest<TestJobKind, TestEntityId>,
    ) -> anyhow::Result<Option<JobLease<TestJob>>> {
        let mut queues = self.queues.lock();

        if let Some(queue) = queues.get_mut(&request.kind) {
            if let Some(queued) = queue.pop_front() {
                let mut jobs = self.jobs.lock();
                let job_id = queued.job_id;

                let now = Utc::now();
                let lease_id = LeaseId::new();

                let job_entry = jobs.get_mut(&job_id).unwrap();
                job_entry.state = JobState::Leased;
                job_entry.lease_owner = Some(request.worker_id.clone());
                job_entry.lease_expires_at = Some(now + request.lease_ttl);
                job_entry.lease_renewals = 0;

                let job = job_entry.job.clone();

                return Ok(Some(JobLease {
                    job_id,
                    lease_id,
                    job,
                    worker_id: request.worker_id,
                    expires_at: now + request.lease_ttl,
                    renewals: 0,
                }));
            }
        }

        Ok(None)
    }

    async fn complete(&self, lease_id: LeaseId) -> anyhow::Result<()> {
        let mut jobs = self.jobs.lock();
        let lease_str = format!("{}", lease_id);

        for job in jobs.values_mut() {
            if let Some(owner) = &job.lease_owner {
                if owner == &lease_str && job.state == JobState::Leased {
                    job.state = JobState::Completed;
                    job.lease_owner = None;
                    job.lease_expires_at = None;
                    return Ok(());
                }
            }
        }

        anyhow::bail!("Lease not found: {}", lease_id)
    }

    async fn fail(
        &self,
        lease_id: LeaseId,
        _retryable: bool,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let mut jobs = self.jobs.lock();

        for job in jobs.values_mut() {
            if job
                .lease_owner
                .as_ref()
                .map(|owner| {
                    let lease_str = format!("{}", lease_id);
                    owner == &lease_str || owner.starts_with(&lease_str)
                })
                .unwrap_or(false)
            {
                if let Some(err) = error {
                    tracing::warn!("Job failed: {}", err);
                }
                job.state = JobState::Failed;
                job.lease_owner = None;
                job.lease_expires_at = None;
                return Ok(());
            }
        }

        anyhow::bail!("Lease not found: {}", lease_id)
    }

    async fn dead_letter(
        &self,
        lease_id: LeaseId,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        let mut jobs = self.jobs.lock();

        for job in jobs.values_mut() {
            if job
                .lease_owner
                .as_ref()
                .map(|owner| {
                    let lease_str = format!("{}", lease_id);
                    owner == &lease_str || owner.starts_with(&lease_str)
                })
                .unwrap_or(false)
            {
                if let Some(err) = error {
                    tracing::warn!("Job dead-lettered: {}", err);
                }
                job.state = JobState::DeadLetter;
                job.lease_owner = None;
                job.lease_expires_at = None;
                return Ok(());
            }
        }

        anyhow::bail!("Lease not found: {}", lease_id)
    }

    async fn renew(
        &self,
        renewal: LeaseRenewal,
    ) -> anyhow::Result<JobLease<TestJob>> {
        let mut jobs = self.jobs.lock();

        for job in jobs.values_mut() {
            let lease_str = format!("{}", renewal.lease_id);
            if job
                .lease_owner
                .as_ref()
                .map(|owner| owner == &lease_str || owner.starts_with(&lease_str))
                .unwrap_or(false)
                && job.lease_owner.as_ref() == Some(&renewal.worker_id)
            {
                job.lease_renewals += 1;
                let new_expires = Utc::now() + renewal.extend_by;
                job.lease_expires_at = Some(new_expires);

                return Ok(JobLease {
                    job_id: job.job_id,
                    lease_id: renewal.lease_id,
                    job: job.job.clone(),
                    worker_id: renewal.worker_id.clone(),
                    expires_at: new_expires,
                    renewals: job.lease_renewals,
                });
            }
        }

        anyhow::bail!("Lease not found")
    }

    async fn queue_depth(&self, kind: TestJobKind) -> anyhow::Result<usize> {
        let queues = self.queues.lock();
        let depth = queues.get(&kind).map_or(0, |q| q.len());
        Ok(depth)
    }

    async fn cancel_job(&self, _job_id: JobId) -> anyhow::Result<()> {
        Ok(())
    }

    async fn release_dependency(
        &self,
        _entity_id: TestEntityId,
        _dependency_key: &DependencyKey,
    ) -> anyhow::Result<u64> {
        Ok(0)
    }
}
