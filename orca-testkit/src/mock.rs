use async_trait::async_trait;
use async_trait::async_trait;
use orca::*;
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Clone)]
pub struct MockDispatcher {
    dispatches: Arc<Mutex<Vec<DispatchRecord>>>,
    result: Arc<Mutex<DispatchStatus>>,
}

#[derive(Clone, Debug)]
pub struct DispatchRecord {
    pub lease_id: LeaseId,
    pub job_id: JobId,
    pub job_kind: String,
}

impl MockDispatcher {
    pub fn new() -> Self {
        Self {
            dispatches: Arc::new(Mutex::new(Vec::new())),
            result: Arc::new(Mutex::new(DispatchStatus::success())),
        }
    }

    pub fn with_result(result: DispatchStatus) -> Self {
        Self {
            dispatches: Arc::new(Mutex::new(Vec::new())),
            result: Arc::new(Mutex::new(result)),
        }
    }

    pub fn record(&self) -> Vec<DispatchRecord> {
        self.dispatches.lock().clone()
    }

    pub fn assert_dispatch_count_eq(&self, expected: usize) {
        assert_eq!(
            self.dispatches.lock().len(),
            expected,
            "Expected {} dispatches, got {}",
            expected,
            self.dispatches.lock().len()
        );
    }

    pub fn set_result(&self, result: DispatchStatus) {
        *self.result.lock() = result;
    }

    pub fn clear(&self) {
        self.dispatches.lock().clear();
    }
}

impl Default for MockDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl JobDispatcher<TestJob> for MockDispatcher {
    async fn dispatch(
        &self,
        lease: &JobLease<TestJob>,
    ) -> anyhow::Result<DispatchStatus> {
        let record = DispatchRecord {
            lease_id: lease.lease_id,
            job_id: lease.job.id(),
            job_kind: lease.job.kind().to_string(),
        };
        self.dispatches.lock().push(record);
        Ok(*self.result.lock())
    }
}

#[derive(Clone, Debug)]
pub struct DispatchRecord {
    pub lease_id: LeaseId,
    pub job_id: JobId,
    pub job_kind: String,
}

impl MockDispatcher {
    pub fn new(result: DispatchStatus) -> Self {
        Self {
            dispatches: Arc::new(Mutex::new(Vec::new())),
            result: Arc::new(Mutex::new(result)),
        }
    }

    pub fn record(&self) -> Vec<DispatchRecord> {
        self.dispatches.lock().clone()
    }

    pub fn assert_dispatch_count_eq(&self, expected: usize) {
        assert_eq!(
            self.dispatches.lock().len(),
            expected,
            "Expected {} dispatches, got {}",
            expected,
            self.dispatches.lock().len()
        );
    }

    pub fn set_result(&self, result: DispatchStatus) {
        *self.result.lock() = result;
    }
}

impl Default for MockDispatcher {
    fn default() -> Self {
        Self::new(DispatchStatus::Success)
    }
}

#[async_trait]
impl<J> JobDispatcher for MockDispatcher
where
    J: Job + 'static,
{
    async fn dispatch(&self, lease: &JobLease<J>) -> DispatchStatus {
        let record = DispatchRecord {
            lease_id: lease.lease_id,
            job_id: lease.job.id,
            job_kind: format!("{:?}", lease.job.kind()),
        };
        self.dispatches.lock().push(record);
        *self.result.lock()
    }
}

pub struct TestBudget;

impl<W> WorkloadBudget<W> for TestBudget {
    async fn acquire(
        &self,
        _workload: W,
        _count: usize,
    ) -> anyhow::Result<BudgetTokens> {
        Ok(BudgetTokens {
            tokens: 100,
            acquired_at: chrono::Utc::now(),
        })
    }

    async fn release(&self, _tokens: BudgetTokens) -> anyhow::Result<()> {
        Ok(())
    }
}
