use async_trait::async_trait;
use orca::*;
use orca::runtime::{DispatchStatus, JobDispatcher};
use parking_lot::Mutex;
use std::sync::Arc;

use crate::TestJob;

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
            result: Arc::new(Mutex::new(DispatchStatus::Success)),
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
    ) -> DispatchStatus {
        let record = DispatchRecord {
            lease_id: lease.lease_id,
            job_id: lease.job_id,
            job_kind: lease.job.kind().to_string(),
        };
        self.dispatches.lock().push(record);
        self.result.lock().clone()
    }
}

