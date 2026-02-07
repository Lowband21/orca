use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A token representing acquired budget for a specific workload and entity.
///
/// When budget is successfully acquired via [`Budget::try_acquire`] or [`Budget::acquire`],
/// a `BudgetToken` is returned. The token must be released via [`Budget::release`]
/// when the workload completes to return capacity to the budget pool.
#[derive(Debug)]
pub struct BudgetToken<W, E> {
    /// The workload type this token was acquired for.
    pub workload: W,
    /// The entity ID associated with this budget acquisition.
    pub entity_id: E,
    /// Timestamp when the budget was acquired.
    pub acquired_at: DateTime<Utc>,
}

impl<W, E> BudgetToken<W, E> {
    /// Create a new budget token
    pub fn new(workload: W, entity_id: E) -> Self {
        Self {
            workload,
            entity_id,
            acquired_at: Utc::now(),
        }
    }
}

/// Configuration for workload budgets
/// Maps each workload type to its concurrency limit
#[derive(Debug, Clone)]
pub struct BudgetConfig<W> {
    /// Per-workload limits
    limits: HashMap<W, usize>,
    /// Default limit for workloads not explicitly configured
    default_limit: usize,
}

impl<W: Eq + Hash + Clone> BudgetConfig<W> {
    /// Create a new budget configuration
    pub fn new(limits: HashMap<W, usize>, default_limit: usize) -> Self {
        Self {
            limits,
            default_limit,
        }
    }

    /// Create a new budget configuration with only a default limit
    pub fn with_default(default_limit: usize) -> Self {
        Self {
            limits: HashMap::new(),
            default_limit,
        }
    }

    /// Add a limit for a specific workload
    pub fn with_limit(mut self, workload: W, limit: usize) -> Self {
        self.limits.insert(workload, limit);
        self
    }

    /// Get the limit for a workload
    pub fn limit(&self, workload: &W) -> usize {
        self.limits
            .get(workload)
            .copied()
            .unwrap_or(self.default_limit)
    }
}

impl<W: Eq + Hash> Default for BudgetConfig<W> {
    fn default() -> Self {
        Self {
            limits: HashMap::new(),
            default_limit: 1,
        }
    }
}

/// Trait for managing workload budgets with backpressure
#[async_trait]
pub trait Budget<W, E>: Send + Sync
where
    W: WorkloadKind,
    E: EntityId,
{
    /// Try to acquire a budget token for the given workload and entity
    /// Returns None if budget is at capacity
    async fn try_acquire(
        &self,
        workload: W,
        entity_id: E,
    ) -> anyhow::Result<Option<Arc<BudgetToken<W, E>>>>;

    /// Acquire a budget token, spinning until budget is available
    async fn acquire(
        &self,
        workload: W,
        entity_id: E,
    ) -> anyhow::Result<Arc<BudgetToken<W, E>>>;

    /// Release a budget token back to the pool
    async fn release(
        &self,
        token: Arc<BudgetToken<W, E>>,
    ) -> anyhow::Result<()>;

    /// Get current budget utilization for a workload type
    /// Returns (current, limit)
    async fn utilization(&self, workload: W) -> anyhow::Result<(usize, usize)>;

    /// Check if budget is available without acquiring
    async fn has_budget(&self, workload: W) -> anyhow::Result<bool>;
}

// Re-export from job module to maintain single definition
pub use super::job::{EntityId, WorkloadKind};

/// Default in-memory implementation of Budget
/// Tracks per-workload counts using tokio::Mutex for thread safety
pub struct InMemoryBudget<W, E> {
    config: BudgetConfig<W>,
    /// Per-workload counters
    counters: Arc<Mutex<HashMap<W, usize>>>,
    _phantom: std::marker::PhantomData<E>,
}

impl<W, E> std::fmt::Debug for InMemoryBudget<W, E>
where
    W: WorkloadKind,
    E: EntityId,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("InMemoryBudget");

        match self.counters.try_lock() {
            Ok(counters) => {
                debug.field("counters", &*counters);
            }
            Err(_) => {
                debug.field("counters", &"<locked>");
            }
        }

        debug.finish_non_exhaustive()
    }
}

impl<W: WorkloadKind, E: EntityId> InMemoryBudget<W, E> {
    /// Create a new in-memory budget
    pub fn new(config: BudgetConfig<W>) -> Self {
        Self {
            config,
            counters: Arc::new(Mutex::new(HashMap::new())),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the current count for a workload
    async fn current_count(&self, workload: W) -> usize {
        let counters = self.counters.lock().await;
        counters.get(&workload).copied().unwrap_or(0)
    }

    /// Increment the counter for a workload
    async fn increment(&self, workload: W) {
        let mut counters = self.counters.lock().await;
        *counters.entry(workload).or_insert(0) += 1;
    }

    /// Decrement the counter for a workload
    async fn decrement(&self, workload: W) {
        let mut counters = self.counters.lock().await;
        if let Some(count) = counters.get_mut(&workload) {
            *count = count.saturating_sub(1);
        }
    }
}

#[async_trait]
impl<W, E> Budget<W, E> for InMemoryBudget<W, E>
where
    W: WorkloadKind,
    E: EntityId,
{
    async fn try_acquire(
        &self,
        workload: W,
        entity_id: E,
    ) -> anyhow::Result<Option<Arc<BudgetToken<W, E>>>> {
        let limit = self.config.limit(&workload);
        let current = self.current_count(workload).await;

        if current < limit {
            self.increment(workload).await;
            Ok(Some(Arc::new(BudgetToken {
                workload,
                entity_id,
                acquired_at: Utc::now(),
            })))
        } else {
            Ok(None)
        }
    }

    async fn acquire(
        &self,
        workload: W,
        entity_id: E,
    ) -> anyhow::Result<Arc<BudgetToken<W, E>>> {
        // Spin with 100ms sleep until budget is available
        loop {
            if let Some(token) = self.try_acquire(workload, entity_id).await? {
                return Ok(token);
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }

    async fn release(
        &self,
        token: Arc<BudgetToken<W, E>>,
    ) -> anyhow::Result<()> {
        self.decrement(token.workload).await;
        Ok(())
    }

    async fn utilization(&self, workload: W) -> anyhow::Result<(usize, usize)> {
        let current = self.current_count(workload).await;
        let limit = self.config.limit(&workload);
        Ok((current, limit))
    }

    async fn has_budget(&self, workload: W) -> anyhow::Result<bool> {
        let (current, limit) = self.utilization(workload).await?;
        Ok(current < limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct TestWorkload(u8);

    impl WorkloadKind for TestWorkload {}

    #[derive(
        Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize,
    )]
    struct TestEntityId(u64);

    impl std::fmt::Display for TestEntityId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl EntityId for TestEntityId {
        fn as_uuid(&self) -> uuid::Uuid {
            uuid::Uuid::from_u128(self.0 as u128)
        }
    }

    #[tokio::test]
    async fn test_budget_enforces_concurrency_limits() {
        // Create InMemoryBudget with limit=2 for TestWorkload
        let config = BudgetConfig::new(HashMap::new(), 2);
        let budget = InMemoryBudget::<TestWorkload, TestEntityId>::new(config);

        let workload = TestWorkload(1);

        // Acquire token 1
        let token1 = budget.acquire(workload, TestEntityId(1)).await.unwrap();

        // Acquire token 2
        let token2 = budget
            .try_acquire(workload, TestEntityId(2))
            .await
            .unwrap()
            .unwrap();

        // Verify utilization
        let (current, limit) = budget.utilization(workload).await.unwrap();
        assert_eq!(current, 2);
        assert_eq!(limit, 2);

        // try_acquire token 3 should return None
        let token3 =
            budget.try_acquire(workload, TestEntityId(3)).await.unwrap();
        assert!(token3.is_none(), "Should return None when at capacity");

        // Verify has_budget returns false
        assert!(!budget.has_budget(workload).await.unwrap());

        // Release token 1
        budget.release(token1).await.unwrap();

        // Now try_acquire token 3 should succeed
        let token3 = budget
            .try_acquire(workload, TestEntityId(3))
            .await
            .unwrap()
            .unwrap();

        // Verify utilization is back to 2
        let (current, _) = budget.utilization(workload).await.unwrap();
        assert_eq!(current, 2);

        // Cleanup
        budget.release(token2).await.unwrap();
        budget.release(token3).await.unwrap();

        // Verify all released
        let (current, _) = budget.utilization(workload).await.unwrap();
        assert_eq!(current, 0);
    }

    #[tokio::test]
    async fn test_budget_per_workload_limits() {
        let workload_a = TestWorkload(1);
        let workload_b = TestWorkload(2);

        let mut limits = HashMap::new();
        limits.insert(workload_a, 1);
        limits.insert(workload_b, 3);

        let config = BudgetConfig::new(limits, 2);
        let budget = InMemoryBudget::<TestWorkload, TestEntityId>::new(config);

        // Workload A: limit=1
        let token_a1 = budget
            .try_acquire(workload_a, TestEntityId(1))
            .await
            .unwrap()
            .unwrap();
        let token_a2 = budget
            .try_acquire(workload_a, TestEntityId(2))
            .await
            .unwrap();
        assert!(token_a2.is_none()); // At capacity for A

        // Workload B: limit=3
        let token_b1 = budget
            .try_acquire(workload_b, TestEntityId(10))
            .await
            .unwrap()
            .unwrap();
        let token_b2 = budget
            .try_acquire(workload_b, TestEntityId(11))
            .await
            .unwrap()
            .unwrap();
        let token_b3 = budget
            .try_acquire(workload_b, TestEntityId(12))
            .await
            .unwrap()
            .unwrap();
        let token_b4 = budget
            .try_acquire(workload_b, TestEntityId(13))
            .await
            .unwrap();
        assert!(token_b4.is_none()); // At capacity for B

        // Cleanup
        budget.release(token_a1).await.unwrap();
        budget.release(token_b1).await.unwrap();
        budget.release(token_b2).await.unwrap();
        budget.release(token_b3).await.unwrap();
    }

    #[tokio::test]
    async fn test_saturating_sub_safety() {
        let config = BudgetConfig::new(HashMap::new(), 1);
        let budget = InMemoryBudget::<TestWorkload, TestEntityId>::new(config);

        let workload = TestWorkload(1);
        let entity_id = TestEntityId(1);

        // Create token without going through acquire (simulates a bug or misuse)
        let token = Arc::new(BudgetToken::new(workload, entity_id));

        // Release without ever acquiring - should not panic
        budget.release(token.clone()).await.unwrap();

        // Verify count is still 0 (saturating_sub worked)
        let (current, _) = budget.utilization(workload).await.unwrap();
        assert_eq!(current, 0);
    }
}
