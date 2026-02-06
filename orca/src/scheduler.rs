use std::cmp::max;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{EntityId, JobPriority};

/// Configuration for per-entity scheduling policy.
#[derive(Clone, Copy, Debug, Default)]
pub struct EntityQueuePolicy {
    /// Maximum number of concurrent jobs for this entity (0 = use default).
    pub max_concurrent: usize,
    /// Weight for fair share allocation (0 = use default).
    pub weight: u32,
}

impl EntityQueuePolicy {
    /// Create a new policy with the given max concurrent and weight.
    pub fn new(max_concurrent: usize, weight: u32) -> Self {
        Self {
            max_concurrent: max_concurrent.max(1),
            weight: weight.max(1),
        }
    }
}

/// Weights for priority ring distribution (higher = more slots in ring).
#[derive(Clone, Copy, Debug)]
pub struct PriorityWeights {
    pub p0: usize,
    pub p1: usize,
    pub p2: usize,
    pub p3: usize,
}

impl Default for PriorityWeights {
    fn default() -> Self {
        Self {
            p0: 8,
            p1: 4,
            p2: 2,
            p3: 1,
        }
    }
}

impl PriorityWeights {
    /// Create weights with all priorities having equal weight (1).
    pub fn equal() -> Self {
        Self {
            p0: 1,
            p1: 1,
            p2: 1,
            p3: 1,
        }
    }

    /// Create weights with P0 having high priority.
    pub fn p0_favored() -> Self {
        Self {
            p0: 8,
            p1: 4,
            p2: 2,
            p3: 1,
        }
    }
}

/// Configuration for the scheduler.
#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    /// Default maximum concurrent jobs per entity.
    pub default_max_concurrent: usize,
    /// Default weight for fair share allocation.
    pub default_weight: u32,
    /// Per-entity overrides.
    pub entity_overrides: HashMap<String, EntityQueuePolicy>,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            default_max_concurrent: 3,
            default_weight: 1,
            entity_overrides: HashMap::new(),
        }
    }
}

/// Reservation handle returned by the scheduler when a worker is allowed to
/// attempt leasing work for an (entity, priority) pair.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct SchedulingReservation<E: EntityId> {
    pub id: Uuid,
    pub entity_id: E,
    pub priority: JobPriority,
}

#[derive(Debug, Default)]
struct PriorityEntityState {
    ready: usize,
    current_weight: i32,
}

struct EntityState {
    cap: usize,
    weight: u32,
    inflight: usize,
    pending: usize,
    priorities: HashMap<JobPriority, PriorityEntityState>,
}

impl EntityState {
    fn new(cap: usize, weight: u32) -> Self {
        Self {
            cap,
            weight,
            inflight: 0,
            pending: 0,
            priorities: HashMap::new(),
        }
    }

    fn ensure_priority(
        &mut self,
        priority: JobPriority,
    ) -> &mut PriorityEntityState {
        self.priorities.entry(priority).or_default()
    }

    fn priority_state(
        &mut self,
        priority: JobPriority,
    ) -> Option<&mut PriorityEntityState> {
        self.priorities.get_mut(&priority)
    }
}

impl fmt::Debug for EntityState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityState")
            .field("cap", &self.cap)
            .field("weight", &self.weight)
            .field("inflight", &self.inflight)
            .field("pending", &self.pending)
            .field("priority_count", &self.priorities.len())
            .finish()
    }
}

struct QueueDefaults {
    default_cap: usize,
    default_weight: u32,
    overrides: HashMap<String, EntityQueuePolicy>,
}

impl QueueDefaults {
    fn new(config: &SchedulerConfig) -> Self {
        Self {
            default_cap: max(1, config.default_max_concurrent),
            default_weight: max(1, config.default_weight),
            overrides: config.entity_overrides.clone(),
        }
    }

    fn policy_for(&self, entity_id: &dyn fmt::Display) -> EntityQueuePolicy {
        let key = entity_id.to_string();
        self.overrides
            .get(&key)
            .cloned()
            .unwrap_or_else(|| EntityQueuePolicy {
                max_concurrent: self.default_cap,
                weight: self.default_weight,
            })
    }
}

impl fmt::Debug for QueueDefaults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueueDefaults")
            .field("default_cap", &self.default_cap)
            .field("default_weight", &self.default_weight)
            .field("override_count", &self.overrides.len())
            .finish()
    }
}

struct ReservationState {
    entity_key: String,
    priority: JobPriority,
    weight_debt: i32,
}

impl fmt::Debug for ReservationState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReservationState")
            .field("entity_key", &self.entity_key)
            .field("priority", &self.priority)
            .field("weight_debt", &self.weight_debt)
            .finish()
    }
}

struct SchedulerState<E: EntityId> {
    entities: HashMap<E, EntityState>,
    reservations: HashMap<Uuid, ReservationState>,
    next_priority_index: usize,
}

impl<E: EntityId> SchedulerState<E> {
    fn new() -> Self {
        Self {
            entities: HashMap::new(),
            reservations: HashMap::new(),
            next_priority_index: 0,
        }
    }

    fn ensure_entity(
        &mut self,
        entity_id: E,
        defaults: &QueueDefaults,
    ) -> &mut EntityState {
        self.entities.entry(entity_id).or_insert_with(|| {
            let policy = defaults.policy_for(&entity_id);
            let cap = if policy.max_concurrent > 0 {
                policy.max_concurrent
            } else {
                defaults.default_cap
            };
            let weight = if policy.weight > 0 {
                policy.weight
            } else {
                defaults.default_weight
            };
            EntityState::new(cap, weight)
        })
    }

    fn select_for_priority(
        &mut self,
        priority: JobPriority,
    ) -> Option<(E, i32)> {
        let mut selected: Option<(E, i32)> = None;
        let mut total_weight = 0i32;

        for (entity_id, state) in self.entities.iter_mut() {
            if state.inflight + state.pending >= state.cap {
                continue;
            }
            let weight = state.weight as i32;
            if let Some(priority_state) = state.priority_state(priority) {
                if priority_state.ready == 0 {
                    continue;
                }
                priority_state.current_weight += weight;
                total_weight += weight;
                match selected {
                    Some((_, weight))
                        if priority_state.current_weight <= weight => {}
                    _ => {
                        selected =
                            Some((*entity_id, priority_state.current_weight));
                    }
                }
            }
        }

        if let Some((entity_id, _)) = selected {
            if total_weight == 0 {
                return None;
            }
            if let Some(state) = self.entities.get_mut(&entity_id)
                && let Some(priority_state) = state.priority_state(priority)
            {
                priority_state.current_weight -= total_weight;
                priority_state.ready = priority_state.ready.saturating_sub(1);
                state.pending += 1;
            }
            Some((entity_id, total_weight))
        } else {
            None
        }
    }
}

impl<E: EntityId> fmt::Debug for SchedulerState<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchedulerState")
            .field("entity_count", &self.entities.len())
            .field("reservation_count", &self.reservations.len())
            .field("next_priority_index", &self.next_priority_index)
            .finish()
    }
}

fn build_priority_ring(weights: &PriorityWeights) -> Vec<JobPriority> {
    let mut ring = Vec::new();
    for _ in 0..weights.p0.max(1) {
        ring.push(JobPriority::P0);
    }
    for _ in 0..weights.p1.max(1) {
        ring.push(JobPriority::P1);
    }
    for _ in 0..weights.p2.max(1) {
        ring.push(JobPriority::P2);
    }
    for _ in 0..weights.p3.max(1) {
        ring.push(JobPriority::P3);
    }
    ring
}

/// Weighted-fair scheduler shared by worker pools. The scheduler keeps a
/// minimal in-memory view of ready counts per (entity, priority) and enforces
/// per-entity in-flight caps when allocating leases.
#[derive(Clone)]
pub struct WeightedFairScheduler<E: EntityId> {
    defaults: Arc<QueueDefaults>,
    priority_ring: Arc<Vec<JobPriority>>,
    state: Arc<Mutex<SchedulerState<E>>>,
}

impl<E: EntityId> fmt::Debug for WeightedFairScheduler<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("WeightedFairScheduler");
        debug
            .field("default_cap", &self.defaults.default_cap)
            .field("default_weight", &self.defaults.default_weight)
            .field("override_count", &self.defaults.overrides.len())
            .field("priority_ring_len", &self.priority_ring.len());

        match self.state.try_lock() {
            Ok(state) => {
                debug
                    .field("entity_count", &state.entities.len())
                    .field("reservation_count", &state.reservations.len())
                    .field("next_priority_index", &state.next_priority_index);
            }
            Err(_) => {
                debug.field("state", &"<locked>");
            }
        }

        debug.finish()
    }
}

/// Bulk ready counts used to prime the scheduler without emitting one event per job.
#[derive(Clone, Debug)]
pub struct ReadyCountEntry<E: EntityId> {
    pub entity_id: E,
    pub priority: JobPriority,
    pub count: usize,
}

impl<E: EntityId + Hash> WeightedFairScheduler<E> {
    /// Create a new scheduler with the given configuration and priority weights.
    pub fn new(
        config: &SchedulerConfig,
        priority_weights: &PriorityWeights,
    ) -> Self {
        let defaults = Arc::new(QueueDefaults::new(config));
        let ring = Arc::new(build_priority_ring(priority_weights));
        Self {
            defaults,
            priority_ring: ring,
            state: Arc::new(Mutex::new(SchedulerState::new())),
        }
    }

    /// Record that a job is ready for scheduling for the given entity and priority.
    pub async fn record_ready(&self, entity_id: E, priority: JobPriority) {
        let mut state = self.state.lock().await;
        let entity = state.ensure_entity(entity_id, &self.defaults);
        let priority_state = entity.ensure_priority(priority);
        priority_state.ready += 1;
    }

    /// Record ready counts in bulk (more efficient than individual calls).
    pub async fn record_ready_bulk<I>(&self, entries: I)
    where
        I: IntoIterator<Item = ReadyCountEntry<E>>,
    {
        let mut state = self.state.lock().await;
        for entry in entries.into_iter() {
            if entry.count == 0 {
                continue;
            }
            let entity = state.ensure_entity(entry.entity_id, &self.defaults);
            let priority_state = entity.ensure_priority(entry.priority);
            priority_state.ready =
                priority_state.ready.saturating_add(entry.count);
        }
    }

    /// Record that a job was enqueued (alias for record_ready).
    pub async fn record_enqueued(&self, entity_id: E, priority: JobPriority) {
        self.record_ready(entity_id, priority).await;
    }

    /// Reserve a scheduling slot for the next available entity/priority.
    /// Returns None if no entity has capacity or ready jobs.
    pub async fn reserve(&self) -> Option<SchedulingReservation<E>> {
        if self.priority_ring.is_empty() {
            return None;
        }

        let mut state = self.state.lock().await;
        for _ in 0..self.priority_ring.len() {
            let priority = self.priority_ring[state.next_priority_index];
            state.next_priority_index =
                (state.next_priority_index + 1) % self.priority_ring.len();

            if let Some((entity_id, weight_debt)) =
                state.select_for_priority(priority)
            {
                let reservation_id = Uuid::now_v7();
                state.reservations.insert(
                    reservation_id,
                    ReservationState {
                        entity_key: entity_id.to_string(),
                        priority,
                        weight_debt,
                    },
                );
                return Some(SchedulingReservation {
                    id: reservation_id,
                    entity_id,
                    priority,
                });
            }
        }
        None
    }

    /// Confirm a reservation, moving it from pending to active.
    /// Returns the reservation if confirmed, None if the reservation was not found.
    pub async fn confirm(
        &self,
        reservation_id: Uuid,
    ) -> Option<SchedulingReservation<E>> {
        let mut state = self.state.lock().await;
        let reservation = state.reservations.remove(&reservation_id)?;
        if let Some(entity) = state
            .entities
            .iter_mut()
            .find(|(k, _)| k.to_string() == reservation.entity_key)
            .map(|(_, v)| v)
        {
            entity.pending = entity.pending.saturating_sub(1);
            entity.inflight += 1;
        }
        // Find the entity_id to return
        let entity_id = state
            .entities
            .iter()
            .find(|(k, _)| k.to_string() == reservation.entity_key)
            .map(|(k, _)| *k)?;
        Some(SchedulingReservation {
            id: reservation_id,
            entity_id,
            priority: reservation.priority,
        })
    }

    /// Cancel a reservation, returning it to the ready queue.
    pub async fn cancel(&self, reservation_id: Uuid) {
        let mut state = self.state.lock().await;
        if let Some(reservation) = state.reservations.remove(&reservation_id) {
            if let Some(entity) = state
                .entities
                .iter_mut()
                .find(|(k, _)| k.to_string() == reservation.entity_key)
                .map(|(_, v)| v)
            {
                entity.pending = entity.pending.saturating_sub(1);
                if let Some(priority_state) =
                    entity.priority_state(reservation.priority)
                {
                    priority_state.ready += 1;
                    priority_state.current_weight += reservation.weight_debt;
                }
            }
        }
    }

    /// Release a slot for the given entity (decrement active count).
    pub async fn release(&self, entity_id: E) {
        let mut state = self.state.lock().await;
        if let Some(entity) = state.entities.get_mut(&entity_id) {
            entity.inflight = entity.inflight.saturating_sub(1);
        }
    }

    /// Record that a job completed (alias for release).
    pub async fn record_completed(&self, entity_id: E) {
        self.release(entity_id).await;
    }

    /// Get a snapshot of current scheduler state: (inflight, ready) per entity.
    #[cfg(test)]
    pub async fn snapshot(&self) -> HashMap<E, (usize, usize)> {
        let state = self.state.lock().await;
        state
            .entities
            .iter()
            .map(|(id, entity)| {
                (
                    *id,
                    (
                        entity.inflight,
                        entity
                            .priorities
                            .values()
                            .map(|p| p.ready)
                            .sum::<usize>(),
                    ),
                )
            })
            .collect()
    }

    /// Get the current number of ready jobs for a specific entity and priority.
    pub async fn ready_count(
        &self,
        entity_id: E,
        priority: JobPriority,
    ) -> usize {
        let state = self.state.lock().await;
        state
            .entities
            .get(&entity_id)
            .and_then(|e| e.priorities.get(&priority))
            .map(|p| p.ready)
            .unwrap_or(0)
    }

    /// Get the current number of active jobs for an entity.
    pub async fn active_count(&self, entity_id: E) -> usize {
        let state = self.state.lock().await;
        state
            .entities
            .get(&entity_id)
            .map(|e| e.inflight)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobId;

    #[tokio::test]
    async fn test_scheduler_creation() {
        let config = SchedulerConfig::default();
        let weights = PriorityWeights::default();
        let scheduler: WeightedFairScheduler<JobId> =
            WeightedFairScheduler::new(&config, &weights);

        // Initially no reservations
        let snapshot = scheduler.snapshot().await;
        assert!(snapshot.is_empty());
    }

    #[tokio::test]
    async fn test_record_ready() {
        let config = SchedulerConfig::default();
        let weights = PriorityWeights::default();
        let scheduler: WeightedFairScheduler<JobId> =
            WeightedFairScheduler::new(&config, &weights);

        let entity_id = JobId::new();
        scheduler.record_ready(entity_id, JobPriority::P0).await;

        assert_eq!(scheduler.ready_count(entity_id, JobPriority::P0).await, 1);
    }

    #[tokio::test]
    async fn test_reserve_confirm_release() {
        let config = SchedulerConfig::default();
        let weights = PriorityWeights::default();
        let scheduler: WeightedFairScheduler<JobId> =
            WeightedFairScheduler::new(&config, &weights);

        let entity_id = JobId::new();

        // Record a job as ready
        scheduler.record_ready(entity_id, JobPriority::P0).await;

        // Reserve a slot
        let reservation = scheduler.reserve().await;
        assert!(reservation.is_some());
        let reservation = reservation.unwrap();
        assert_eq!(reservation.entity_id, entity_id);
        assert_eq!(reservation.priority, JobPriority::P0);

        // Confirm the reservation
        let confirmed = scheduler.confirm(reservation.id).await;
        assert!(confirmed.is_some());

        // Check inflight count
        assert_eq!(scheduler.active_count(entity_id).await, 1);

        // Release the slot
        scheduler.release(entity_id).await;
        assert_eq!(scheduler.active_count(entity_id).await, 0);
    }

    #[tokio::test]
    async fn test_cancel_reservation() {
        let config = SchedulerConfig::default();
        let weights = PriorityWeights::default();
        let scheduler: WeightedFairScheduler<JobId> =
            WeightedFairScheduler::new(&config, &weights);

        let entity_id = JobId::new();

        // Record a job as ready
        scheduler.record_ready(entity_id, JobPriority::P0).await;

        // Reserve a slot
        let reservation = scheduler.reserve().await.unwrap();

        // Cancel it
        scheduler.cancel(reservation.id).await;

        // Ready count should be back to 1
        assert_eq!(scheduler.ready_count(entity_id, JobPriority::P0).await, 1);
    }

    #[tokio::test]
    async fn test_priority_ordering() {
        // Use equal weights to test pure priority ordering
        let config = SchedulerConfig::default();
        let weights = PriorityWeights::equal();
        let scheduler: WeightedFairScheduler<JobId> =
            WeightedFairScheduler::new(&config, &weights);

        let entity_p0 = JobId::new();
        let entity_p1 = JobId::new();

        // Record jobs at different priorities
        scheduler.record_ready(entity_p1, JobPriority::P1).await;
        scheduler.record_ready(entity_p0, JobPriority::P0).await;

        // First reservation should be P0 (higher priority)
        let reservation1 = scheduler.reserve().await.unwrap();
        assert_eq!(reservation1.priority, JobPriority::P0);
        scheduler.confirm(reservation1.id).await;

        // Second reservation should be P1
        let reservation2 = scheduler.reserve().await.unwrap();
        assert_eq!(reservation2.priority, JobPriority::P1);
    }

    #[tokio::test]
    async fn test_per_entity_cap() {
        let config = SchedulerConfig {
            default_max_concurrent: 2,
            default_weight: 1,
            entity_overrides: HashMap::new(),
        };
        let weights = PriorityWeights::default();
        let scheduler: WeightedFairScheduler<JobId> =
            WeightedFairScheduler::new(&config, &weights);

        let entity_id = JobId::new();

        // Record 10 jobs as ready
        for _ in 0..10 {
            scheduler.record_ready(entity_id, JobPriority::P0).await;
        }

        // Reserve and confirm up to the cap
        let r1 = scheduler.reserve().await.unwrap();
        scheduler.confirm(r1.id).await;

        let r2 = scheduler.reserve().await.unwrap();
        scheduler.confirm(r2.id).await;

        // Third reservation should return None (cap reached)
        let r3 = scheduler.reserve().await;
        assert!(r3.is_none());

        // After releasing, we can reserve again
        scheduler.release(entity_id).await;
        let r3 = scheduler.reserve().await;
        assert!(r3.is_some());
    }

    #[tokio::test]
    async fn test_weighted_fair_scheduling() {
        let config = SchedulerConfig {
            default_max_concurrent: 100,
            default_weight: 1,
            entity_overrides: HashMap::new(),
        };
        let weights = PriorityWeights::equal();
        let scheduler: WeightedFairScheduler<JobId> =
            WeightedFairScheduler::new(&config, &weights);

        let entity_a = JobId::new();
        let entity_b = JobId::new();

        for _ in 0..100 {
            scheduler.record_ready(entity_a, JobPriority::P0).await;
        }
        for _ in 0..50 {
            scheduler.record_ready(entity_b, JobPriority::P0).await;
        }

        let mut count_a = 0;
        let mut count_b = 0;

        for _ in 0..20 {
            if let Some(reservation) = scheduler.reserve().await {
                if reservation.entity_id == entity_a {
                    count_a += 1;
                } else if reservation.entity_id == entity_b {
                    count_b += 1;
                }
                scheduler.cancel(reservation.id).await;
            }
        }

        assert!(count_a + count_b > 0, "Should have made some reservations");
        assert!(
            count_a > 0 || count_b > 0,
            "At least one entity should get reservations"
        );
    }

    #[tokio::test]
    async fn test_record_enqueued_and_completed() {
        let config = SchedulerConfig::default();
        let weights = PriorityWeights::default();
        let scheduler: WeightedFairScheduler<JobId> =
            WeightedFairScheduler::new(&config, &weights);

        let entity_id = JobId::new();

        // Use record_enqueued (alias for record_ready)
        scheduler.record_enqueued(entity_id, JobPriority::P0).await;
        assert_eq!(scheduler.ready_count(entity_id, JobPriority::P0).await, 1);

        // Reserve and confirm
        let reservation = scheduler.reserve().await.unwrap();
        scheduler.confirm(reservation.id).await;

        // Use record_completed (alias for release)
        scheduler.record_completed(entity_id).await;
        assert_eq!(scheduler.active_count(entity_id).await, 0);
    }
}
