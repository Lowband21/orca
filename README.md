# Orca

> Generic orchestration infrastructure for Rust. Extracted from Ferrex.

A foundational crate providing core traits and types for distributed job queue systems with workload budgeting, event publishing, and lease-based job processing.

Built to avoid rewriting leases, retries, and scheduling for every project. Works for my current use cases but will likely evolve over time.

---

## Table of Contents

- [Overview](#overview)
- [Core Concepts](#core-concepts)
- [Architecture Decisions](#architecture-decisions)
- [Examples](#examples)
- [API Overview](#api-overview)
- [Configuration](#configuration)

---

## Overview

Orca is a production-grade job orchestration crate extracted from ferrex media scanning orchestration system. It provides:

- **Generic Job System**: Define your own job types using enums that implement the `Job` trait (hybrid design)
- **Pluggable Queues**: In-memory for testing, PostgreSQL for production (SKIP LOCKED, JSONB, partial indexes)
- **Workload Budgets**: Concurrency limiting per workload type with `InMemoryBudget`
- **Fair Scheduling**: Weighted fair scheduler with priority rings (P0-P3)
- **Lease Management**: Automatic lease expiry scanning and resurrection with exponential backoff
- **Event System**: In-process event bus for reactive workflows and observability
- **Graceful Shutdown**: Clean worker termination with proper in-flight job handling

---

## Core Concepts

### Job

The fundamental unit of work. Jobs are user-defined types that implement the `Job` trait. This is a **hybrid design** where consumers define their own enums and implement `Job` on them.

```rust
#[derive(Clone, Serialize, Deserialize)]
enum MyJob {
    ProcessFile { path: String, library_id: Uuid },
    IndexDocument { content: String, doc_id: Uuid },
}

impl Job for MyJob {
    type EntityId = LibraryId;    // Your entity type
    type Kind = MyJobKind;         // Enum of job variants
    type DomainEvent = MyEvent;    // Your domain events
    type WorkloadKind = Workload;  // Budget categories

    fn entity_id(&self) -> Self::EntityId { /* ... */ }
    fn kind(&self) -> Self::Kind { /* ... */ }
    fn dedupe_key(&self) -> String { /* ... */ }
    fn priority(&self) -> JobPriority { /* ... */ }
}
```

### Queue

The `QueueService` trait abstracts queue backends. Orca provides:
- `InMemoryQueueService` (via `orca-testkit`) for testing
- `PostgresQueueService` for production with durability and SKIP LOCKED

### Budget

The `Budget` trait provides backpressure and concurrency limiting for workload types:

```rust
let budget = InMemoryBudget::new(
    BudgetConfig::new(HashMap::new(), 10)  // 10 concurrent jobs per workload
        .with_limit(Workload::Heavy, 2)   // Only 2 heavy jobs at once
);
```

### Scheduler

The `WeightedFairScheduler` provides fair allocation of worker capacity across entities and priorities:

```rust
let scheduler = WeightedFairScheduler::new(
    &SchedulerConfig::default(),
    &PriorityWeights::default(),  // P0=8, P1=4, P2=2, P3=1
);
```

### Runtime

The `OrchestratorRuntime` ties together all components:

```rust
let runtime = StandardOrchestratorRuntimeBuilder::new(config)
    .with_queue(queue)
    .with_budget(budget)
    .with_dispatcher(dispatcher)
    .with_events(event_bus)
    .with_workload_mapper(mapper)
    .build()?;
```

---

## Architecture Decisions

### Hybrid Job Design

Orca uses a **hybrid trait/enum design** for jobs:

- Consumers define their own job enums with domain-specific variants
- They implement the `Job` trait on their enum
- This allows compile-time dispatch while maintaining JSONB round-tripping via serde

**Benefits**:
- Type-safe job definitions at compile time
- Extensibility without modifying orca
- Natural enum-based pattern matching in dispatchers
- Clean serialization to JSONB for storage

### Generic Runtime

The `OrchestratorRuntime` is fully generic over the job type:

```rust
OrchestratorRuntime<J, Q, B, S, D, M>
where
    J: Job,
    Q: QueueService<J> + LeaseExpiryScanner,
    B: Budget<J::WorkloadKind, J::EntityId>,
    S: Scheduler<J::EntityId>,
    D: JobDispatcher<J>,
    M: WorkloadToKindMapper<J>,
```

This allows different domains (ferrex media scanning, forensics artifact parsing) to share the same runtime infrastructure while having completely different job definitions.

### Trait-First Design

Orca defines core operations as traits:

- `QueueService` - abstract queue operations
- `Budget` - concurrency limiting
- `Scheduler` - fair scheduling
- `JobDispatcher` - job execution

This allows consumers to provide custom implementations or use the built-in ones.

### PostgreSQL Schema

The schema uses modern PostgreSQL features:
- **UUID v7** for time-ordered IDs (efficient indexing)
- **JSONB** for job payloads (flexible, indexable)
- **SKIP LOCKED** for concurrent dequeue (no contention)
- **Partial indexes** for queue depth queries (efficient)

See `orca/migrations/001_initial_schema.sql` for the full schema.

---

## Examples

### Simple Queue Example

See `examples/simple_queue.rs` for basic enqueue/dequeue with an in-memory queue:

```bash
cargo run --example simple_queue
```

### Runtime with In-Memory Queue

See `examples/runtime_basic.rs` for a full runtime with in-memory queue:

```bash
cargo run --example runtime_basic
```

### Runtime with PostgreSQL

See `examples/postgres_runtime.rs` for a production-ready runtime with Postgres persistence:

```bash
# First, set up the database
createdb orca_example
psql orca_example -f orca/migrations/001_initial_schema.sql

# Then run the example
DATABASE_URL=postgres://localhost/orca_example cargo run --example postgres_runtime
```

---

## API Overview

### Core Traits

| Trait | Purpose | Key Methods |
|-------|---------|-------------|
| `Job` | Define job types | `entity_id()`, `kind()`, `dedupe_key()`, `priority()` |
| `JobKind` | Job categorization | `as_str()` |
| `EntityId` | Entity identifiers | `as_uuid()` |
| `WorkloadKind` | Budget categories | Marker trait |
| `QueueService` | Queue operations | `enqueue()`, `dequeue()`, `complete()`, `fail()` |
| `Budget` | Concurrency limits | `try_acquire()`, `release()` |
| `Scheduler` | Job scheduling | `reserve()`, `confirm()`, `cancel()` |
| `JobDispatcher` | Job execution | `dispatch()` |

### Key Types

| Type | Description |
|------|-------------|
| `OrchestratorRuntime` | Main runtime coordinating all components |
| `JobLease` | Represents a leased job being processed |
| `JobHandle` | Handle returned from enqueue operations |
| `JobId` | Unique job identifier (UUID v7) |
| `LeaseId` | Unique lease identifier |
| `JobPriority` | Priority levels (P0-P3) |
| `JobState` | Job lifecycle states |
| `DispatchStatus` | Result of job dispatch |

---

## Configuration

### Runtime Configuration

```rust
let config = OrchestratorRuntimeConfig {
    default_lease_ttl_secs: 30,      // 30 second lease TTL
    renew_min_margin_secs: 2,        // Renew 2s before expiry
    renew_at_fraction: 0.5,          // Renew at 50% of TTL
    max_lease_renewals: 10,          // Max 10 renewals
    housekeeper_interval_ms: 5000,   // Scan every 5s
    scheduler: SchedulerConfig::default(),
    priority_weights: PriorityWeights::default(),
};
```

### Budget Configuration

```rust
let budget_config = BudgetConfig::new(
    HashMap::from([
        (Workload::Heavy, 2),      // Only 2 heavy jobs
        (Workload::Light, 20),     // Up to 20 light jobs
    ]),
    5,  // Default: 5 concurrent
);
```

### Scheduler Configuration

```rust
let scheduler_config = SchedulerConfig {
    default_max_concurrent: 3,
    default_weight: 1,
    entity_overrides: HashMap::from([
        ("critical".to_string(), EntityQueuePolicy::new(10, 5)),
    ]),
};
```
