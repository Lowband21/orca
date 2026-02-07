-- =============================================================================
-- ORCA Generic Job Queue Schema
-- Migration: 001_initial_schema
-- =============================================================================
-- 
-- This migration creates the domain-neutral orca_jobs table for the generic
-- job queue system used by both ferrex (media scanning) and forensics 
-- (artifact parsing). The entity_scope column distinguishes which domain owns
-- each job.
--
-- Design decisions:
-- - uuidv7() for sortable, time-ordered IDs
-- - TIMESTAMPTZ for timezone safety
-- - TEXT with CHECK constraints instead of VARCHAR(N)
-- - Optimized indexes for SKIP LOCKED dequeue operations
--
-- =============================================================================

-- =============================================================================
-- Enable required extensions
-- =============================================================================
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================================================
-- Custom function for uuidv7 generation
-- Falls back to uuidv4 if the extension is not available
-- =============================================================================
CREATE OR REPLACE FUNCTION uuidv7()
RETURNS uuid AS $$
DECLARE
    unix_timestamp bigint;
    uuid_bytes bytea;
    result uuid;
BEGIN
    -- Current Unix timestamp in milliseconds
    unix_timestamp := extract(epoch from clock_timestamp()) * 1000;
    
    -- Generate random bytes for the rest
    uuid_bytes := gen_random_bytes(16);
    
    -- Overwrite first 6 bytes with timestamp (big-endian)
    uuid_bytes := set_byte(uuid_bytes, 0, (unix_timestamp >> 40)::int);
    uuid_bytes := set_byte(uuid_bytes, 1, ((unix_timestamp >> 32) & 255)::int);
    uuid_bytes := set_byte(uuid_bytes, 2, ((unix_timestamp >> 24) & 255)::int);
    uuid_bytes := set_byte(uuid_bytes, 3, ((unix_timestamp >> 16) & 255)::int);
    uuid_bytes := set_byte(uuid_bytes, 4, ((unix_timestamp >> 8) & 255)::int);
    uuid_bytes := set_byte(uuid_bytes, 5, (unix_timestamp & 255)::int);
    
    -- Set version (4) and variant (2) bits per UUID spec
    uuid_bytes := set_byte(uuid_bytes, 6, (get_byte(uuid_bytes, 6) & 15) | 112);
    uuid_bytes := set_byte(uuid_bytes, 8, (get_byte(uuid_bytes, 8) & 63) | 128);
    
    -- Convert to uuid
    result := encode(uuid_bytes, 'hex')::uuid;
    RETURN result;
EXCEPTION
    WHEN undefined_function THEN
        -- Fallback to uuidv4 if gen_random_bytes not available
        RETURN gen_random_uuid();
END;
$$ LANGUAGE plpgsql VOLATILE;

-- =============================================================================
-- Job states
-- Text enum using CHECK constraints for flexibility
-- =============================================================================

-- =============================================================================
-- Main table: orca_jobs
-- Generic job queue supporting multiple domains via entity_scope
-- =============================================================================
CREATE TABLE orca_jobs (
    -- -------------------------------------------------------------------------
    -- Primary identification
    -- -------------------------------------------------------------------------
    
    -- Sortable UUIDv7 primary key for time-ordered retrieval
    -- Uses our custom uuidv7() function
    id uuid DEFAULT uuidv7() NOT NULL,
    
    -- -------------------------------------------------------------------------
    -- Entity context (domain-neutral)
    -- Replaces library_id from ferrex-specific schema
    -- -------------------------------------------------------------------------
    
    -- Entity identifier within the scope (e.g., library_id for ferrex, 
    -- analysis_id for forensics)
    entity_id text NOT NULL,
    
    -- Domain scope that owns this job - distinguishes between different
    -- domains using the same job queue (ferrex, forensics, etc.)
    entity_scope text NOT NULL,
    
    -- Job kind identifier (e.g., "scan", "analyze", "transform")
    -- Text for flexibility - domains define their own job kinds
    job_kind text NOT NULL,
    
    -- -------------------------------------------------------------------------
    -- Payload and execution
    -- -------------------------------------------------------------------------
    
    -- JSONB payload for domain-specific job parameters
    payload jsonb NOT NULL DEFAULT '{}',
    
    -- Current job state in lifecycle
    state text NOT NULL,
    
    -- Priority level (0 = highest, 3 = lowest)
    priority smallint NOT NULL DEFAULT 2,
    
    -- -------------------------------------------------------------------------
    -- Deduplication and dependency
    -- -------------------------------------------------------------------------
    
    -- Deduplication key - prevents duplicate pending jobs
    -- NULL means no deduplication
    dedupe_key text,
    
    -- Dependency key - this job cannot start until the job with
    -- matching dedupe_key and same entity completes
    -- NULL means no dependency
    dependency_key text,
    
    -- -------------------------------------------------------------------------
    -- Leasing (distributed work coordination)
    -- -------------------------------------------------------------------------
    
    -- Identifies which worker owns the lease on this job
    -- NULL when job is not leased
    lease_owner text,
    
    -- Unique lease identifier for idempotent lease operations
    -- NULL when job is not leased
    lease_id uuid,
    
    -- Lease expiration timestamp - job returns to 'ready' if not completed
    -- NULL for jobs in non-leased states
    lease_expires_at timestamptz,
    
    -- -------------------------------------------------------------------------
    -- Scheduling and retry
    -- -------------------------------------------------------------------------
    
    -- When job becomes available for execution (allows future scheduling)
    available_at timestamptz NOT NULL DEFAULT now(),
    
    -- When job can be retried after failure (enables exponential backoff)
    -- NULL when not in backoff
    backoff_until timestamptz,
    
    -- Number of execution attempts made
    attempts smallint NOT NULL DEFAULT 0,
    
    -- Maximum number of attempts before dead-letter
    max_attempts smallint NOT NULL DEFAULT 3,
    
    -- Error message from last failed attempt
    -- NULL when never failed or after success
    last_error text,
    
    -- -------------------------------------------------------------------------
    -- Metadata and auditing
    -- -------------------------------------------------------------------------
    
    -- Record creation timestamp
    created_at timestamptz NOT NULL DEFAULT now(),
    
    -- Last modification timestamp
    updated_at timestamptz NOT NULL DEFAULT now(),
    
    -- Completion timestamp (NULL until completed)
    completed_at timestamptz,
    
    -- Actual execution duration in milliseconds (NULL until completed)
    duration_ms integer,
    
    -- -------------------------------------------------------------------------
    -- Constraints
    -- -------------------------------------------------------------------------
    
    -- Primary key
    CONSTRAINT orca_jobs_pkey PRIMARY KEY (id),
    
    -- Valid state constraint
    CONSTRAINT orca_jobs_state_check CHECK (state IN (
        'ready',      -- Available for pickup
        'deferred',   -- Waiting for dependency
        'leased',     -- Currently being processed
        'completed',  -- Successfully finished
        'failed',     -- Failed, may be retried
        'dead_letter' -- Permanently failed, requires manual intervention
    )),
    
    -- Valid priority constraint (0 = highest, 3 = lowest)
    CONSTRAINT orca_jobs_priority_check CHECK (priority >= 0 AND priority <= 3),
    
    -- Valid entity scope (prevents arbitrary strings, maintains data quality)
    -- Domains register their scope names here
    CONSTRAINT orca_jobs_entity_scope_check CHECK (entity_scope IN (
        'ferrex',    -- Media server scanning
        'forensics'  -- Artifact analysis
    )),
    
    -- Ensure lease fields are consistent within leased state
    CONSTRAINT orca_jobs_lease_consistency CHECK (
        (state = 'leased' AND lease_owner IS NOT NULL AND lease_id IS NOT NULL) OR
        (state != 'leased')
    ),
    
    -- Ensure dependency_key implies waiting state (deferred or ready)
    CONSTRAINT orca_jobs_dependency_consistency CHECK (
        dependency_key IS NULL OR state IN ('ready', 'deferred', 'leased', 'completed', 'failed')
    ),
    
    -- Ensure positive attempt counts
    CONSTRAINT orca_jobs_attempts_positive CHECK (attempts >= 0),
    CONSTRAINT orca_jobs_max_attempts_positive CHECK (max_attempts > 0),
    
    -- Ensure duration is only set for completed jobs
    CONSTRAINT orca_jobs_duration_consistency CHECK (
        duration_ms IS NULL OR state = 'completed'
    )
);

-- =============================================================================
-- Column-level comments for documentation
-- =============================================================================
COMMENT ON TABLE orca_jobs IS 'Generic job queue table supporting multiple domains (ferrex, forensics) via entity_scope';

COMMENT ON COLUMN orca_jobs.id IS 'Sortable UUIDv7 primary key for time-ordered retrieval';
COMMENT ON COLUMN orca_jobs.entity_id IS 'Entity identifier within scope (e.g., library_id for ferrex)';
COMMENT ON COLUMN orca_jobs.entity_scope IS 'Domain scope: ferrex (media scanning) or forensics (artifact parsing)';
COMMENT ON COLUMN orca_jobs.job_kind IS 'Job type identifier - domains define their own kinds';
COMMENT ON COLUMN orca_jobs.payload IS 'JSONB job parameters - domain-specific data';
COMMENT ON COLUMN orca_jobs.state IS 'Lifecycle state: ready, deferred, leased, completed, failed, dead_letter';
COMMENT ON COLUMN orca_jobs.priority IS 'Execution priority: 0=highest, 1=high, 2=normal, 3=low';
COMMENT ON COLUMN orca_jobs.dedupe_key IS 'Optional key for preventing duplicate pending jobs';
COMMENT ON COLUMN orca_jobs.dependency_key IS 'Optional key referencing another jobs dedupe_key - creates execution chain';
COMMENT ON COLUMN orca_jobs.lease_owner IS 'Worker identifier that owns current lease';
COMMENT ON COLUMN orca_jobs.lease_id IS 'Unique lease identifier for idempotent operations';
COMMENT ON COLUMN orca_jobs.lease_expires_at IS 'Timestamp when lease expires and job returns to ready';
COMMENT ON COLUMN orca_jobs.available_at IS 'Timestamp when job becomes eligible for pickup (future scheduling)';
COMMENT ON COLUMN orca_jobs.backoff_until IS 'Timestamp when failed job can be retried (exponential backoff)';
COMMENT ON COLUMN orca_jobs.attempts IS 'Number of execution attempts made so far';
COMMENT ON COLUMN orca_jobs.max_attempts IS 'Maximum attempts before dead-lettering';
COMMENT ON COLUMN orca_jobs.last_error IS 'Error message from last failed attempt';
COMMENT ON COLUMN orca_jobs.created_at IS 'Record creation timestamp';
COMMENT ON COLUMN orca_jobs.updated_at IS 'Last modification timestamp';
COMMENT ON COLUMN orca_jobs.completed_at IS 'Completion timestamp (NULL until done)';
COMMENT ON COLUMN orca_jobs.duration_ms IS 'Actual execution duration in milliseconds';

-- =============================================================================
-- Indexes
-- =============================================================================

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_dequeue: Critical index for SKIP LOCKED dequeue operations
-- 
-- Query pattern (from persistence.rs):
--   SELECT ... FROM orca_jobs
--   WHERE state = 'ready'
--     AND available_at <= NOW()
--     AND (backoff_until IS NULL OR backoff_until <= NOW())
--     AND (dependency_key IS NULL OR dependency_satisfied)
--   ORDER BY priority ASC, available_at ASC, created_at ASC
--   FOR UPDATE SKIP LOCKED
--   LIMIT N
--
-- The job_kind and entity_id in the index enable domain-specific dequeue
-- strategies while maintaining the sort order for FIFO-within-priority.
-- -----------------------------------------------------------------------------
CREATE INDEX idx_orca_jobs_dequeue 
ON orca_jobs (state, job_kind, entity_id, priority, available_at, created_at)
WHERE state = 'ready';

COMMENT ON INDEX idx_orca_jobs_dequeue IS 'Critical composite index for SKIP LOCKED dequeue with priority and FIFO ordering';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_entity: Entity lookup for domain operations
-- 
-- Query patterns:
--   - Find all jobs for a specific entity
--   - Count pending jobs by scope
--   - Bulk operations by entity
-- -----------------------------------------------------------------------------
CREATE INDEX idx_orca_jobs_entity 
ON orca_jobs (entity_scope, entity_id);

COMMENT ON INDEX idx_orca_jobs_entity IS 'Entity lookup for domain-specific job operations';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_dedupe: Deduplication uniqueness enforcement
-- 
-- Prevents duplicate pending jobs with the same dedupe_key.
-- Partial index covers only active states to allow re-enqueue after completion.
-- -----------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_orca_jobs_dedupe 
ON orca_jobs (dedupe_key)
WHERE dedupe_key IS NOT NULL 
  AND state IN ('ready', 'deferred', 'leased');

COMMENT ON INDEX idx_orca_jobs_dedupe IS 'Unique constraint preventing duplicate pending jobs via dedupe_key';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_dependency: Dependency resolution
-- 
-- Query pattern: Find jobs that can be unblocked when a dependency completes
--   SELECT * FROM orca_jobs 
--   WHERE dependency_key = ? AND state = 'deferred'
-- -----------------------------------------------------------------------------
CREATE INDEX idx_orca_jobs_dependency 
ON orca_jobs (dependency_key, state)
WHERE dependency_key IS NOT NULL;

COMMENT ON INDEX idx_orca_jobs_dependency IS 'Dependency chain resolution - find blocked jobs waiting for completion';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_lease_expiry: Housekeeping for expired leases
-- 
-- Query pattern (from persistence.rs scan_expired_leases):
--   SELECT id, attempts, entity_id, payload
--   FROM orca_jobs
--   WHERE state = 'leased'
--     AND lease_expires_at IS NOT NULL
--     AND lease_expires_at < NOW()
-- -----------------------------------------------------------------------------
CREATE INDEX idx_orca_jobs_lease_expiry 
ON orca_jobs (lease_expires_at)
WHERE state = 'leased' AND lease_expires_at IS NOT NULL;

COMMENT ON INDEX idx_orca_jobs_lease_expiry IS 'Expired lease detection for housekeeping operations';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_state_kind: Status reporting and monitoring
-- 
-- Query patterns:
--   - Dashboard counts by state and kind
--   - Health checks for stuck jobs
-- -----------------------------------------------------------------------------
CREATE INDEX idx_orca_jobs_state_kind 
ON orca_jobs (state, job_kind);

COMMENT ON INDEX idx_orca_jobs_state_kind IS 'Status aggregation and monitoring queries';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_available_at: Scheduled job retrieval
-- 
-- Query pattern: Jobs scheduled for future execution
--   SELECT * FROM orca_jobs 
--   WHERE available_at > NOW() 
--     AND state = 'ready'
-- -----------------------------------------------------------------------------
CREATE INDEX idx_orca_jobs_available_at 
ON orca_jobs (available_at)
WHERE state = 'ready';

COMMENT ON INDEX idx_orca_jobs_available_at IS 'Future-scheduled job retrieval';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_backoff_until: Retry scheduling
-- 
-- Query pattern: Find jobs ready to retry after backoff
--   SELECT * FROM orca_jobs 
--   WHERE backoff_until IS NOT NULL 
--     AND backoff_until <= NOW()
-- -----------------------------------------------------------------------------
CREATE INDEX idx_orca_jobs_backoff_until 
ON orca_jobs (backoff_until)
WHERE backoff_until IS NOT NULL;

COMMENT ON INDEX idx_orca_jobs_backoff_until IS 'Retry scheduling for failed jobs with exponential backoff';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_payload_gin: JSONB payload queries
-- 
-- Supports queries filtering by payload content (e.g., specific file paths,
-- analysis parameters, etc.)
-- -----------------------------------------------------------------------------
CREATE INDEX idx_orca_jobs_payload_gin 
ON orca_jobs USING gin (payload);

COMMENT ON INDEX idx_orca_jobs_payload_gin IS 'JSONB payload indexing for content-based queries';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_created_at: Time-based queries and cleanup
-- 
-- Query patterns:
--   - Find old completed jobs for archival
--   - Purge jobs older than retention period
-- -----------------------------------------------------------------------------
CREATE INDEX idx_orca_jobs_created_at 
ON orca_jobs (created_at DESC);

COMMENT ON INDEX idx_orca_jobs_created_at IS 'Time-based queries and retention management';

-- -----------------------------------------------------------------------------
-- idx_orca_jobs_lease_id: Lease validation
-- 
-- Partial unique index ensures lease_id uniqueness for active leases.
-- This supports idempotent lease operations.
-- -----------------------------------------------------------------------------
CREATE UNIQUE INDEX idx_orca_jobs_lease_id 
ON orca_jobs (lease_id)
WHERE lease_id IS NOT NULL;

COMMENT ON INDEX idx_orca_jobs_lease_id IS 'Unique lease identifier for idempotent lease operations';

-- =============================================================================
-- Helper function: update_updated_at_column()
-- Auto-updates the updated_at timestamp on row modifications
-- =============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- Trigger: Auto-update updated_at
-- =============================================================================
CREATE TRIGGER update_orca_jobs_updated_at
    BEFORE UPDATE ON orca_jobs
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- Migration complete
-- =============================================================================
