-- =============================================================================
-- ORCA Job Queue Schema Update
-- Migration: 002_add_renewals_column
-- =============================================================================
--
-- Adds a `renewals` column to track how many times a lease has been renewed.
-- Previously, renew() always returned `renewals: 1` regardless of actual count.
--
-- =============================================================================

ALTER TABLE orca_jobs ADD COLUMN IF NOT EXISTS renewals INTEGER NOT NULL DEFAULT 0;

COMMENT ON COLUMN orca_jobs.renewals IS 'Number of times the lease on this job has been renewed';
