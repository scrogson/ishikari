-- Ishikari Migration V05: nodes, rescue, unique jobs, error records, pending
-- This migration adds what running jobs safely across restarts needs: which node runs a job
-- (and whether it is still alive), uniqueness on insert, structured error records, and a
-- `pending` state for workflow steps waiting on their dependencies.

-- Running engines, one row each, refreshed while they run
CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_nodes (
    name TEXT PRIMARY KEY,
    queues TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Executing jobs, for finding the ones whose node stopped
CREATE INDEX IF NOT EXISTS ishikari_jobs_executing_index
    ON {SCHEMA}.ishikari_jobs (attempted_at)
    WHERE state = 'executing';

-- Unique jobs: a key held while the job is in one of its unique states
ALTER TABLE {SCHEMA}.ishikari_jobs ADD COLUMN IF NOT EXISTS unique_key TEXT;
ALTER TABLE {SCHEMA}.ishikari_jobs
    ADD COLUMN IF NOT EXISTS unique_states {SCHEMA}.ishikari_job_state[];

CREATE UNIQUE INDEX IF NOT EXISTS ishikari_jobs_unique_key_index
    ON {SCHEMA}.ishikari_jobs (unique_key)
    WHERE unique_key IS NOT NULL;

-- Errors become records ({attempt, at, kind, error}); old plain strings are kept as {error}
UPDATE {SCHEMA}.ishikari_jobs
SET errors = ARRAY(
    SELECT CASE WHEN jsonb_typeof(e) = 'string' THEN jsonb_build_object('error', e) ELSE e END
    FROM unnest(errors) AS e
)
WHERE EXISTS (SELECT 1 FROM unnest(errors) AS e WHERE jsonb_typeof(e) = 'string');

-- Workflow steps wait in `pending` until their dependencies are met. Last, because a new enum
-- value can't be used in the transaction that adds it.
ALTER TYPE {SCHEMA}.ishikari_job_state ADD VALUE IF NOT EXISTS 'pending';
