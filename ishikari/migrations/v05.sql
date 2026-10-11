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

-- Node executions (workflow definitions): the columns and status the execution code uses
ALTER TABLE {SCHEMA}.ishikari_node_executions ADD COLUMN IF NOT EXISTS skip_reason TEXT;
ALTER TABLE {SCHEMA}.ishikari_node_executions
    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE {SCHEMA}.ishikari_node_executions
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE {SCHEMA}.ishikari_node_executions ALTER COLUMN started_at DROP NOT NULL;
ALTER TABLE {SCHEMA}.ishikari_node_executions ALTER COLUMN started_at DROP DEFAULT;
ALTER TABLE {SCHEMA}.ishikari_node_executions DROP CONSTRAINT IF EXISTS valid_execution_status;
ALTER TABLE {SCHEMA}.ishikari_node_executions
    ADD CONSTRAINT valid_execution_status
    CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped'));

-- Workflow steps wait in `pending` until their dependencies are met. Last, because a new enum
-- value can't be used in the transaction that adds it.
ALTER TYPE {SCHEMA}.ishikari_job_state ADD VALUE IF NOT EXISTS 'pending';
