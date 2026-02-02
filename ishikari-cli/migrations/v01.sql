-- Ishikari Migration V01: Core Jobs Table
-- This migration creates the base job processing infrastructure.

-- Create ishikari_job_state enum type
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type
                   WHERE typname = 'ishikari_job_state'
                     AND typnamespace = '{ESCAPED_PREFIX}'::regnamespace::oid) THEN
        CREATE TYPE {SCHEMA}.ishikari_job_state AS ENUM (
            'available',
            'scheduled',
            'executing',
            'retryable',
            'completed',
            'discarded',
            'cancelled'
        );
    END IF;
END$$;

-- Create ishikari_jobs table
CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_jobs (
    id BIGSERIAL PRIMARY KEY,
    state {SCHEMA}.ishikari_job_state NOT NULL DEFAULT 'available'::{SCHEMA}.ishikari_job_state,
    queue TEXT NOT NULL DEFAULT 'default' CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    worker TEXT NOT NULL CHECK (char_length(worker) > 0 AND char_length(worker) < 128),
    args JSONB NOT NULL DEFAULT '{}',
    errors JSONB[] NOT NULL DEFAULT ARRAY[]::JSONB[],
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 20 CHECK (max_attempts > 0),
    attempted_by TEXT[],
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority >= 0 AND priority <= 3),
    tags VARCHAR(255)[] DEFAULT ARRAY[]::VARCHAR[],
    meta JSONB DEFAULT '{}',
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempted_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    discarded_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    CONSTRAINT ishikari_jobs_attempt_range CHECK (attempt >= 0 AND attempt <= max_attempts)
);

-- Create performance indexes
CREATE INDEX IF NOT EXISTS ishikari_jobs_state_queue_priority_scheduled_at_id_index
    ON {SCHEMA}.ishikari_jobs (state, queue, priority, scheduled_at, id);

CREATE INDEX IF NOT EXISTS ishikari_jobs_args_index
    ON {SCHEMA}.ishikari_jobs USING GIN (args);

CREATE INDEX IF NOT EXISTS ishikari_jobs_meta_index
    ON {SCHEMA}.ishikari_jobs USING GIN (meta);

-- Create notification function
CREATE OR REPLACE FUNCTION {SCHEMA}.ishikari_jobs_notify()
RETURNS TRIGGER AS $$
DECLARE
    channel TEXT;
    notice JSON;
BEGIN
    IF NEW.state = 'available' THEN
        channel := '{ESCAPED_PREFIX}.ishikari_insert';
        notice := json_build_object('queue', NEW.queue);
        PERFORM pg_notify(channel, notice::text);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for job notifications
DROP TRIGGER IF EXISTS ishikari_jobs_notify_trigger ON {SCHEMA}.ishikari_jobs;
CREATE TRIGGER ishikari_jobs_notify_trigger
    AFTER INSERT ON {SCHEMA}.ishikari_jobs
    FOR EACH ROW
    EXECUTE FUNCTION {SCHEMA}.ishikari_jobs_notify();
