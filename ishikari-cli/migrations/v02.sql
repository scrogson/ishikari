-- Ishikari Migration V02: Workflows
-- This migration adds workflow tracking and links jobs to workflows.

-- Workflow instance tracking
CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_workflows (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'running',
    metadata JSONB NOT NULL DEFAULT '{}',
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT ishikari_workflows_state_check CHECK (
        state IN ('running', 'completed', 'failed', 'cancelled', 'compensating', 'compensated')
    )
);

CREATE INDEX IF NOT EXISTS ishikari_workflows_state_idx
    ON {SCHEMA}.ishikari_workflows (state);
CREATE INDEX IF NOT EXISTS ishikari_workflows_name_idx
    ON {SCHEMA}.ishikari_workflows (name);

-- Add workflow_id column to jobs if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = '{ESCAPED_PREFIX}'
          AND table_name = 'ishikari_jobs'
          AND column_name = 'workflow_id'
    ) THEN
        ALTER TABLE {SCHEMA}.ishikari_jobs ADD COLUMN workflow_id BIGINT;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS ishikari_jobs_workflow_id_idx
    ON {SCHEMA}.ishikari_jobs (workflow_id)
    WHERE workflow_id IS NOT NULL;

-- Add foreign key from jobs to workflows
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ishikari_jobs_workflow_id_fkey'
    ) THEN
        ALTER TABLE {SCHEMA}.ishikari_jobs
            ADD CONSTRAINT ishikari_jobs_workflow_id_fkey
            FOREIGN KEY (workflow_id) REFERENCES {SCHEMA}.ishikari_workflows(id);
    END IF;
END$$;
