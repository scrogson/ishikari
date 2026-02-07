-- Ishikari Migration V03: Dependencies and Sagas
-- This migration adds job dependency tracking and saga step management.

-- Job dependencies table
CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_job_dependencies (
    job_id BIGINT NOT NULL,
    depends_on_job_id BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',

    PRIMARY KEY (job_id, depends_on_job_id),

    CONSTRAINT ishikari_job_dependencies_state_check CHECK (
        state IN ('pending', 'satisfied', 'failed')
    )
);

CREATE INDEX IF NOT EXISTS ishikari_job_dependencies_depends_on_idx
    ON {SCHEMA}.ishikari_job_dependencies (depends_on_job_id);

CREATE INDEX IF NOT EXISTS ishikari_job_dependencies_pending_idx
    ON {SCHEMA}.ishikari_job_dependencies (job_id)
    WHERE state = 'pending';

-- Saga steps table
CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_saga_steps (
    id BIGSERIAL PRIMARY KEY,
    workflow_id BIGINT NOT NULL,
    job_id BIGINT,
    compensation_job_id BIGINT,
    step_order INT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',

    CONSTRAINT ishikari_saga_steps_state_check CHECK (
        state IN ('pending', 'executing', 'completed', 'failed', 'compensating', 'compensated', 'compensation_failed')
    )
);

CREATE INDEX IF NOT EXISTS ishikari_saga_steps_workflow_idx
    ON {SCHEMA}.ishikari_saga_steps (workflow_id);

-- Add foreign key from saga_steps to workflows
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ishikari_saga_steps_workflow_id_fkey'
    ) THEN
        ALTER TABLE {SCHEMA}.ishikari_saga_steps
            ADD CONSTRAINT ishikari_saga_steps_workflow_id_fkey
            FOREIGN KEY (workflow_id) REFERENCES {SCHEMA}.ishikari_workflows(id) ON DELETE CASCADE;
    END IF;
END$$;
