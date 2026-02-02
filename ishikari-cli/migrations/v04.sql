-- Ishikari Migration V04: Workflow Definitions and Execution Tracking
-- This migration adds workflow definition storage and execution tracking.

-- Workflow definitions table
CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_workflow_definitions (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    description TEXT,
    input_schema JSONB NOT NULL DEFAULT '{}',
    nodes JSONB NOT NULL,
    output_schema JSONB NOT NULL DEFAULT '{}',
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT unique_workflow_name_version UNIQUE (name, version)
);

CREATE INDEX IF NOT EXISTS idx_workflow_definitions_name
    ON {SCHEMA}.ishikari_workflow_definitions (name);
CREATE INDEX IF NOT EXISTS idx_workflow_definitions_name_version
    ON {SCHEMA}.ishikari_workflow_definitions (name, version DESC);

-- Workflow runs table
CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_workflow_runs (
    id BIGSERIAL PRIMARY KEY,
    definition_id BIGINT NOT NULL,
    definition_name TEXT NOT NULL,
    definition_version INTEGER NOT NULL,
    workflow_id BIGINT NOT NULL,
    inputs JSONB NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'running',
    outputs JSONB,
    execution_id UUID,
    error TEXT,
    started_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT valid_run_status CHECK (status IN ('running', 'completed', 'failed', 'cancelled'))
);

-- Add foreign keys for workflow_runs
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ishikari_workflow_runs_definition_id_fkey'
    ) THEN
        ALTER TABLE {SCHEMA}.ishikari_workflow_runs
            ADD CONSTRAINT ishikari_workflow_runs_definition_id_fkey
            FOREIGN KEY (definition_id) REFERENCES {SCHEMA}.ishikari_workflow_definitions(id) ON DELETE CASCADE;
    END IF;
END$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ishikari_workflow_runs_workflow_id_fkey'
    ) THEN
        ALTER TABLE {SCHEMA}.ishikari_workflow_runs
            ADD CONSTRAINT ishikari_workflow_runs_workflow_id_fkey
            FOREIGN KEY (workflow_id) REFERENCES {SCHEMA}.ishikari_workflows(id) ON DELETE CASCADE;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_workflow_runs_definition_id
    ON {SCHEMA}.ishikari_workflow_runs (definition_id);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_workflow_id
    ON {SCHEMA}.ishikari_workflow_runs (workflow_id);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_status
    ON {SCHEMA}.ishikari_workflow_runs (status);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_created_at
    ON {SCHEMA}.ishikari_workflow_runs (created_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_workflow_runs_execution_id
    ON {SCHEMA}.ishikari_workflow_runs (execution_id)
    WHERE execution_id IS NOT NULL;

-- Node executions table
CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_node_executions (
    id BIGSERIAL PRIMARY KEY,
    workflow_run_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    node_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    inputs JSONB,
    output JSONB,
    error TEXT,
    duration_ms BIGINT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT unique_execution_run_node UNIQUE (workflow_run_id, node_id),
    CONSTRAINT valid_execution_status CHECK (status IN ('running', 'completed', 'failed', 'skipped'))
);

-- Add foreign key for node_executions
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ishikari_node_executions_workflow_run_id_fkey'
    ) THEN
        ALTER TABLE {SCHEMA}.ishikari_node_executions
            ADD CONSTRAINT ishikari_node_executions_workflow_run_id_fkey
            FOREIGN KEY (workflow_run_id) REFERENCES {SCHEMA}.ishikari_workflow_runs(id) ON DELETE CASCADE;
    END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_node_executions_workflow_run_id
    ON {SCHEMA}.ishikari_node_executions (workflow_run_id);
CREATE INDEX IF NOT EXISTS idx_node_executions_run_node
    ON {SCHEMA}.ishikari_node_executions (workflow_run_id, node_id);
CREATE INDEX IF NOT EXISTS idx_node_executions_status
    ON {SCHEMA}.ishikari_node_executions (status);
