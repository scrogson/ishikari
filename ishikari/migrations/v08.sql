-- Ishikari Migration V08: node modules, node types the host's operators upload through the admin
-- (WebAssembly components, run by ishikari-wasm). Every upload of a module's name is its next
-- version; a run uses the latest version uploaded before it was created. A module may reach only
-- what it was granted: the hosts it may send HTTP requests to, and its time and memory limits
-- (null for the runtime's defaults).

CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_node_modules (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    spec JSONB NOT NULL,
    bytes BYTEA NOT NULL,
    allowed_hosts TEXT[] NOT NULL DEFAULT '{}',
    timeout_ms INTEGER,
    memory_limit_mb INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT ishikari_node_modules_name_version UNIQUE (name, version),
    CONSTRAINT ishikari_node_modules_positive_limits
        CHECK (COALESCE(timeout_ms, 1) > 0 AND COALESCE(memory_limit_mb, 1) > 0)
);
