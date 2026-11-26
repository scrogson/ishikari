-- Ishikari Migration V11: cron entries kept in the database (`ishikari::Crons`), which a host
-- (or an agent it lets) puts and deletes while its engines run. Every node reads them on each
-- cron check; a slot's job is inserted once, by its unique key, as for the engine's own entries.
-- `updated_at` is when the entry last changed (or was paused or resumed): no slot before it is
-- inserted. A paused entry inserts none until it's resumed.

CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_crons (
    name TEXT PRIMARY KEY CHECK (char_length(name) > 0),
    pattern TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    worker TEXT NOT NULL CHECK (char_length(worker) > 0 AND char_length(worker) < 128),
    args JSONB NOT NULL,
    queue TEXT NOT NULL CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    priority INTEGER NOT NULL DEFAULT 0 CHECK (priority >= 0 AND priority <= 3),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    tags VARCHAR(255)[] NOT NULL DEFAULT '{}' CHECK (array_position(tags, NULL) IS NULL),
    -- What a node does with the slots missed while none ran: all, the latest, or none.
    catch_up TEXT NOT NULL DEFAULT 'all' CHECK (catch_up IN ('all', 'latest', 'none')),
    paused BOOLEAN NOT NULL DEFAULT false,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One owner's entries, by a prefix of their names (`LIKE 'tenant-42:%'`), or by a tag.
CREATE INDEX IF NOT EXISTS ishikari_crons_name_prefix_idx
    ON {SCHEMA}.ishikari_crons (name text_pattern_ops);
CREATE INDEX IF NOT EXISTS ishikari_crons_tags_idx
    ON {SCHEMA}.ishikari_crons USING GIN (tags);
