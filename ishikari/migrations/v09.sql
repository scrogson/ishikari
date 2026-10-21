-- Ishikari Migration V09: secrets. A node module may read the host's secrets it was granted, by
-- name.

ALTER TABLE {SCHEMA}.ishikari_node_modules
    ADD COLUMN IF NOT EXISTS secrets TEXT[] NOT NULL DEFAULT '{}';
