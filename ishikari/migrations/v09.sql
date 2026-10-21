-- Ishikari Migration V09: secrets. A node module may read the host's secrets it was granted, by
-- name; a host may keep them here (`ishikari::secrets::PostgresSecrets`), each value encrypted
-- with the host's key of `key_id` (XChaCha20-Poly1305, bound to its name), which isn't kept.

ALTER TABLE {SCHEMA}.ishikari_node_modules
    ADD COLUMN IF NOT EXISTS secrets TEXT[] NOT NULL DEFAULT '{}';

CREATE TABLE IF NOT EXISTS {SCHEMA}.ishikari_secrets (
    name TEXT PRIMARY KEY,
    ciphertext BYTEA NOT NULL,
    nonce BYTEA NOT NULL,
    key_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
