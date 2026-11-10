-- Ishikari Migration V10: claims, the hosts secrets may be sent to, and the versions of modules
-- runs keep.
--
-- Claims. Each claim of a job counts one, and a run's outcome is written
-- only while its job is still on the claim it ran for. Its attempt can't tell two runs apart: a
-- retry, a give-back or a cancel and retry refunds it, and the next claim reuses its number.

ALTER TABLE {SCHEMA}.ishikari_jobs ADD COLUMN IF NOT EXISTS claims BIGINT NOT NULL DEFAULT 0;

-- The hosts a module may send each secret to, in a request's headers. A secret set before is
-- bound to none, until it's bound.
ALTER TABLE {SCHEMA}.ishikari_secrets ADD COLUMN IF NOT EXISTS hosts TEXT[] NOT NULL DEFAULT '{}';

-- The version of each module a run's nodes ran first, by name: what its other nodes of the module
-- and its retries run, whatever is uploaded meanwhile.
ALTER TABLE {SCHEMA}.ishikari_workflow_runs
    ADD COLUMN IF NOT EXISTS module_versions JSONB NOT NULL DEFAULT '{}';
