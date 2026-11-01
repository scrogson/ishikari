-- Ishikari Migration V10: claims. Each claim of a job counts one, and a run's outcome is written
-- only while its job is still on the claim it ran for. Its attempt can't tell two runs apart: a
-- retry, a give-back or a cancel and retry refunds it, and the next claim reuses its number.

ALTER TABLE {SCHEMA}.ishikari_jobs ADD COLUMN IF NOT EXISTS claims BIGINT NOT NULL DEFAULT 0;
