-- Ishikari Migration V06: wake queues whenever jobs become available, and index staging.

-- Notify '{schema}.ishikari_insert' with the job's queue when a job is inserted available or
-- becomes available: staged, released by its upstreams, retried or given back. An engine
-- listening there claims it at once rather than at its queue's next poll. Notifications with
-- the same queue in one transaction arrive as one.
CREATE OR REPLACE FUNCTION {SCHEMA}.ishikari_jobs_notify()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('{ESCAPED_PREFIX}.ishikari_insert',
                      json_build_object('queue', NEW.queue)::text);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ishikari_jobs_notify_trigger ON {SCHEMA}.ishikari_jobs;
CREATE TRIGGER ishikari_jobs_notify_trigger
    AFTER INSERT ON {SCHEMA}.ishikari_jobs
    FOR EACH ROW
    WHEN (NEW.state = 'available')
    EXECUTE FUNCTION {SCHEMA}.ishikari_jobs_notify();

DROP TRIGGER IF EXISTS ishikari_jobs_available_trigger ON {SCHEMA}.ishikari_jobs;
CREATE TRIGGER ishikari_jobs_available_trigger
    AFTER UPDATE OF state ON {SCHEMA}.ishikari_jobs
    FOR EACH ROW
    WHEN (NEW.state = 'available' AND OLD.state <> 'available')
    EXECUTE FUNCTION {SCHEMA}.ishikari_jobs_notify();

-- The stager's due jobs, oldest first, without scanning every scheduled job (compensations wait
-- as scheduled far in the future). On a large table, create it CONCURRENTLY beforehand; this
-- then skips it.
CREATE INDEX IF NOT EXISTS ishikari_jobs_staging_index
    ON {SCHEMA}.ishikari_jobs (scheduled_at, id)
    WHERE state IN ('scheduled', 'retryable');

-- Saga steps by their job and by their compensation, as the resolver looks them up.
CREATE INDEX IF NOT EXISTS ishikari_saga_steps_job_idx
    ON {SCHEMA}.ishikari_saga_steps (job_id);
CREATE INDEX IF NOT EXISTS ishikari_saga_steps_compensation_idx
    ON {SCHEMA}.ishikari_saga_steps (compensation_job_id);
