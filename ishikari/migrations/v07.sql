-- Ishikari Migration V07: sagas roll back in the transaction that fails them.
-- Compensations no longer wait on each other through dependencies: each is released when the one
-- after it ends, whatever its outcome, so a failed one no longer cancels the rest. A rollback in
-- progress is brought in line: the dependencies it set up between compensations are dropped, and
-- its steps marked compensating whose compensation is still parked (scheduled in the year 3000)
-- wait their turn again as completed. Data only; nothing is created.

DELETE FROM {SCHEMA}.ishikari_job_dependencies d
USING {SCHEMA}.ishikari_saga_steps a, {SCHEMA}.ishikari_saga_steps b
WHERE d.job_id = a.compensation_job_id
  AND d.depends_on_job_id = b.compensation_job_id;

UPDATE {SCHEMA}.ishikari_saga_steps s
SET state = 'completed'
FROM {SCHEMA}.ishikari_jobs j
WHERE j.id = s.compensation_job_id
  AND s.state = 'compensating'
  AND j.state = 'scheduled'
  AND j.scheduled_at >= '3000-01-01';
