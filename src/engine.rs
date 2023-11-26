//! Defines an Engine trait for job orchestration.
//!
//! Engines are responsible for all non-plugin database interaction, from inserting through executing jobs.
//!
//! Ishikari ships with two Engine implementations:
//!
//! Basic — The default engine for development, production, and manual testing mode.
//! Inline — Designed specifically for testing, it executes jobs immediately, in-memory, as they are inserted.
//#[async_trait::async_trait]
//trait Engine {
///// Mark many executing, available, scheduled or retryable job as cancelled to prevent them from running.
////async fn cancel_all_jobs(conf, queryable)

///// Mark an executing, available, scheduled or retryable job as cancelled to prevent it from running.
//async fn cancel_job(conf, t)

///// Format engine meta in a digestible format for queue inspection.
////async fn check_meta(conf, meta, running)

///// Record that a job completed successfully.
//async fn complete_job(conf, t)

///// Transition a job to discarded and record an optional reason that it shouldn't be ran again.
//async fn discard_job(conf, t)

///// Record an executing job's errors and either retry or discard it, depending on whether it has exhausted its available attempts.
//async fn error_job(conf, t, seconds)

///// Fetch available jobs for the given queue, up to configured limits.
//async fn fetch_jobs(conf, meta, running)

///// Initialize metadata for a queue engine.
//async fn init(conf, opts)

///// Insert multiple jobs into the database.
//async fn insert_all_jobs(conf, list, opts)

///// Insert a job into the database.
//async fn insert_job(conf, changeset, opts)

///// Delete completed, cancelled and discarded jobs.
//async fn prune_jobs(conf, queryable, opts)

///// Store the given key/value pair in the engine meta.
//async fn put_meta(conf, meta, atom, term)

///// Refresh a queue to indicate that it is still alive.
//async fn refresh(conf, meta)

///// Mark many jobs as available, adding attempts if already maxed out. Any jobs currently available, executing or scheduled should be ignored.
////async fn retry_all_jobs(conf, queryable)

///// Mark a job as available, adding attempts if already maxed out. If the job is currently available, executing or scheduled it should be ignored.
//async fn retry_job(conf, t)

///// Prepare a queue engine for shutdown.
////async shutdown(conf, meta)

///// Reschedule an executing job to run some number of seconds in the future.
//async snooze_job(conf, t, seconds)

///// Transition scheduled or retryable jobs to available prior to execution.
//async fn stage_jobs(conf, queryable, opts)
//}
