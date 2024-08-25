use chrono::{DateTime, Utc};

use crate::Job;

#[async_trait::async_trait]
pub trait Storage: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Mark an `executing`, `available`, `scheduled` or `retryable` job as
    /// cancelled to prevent it from running.
    async fn cancel_job(&self, id: i64) -> Result<(), Self::Error>;

    /// Record that a job completed successfully.
    async fn complete_job(&self, id: i64) -> Result<(), Self::Error>;

    /// Transition a job to `discarded` and record an optional reason that it
    /// shouldn't be ran again.
    async fn discard_job(&self, id: i64) -> Result<(), Self::Error>;

    /// Record an executing job's errors and either retry or discard it,
    /// depending on whether it has exhausted its available attempts.
    async fn error_job(
        &self,
        id: i64,
        error_message: &str,
        schedule_at: DateTime<Utc>,
    ) -> Result<(), Self::Error>;

    /// Mark a job as `available`, adding attempts if already maxed out. If the
    /// job is currently `available`, `executing` or `scheduled` it should be
    /// ignored.
    async fn retry_job(&self, id: i64) -> Result<(), Self::Error>;

    /// Reschedule an `executing` job to run some number of seconds in the future.
    async fn snooze_job(&self, id: i64, snooze: u64) -> Result<(), Self::Error>;

    /// Fetch `available` jobs for the given queue, up to configured limits.
    async fn fetch_jobs(&self) -> Result<Vec<Job>, Self::Error>;

    /// Delete `completed`, `cancelled` and `discarded` jobs.
    async fn prune_jobs(&self) -> Result<Vec<Job>, Self::Error>;

    /// Transition `scheduled` or `retryable` jobs to `available` prior to execution.
    async fn stage_jobs(&self, concurrency: i32) -> Result<usize, Self::Error>;

    /// Fetch `available` jobs for the given queue and mark them as `executing`.
    ///
    /// This should be called by a queue and the jobs should be sent to an executor.
    async fn fetch_and_execute_jobs(
        &self,
        queue_name: &str,
        limit: i32,
    ) -> Result<Vec<Job>, Self::Error>;
}
