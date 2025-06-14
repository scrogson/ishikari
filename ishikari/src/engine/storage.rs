use chrono::{DateTime, Utc};

use crate::Job;

#[async_trait::async_trait]
/// Storage backend for Ishikari job processing.
///
/// This trait provides the interface for job storage and management. It is designed to be implemented by different storage backends, with PostgreSQL being the primary implementation.
///
/// The storage backend is responsible for:
/// - Persisting job data
/// - Managing job state transitions
/// - Handling job retries and scheduling
/// - Providing job execution coordination
///
/// # Job States
///
/// Jobs can be in one of several states:
/// - `available`: Ready to be executed
/// - `scheduled`: Scheduled for future execution
/// - `executing`: Currently being processed
/// - `retryable`: Failed and waiting for retry
/// - `completed`: Successfully finished
/// - `discarded`: Failed permanently
/// - `cancelled`: Explicitly cancelled
///
/// # Example Implementation
///
/// ```rust,no_run
/// use ishikari::{Job, Storage};
/// use chrono::{DateTime, Utc};
/// use async_trait::async_trait;
///
/// struct MyStorage;
///
/// #[async_trait]
/// impl Storage for MyStorage {
///     type Error = std::io::Error;
///
///     async fn cancel_job(&self, id: i64) -> Result<(), Self::Error> {
///         // Implementation
///         Ok(())
///     }
///     # async fn complete_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
///     # async fn discard_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
///     # async fn error_job(&self, _id: i64, _msg: &str, _at: DateTime<Utc>) -> Result<(), Self::Error> { unimplemented!() }
///     # async fn retry_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
///     # async fn snooze_job(&self, _id: i64, _snooze: u64) -> Result<(), Self::Error> { unimplemented!() }
///     # async fn fetch_jobs(&self) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
///     # async fn prune_jobs(&self) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
///     # async fn stage_jobs(&self, _concurrency: i32) -> Result<usize, Self::Error> { unimplemented!() }
///     # async fn fetch_and_execute_jobs(&self, _queue: &str, _limit: i32) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
/// }
/// ```
///
/// # Type Parameters
///
/// * `Error` - The error type returned by storage operations. Must implement
///   `std::error::Error`, `Send`, and `Sync`.
pub trait Storage: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Mark a job as cancelled to prevent it from running.
    ///
    /// This method can be called on jobs in the following states:
    /// - `executing`
    /// - `available`
    /// - `scheduled`
    /// - `retryable`
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the job to cancel
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the job was successfully cancelled, or an error if
    /// the operation failed.
    async fn cancel_job(&self, id: i64) -> Result<(), Self::Error>;

    /// Record that a job completed successfully.
    ///
    /// This method should be called when a job has finished processing
    /// successfully. It will transition the job to the `completed` state.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the completed job
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the job was successfully marked as completed, or an
    /// error if the operation failed.
    async fn complete_job(&self, id: i64) -> Result<(), Self::Error>;

    /// Mark a job as discarded and record why it shouldn't be run again.
    ///
    /// This method should be called when a job has failed permanently and
    /// should not be retried. It will transition the job to the `discarded`
    /// state.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the job to discard
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the job was successfully discarded, or an error if
    /// the operation failed.
    async fn discard_job(&self, id: i64) -> Result<(), Self::Error>;

    /// Record a job's error and schedule it for retry or discard.
    ///
    /// This method is called when a job fails during execution. It will:
    /// 1. Record the error message
    /// 2. Either schedule the job for retry or mark it as discarded
    /// 3. Update the job's attempt count
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the failed job
    /// * `error_message` - A description of why the job failed
    /// * `schedule_at` - When the job should be retried, or now if it should be discarded
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the error was recorded successfully, or an error if
    /// the operation failed.
    async fn error_job(
        &self,
        id: i64,
        error_message: &str,
        schedule_at: DateTime<Utc>,
    ) -> Result<(), Self::Error>;

    /// Mark a job as available for execution.
    ///
    /// This method will:
    /// 1. Reset the job's attempt count if it was maxed out
    /// 2. Transition the job to the `available` state
    ///
    /// The job will be ignored if it is currently:
    /// - `available`
    /// - `executing`
    /// - `scheduled`
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the job to retry
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the job was successfully marked as available, or an
    /// error if the operation failed.
    async fn retry_job(&self, id: i64) -> Result<(), Self::Error>;

    /// Reschedule a job to run after a delay.
    ///
    /// This method will transition an `executing` job to the `scheduled` state
    /// and set its execution time to the current time plus the specified delay.
    ///
    /// # Arguments
    ///
    /// * `id` - The ID of the job to snooze
    /// * `snooze` - The number of seconds to delay execution
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the job was successfully snoozed, or an error if
    /// the operation failed.
    async fn snooze_job(&self, id: i64, snooze: u64) -> Result<(), Self::Error>;

    /// Fetch available jobs from the storage backend.
    ///
    /// This method should return all jobs that are in the `available` state,
    /// up to the configured concurrency limit.
    ///
    /// # Returns
    ///
    /// Returns a vector of available jobs, or an error if the operation failed.
    async fn fetch_jobs(&self) -> Result<Vec<Job>, Self::Error>;

    /// Remove completed, cancelled, and discarded jobs.
    ///
    /// This method should delete jobs that are in a terminal state:
    /// - `completed`
    /// - `cancelled`
    /// - `discarded`
    ///
    /// # Returns
    ///
    /// Returns a vector of the deleted jobs, or an error if the operation failed.
    async fn prune_jobs(&self) -> Result<Vec<Job>, Self::Error>;

    /// Transition scheduled or retryable jobs to available.
    ///
    /// This method should:
    /// 1. Find all `scheduled` or `retryable` jobs that are due for execution
    /// 2. Transition them to the `available` state
    /// 3. Return the number of jobs that were staged
    ///
    /// # Arguments
    ///
    /// * `concurrency` - The maximum number of jobs to stage
    ///
    /// # Returns
    ///
    /// Returns the number of jobs that were staged, or an error if the operation failed.
    async fn stage_jobs(&self, concurrency: i32) -> Result<usize, Self::Error>;

    /// Fetch and mark jobs as executing.
    ///
    /// This method should:
    /// 1. Find available jobs in the specified queue
    /// 2. Mark them as `executing`
    /// 3. Return them for processing
    ///
    /// This is the primary method used by queues to get jobs for execution.
    ///
    /// # Arguments
    ///
    /// * `queue_name` - The name of the queue to fetch jobs from
    /// * `limit` - The maximum number of jobs to fetch
    ///
    /// # Returns
    ///
    /// Returns a vector of jobs that are now executing, or an error if the
    /// operation failed.
    async fn fetch_and_execute_jobs(
        &self,
        queue_name: &str,
        limit: i32,
    ) -> Result<Vec<Job>, Self::Error>;
}
