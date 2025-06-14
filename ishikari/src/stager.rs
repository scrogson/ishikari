//! Ishikari Stager
//!
//! The Stager is responsible for moving tasks from scheduled/retryable to available.

use crate::engine::Storage;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, instrument};

/// A component responsible for periodically moving jobs from scheduled/retryable states to available state.
///
/// The Stager runs in the background and periodically checks for jobs that are ready to be processed.
/// It moves jobs from scheduled/retryable states to the available state in batches, up to a specified limit.
///
/// # Type Parameters
///
/// * `S` - The storage implementation that must implement the `Storage` trait
///
/// # Examples
///
/// ```rust,no_run
/// use ishikari::{Storage, Stager, Job};
/// use std::sync::Arc;
/// use std::time::Duration;
/// use async_trait::async_trait;
/// use chrono::Utc;
///
/// // Create your storage implementation
/// struct MyStorage;
///
/// #[async_trait]
/// impl Storage for MyStorage {
///     type Error = std::io::Error;
///     
///     async fn stage_jobs(&self, _limit: i32) -> Result<usize, Self::Error> {
///         Ok(0)
///     }
///
///     async fn cancel_job(&self, _id: i64) -> Result<(), Self::Error> {
///         Ok(())
///     }
///
///     async fn complete_job(&self, _id: i64) -> Result<(), Self::Error> {
///         Ok(())
///     }
///
///     async fn discard_job(&self, _id: i64) -> Result<(), Self::Error> {
///         Ok(())
///     }
///
///     async fn error_job(&self, _id: i64, _error: &str, _at: chrono::DateTime<Utc>) -> Result<(), Self::Error> {
///         Ok(())
///     }
///
///     async fn retry_job(&self, _id: i64) -> Result<(), Self::Error> {
///         Ok(())
///     }
///
///     async fn snooze_job(&self, _id: i64, _seconds: u64) -> Result<(), Self::Error> {
///         Ok(())
///     }
///
///     async fn fetch_jobs(&self) -> Result<Vec<Job>, Self::Error> {
///         Ok(vec![])
///     }
///
///     async fn prune_jobs(&self) -> Result<Vec<Job>, Self::Error> {
///         Ok(vec![])
///     }
///
///     async fn fetch_and_execute_jobs(&self, _worker_id: &str, _limit: i32) -> Result<Vec<Job>, Self::Error> {
///         Ok(vec![])
///     }
/// }
///
/// let storage = Arc::new(MyStorage);
/// let stager = Stager::new(storage, Duration::from_secs(1), 100);
/// let handle = stager.start();
/// ```
#[derive(Debug)]
pub struct Stager<S>
where
    S: Storage + 'static,
{
    storage: Arc<S>,
    interval: Duration,
    limit: i32,
}

impl<S> Stager<S>
where
    S: Storage + 'static,
{
    /// Creates a new Stager instance.
    ///
    /// # Arguments
    ///
    /// * `storage` - An Arc-wrapped storage implementation
    /// * `interval` - The duration between staging attempts
    /// * `limit` - The maximum number of jobs to stage in each attempt
    ///
    /// # Returns
    ///
    /// A new `Stager` instance
    pub fn new(storage: Arc<S>, interval: Duration, limit: i32) -> Self {
        Self {
            storage,
            interval,
            limit,
        }
    }

    /// Starts the stager in a background task.
    ///
    /// This method spawns a new task that will periodically attempt to stage jobs
    /// according to the configured interval and limit.
    ///
    /// # Returns
    ///
    /// A `JoinHandle` that can be used to await the completion of the stager task.
    /// The task will run indefinitely until cancelled.
    #[instrument(skip(self))]
    pub fn start(self) -> tokio::task::JoinHandle<Result<(), S::Error>> {
        tokio::spawn(async move {
            info!("starting stager");
            let mut interval = pin!(tokio::time::interval(self.interval));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let count = self.storage.stage_jobs(self.limit).await?;
                        debug!(count = count, "staging jobs");

                        if count > 0 {
                            info!(count = count, "staged jobs");
                        }
                    }
                }
            }
        })
    }
}
