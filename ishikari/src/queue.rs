//! Queue management for job processing.
//!
//! This module provides the core queue functionality for Ishikari, including:
//!
//! - `Queue` - The main queue processor that handles job execution
//! - `QueueBuilder` - Builder pattern for configuring queues
//! - `QueueName` - Type-safe queue name handling
//!
//! Queues are responsible for:
//! - Fetching jobs from storage
//! - Executing jobs concurrently
//! - Managing job lifecycle (success, failure, retry)
//! - Respecting concurrency limits
//! - Handling backoff strategies for failed jobs
//!
//! # Example
//!
//! ```rust,no_run
//! use ishikari::{Queue, Postgres, Engine};
//! use std::{sync::Arc, time::Duration};
//! use sqlx::PgPool;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let database_url = std::env::var("DATABASE_URL")?;
//!     let pool = PgPool::connect(&database_url).await?;
//!     let storage = Arc::new(Postgres::new(pool));
//!     let state = Arc::new(());
//!
//!     let queue = Queue::builder("my_queue")
//!         .concurrency(5)
//!         .interval(Duration::from_secs(1))
//!         .build(storage, state);
//!
//!     queue.start();
//! #   Ok(())
//! # }
//! ```

use crate::{Backoff, Context, State, Status, Storage};
use chrono::Duration as ChronoDuration;
use futures::FutureExt;
use std::any::Any;
use std::marker::PhantomData;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueueName(Arc<str>);

impl From<&str> for QueueName {
    fn from(name: &str) -> Self {
        Self(name.into())
    }
}

impl QueueName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A builder for configuring and creating a new `Queue`.
///
/// This struct allows you to set the queue's name, concurrency, and polling interval before building the queue.
///
/// # Example
///
/// ```rust,no_run
/// use ishikari::{Queue, Storage, Job};
/// use chrono::{DateTime, Utc};
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use std::time::Duration;
/// #
/// struct MyStorage;
/// #
/// # #[async_trait]
/// # impl Storage for MyStorage {
/// #     type Error = std::io::Error;
/// #     async fn cancel_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn complete_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn discard_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn error_job(&self, _id: i64, _msg: &str, _at: DateTime<Utc>) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn retry_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn snooze_job(&self, _id: i64, _snooze: u64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn fetch_jobs(&self) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
/// #     async fn prune_jobs(&self) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
/// #     async fn stage_jobs(&self, _concurrency: i32) -> Result<usize, Self::Error> { unimplemented!() }
/// #     async fn fetch_and_execute_jobs(&self, _queue: &str, _schema: Option<&str>, _limit: i32) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
/// # }
///
/// let storage = Arc::new(MyStorage);
/// let state = Arc::new(());
///
/// let queue = Queue::builder("my_queue")
///     .concurrency(5)
///     .interval(Duration::from_secs(2))
///     .build(storage, state);
/// ```
#[derive(Debug)]
pub struct QueueBuilder<S>
where
    S: Storage + 'static,
{
    pub name: QueueName,
    pub concurrency: Option<u32>,
    pub interval: Option<Duration>,
    pub schema: Option<String>,
    pub storage: PhantomData<S>,
}

impl<S> QueueBuilder<S>
where
    S: Storage + 'static,
{
    /// Sets the concurrency level for the queue.
    ///
    /// This determines how many jobs can be executed concurrently.
    pub fn concurrency(mut self, concurrency: u32) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    /// Sets the polling interval for the queue.
    ///
    /// This determines how often the queue checks for new jobs.
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Sets the schema for the queue.
    ///
    /// This determines which schema to query for jobs. Defaults to public schema.
    pub fn schema<T: Into<String>>(mut self, schema: T) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Builds the queue with the specified storage and state.
    ///
    /// # Arguments
    ///
    /// * `storage` - The storage backend for job persistence.
    /// * `state` - The shared state for job execution.
    ///
    /// # Returns
    ///
    /// Returns a new `Queue` instance.
    pub fn build(self, storage: Arc<S>, state: State) -> Queue<S> {
        let name = self.name.clone();
        let concurrency = self.concurrency.unwrap_or(10);
        let interval = self.interval.unwrap_or(Duration::from_secs(1));
        let schema = self.schema;

        Queue {
            concurrency,
            interval,
            name,
            schema,
            state,
            storage,
        }
    }
}

/// A queue for processing jobs asynchronously.
///
/// This struct manages job execution, polling, and concurrency.
///
/// # Fields
///
/// * `concurrency` - The number of jobs that can be executed concurrently.
/// * `interval` - The polling interval for checking new jobs.
/// * `name` - The name of the queue.
/// * `state` - The shared state for job execution.
/// * `storage` - The storage backend for job persistence.
///
/// # Example
///
/// ```rust,no_run
/// use ishikari::{Queue, Storage, Job};
/// use chrono::{DateTime, Utc};
/// use async_trait::async_trait;
/// use std::sync::Arc;
/// use std::time::Duration;
/// #
/// struct MyStorage;
/// #
/// # #[async_trait]
/// # impl Storage for MyStorage {
/// #     type Error = std::io::Error;
/// #     async fn cancel_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn complete_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn discard_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn error_job(&self, _id: i64, _msg: &str, _at: DateTime<Utc>) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn retry_job(&self, _id: i64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn snooze_job(&self, _id: i64, _snooze: u64) -> Result<(), Self::Error> { unimplemented!() }
/// #     async fn fetch_jobs(&self) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
/// #     async fn prune_jobs(&self) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
/// #     async fn stage_jobs(&self, _concurrency: i32) -> Result<usize, Self::Error> { unimplemented!() }
/// #     async fn fetch_and_execute_jobs(&self, _queue: &str, _schema: Option<&str>, _limit: i32) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
/// # }
///
/// let storage = Arc::new(MyStorage);
/// let state = Arc::new(());
///
/// let queue = Queue::builder("my_queue")
///     .concurrency(5)
///     .interval(Duration::from_secs(2))
///     .build(storage, state);
///
/// queue.start();
/// ```
#[derive(Debug)]
pub struct Queue<S>
where
    S: Storage + 'static,
{
    pub concurrency: u32,
    pub interval: Duration,
    pub name: QueueName,
    pub schema: Option<String>,
    pub state: State,
    pub storage: Arc<S>,
}

impl<S> Queue<S>
where
    S: Storage + 'static,
{
    /// Creates a new `QueueBuilder` with the specified queue name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the queue.
    ///
    /// # Returns
    ///
    /// Returns a new `QueueBuilder` instance.
    pub fn builder(name: &str) -> QueueBuilder<S> {
        QueueBuilder {
            name: name.into(),
            concurrency: None,
            interval: None,
            schema: None,
            storage: PhantomData,
        }
    }

    /// Starts the queue, polling for jobs at the specified interval.
    ///
    /// This method spawns a new task to run the queue asynchronously.
    #[instrument(skip(self), fields(queue = self.name.as_str()))]
    pub fn start(self) {
        info!("starting queue");
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                error!("Queue run loop failed: {}", e);
            }
        });
    }

    /// Runs the queue, polling for jobs at the specified interval.
    ///
    /// This method is called internally by `start()`.
    #[instrument(skip(self), fields(queue = self.name.as_str()))]
    async fn run(self) -> anyhow::Result<()> {
        let mut interval = pin!(tokio::time::interval(self.interval));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("polling jobs");
                    execute_jobs(&self).await;
                }
            }
        }
    }
}

/// Extracts a human-readable message from a panic payload.
fn extract_panic_message(panic: &Box<dyn Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[instrument(skip(queue), fields(queue = queue.name.as_str()))]
async fn execute_jobs<S: Storage + 'static>(queue: &Queue<S>) {
    match queue
        .storage
        .fetch_and_execute_jobs(
            queue.name.as_str(),
            queue.schema.as_deref(),
            queue.concurrency as i32,
        )
        .await
    {
        Ok(jobs) => {
            if jobs.is_empty() {
                return;
            }

            info!(count = jobs.len(), "executing jobs");

            for job in jobs {
                let state = Arc::clone(&queue.state);
                let storage = Arc::clone(&queue.storage);

                tokio::spawn(async move {
                    let worker_result = job.worker();
                    let worker = match worker_result {
                        Ok(worker) => worker,
                        Err(e) => {
                            error!(
                                id = job.id,
                                error = e.to_string(),
                                "failed to deserialize worker"
                            );
                            if let Err(e) = storage
                                .error_job(
                                    job.id,
                                    &format!("Failed to deserialize worker: {e}"),
                                    Backoff::Exponential(ChronoDuration::seconds(5))
                                        .next_retry(job.attempt),
                                )
                                .await
                            {
                                error!(id = job.id, error = %e, "failed to mark job as errored in storage");
                            }
                            return;
                        }
                    };
                    let context = Context::new(job.clone().into(), state);

                    // Wrap perform in catch_unwind to handle panics gracefully
                    let result = AssertUnwindSafe(worker.perform(context))
                        .catch_unwind()
                        .await;

                    match result {
                        Ok(Ok(status)) => match status {
                            Status::Complete(complete) => {
                                info!(id = job.id, result = complete.0, "job completed");
                                if let Err(e) = storage.complete_job(job.id).await {
                                    error!(id = job.id, error = %e, "failed to complete job in storage");
                                }
                            }
                            Status::Cancel(cancel) => {
                                info!(id = job.id, reason = cancel.0, "job cancelled");
                                if let Err(e) = storage.cancel_job(job.id).await {
                                    error!(id = job.id, error = %e, "failed to cancel job in storage");
                                }
                            }
                            Status::Snooze(snooze) => {
                                info!(id = job.id, snooze = snooze.0, "job snoozed");
                                if let Err(e) = storage.snooze_job(job.id, snooze.0).await {
                                    error!(id = job.id, error = %e, "failed to snooze job in storage");
                                }
                            }
                        },
                        Ok(Err(e)) => {
                            error!(
                                id = job.id,
                                attempt = job.attempt,
                                error = e.to_string(),
                                "job failed",
                            );
                            if let Err(storage_err) = storage
                                .error_job(job.id, &e.to_string(), worker.backoff(job.attempt))
                                .await
                            {
                                error!(id = job.id, error = %storage_err, "failed to mark job as errored in storage");
                            }

                            if job.attempt >= job.max_attempts {
                                info!(id = job.id, "job discarded");
                                if let Err(e) = storage.discard_job(job.id).await {
                                    error!(id = job.id, error = %e, "failed to discard job in storage");
                                }
                            }
                        }
                        Err(panic_info) => {
                            let panic_msg = extract_panic_message(&panic_info);
                            error!(
                                id = job.id,
                                attempt = job.attempt,
                                panic = %panic_msg,
                                "worker panicked"
                            );
                            if let Err(storage_err) = storage
                                .error_job(
                                    job.id,
                                    &format!("Worker panicked: {panic_msg}"),
                                    worker.backoff(job.attempt),
                                )
                                .await
                            {
                                error!(id = job.id, error = %storage_err, "failed to mark panicked job as errored in storage");
                            }

                            if job.attempt >= job.max_attempts {
                                info!(id = job.id, "job discarded after panic");
                                if let Err(e) = storage.discard_job(job.id).await {
                                    error!(id = job.id, error = %e, "failed to discard panicked job in storage");
                                }
                            }
                        }
                    }
                });
            }
        }
        Err(e) => error!(error = ?e, "failed to fetch jobs"),
    }
}
