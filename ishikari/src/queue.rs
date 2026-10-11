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

use crate::{Backoff, Context, ErrorKind, Job, Postgres, State, Status};
use chrono::Duration as ChronoDuration;
use futures::FutureExt;
use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, instrument, warn};

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
/// This struct allows you to set the queue's name, concurrency, and polling interval before
/// adding it to an [`Engine`](crate::Engine).
///
/// # Example
///
/// ```rust
/// use ishikari::Queue;
/// use std::time::Duration;
///
/// let queue = Queue::builder("my_queue")
///     .concurrency(5)
///     .interval(Duration::from_secs(2));
/// ```
#[derive(Debug)]
pub struct QueueBuilder {
    pub name: QueueName,
    pub concurrency: Option<u32>,
    pub interval: Option<Duration>,
}

impl QueueBuilder {
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
    pub fn build(self, storage: Arc<Postgres>, state: State) -> Queue {
        let node = format!("{}-{}", self.name.as_str(), std::process::id());
        self.build_on(storage, state, node.into())
    }

    /// Build the queue to run on `node` (the engine's node name).
    pub(crate) fn build_on(self, storage: Arc<Postgres>, state: State, node: Arc<str>) -> Queue {
        let concurrency = self.concurrency.unwrap_or(10);
        Queue {
            node,
            concurrency,
            interval: self.interval.unwrap_or(Duration::from_secs(1)),
            name: self.name,
            state,
            storage,
            slots: Arc::new(Semaphore::new(concurrency as usize)),
        }
    }
}

/// A queue for processing jobs asynchronously.
///
/// This struct manages job execution, polling, and concurrency. Queues are usually built and
/// started by an [`Engine`](crate::Engine).
///
/// # Fields
///
/// * `concurrency` - The number of jobs that can be executed concurrently.
/// * `interval` - The polling interval for checking new jobs.
/// * `name` - The name of the queue.
/// * `state` - The shared state for job execution.
/// * `storage` - The storage backend for job persistence.
#[derive(Debug)]
pub struct Queue {
    /// The node jobs are claimed for.
    pub node: Arc<str>,
    pub concurrency: u32,
    pub interval: Duration,
    pub name: QueueName,
    pub state: State,
    pub storage: Arc<Postgres>,
    /// One permit per job that may run at once; a running job holds one until its outcome is
    /// written.
    slots: Arc<Semaphore>,
}

impl Queue {
    /// Creates a new `QueueBuilder` with the specified queue name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the queue.
    ///
    /// # Returns
    ///
    /// Returns a new `QueueBuilder` instance.
    pub fn builder(name: &str) -> QueueBuilder {
        QueueBuilder {
            name: name.into(),
            concurrency: None,
            interval: None,
        }
    }

    /// Starts the queue, polling for jobs at the specified interval.
    ///
    /// This method spawns a new task to run the queue asynchronously.
    #[instrument(skip(self), fields(queue = self.name.as_str()))]
    pub fn start(self) {
        self.spawn();
    }

    /// Start polling in a background task, returning its handle.
    pub(crate) fn spawn(self) -> tokio::task::JoinHandle<()> {
        info!(queue = self.name.as_str(), "starting queue");
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                error!("Queue run loop failed: {}", e);
            }
        })
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
async fn execute_jobs(queue: &Queue) {
    // Only claim as many jobs as there are free slots, so jobs still running from earlier ticks
    // count against the limit.
    let free = queue.slots.available_permits();
    if free == 0 {
        return;
    }
    let jobs = match queue
        .storage
        .claim(queue.name.as_str(), &queue.node, free as i32)
        .await
    {
        Ok(jobs) => jobs,
        Err(e) => {
            error!(error = ?e, "failed to fetch jobs");
            return;
        }
    };
    if jobs.is_empty() {
        return;
    }
    info!(count = jobs.len(), "executing jobs");
    for job in jobs {
        let slot = Arc::clone(&queue.slots)
            .try_acquire_owned()
            .expect("claimed no more jobs than free slots");
        let storage = Arc::clone(&queue.storage);
        let state = Arc::clone(&queue.state);
        tokio::spawn(async move {
            run_job(storage, state, job).await;
            drop(slot);
        });
    }
}

/// Run one claimed job and record its outcome.
async fn run_job(storage: Arc<Postgres>, state: State, job: Job) {
    let worker = match job.worker() {
        Ok(worker) => worker,
        Err(e) => {
            error!(id = job.id, error = %e, "failed to deserialize worker");
            let retry_at = Backoff::Exponential(ChronoDuration::seconds(5)).next_retry(job.attempt);
            let written = storage
                .error(
                    &job,
                    ErrorKind::Error,
                    &format!("Failed to deserialize worker: {e}"),
                    retry_at,
                )
                .await;
            log_write(&job, "error", written);
            return;
        }
    };
    let context = Context::new(Arc::new(job.clone()), state);

    // Wrap perform in catch_unwind to handle panics gracefully
    let result = AssertUnwindSafe(worker.perform(context))
        .catch_unwind()
        .await;

    match result {
        Ok(Ok(Status::Complete(complete))) => {
            info!(id = job.id, result = complete.0, "job completed");
            log_write(&job, "complete", storage.complete(&job).await);
        }
        Ok(Ok(Status::Cancel(cancel))) => {
            info!(id = job.id, reason = cancel.0, "job cancelled");
            log_write(
                &job,
                "cancel",
                storage.cancel(&job, cancel.0.as_deref()).await,
            );
        }
        Ok(Ok(Status::Snooze(snooze))) => {
            info!(id = job.id, snooze = snooze.0, "job snoozed");
            log_write(&job, "snooze", storage.snooze(&job, snooze.0).await);
        }
        Ok(Err(e)) => {
            error!(
                id = job.id,
                attempt = job.attempt,
                error = e.to_string(),
                "job failed"
            );
            let written = storage
                .error(
                    &job,
                    ErrorKind::Error,
                    &e.to_string(),
                    worker.backoff(job.attempt),
                )
                .await;
            log_write(&job, "error", written);
        }
        Err(panic_info) => {
            let panic_msg = extract_panic_message(&panic_info);
            error!(id = job.id, attempt = job.attempt, panic = %panic_msg, "worker panicked");
            let written = storage
                .error(
                    &job,
                    ErrorKind::Panic,
                    &format!("Worker panicked: {panic_msg}"),
                    worker.backoff(job.attempt),
                )
                .await;
            log_write(&job, "error", written);
        }
    }
}

/// Log an outcome write that failed, or that found the job had moved on (rescued, cancelled or
/// retried while this run was going): its result is dropped.
fn log_write(job: &Job, outcome: &str, written: Result<bool, sqlx::Error>) {
    match written {
        Ok(true) => {}
        Ok(false) => warn!(
            id = job.id,
            attempt = job.attempt,
            outcome,
            "job changed while it ran; result dropped"
        ),
        Err(e) => error!(id = job.id, outcome, error = %e, "failed to record job outcome"),
    }
}
