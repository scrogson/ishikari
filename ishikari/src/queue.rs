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

use crate::{Backoff, Context, ErrorKind, Job, Postgres, State, Status, Worker};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::FutureExt;
use std::any::Any;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use tokio::task::JoinSet;
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
    /// This determines how often the queue checks for new jobs. Panics if `interval` is zero.
    #[track_caller]
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(crate::nonzero(interval, "the queue's interval"));
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
        let (_, stop) = watch::channel(None);
        self.build_on(storage, state, node.into(), stop)
    }

    /// Build the queue to run on `node` (the engine's node name), stopping when `stop` is set.
    pub(crate) fn build_on(
        self,
        storage: Arc<Postgres>,
        state: State,
        node: Arc<str>,
        stop: watch::Receiver<Option<Instant>>,
    ) -> Queue {
        let concurrency = self.concurrency.unwrap_or(10);
        Queue {
            node,
            concurrency,
            interval: self.interval.unwrap_or(Duration::from_secs(1)),
            name: self.name,
            state,
            storage,
            stop,
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
    /// Set to a deadline to stop the queue.
    stop: watch::Receiver<Option<Instant>>,
}

impl Queue {
    /// Creates a new `QueueBuilder` with the specified queue name.
    pub fn builder(name: &str) -> QueueBuilder {
        QueueBuilder {
            name: name.into(),
            concurrency: None,
            interval: None,
        }
    }

    /// Starts the queue, polling for jobs at the specified interval.
    ///
    /// This method spawns a new task to run the queue asynchronously. A queue started this way
    /// runs until the runtime stops; queues an [`Engine`](crate::Engine) starts stop with
    /// [`Engine::shutdown`](crate::Engine::shutdown).
    #[instrument(skip(self), fields(queue = self.name.as_str()))]
    pub fn start(self) {
        self.spawn();
    }

    /// Start polling in a background task, returning its handle.
    pub(crate) fn spawn(self) -> tokio::task::JoinHandle<QueueStop> {
        info!(queue = self.name.as_str(), "starting queue");
        tokio::spawn(self.run())
    }

    /// Poll until told to stop, then drain.
    #[instrument(skip(self), fields(queue = self.name.as_str()))]
    async fn run(mut self) -> QueueStop {
        let mut interval = tokio::time::interval(self.interval);
        let mut running = Running::default();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    debug!("polling jobs");
                    running.reap();
                    self.execute_jobs(&mut running).await;
                }
                deadline = stop_requested(&mut self.stop) => {
                    return self.drain(running, deadline).await;
                }
            }
        }
    }

    /// Claim as many jobs as there are free slots and start them. Jobs still running from
    /// earlier ticks count against the limit.
    async fn execute_jobs(&self, running: &mut Running) {
        let free = self.concurrency as usize - running.set.len().min(self.concurrency as usize);
        if free == 0 {
            return;
        }
        let jobs = match self
            .storage
            .claim(self.name.as_str(), &self.node, free as i32)
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
            let claimed = (job.id, job.attempt);
            let handle = running.set.spawn(run_job(
                Arc::clone(&self.storage),
                Arc::clone(&self.state),
                job,
            ));
            running.jobs.insert(handle.id(), claimed);
        }
    }

    /// Stop claiming, let running jobs finish until `deadline`, then abort the rest and give
    /// their jobs back to the queue with the attempt refunded: they were interrupted, not
    /// failed.
    async fn drain(self, mut running: Running, deadline: Instant) -> QueueStop {
        let mut finished = 0;
        let grace = tokio::time::sleep_until(deadline.into());
        tokio::pin!(grace);
        while !running.set.is_empty() {
            tokio::select! {
                Some(done) = running.set.join_next_with_id() => {
                    let id = match done {
                        Ok((id, ())) => id,
                        Err(e) => e.id(),
                    };
                    running.jobs.remove(&id);
                    finished += 1;
                }
                _ = &mut grace => break,
            }
        }
        let left: Vec<(i64, i32)> = running.jobs.values().copied().collect();
        running.set.abort_all();
        while running.set.join_next().await.is_some() {}
        let returned = if left.is_empty() {
            0
        } else {
            match self.storage.give_back(&left).await {
                Ok(n) => n,
                Err(e) => {
                    error!(error = %e, "failed to give back interrupted jobs");
                    0
                }
            }
        };
        info!(finished, returned, "queue stopped");
        QueueStop { finished, returned }
    }
}

/// What a queue did when it stopped.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueStop {
    /// Jobs that finished during the grace period.
    pub finished: usize,
    /// Jobs interrupted when the grace period ran out, given back to the queue.
    pub returned: usize,
}

/// A queue's running jobs: their tasks, and the (id, attempt) each task runs.
#[derive(Default)]
struct Running {
    set: JoinSet<()>,
    jobs: HashMap<tokio::task::Id, (i64, i32)>,
}

impl Running {
    /// Forget tasks that have finished.
    fn reap(&mut self) {
        while let Some(done) = self.set.try_join_next_with_id() {
            let id = match done {
                Ok((id, ())) => id,
                Err(e) => e.id(),
            };
            self.jobs.remove(&id);
        }
    }
}

/// Resolves with the deadline once a stop is requested; never, for a queue nobody can stop.
async fn stop_requested(stop: &mut watch::Receiver<Option<Instant>>) -> Instant {
    loop {
        if let Some(deadline) = *stop.borrow_and_update() {
            return deadline;
        }
        if stop.changed().await.is_err() {
            std::future::pending::<()>().await;
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

/// What a run came to, to be written back to its job.
enum Outcome {
    Complete,
    Cancel(Option<String>),
    Snooze(u64),
    Error(ErrorKind, String, DateTime<Utc>),
}

impl Outcome {
    fn name(&self) -> &'static str {
        match self {
            Outcome::Complete => "complete",
            Outcome::Cancel(_) => "cancel",
            Outcome::Snooze(_) => "snooze",
            Outcome::Error(..) => "error",
        }
    }
}

/// Run one claimed job and record its outcome.
async fn run_job(storage: Arc<Postgres>, state: State, job: Job) {
    let outcome = run(&job, state).await;
    record(&storage, &job, &outcome).await;
}

/// Run the job's worker, turning its panics and its timeout into errors.
async fn run(job: &Job, state: State) -> Outcome {
    let worker = match job.worker() {
        Ok(worker) => worker,
        Err(e) => {
            error!(id = job.id, error = %e, "failed to deserialize worker");
            return Outcome::Error(
                ErrorKind::Error,
                format!("Failed to deserialize worker: {e}"),
                default_backoff(job.attempt),
            );
        }
    };
    let limit = match catch_unwind(AssertUnwindSafe(|| worker.timeout())) {
        Ok(limit) => limit,
        Err(panic) => {
            let panic_msg = extract_panic_message(&panic);
            error!(id = job.id, attempt = job.attempt, panic = %panic_msg, "worker's timeout panicked");
            return Outcome::Error(
                ErrorKind::Panic,
                format!("Worker's timeout panicked: {panic_msg}"),
                retry_at(&*worker, job),
            );
        }
    };
    let context = Context::new(Arc::new(job.clone()), state);

    // Wrap perform in catch_unwind to handle panics gracefully, and in the worker's timeout.
    let run = AssertUnwindSafe(worker.perform(context)).catch_unwind();
    let result = match limit {
        Some(limit) => match tokio::time::timeout(limit, run).await {
            Ok(result) => result,
            Err(_) => {
                error!(id = job.id, attempt = job.attempt, ?limit, "job timed out");
                return Outcome::Error(
                    ErrorKind::Timeout,
                    format!("timed out after {limit:?}"),
                    retry_at(&*worker, job),
                );
            }
        },
        None => run.await,
    };

    match result {
        Ok(Ok(Status::Complete(complete))) => {
            info!(id = job.id, result = complete.0, "job completed");
            Outcome::Complete
        }
        Ok(Ok(Status::Cancel(cancel))) => {
            info!(id = job.id, reason = cancel.0, "job cancelled");
            Outcome::Cancel(cancel.0)
        }
        Ok(Ok(Status::Snooze(snooze))) => {
            info!(id = job.id, snooze = snooze.0, "job snoozed");
            Outcome::Snooze(snooze.0)
        }
        Ok(Err(e)) => {
            error!(
                id = job.id,
                attempt = job.attempt,
                error = e.to_string(),
                "job failed"
            );
            Outcome::Error(ErrorKind::Error, e.to_string(), retry_at(&*worker, job))
        }
        Err(panic_info) => {
            let panic_msg = extract_panic_message(&panic_info);
            error!(id = job.id, attempt = job.attempt, panic = %panic_msg, "worker panicked");
            Outcome::Error(
                ErrorKind::Panic,
                format!("Worker panicked: {panic_msg}"),
                retry_at(&*worker, job),
            )
        }
    }
}

/// When to run a failed job again: the worker's backoff, or the default when it panics.
fn retry_at(worker: &dyn Worker, job: &Job) -> DateTime<Utc> {
    catch_unwind(AssertUnwindSafe(|| worker.backoff(job.attempt))).unwrap_or_else(|panic| {
        let panic_msg = extract_panic_message(&panic);
        error!(id = job.id, attempt = job.attempt, panic = %panic_msg, "worker's backoff panicked; using the default");
        default_backoff(job.attempt)
    })
}

/// The default [`Worker::backoff`].
fn default_backoff(attempt: i32) -> DateTime<Utc> {
    Backoff::Exponential(ChronoDuration::seconds(5)).next_retry(attempt)
}

/// How long a failed outcome write is retried. Until it is written the job stays executing, and
/// a live node's jobs aren't rescued, so a transient error (a dropped connection, a failover)
/// mustn't drop it.
const RECORD_FOR: Duration = Duration::from_secs(60);

/// Write the outcome, retrying a failed write with backoff for up to [`RECORD_FOR`]. A job that
/// moved on while it ran (rescued, cancelled or retried) keeps its new state; the result is
/// dropped.
async fn record(storage: &Postgres, job: &Job, outcome: &Outcome) {
    let started = Instant::now();
    let mut delay = Duration::from_millis(100);
    loop {
        let written = match outcome {
            Outcome::Complete => storage.complete(job).await,
            Outcome::Cancel(reason) => storage.cancel(job, reason.as_deref()).await,
            Outcome::Snooze(seconds) => storage.snooze(job, *seconds).await,
            Outcome::Error(kind, error, retry_at) => {
                storage.error(job, *kind, error, *retry_at).await
            }
        };
        let outcome = outcome.name();
        match written {
            Ok(true) => return,
            Ok(false) => {
                warn!(
                    id = job.id,
                    attempt = job.attempt,
                    outcome,
                    "job changed while it ran; result dropped"
                );
                return;
            }
            Err(e) if started.elapsed() + delay < RECORD_FOR => {
                warn!(id = job.id, outcome, error = %e, retry_in = ?delay, "failed to record job outcome; retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(e) => {
                error!(id = job.id, outcome, error = %e, "failed to record job outcome; giving up, the job stays executing until its node stops");
                return;
            }
        }
    }
}
