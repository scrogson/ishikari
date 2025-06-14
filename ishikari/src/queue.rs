use crate::{Backoff, Context, State, Status, Storage};
use chrono::Duration as ChronoDuration;
use std::marker::PhantomData;
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
/// #     async fn fetch_and_execute_jobs(&self, _queue: &str, _limit: i32) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
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

        Queue {
            concurrency,
            interval,
            name,
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
/// #     async fn fetch_and_execute_jobs(&self, _queue: &str, _limit: i32) -> Result<Vec<Job>, Self::Error> { unimplemented!() }
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
            self.run().await.unwrap();
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

#[instrument(skip(queue), fields(queue = queue.name.as_str()))]
async fn execute_jobs<S: Storage + 'static>(queue: &Queue<S>) {
    match queue
        .storage
        .fetch_and_execute_jobs(queue.name.as_str(), queue.concurrency as i32)
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
                            let _ = storage
                                .error_job(
                                    job.id,
                                    &format!("Failed to deserialize worker: {}", e),
                                    Backoff::Exponential(ChronoDuration::seconds(5))
                                        .next_retry(job.attempt),
                                )
                                .await;
                            return;
                        }
                    };
                    let context = Context::new(job.clone().into(), state);

                    // TODO: handle panics and storage errors.
                    match worker.perform(context).await {
                        Ok(result) => match result {
                            Status::Complete(complete) => {
                                info!(id = job.id, result = complete.0, "job completed");
                                let _ = storage.complete_job(job.id).await;
                            }
                            Status::Cancel(cancel) => {
                                info!(id = job.id, reason = cancel.0, "job cancelled");
                                let _ = storage.cancel_job(job.id).await;
                            }
                            Status::Snooze(snooze) => {
                                info!(id = job.id, snooze = snooze.0, "job snoozed");
                                let _ = storage.snooze_job(job.id, snooze.0).await;
                            }
                        },
                        Err(e) => {
                            error!(
                                id = job.id,
                                attempt = job.attempt,
                                error = e.to_string(),
                                "job failed",
                            );
                            let _ = storage
                                .error_job(job.id, &e.to_string(), worker.backoff(job.attempt))
                                .await;

                            if job.attempt >= job.max_attempts {
                                info!(id = job.id, "job discarded");
                                let _ = storage.discard_job(job.id).await;
                            }
                        }
                    }
                });
            }
        }
        Err(e) => error!(error = ?e, "failed to fetch jobs"),
    }
}
