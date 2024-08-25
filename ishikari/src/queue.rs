//! Ishikari Queue

use crate::{Context, State, Status, Storage};
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
    pub fn concurrency(mut self, concurrency: u32) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

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
    pub fn builder(name: &str) -> QueueBuilder<S> {
        QueueBuilder {
            name: name.into(),
            concurrency: None,
            interval: None,
            storage: PhantomData,
        }
    }

    #[instrument(skip(self), fields(queue = self.name.as_str()))]
    pub fn start(self) {
        info!("starting queue");
        tokio::spawn(async move {
            self.run().await.unwrap();
        });
    }

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
        .fetch_and_execute_jobs(&queue.name.as_str(), queue.concurrency as i32)
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
                    // TODO: remove the unwrap!
                    // called `Result::unwrap()` on an `Err` value: Failed to deserialize worker: unknown variant `Summoner`, expected `Fail` or `Sum`
                    let worker = &job.worker().unwrap();
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
