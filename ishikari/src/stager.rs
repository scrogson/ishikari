//! Ishikari Stager
//!
//! The Stager is responsible for moving tasks from scheduled/retryable to available.

use crate::engine::Storage;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, instrument};

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
    pub fn new(storage: Arc<S>, interval: Duration, limit: i32) -> Self {
        Self {
            storage,
            interval,
            limit,
        }
    }

    #[instrument(skip(self))]
    pub fn start(self) -> tokio::task::JoinHandle<Result<(), S::Error>> {
        tokio::spawn(async move {
            info!("starting stager");
            let mut interval = pin!(tokio::time::interval(self.interval));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let count = self.storage.stage_jobs(self.limit).await?;

                        if count > 0 {
                            info!(count = count, "staged jobs");
                        }
                    }
                }
            }
        })
    }
}
