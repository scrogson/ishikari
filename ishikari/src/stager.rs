//! Job staging functionality for scheduled and retryable jobs.
//!
//! The Stager is responsible for moving jobs from scheduled/retryable states to available
//! state when they're ready to be processed. This includes:
//!
//! - Moving scheduled jobs to available when their scheduled time arrives
//! - Moving retryable jobs to available after their backoff period expires
//!
//! The stager runs on a configurable interval and processes a limited number of jobs
//! per iteration to avoid overwhelming the database.
//!
//! # Example
//!
//! ```rust,no_run
//! use ishikari::{Stager, Postgres};
//! use std::{sync::Arc, time::Duration};
//! use sqlx::PgPool;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let database_url = std::env::var("DATABASE_URL")?;
//!     let pool = PgPool::connect(&database_url).await?;
//!     let storage = Arc::new(Postgres::new(pool));
//!
//!     let stager = Stager::new(storage, Duration::from_secs(30), 100);
//!     stager.start();
//! #   Ok(())
//! # }
//! ```

use crate::Postgres;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, instrument};

/// A component responsible for periodically moving jobs from scheduled/retryable states to
/// available state.
///
/// The Stager runs in the background and periodically checks for jobs that are ready to be
/// processed. It moves jobs from scheduled/retryable states to the available state in batches,
/// up to a specified limit. A failed database call is logged and retried on the next tick; it
/// never stops the stager.
#[derive(Debug)]
pub struct Stager {
    storage: Arc<Postgres>,
    interval: Duration,
    limit: i32,
}

impl Stager {
    /// Creates a new Stager instance.
    ///
    /// # Arguments
    ///
    /// * `storage` - The job storage
    /// * `interval` - The duration between staging attempts
    /// * `limit` - The maximum number of jobs to stage in each attempt
    pub fn new(storage: Arc<Postgres>, interval: Duration, limit: i32) -> Self {
        Self {
            storage,
            interval,
            limit,
        }
    }

    /// Starts the stager in a background task, which runs until it is aborted.
    #[instrument(skip(self))]
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            info!("starting stager");
            let mut interval = tokio::time::interval(self.interval);
            loop {
                interval.tick().await;
                self.stage_once().await;
            }
        })
    }

    /// Stage one batch, logging rather than returning a failure.
    pub async fn stage_once(&self) -> usize {
        match self.storage.stage(self.limit).await {
            Ok(count) => {
                debug!(count, "staging jobs");
                if count > 0 {
                    info!(count, "staged jobs");
                }
                count
            }
            Err(e) => {
                error!(error = %e, "failed to stage jobs; retrying next tick");
                0
            }
        }
    }
}
