pub use serde;
use serde::Serialize;
use sqlx::FromRow;
use sqlx::PgExecutor;

mod model;
mod queue;
mod result;
mod stager;

pub use ishikari_macros::{job, worker};

pub use model::{Job, JobState};
pub use queue::Queue;
pub use result::{Cancel, Complete, Discard, PerformError, PerformResult, Snooze, Status};
pub use stager::Stager;

/// A prelude for building Ishikari workers.
pub mod prelude {
    pub use crate::{Cancel, Complete, Context, Discard, PerformResult, Snooze, Status, Worker};
}

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{info, instrument};

/// A context for a worker.
#[derive(Debug)]
pub struct Context {
    pub job: Arc<Job>,
    pub queue: Arc<Queue>,
}

impl Context {
    pub fn new(job: Arc<Job>, queue: Arc<Queue>) -> Self {
        Self { job, queue }
    }

    pub fn job(&self) -> Arc<Job> {
        Arc::clone(&self.job)
    }

    pub fn queue(&self) -> Arc<Queue> {
        Arc::clone(&self.queue)
    }

    pub fn extract<T: Any + Send + Sync + 'static>(&self) -> Result<Arc<T>, &'static str> {
        // Attempt to clone and downcast the Arc
        if let Some(downcasted) = Arc::clone(&self.queue.context).downcast::<T>().ok() {
            Ok(downcasted)
        } else {
            Err("Failed to extract the specified type from the context")
        }
    }
}

#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    /// Configure the queue the job should be inserted into. Defaults to `default`.
    fn queue(&self) -> &'static str {
        "default"
    }

    /// Configure the max number of times a job can be retried before discarding.
    fn max_attempts(&self) -> i32 {
        20
    }

    fn cron(&self) -> Option<()> {
        None
    }

    /// Control when the next attempt should be scheduled.
    ///
    /// Given a current attempt, this should calculate the number of seconds in the future the job
    /// should be retried.
    fn backoff(&self, _attempt: u32) -> u32 {
        // TODO: figure out how this should be done
        2
    }

    /// Perform the job.
    async fn perform(&self, context: Context) -> PerformResult;
}

// TODO: use an ishikari::Error
#[instrument(skip(executor))]
pub async fn insert<'a, J, E>(job: J, executor: E) -> Result<Job, sqlx::Error>
where
    J: Debug + Serialize + Worker + Send + Sync + 'static,
    E: PgExecutor<'a>,
{
    // TODO: remove this unwrap
    let args = serde_json::to_value(&job as &dyn Worker).unwrap();

    let row =
        sqlx::query(r#"insert into jobs (queue, worker, args, max_attempts) values ($1, $2, $3, $4) returning *"#)
            .bind(&job.queue())
            .bind("NativeWorker")
            .bind(args)
            .bind(&job.max_attempts())
            .fetch_one(executor)
            .await?;

    let inserted = Job::from_row(&row)?;

    info!("Job inserted id={}, args={:?}", inserted.id, job);

    Ok(inserted)
}
