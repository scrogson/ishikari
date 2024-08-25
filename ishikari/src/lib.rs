use chrono::{DateTime, Duration, Utc};
use rand::Rng;
pub use serde;
use serde::Serialize;
use sqlx::FromRow;
use sqlx::PgExecutor;

mod engine;
mod model;
mod queue;
mod result;
mod stager;

pub use ishikari_macros::{job, worker};

pub use engine::{Engine, Postgres, Storage};
pub use model::{Job, JobState};
pub use queue::Queue;
pub use result::{Cancel, Complete, PerformError, PerformResult, Snooze, Status};
pub use stager::Stager;

/// A prelude for building Ishikari workers.
pub mod prelude {
    pub use crate::{Cancel, Complete, Context, PerformResult, Snooze, Status, Worker};
}

pub(crate) type State = Arc<dyn std::any::Any + Send + Sync>;

/// A backoff strategy for retrying jobs.
pub enum Backoff {
    /// Fixed delay duration
    Fixed(Duration),
    /// Linear backoff based on attempt number
    Linear(Duration),
    /// Exponential backoff with a base duration
    Exponential(Duration),
    /// Exponential with jitter
    ExponentialJitter(Duration),
    /// Custom backoff strategy
    Custom(Box<dyn Fn(i32) -> DateTime<Utc> + Send + Sync>),
}

impl Backoff {
    pub fn next_retry(&self, attempt: i32) -> DateTime<Utc> {
        match self {
            Backoff::Fixed(duration) => Utc::now() + *duration,
            Backoff::Linear(base) => Utc::now() + (*base * attempt),
            Backoff::Exponential(base) => {
                let base_seconds = base.num_seconds();
                let exp_delay = base_seconds * 2_i64.pow(attempt as u32);
                Utc::now() + Duration::seconds(exp_delay)
            }
            Backoff::ExponentialJitter(base) => {
                let base_seconds = base.num_seconds();
                let exp_delay = base_seconds * 2_i64.pow(attempt as u32);
                let jitter = rand::thread_rng().gen_range(0..exp_delay);
                Utc::now() + Duration::seconds(exp_delay + jitter)
            }
            Backoff::Custom(strategy) => strategy(attempt),
        }
    }
}

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{info, instrument};

/// A context for a worker.
#[derive(Debug)]
pub struct Context {
    pub job: Arc<Job>,
    pub state: State,
}

/// Worker context.
///
/// The context provides access to the `Job` being executed and the state
/// registered with the `Engine`.
impl Context {
    pub(crate) fn new(job: Arc<Job>, state: State) -> Self {
        Self { job, state }
    }

    /// Return the `Job` being executed.
    pub fn job(&self) -> Arc<Job> {
        Arc::clone(&self.job)
    }

    /// Return the state which was registered with `Engine::with_state`
    pub fn state<T: Any + Send + Sync + 'static>(&self) -> Result<Arc<T>, &'static str> {
        // Attempt to clone and downcast the Arc
        if let Some(downcasted) = Arc::clone(&self.state).downcast::<T>().ok() {
            Ok(downcasted)
        } else {
            Err("Failed to extract the specified type from the context")
        }
    }
}

#[typetag::serde(tag = "ishikari_worker")]
#[async_trait::async_trait]
pub trait Worker: Send + Sync {
    fn worker() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }

    /// Configure the queue the job should be inserted into. Defaults to `default`.
    fn queue(&self) -> &'static str {
        "default"
    }

    /// Configure the max number of times a job can be retried before discarding.
    fn max_attempts(&self) -> i32 {
        20
    }

    /// Control when the next attempt should be scheduled.
    ///
    /// Given a current attempt, this should calculate the number of seconds in the future the job
    /// should be retried.
    ///
    /// Defaults to `Backoff::Exponential(Duration::seconds(5))`
    fn backoff(&self, attempt: i32) -> DateTime<Utc> {
        Backoff::Exponential(Duration::seconds(5)).next_retry(attempt)
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
            .bind(&J::worker())
            .bind(args)
            .bind(&job.max_attempts())
            .fetch_one(executor)
            .await?;

    let inserted = Job::from_row(&row)?;

    info!("Job inserted id={}, args={:?}", inserted.id, job);

    Ok(inserted)
}
