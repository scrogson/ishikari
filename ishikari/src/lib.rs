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
use std::sync::Arc;

/// A context for a worker.
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
    async fn perform(&self, context: Context) -> PerformResult;
}

pub async fn insert<'a, J, E>(executor: E, queue: &str, job: J) -> Result<Job, sqlx::Error>
where
    J: std::fmt::Debug + Serialize + Worker + Send + Sync + 'static,
    E: PgExecutor<'a>,
{
    // TODO: remove this unwrap
    let args = serde_json::to_value(&job as &dyn Worker).unwrap();

    let row =
        sqlx::query(r#"insert into jobs (queue, worker, args) values ($1, $2, $3) returning *"#)
            .bind(queue)
            .bind("NativeWorker")
            .bind(args)
            .fetch_one(executor)
            .await?;

    let inserted = Job::from_row(&row)?;

    tracing::info!("Job inserted id={}, args={:?}", inserted.id, job);

    Ok(inserted)
}
