pub use async_trait::async_trait;
pub use serde;
pub use serde::{Deserialize, Serialize};

use sqlx::FromRow;
use sqlx::PgExecutor;

mod job;
mod queue;
mod stager;
pub mod worker;

pub use job::{Job, JobState};
pub use queue::Queue;
pub use stager::Stager;
pub use worker::Worker;

pub async fn insert<'a, J, E>(executor: E, queue: &str, job: J) -> Result<Job, sqlx::Error>
where
    J: std::fmt::Debug + Send + Sync + Serialize + Worker + 'static,
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

    tracing::info!("Created Job(id={}, args={:?})", inserted.id, job);

    Ok(inserted)
}
