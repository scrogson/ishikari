//! Inserting a workflow's jobs.

use crate::insert::{insert_row, Row};
use crate::{InsertOpts, Job, JobState, Worker};
use serde::Serialize;
use sqlx::PgConnection;
use std::fmt::Debug;

/// How a step's job starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StepStart {
    /// No upstream: available at once.
    Ready,
    /// Waits in `pending` until every job it depends on has completed.
    Waiting,
    /// A saga compensation: parked in `pending`, with nothing to wait for, until its saga's
    /// rollback releases it.
    Compensation,
}

/// A step's job with its type erased, so steps of different types go in one workflow.
#[async_trait::async_trait]
pub(crate) trait ErasedStep: Debug + Send + Sync {
    async fn insert_step(
        &self,
        conn: &mut PgConnection,
        schema: Option<&str>,
        workflow_id: i64,
        name: &str,
        start: StepStart,
    ) -> Result<Job, sqlx::Error>;
}

#[async_trait::async_trait]
impl<J> ErasedStep for J
where
    J: Debug + Serialize + Worker + Send + Sync + Clone + 'static,
{
    async fn insert_step(
        &self,
        conn: &mut PgConnection,
        schema: Option<&str>,
        workflow_id: i64,
        name: &str,
        start: StepStart,
    ) -> Result<Job, sqlx::Error> {
        let opts = InsertOpts::new().meta(serde_json::json!({ "ishikari_step": name }));
        let state = match start {
            StepStart::Ready => JobState::Available,
            StepStart::Waiting | StepStart::Compensation => JobState::Pending,
        };
        // A step belongs to its workflow: the worker's uniqueness doesn't apply.
        let row = Row::new(self, &opts)?.without_unique();
        insert_row(conn, schema, &row, Some((state, Some(workflow_id))))
            .await
            .map(crate::Inserted::into_job)
    }
}
