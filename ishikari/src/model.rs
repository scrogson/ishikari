//! Data models for jobs and job states.
//!
//! This module contains the core data structures used throughout Ishikari:
//!
//! - `Job` - Represents a job in the system with all its metadata
//! - `JobState` - Enum representing the various states a job can be in
//!
//! Jobs progress through different states during their lifecycle:
//! - `Pending` - Waiting on the jobs it depends on (workflow steps)
//! - `Available` - Ready to be processed
//! - `Scheduled` - Scheduled for future execution
//! - `Executing` - Currently being processed
//! - `Retryable` - Failed but will be retried
//! - `Completed` - Successfully finished
//! - `Discarded` - Failed permanently
//! - `Cancelled` - Cancelled by user or system
//!
//! The `Job` struct contains all the metadata needed for job processing,
//! including queue information, retry counts, timestamps, and the serialized
//! job data.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Waiting on the jobs it depends on (a workflow step).
    Pending,
    Available,
    Scheduled,
    Executing,
    Retryable,
    Completed,
    Discarded,
    Cancelled,
}

impl JobState {
    /// The state's name in the database.
    pub fn as_str(&self) -> &'static str {
        match self {
            JobState::Pending => "pending",
            JobState::Available => "available",
            JobState::Scheduled => "scheduled",
            JobState::Executing => "executing",
            JobState::Retryable => "retryable",
            JobState::Completed => "completed",
            JobState::Discarded => "discarded",
            JobState::Cancelled => "cancelled",
        }
    }

    /// Whether a job in this state will never run again on its own.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobState::Completed | JobState::Discarded | JobState::Cancelled
        )
    }
}

impl std::str::FromStr for JobState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "pending" => JobState::Pending,
            "available" => JobState::Available,
            "scheduled" => JobState::Scheduled,
            "executing" => JobState::Executing,
            "retryable" => JobState::Retryable,
            "completed" => JobState::Completed,
            "discarded" => JobState::Discarded,
            "cancelled" => JobState::Cancelled,
            other => return Err(format!("unknown job state: {other}")),
        })
    }
}

// The state column is the `ishikari_job_state` enum of whichever schema the jobs live in, so
// it is matched by name in any schema rather than as one fixed type. Bound as a parameter it is
// text; queries cast it to their schema's enum.
impl sqlx::Type<sqlx::Postgres> for JobState {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <&str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        use sqlx::TypeInfo;
        let name = ty.name();
        name == "ishikari_job_state"
            || name.ends_with(".ishikari_job_state")
            || <&str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JobState {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <&str as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(s.parse()?)
    }
}

impl sqlx::Encode<'_, sqlx::Postgres> for JobState {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Job {
    pub id: i64,
    pub state: JobState,
    pub queue: String,
    pub worker: String,
    pub args: JsonValue,
    pub errors: Vec<JsonValue>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub priority: i32,
    pub tags: Vec<String>,
    pub meta: Option<JsonValue>,
    pub attempted_by: Option<Vec<String>>,
    pub inserted_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub discarded_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    /// The workflow this job belongs to (if part of a workflow).
    /// Used for pipelines, DAGs, and sagas.
    pub workflow_id: Option<i64>,
}

impl Job {
    pub fn worker(&self) -> anyhow::Result<Box<dyn crate::Worker>> {
        serde_json::from_value::<Box<dyn crate::Worker>>(self.args.clone())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize worker: {}", e))
    }
}
