use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgHasArrayType, PgTypeInfo};

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "job_state", rename_all = "lowercase")]
pub enum JobState {
    Available,
    Scheduled,
    Executing,
    Retryable,
    Completed,
    Discarded,
    Cancelled,
}

impl PgHasArrayType for JobState {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_job_state")
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
    pub inserted_at: DateTime<Utc>,
    pub scheduled_at: DateTime<Utc>,
    pub attempted_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub attempted_by: Option<Vec<String>>,
    pub discarded_at: Option<DateTime<Utc>>,
    pub priority: i32,
    pub tags: Vec<String>,
    pub meta: Option<JsonValue>,
    pub cancelled_at: Option<DateTime<Utc>>,
}

impl Job {
    pub fn worker(&self) -> anyhow::Result<Box<dyn crate::Worker>> {
        serde_json::from_value::<Box<dyn crate::Worker>>(self.args.clone())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize worker: {}", e))
    }
}
