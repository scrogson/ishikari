//! Data models for jobs and job states.
//!
//! This module contains the core data structures used throughout Ishikari:
//!
//! - `Job` - Represents a job in the system with all its metadata
//! - `JobState` - Enum representing the various states a job can be in
//!
//! Jobs progress through different states during their lifecycle:
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

#[derive(Clone, Debug, Serialize, Deserialize, sqlx::Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "ishikari_job_state", rename_all = "lowercase")]
pub enum JobState {
    Available,
    Scheduled,
    Executing,
    Retryable,
    Completed,
    Discarded,
    Cancelled,
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
}

impl Job {
    pub fn worker(&self) -> anyhow::Result<Box<dyn crate::Worker>> {
        serde_json::from_value::<Box<dyn crate::Worker>>(self.args.clone())
            .map_err(|e| anyhow::anyhow!("Failed to deserialize worker: {}", e))
    }
}
