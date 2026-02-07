//! Workflow tracking and orchestration.
//!
//! This module provides the core workflow abstraction that pipelines,
//! DAGs, and sagas are built upon.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// Row type for workflow database queries.
type WorkflowRow = (
    i64,
    String,
    String,
    serde_json::Value,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
);

/// The state of a workflow.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowState {
    /// The workflow is currently running.
    #[default]
    Running,
    /// The workflow completed successfully.
    Completed,
    /// The workflow failed.
    Failed,
    /// The workflow was cancelled.
    Cancelled,
    /// The workflow is compensating (saga rollback in progress).
    Compensating,
    /// The workflow compensation completed.
    Compensated,
}

impl WorkflowState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Compensating => "compensating",
            Self::Compensated => "compensated",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "compensating" => Some(Self::Compensating),
            "compensated" => Some(Self::Compensated),
            _ => None,
        }
    }

    /// Returns true if the workflow is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Compensated
        )
    }
}

/// A workflow instance.
#[derive(Debug, Clone)]
pub struct Workflow {
    /// Unique identifier for this workflow instance.
    pub id: i64,
    /// The name/type of the workflow.
    pub name: String,
    /// Current state of the workflow.
    pub state: WorkflowState,
    /// Arbitrary metadata associated with the workflow.
    pub metadata: serde_json::Value,
    /// When the workflow was created.
    pub inserted_at: DateTime<Utc>,
    /// When the workflow completed (if terminal).
    pub completed_at: Option<DateTime<Utc>>,
}

impl Workflow {
    /// Create a new workflow.
    pub async fn create(
        pool: &PgPool,
        name: &str,
        metadata: serde_json::Value,
        schema: Option<&str>,
    ) -> super::error::Result<Self> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_workflows", s),
            None => "ishikari_workflows".to_string(),
        };

        let query = format!(
            r#"
            INSERT INTO {} (name, state, metadata)
            VALUES ($1, 'running', $2)
            RETURNING id, name, state, metadata, inserted_at, completed_at
            "#,
            table
        );

        let row: WorkflowRow = sqlx::query_as(&query)
            .bind(name)
            .bind(&metadata)
            .fetch_one(pool)
            .await?;

        Ok(Workflow {
            id: row.0,
            name: row.1,
            state: WorkflowState::parse(&row.2).unwrap_or_default(),
            metadata: row.3,
            inserted_at: row.4,
            completed_at: row.5,
        })
    }

    /// Get a workflow by ID.
    pub async fn get(
        pool: &PgPool,
        id: i64,
        schema: Option<&str>,
    ) -> super::error::Result<Option<Self>> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_workflows", s),
            None => "ishikari_workflows".to_string(),
        };

        let query = format!(
            "SELECT id, name, state, metadata, inserted_at, completed_at FROM {} WHERE id = $1",
            table
        );

        let row: Option<WorkflowRow> = sqlx::query_as(&query).bind(id).fetch_optional(pool).await?;

        Ok(row.map(|r| Workflow {
            id: r.0,
            name: r.1,
            state: WorkflowState::parse(&r.2).unwrap_or_default(),
            metadata: r.3,
            inserted_at: r.4,
            completed_at: r.5,
        }))
    }

    /// Update the workflow state.
    pub async fn set_state(
        pool: &PgPool,
        id: i64,
        state: WorkflowState,
        schema: Option<&str>,
    ) -> super::error::Result<()> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_workflows", s),
            None => "ishikari_workflows".to_string(),
        };

        let query = if state.is_terminal() {
            format!(
                "UPDATE {} SET state = $1, completed_at = now() WHERE id = $2",
                table
            )
        } else {
            format!("UPDATE {} SET state = $1 WHERE id = $2", table)
        };

        sqlx::query(&query)
            .bind(state.as_str())
            .bind(id)
            .execute(pool)
            .await?;

        Ok(())
    }

    /// Mark the workflow as completed.
    pub async fn complete(
        pool: &PgPool,
        id: i64,
        schema: Option<&str>,
    ) -> super::error::Result<()> {
        Self::set_state(pool, id, WorkflowState::Completed, schema).await
    }

    /// Mark the workflow as failed.
    pub async fn fail(pool: &PgPool, id: i64, schema: Option<&str>) -> super::error::Result<()> {
        Self::set_state(pool, id, WorkflowState::Failed, schema).await
    }

    /// Mark the workflow as cancelled.
    pub async fn cancel(pool: &PgPool, id: i64, schema: Option<&str>) -> super::error::Result<()> {
        Self::set_state(pool, id, WorkflowState::Cancelled, schema).await
    }
}
