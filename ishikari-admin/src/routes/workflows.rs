//! Workflow route handlers.

#![allow(clippy::type_complexity)]

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use sqlx::PgPool;

use crate::templates::{WorkflowDetailTemplate, WorkflowsListTemplate};
use crate::AppState;

/// Query parameters for workflow listing.
#[derive(Debug, Deserialize, Default)]
pub struct WorkflowsQuery {
    pub state: Option<String>,
    pub name: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// Workflow information for display.
#[derive(Debug)]
pub struct WorkflowInfo {
    pub id: i64,
    pub name: String,
    pub state: String,
    pub job_count: i64,
    pub completed_jobs: i64,
    pub failed_jobs: i64,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl WorkflowInfo {
    /// Calculate progress percentage.
    pub fn progress(&self) -> i64 {
        if self.job_count == 0 {
            0
        } else {
            ((self.completed_jobs + self.failed_jobs) * 100) / self.job_count
        }
    }

    /// Get a CSS class for the state badge.
    pub fn state_class(&self) -> &'static str {
        match self.state.as_str() {
            "running" => "badge-info",
            "completed" => "badge-success",
            "failed" => "badge-error",
            "cancelled" => "badge-warning",
            "compensating" => "badge-warning",
            "compensated" => "badge-neutral",
            _ => "badge-ghost",
        }
    }
}

/// Detailed workflow information.
#[derive(Debug)]
pub struct WorkflowDetail {
    pub id: i64,
    pub name: String,
    pub state: String,
    pub metadata: serde_json::Value,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Workflow run inputs (if this workflow has a run record)
    pub run_inputs: Option<serde_json::Value>,
    /// Workflow run outputs (final result)
    pub run_outputs: Option<serde_json::Value>,
    /// Node executions for this workflow run
    pub node_executions: Vec<NodeExecution>,
}

/// Execution data from a node in a workflow.
#[derive(Debug)]
pub struct NodeExecution {
    pub node_id: String,
    pub node_type: String,
    pub status: String,
    pub inputs: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub duration_ms: Option<i64>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl WorkflowDetail {
    /// Format metadata as pretty JSON.
    pub fn metadata_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.metadata).unwrap_or_else(|_| self.metadata.to_string())
    }

    /// Get a CSS class for the state badge.
    pub fn state_class(&self) -> &'static str {
        match self.state.as_str() {
            "running" => "badge-info",
            "completed" => "badge-success",
            "failed" => "badge-error",
            "cancelled" => "badge-warning",
            "compensating" => "badge-warning",
            "compensated" => "badge-neutral",
            _ => "badge-ghost",
        }
    }

    /// Check if workflow has run inputs.
    pub fn has_run_inputs(&self) -> bool {
        self.run_inputs.as_ref().map_or(false, |v| {
            !v.is_null() && v.as_object().map_or(false, |o| !o.is_empty())
        })
    }

    /// Format run inputs as pretty JSON.
    pub fn run_inputs_pretty(&self) -> String {
        self.run_inputs
            .as_ref()
            .map(|v| serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string()))
            .unwrap_or_default()
    }

    /// Check if workflow has node executions.
    pub fn has_node_executions(&self) -> bool {
        !self.node_executions.is_empty()
    }

    /// Check if workflow has run outputs.
    pub fn has_run_outputs(&self) -> bool {
        self.run_outputs.as_ref().map_or(false, |v| {
            !v.is_null() && v.as_object().map_or(false, |o| !o.is_empty())
        })
    }

    /// Format run outputs as pretty JSON.
    pub fn run_outputs_pretty(&self) -> String {
        self.run_outputs
            .as_ref()
            .map(|v| serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string()))
            .unwrap_or_default()
    }
}

impl NodeExecution {
    /// Format output as pretty JSON.
    pub fn output_pretty(&self) -> String {
        self.output
            .as_ref()
            .map(|o| serde_json::to_string_pretty(o).unwrap_or_else(|_| o.to_string()))
            .unwrap_or_default()
    }

    /// Format inputs as pretty JSON.
    pub fn inputs_pretty(&self) -> String {
        self.inputs
            .as_ref()
            .map(|i| serde_json::to_string_pretty(i).unwrap_or_else(|_| i.to_string()))
            .unwrap_or_default()
    }

    /// Check if has output.
    pub fn has_output(&self) -> bool {
        self.output.is_some()
    }

    /// Check if has inputs.
    pub fn has_inputs(&self) -> bool {
        self.inputs.as_ref().map_or(false, |v| !v.is_null())
    }

    /// Get status badge class.
    pub fn status_class(&self) -> &'static str {
        match self.status.as_str() {
            "completed" => "badge-success",
            "failed" => "badge-error",
            "running" => "badge-info",
            "skipped" => "badge-warning",
            _ => "badge-ghost",
        }
    }
}

/// Job within a workflow.
#[derive(Debug)]
pub struct WorkflowJobInfo {
    pub id: i64,
    pub worker: String,
    pub state: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub dependencies: Vec<i64>,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Whether this is a compensation job (from a saga).
    pub is_compensation: bool,
}

impl WorkflowJobInfo {
    /// Get a CSS class for the state badge.
    pub fn state_class(&self) -> &'static str {
        match self.state.as_str() {
            "available" => "badge-primary",
            "scheduled" => "badge-secondary",
            "executing" => "badge-info",
            "completed" => "badge-success",
            // Cancelled compensation jobs = not needed (saga succeeded) - neutral
            "cancelled" if self.is_compensation => "badge-neutral",
            "discarded" | "cancelled" => "badge-error",
            "retryable" => "badge-warning",
            _ => "badge-ghost",
        }
    }
}

/// List workflows page.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<WorkflowsQuery>,
) -> WorkflowsListTemplate {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(25).min(100);
    let offset = (page - 1) * per_page;

    let (workflows, total) = get_workflows(
        &state.pool,
        state.schema(),
        query.state.as_deref(),
        query.name.as_deref(),
        per_page,
        offset,
    )
    .await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    WorkflowsListTemplate {
        workflows,
        current_state: query.state,
        current_name: query.name,
        page,
        total,
        total_pages,
    }
}

/// Show single workflow.
pub async fn show(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<WorkflowDetailTemplate, Response> {
    let workflow = get_workflow(&state.pool, state.schema(), id)
        .await
        .ok_or_else(|| (axum::http::StatusCode::NOT_FOUND, "Workflow not found").into_response())?;

    let jobs = get_workflow_jobs(&state.pool, state.schema(), id).await;

    Ok(WorkflowDetailTemplate { workflow, jobs })
}

/// Get workflows with filtering and pagination.
async fn get_workflows(
    pool: &PgPool,
    schema: Option<&str>,
    state_filter: Option<&str>,
    name_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> (Vec<WorkflowInfo>, i64) {
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let mut conditions = Vec::new();
    let mut params: Vec<String> = Vec::new();

    if let Some(s) = state_filter {
        params.push(s.to_string());
        conditions.push(format!("w.state = ${}", params.len()));
    }
    if let Some(n) = name_filter {
        params.push(format!("%{}%", n));
        conditions.push(format!("w.name ILIKE ${}", params.len()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let count_query = format!(
        "SELECT COUNT(*) FROM {} w {}",
        workflows_table, where_clause
    );
    let list_query = format!(
        r#"
        SELECT
            w.id, w.name, w.state, w.inserted_at, w.completed_at,
            COUNT(j.id) as job_count,
            COUNT(j.id) FILTER (WHERE j.state = 'completed') as completed_jobs,
            COUNT(j.id) FILTER (WHERE j.state IN ('discarded', 'cancelled')) as failed_jobs
        FROM {} w
        LEFT JOIN {} j ON j.workflow_id = w.id
        {}
        GROUP BY w.id
        ORDER BY w.id DESC
        LIMIT {} OFFSET {}
        "#,
        workflows_table, jobs_table, where_clause, limit, offset
    );

    // Build count query
    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    for p in &params {
        count_q = count_q.bind(p);
    }
    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);

    // Build list query
    let mut list_q = sqlx::query_as::<
        _,
        (
            i64,
            String,
            String,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            i64,
            i64,
            i64,
        ),
    >(&list_query);
    for p in &params {
        list_q = list_q.bind(p);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let workflows = rows
        .into_iter()
        .map(|row| WorkflowInfo {
            id: row.0,
            name: row.1,
            state: row.2,
            inserted_at: row.3,
            completed_at: row.4,
            job_count: row.5,
            completed_jobs: row.6,
            failed_jobs: row.7,
        })
        .collect();

    (workflows, total)
}

/// Get a single workflow by ID.
async fn get_workflow(pool: &PgPool, schema: Option<&str>, id: i64) -> Option<WorkflowDetail> {
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };
    let runs_table = match schema {
        Some(s) => format!("{}.ishikari_workflow_runs", s),
        None => "ishikari_workflow_runs".to_string(),
    };
    let executions_table = match schema {
        Some(s) => format!("{}.ishikari_node_executions", s),
        None => "ishikari_node_executions".to_string(),
    };

    // Get workflow with run inputs and outputs
    let query = format!(
        r#"
        SELECT w.id, w.name, w.state, w.metadata, w.inserted_at, w.completed_at, r.inputs, r.outputs, r.id as run_id
        FROM {} w
        LEFT JOIN {} r ON r.workflow_id = w.id
        WHERE w.id = $1
        "#,
        workflows_table, runs_table
    );

    let row: Option<(
        i64,
        String,
        String,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
        Option<serde_json::Value>,
        Option<i64>,
    )> = sqlx::query_as(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()?;

    let row = row?;
    let run_id = row.8;

    // Get node executions if there's a run
    let node_executions = if let Some(rid) = run_id {
        let executions_query = format!(
            r#"
            SELECT node_id, node_type, status, inputs, output, error, duration_ms, completed_at
            FROM {}
            WHERE workflow_run_id = $1
            ORDER BY completed_at NULLS LAST
            "#,
            executions_table
        );

        let rows: Vec<(
            String,
            String,
            String,
            Option<serde_json::Value>,
            Option<serde_json::Value>,
            Option<String>,
            Option<i64>,
            Option<chrono::DateTime<chrono::Utc>>,
        )> = sqlx::query_as(&executions_query)
            .bind(rid)
            .fetch_all(pool)
            .await
            .unwrap_or_default();

        rows.into_iter()
            .map(
                |(node_id, node_type, status, inputs, output, error, duration_ms, completed_at)| {
                    NodeExecution {
                        node_id,
                        node_type,
                        status,
                        inputs,
                        output,
                        error,
                        duration_ms,
                        completed_at,
                    }
                },
            )
            .collect()
    } else {
        vec![]
    };

    Some(WorkflowDetail {
        id: row.0,
        name: row.1,
        state: row.2,
        metadata: row.3,
        inserted_at: row.4,
        completed_at: row.5,
        run_inputs: row.6,
        run_outputs: row.7,
        node_executions,
    })
}

/// Get jobs for a workflow with their dependencies.
async fn get_workflow_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    workflow_id: i64,
) -> Vec<WorkflowJobInfo> {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            j.id, j.worker, j.state::text, j.attempt, j.max_attempts,
            j.inserted_at, j.completed_at,
            COALESCE(array_agg(d.depends_on_job_id) FILTER (WHERE d.depends_on_job_id IS NOT NULL), '{{}}') as dependencies,
            EXISTS(SELECT 1 FROM {} s WHERE s.compensation_job_id = j.id) as is_compensation
        FROM {} j
        LEFT JOIN {} d ON d.job_id = j.id
        WHERE j.workflow_id = $1
        GROUP BY j.id
        ORDER BY j.id
        "#,
        saga_steps_table, jobs_table, deps_table
    );

    let rows: Vec<(
        i64,
        String,
        String,
        i32,
        i32,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        Vec<i64>,
        bool,
    )> = sqlx::query_as(&query)
        .bind(workflow_id)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|row| WorkflowJobInfo {
            id: row.0,
            worker: row.1,
            state: row.2,
            attempt: row.3,
            max_attempts: row.4,
            inserted_at: row.5,
            completed_at: row.6,
            dependencies: row.7,
            is_compensation: row.8,
        })
        .collect()
}
