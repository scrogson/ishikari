//! Enhanced job route handlers with dependency information.

#![allow(clippy::type_complexity)]

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;
use sqlx::PgPool;

use crate::templates::{EnhancedJobDetailTemplate, EnhancedJobsListTemplate};
use crate::AppState;

/// Type alias for backward compatibility.
pub type JobInfo = EnhancedJobInfo;

/// Type alias for backward compatibility.
pub type JobDetail = EnhancedJobDetail;

/// Query parameters for enhanced jobs listing.
#[derive(Debug, Deserialize, Default)]
pub struct EnhancedJobsQuery {
    /// Filter by job state.
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub state: Option<String>,
    /// Filter by queue.
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub queue: Option<String>,
    /// Filter by worker.
    #[serde(default, deserialize_with = "empty_string_as_none")]
    pub worker: Option<String>,
    /// Show only jobs with dependencies.
    #[serde(default, deserialize_with = "empty_string_as_none_bool")]
    pub has_deps: Option<bool>,
    /// Show only jobs that are blocking other jobs.
    #[serde(default, deserialize_with = "empty_string_as_none_bool")]
    pub is_blocking: Option<bool>,
    /// Show only jobs that are part of a workflow.
    #[serde(default, deserialize_with = "empty_string_as_none_bool")]
    pub in_workflow: Option<bool>,
    /// Filter by workflow ID.
    pub workflow_id: Option<i64>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// Deserialize empty strings as None for Option<String>.
fn empty_string_as_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt.filter(|s| !s.is_empty()))
}

/// Deserialize empty strings as None for Option<bool>.
fn empty_string_as_none_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrBool {
        String(String),
        Bool(bool),
    }

    match Option::<StringOrBool>::deserialize(deserializer)? {
        None => Ok(None),
        Some(StringOrBool::Bool(b)) => Ok(Some(b)),
        Some(StringOrBool::String(s)) => match s.as_str() {
            "" => Ok(None),
            "true" => Ok(Some(true)),
            "false" => Ok(Some(false)),
            _ => Ok(None),
        },
    }
}

/// Enhanced job info with dependency data.
#[derive(Debug)]
pub struct EnhancedJobInfo {
    pub id: i64,
    pub worker: String,
    pub queue: String,
    pub state: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub workflow_id: Option<i64>,
    pub workflow_name: Option<String>,
    pub dependency_count: i64,
    pub blocking_count: i64,
    pub satisfied_deps: i64,
    pub pending_deps: i64,
    pub failed_deps: i64,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
}

impl EnhancedJobInfo {
    /// Get CSS class for state badge.
    pub fn state_class(&self) -> &'static str {
        match self.state.as_str() {
            "available" => "badge-primary",
            "scheduled" => "badge-secondary",
            "executing" => "badge-info",
            "completed" => "badge-success",
            "discarded" | "cancelled" => "badge-error",
            "retryable" => "badge-warning",
            _ => "badge-ghost",
        }
    }

    /// Check if job has dependencies.
    pub fn has_dependencies(&self) -> bool {
        self.dependency_count > 0
    }

    /// Check if job is blocking other jobs.
    pub fn is_blocking(&self) -> bool {
        self.blocking_count > 0
    }

    /// Check if job is part of a workflow.
    pub fn in_workflow(&self) -> bool {
        self.workflow_id.is_some()
    }

    /// Check if job is blocked by dependencies.
    pub fn is_blocked(&self) -> bool {
        self.pending_deps > 0 || self.failed_deps > 0
    }

    /// Get dependency status badge class.
    pub fn dep_status_class(&self) -> &'static str {
        if self.failed_deps > 0 {
            "badge-error"
        } else if self.pending_deps > 0 {
            "badge-warning"
        } else if self.dependency_count > 0 {
            "badge-success"
        } else {
            "badge-ghost"
        }
    }

    /// Get dependency status text.
    pub fn dep_status_text(&self) -> &'static str {
        if self.failed_deps > 0 {
            "Failed"
        } else if self.pending_deps > 0 {
            "Blocked"
        } else if self.dependency_count > 0 {
            "Ready"
        } else {
            "None"
        }
    }
}

/// Detailed job with dependencies.
#[derive(Debug)]
pub struct EnhancedJobDetail {
    pub id: i64,
    pub worker: String,
    pub queue: String,
    pub state: String,
    pub args: serde_json::Value,
    pub attempt: i32,
    pub max_attempts: i32,
    pub priority: i32,
    pub workflow_id: Option<i64>,
    pub workflow_name: Option<String>,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
    pub scheduled_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub errors: Vec<serde_json::Value>,
    pub dependencies: Vec<JobDependency>,
    pub dependents: Vec<JobDependency>,
    /// Node output for workflow jobs (from ishikari_node_executions table)
    pub node_output: Option<serde_json::Value>,
    /// Node inputs for workflow jobs - resolved values
    pub node_inputs: Option<serde_json::Value>,
    /// Raw node inputs (templates with expressions)
    pub raw_node_inputs: Option<serde_json::Value>,
    /// Node type for workflow jobs (from job args.node_type)
    pub node_type: Option<String>,
    /// Node ID for workflow jobs (from job args.node_id)
    pub node_id: Option<String>,
}

impl EnhancedJobDetail {
    pub fn state_class(&self) -> &'static str {
        match self.state.as_str() {
            "available" => "badge-primary",
            "scheduled" => "badge-secondary",
            "executing" => "badge-info",
            "completed" => "badge-success",
            "discarded" | "cancelled" => "badge-error",
            "retryable" => "badge-warning",
            _ => "badge-ghost",
        }
    }

    pub fn args_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.args).unwrap_or_else(|_| self.args.to_string())
    }

    pub fn is_blocked(&self) -> bool {
        self.dependencies.iter().any(|d| d.state != "satisfied")
    }

    pub fn blocking_deps(&self) -> Vec<&JobDependency> {
        self.dependencies
            .iter()
            .filter(|d| d.state != "satisfied")
            .collect()
    }

    /// Check if job has errors.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get error message for a specific attempt (1-indexed).
    pub fn error_for_attempt(&self, attempt: i32) -> Option<String> {
        self.errors.get((attempt - 1) as usize).and_then(|e| {
            if e.is_null() {
                None
            } else if let Some(s) = e.as_str() {
                Some(s.to_string())
            } else if let Some(obj) = e.as_object() {
                // Try common error object formats
                obj.get("message")
                    .or_else(|| obj.get("error"))
                    .or_else(|| obj.get("reason"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| Some(e.to_string()))
            } else {
                Some(e.to_string())
            }
        })
    }

    /// Get all errors with their attempt numbers.
    pub fn error_history(&self) -> Vec<(i32, String)> {
        self.errors
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                if e.is_null() {
                    None
                } else {
                    let msg = if let Some(s) = e.as_str() {
                        s.to_string()
                    } else if let Some(obj) = e.as_object() {
                        obj.get("message")
                            .or_else(|| obj.get("error"))
                            .or_else(|| obj.get("reason"))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| e.to_string())
                    } else {
                        e.to_string()
                    };
                    Some(((i + 1) as i32, msg))
                }
            })
            .collect()
    }

    /// Check if job has output.
    pub fn has_output(&self) -> bool {
        self.node_output.is_some()
    }

    /// Get output as formatted JSON.
    pub fn output_pretty(&self) -> String {
        self.node_output
            .as_ref()
            .map(|o| serde_json::to_string_pretty(o).unwrap_or_else(|_| o.to_string()))
            .unwrap_or_default()
    }

    /// Check if this is a workflow node job.
    pub fn is_node_job(&self) -> bool {
        self.node_id.is_some()
    }

    /// Check if job has node inputs.
    pub fn has_node_inputs(&self) -> bool {
        self.node_inputs.as_ref().map_or(false, |v| !v.is_null() && v.as_object().map_or(false, |o| !o.is_empty()))
    }

    /// Get node inputs as formatted JSON.
    pub fn node_inputs_pretty(&self) -> String {
        self.node_inputs
            .as_ref()
            .map(|o| serde_json::to_string_pretty(o).unwrap_or_else(|_| o.to_string()))
            .unwrap_or_default()
    }

    /// Check if job has raw node inputs (templates).
    pub fn has_raw_node_inputs(&self) -> bool {
        self.raw_node_inputs.as_ref().map_or(false, |v| !v.is_null() && v.as_object().map_or(false, |o| !o.is_empty()))
    }

    /// Get raw node inputs as formatted JSON.
    pub fn raw_node_inputs_pretty(&self) -> String {
        self.raw_node_inputs
            .as_ref()
            .map(|o| serde_json::to_string_pretty(o).unwrap_or_else(|_| o.to_string()))
            .unwrap_or_default()
    }
}

/// Dependency relationship.
#[derive(Debug)]
pub struct JobDependency {
    pub job_id: i64,
    pub worker: String,
    pub job_state: String,
    pub state: String, // pending, satisfied, failed
}

impl JobDependency {
    pub fn job_state_class(&self) -> &'static str {
        match self.job_state.as_str() {
            "available" => "badge-primary",
            "scheduled" => "badge-secondary",
            "executing" => "badge-info",
            "completed" => "badge-success",
            "discarded" | "cancelled" => "badge-error",
            "retryable" => "badge-warning",
            _ => "badge-ghost",
        }
    }

    pub fn dep_state_class(&self) -> &'static str {
        match self.state.as_str() {
            "satisfied" => "badge-success",
            "pending" => "badge-warning",
            "failed" => "badge-error",
            _ => "badge-ghost",
        }
    }
}

/// List enhanced jobs.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<EnhancedJobsQuery>,
) -> EnhancedJobsListTemplate {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(50).min(100);
    let offset = (page - 1) * per_page;

    let (jobs, total) =
        get_enhanced_jobs(&state.pool, state.schema(), &query, per_page, offset).await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    EnhancedJobsListTemplate {
        jobs,
        current_state: query.state,
        current_queue: query.queue,
        current_worker: query.worker,
        has_deps_filter: query.has_deps,
        is_blocking_filter: query.is_blocking,
        in_workflow_filter: query.in_workflow,
        workflow_id_filter: query.workflow_id,
        page,
        total,
        total_pages,
    }
}

/// Show enhanced job detail.
pub async fn show(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<EnhancedJobDetailTemplate, Response> {
    match get_enhanced_job(&state.pool, state.schema(), id).await {
        Ok(Some(job)) => Ok(EnhancedJobDetailTemplate { job }),
        Ok(None) => Err((axum::http::StatusCode::NOT_FOUND, "Job not found").into_response()),
        Err(e) => Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
            .into_response()),
    }
}

/// Get enhanced jobs with filtering.
async fn get_enhanced_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    query: &EnhancedJobsQuery,
    limit: i64,
    offset: i64,
) -> (Vec<EnhancedJobInfo>, i64) {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    let mut conditions = Vec::new();
    let mut params: Vec<String> = Vec::new();

    if let Some(ref s) = query.state {
        params.push(s.to_string());
        conditions.push(format!("j.state::text = ${}", params.len()));
    }
    if let Some(ref q) = query.queue {
        params.push(q.to_string());
        conditions.push(format!("j.queue = ${}", params.len()));
    }
    if let Some(ref w) = query.worker {
        params.push(format!("%{}%", w));
        conditions.push(format!("j.worker ILIKE ${}", params.len()));
    }
    if query.has_deps == Some(true) {
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM {} d WHERE d.job_id = j.id)",
            deps_table
        ));
    }
    if query.is_blocking == Some(true) {
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM {} d WHERE d.depends_on_job_id = j.id)",
            deps_table
        ));
    }
    if query.in_workflow == Some(true) {
        conditions.push("j.workflow_id IS NOT NULL".to_string());
    }
    if let Some(wf_id) = query.workflow_id {
        params.push(wf_id.to_string());
        conditions.push(format!("j.workflow_id = ${}", params.len()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let count_query = format!("SELECT COUNT(*) FROM {} j {}", jobs_table, where_clause);

    let list_query = format!(
        r#"
        SELECT
            j.id, j.worker, j.queue, j.state::text, j.attempt, j.max_attempts,
            j.workflow_id, w.name as workflow_name, j.inserted_at,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.job_id = j.id), 0) as dependency_count,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.depends_on_job_id = j.id), 0) as blocking_count,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.job_id = j.id AND d.state = 'satisfied'), 0) as satisfied_deps,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.job_id = j.id AND d.state = 'pending'), 0) as pending_deps,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.job_id = j.id AND d.state = 'failed'), 0) as failed_deps
        FROM {jobs} j
        LEFT JOIN {workflows} w ON w.id = j.workflow_id
        {where_clause}
        ORDER BY j.id DESC
        LIMIT {limit} OFFSET {offset}
        "#,
        deps = deps_table,
        jobs = jobs_table,
        workflows = workflows_table,
        where_clause = where_clause,
        limit = limit,
        offset = offset
    );

    // Execute count query
    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    for p in &params {
        count_q = count_q.bind(p);
    }
    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);

    // Execute list query
    let mut list_q = sqlx::query_as::<
        _,
        (
            i64,
            String,
            String,
            String,
            i32,
            i32,
            Option<i64>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
            i64,
            i64,
            i64,
            i64,
            i64,
        ),
    >(&list_query);
    for p in &params {
        list_q = list_q.bind(p);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let jobs = rows
        .into_iter()
        .map(|row| EnhancedJobInfo {
            id: row.0,
            worker: row.1,
            queue: row.2,
            state: row.3,
            attempt: row.4,
            max_attempts: row.5,
            workflow_id: row.6,
            workflow_name: row.7,
            inserted_at: row.8,
            dependency_count: row.9,
            blocking_count: row.10,
            satisfied_deps: row.11,
            pending_deps: row.12,
            failed_deps: row.13,
        })
        .collect();

    (jobs, total)
}

/// Get enhanced job detail.
async fn get_enhanced_job(
    pool: &PgPool,
    schema: Option<&str>,
    id: i64,
) -> Result<Option<EnhancedJobDetail>, String> {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    // Check if workflows table exists (it might not if ishikari-pro migrations haven't run)
    let workflows_exist = check_table_exists(pool, schema, "ishikari_workflows").await;

    // Get job info - use LEFT JOIN only if workflows table exists
    let job_query = if workflows_exist {
        format!(
            r#"
            SELECT
                j.id, j.worker, j.queue, j.state::text, j.args, j.attempt, j.max_attempts,
                j.priority, j.workflow_id, w.name as workflow_name,
                j.inserted_at, j.scheduled_at, j.completed_at, j.errors
            FROM {} j
            LEFT JOIN {} w ON w.id = j.workflow_id
            WHERE j.id = $1
            "#,
            jobs_table, workflows_table
        )
    } else {
        format!(
            r#"
            SELECT
                j.id, j.worker, j.queue, j.state::text, j.args, j.attempt, j.max_attempts,
                j.priority, j.workflow_id, NULL::text as workflow_name,
                j.inserted_at, j.scheduled_at, j.completed_at, j.errors
            FROM {} j
            WHERE j.id = $1
            "#,
            jobs_table
        )
    };

    let job_row: Option<(
        i64,
        String,
        String,
        String,
        serde_json::Value,
        i32,
        i32,
        i32,
        Option<i64>,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        Vec<serde_json::Value>,
    )> = sqlx::query_as(&job_query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| format!("Failed to fetch job: {}", e))?;

    let job_row = match job_row {
        Some(row) => row,
        None => return Ok(None),
    };

    // Check if dependencies table exists
    let deps_exist = check_table_exists(pool, schema, "ishikari_job_dependencies").await;

    let (deps, dependents) = if deps_exist {
        // Get dependencies
        let deps_query = format!(
            r#"
            SELECT dep_job.id, dep_job.worker, dep_job.state::text, d.state
            FROM {} d
            JOIN {} dep_job ON dep_job.id = d.depends_on_job_id
            WHERE d.job_id = $1
            ORDER BY dep_job.id
            "#,
            deps_table, jobs_table
        );

        let deps: Vec<(i64, String, String, String)> = sqlx::query_as(&deps_query)
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();

        // Get dependents
        let dependents_query = format!(
            r#"
            SELECT j.id, j.worker, j.state::text, d.state
            FROM {} d
            JOIN {} j ON j.id = d.job_id
            WHERE d.depends_on_job_id = $1
            ORDER BY j.id
            "#,
            deps_table, jobs_table
        );

        let dependents: Vec<(i64, String, String, String)> = sqlx::query_as(&dependents_query)
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();

        (deps, dependents)
    } else {
        (vec![], vec![])
    };

    // errors is already a Vec<serde_json::Value> from the JSONB[] column
    let errors = job_row.13;
    let args = &job_row.4;

    // Try to fetch node output if this is a workflow node job
    let node_output = fetch_node_output(pool, schema, args).await;

    // Extract node job metadata from args
    let node_id = args.get("node_id").and_then(|v| v.as_str()).map(String::from);
    let node_type = args.get("node_type").and_then(|v| v.as_str()).map(String::from);
    let raw_node_inputs = args.get("raw_inputs").cloned();

    // Fetch resolved inputs from node_executions table
    let node_inputs = fetch_node_inputs(pool, schema, args).await;

    Ok(Some(EnhancedJobDetail {
        id: job_row.0,
        worker: job_row.1,
        queue: job_row.2,
        state: job_row.3,
        args: job_row.4,
        attempt: job_row.5,
        max_attempts: job_row.6,
        priority: job_row.7,
        workflow_id: job_row.8,
        workflow_name: job_row.9,
        inserted_at: job_row.10,
        scheduled_at: job_row.11,
        completed_at: job_row.12,
        errors,
        dependencies: deps
            .into_iter()
            .map(|(id, worker, job_state, state)| JobDependency {
                job_id: id,
                worker,
                job_state,
                state,
            })
            .collect(),
        dependents: dependents
            .into_iter()
            .map(|(id, worker, job_state, state)| JobDependency {
                job_id: id,
                worker,
                job_state,
                state,
            })
            .collect(),
        node_output,
        node_inputs,
        raw_node_inputs,
        node_type,
        node_id,
    }))
}

/// Fetch node output from ishikari_node_executions table.
/// Returns None if the job is not a workflow node job or if no output exists.
async fn fetch_node_output(
    pool: &PgPool,
    schema: Option<&str>,
    args: &serde_json::Value,
) -> Option<serde_json::Value> {
    // Extract workflow_run_id and node_id from args
    let workflow_run_id = args.get("workflow_run_id")?.as_i64()?;
    let node_id = args.get("node_id")?.as_str()?;

    let table = match schema {
        Some(s) => format!("{}.ishikari_node_executions", s),
        None => "ishikari_node_executions".to_string(),
    };

    // Check if table exists
    if !check_table_exists(pool, schema, "ishikari_node_executions").await {
        return None;
    }

    let query = format!(
        "SELECT output FROM {} WHERE workflow_run_id = $1 AND node_id = $2 AND output IS NOT NULL",
        table
    );

    sqlx::query_scalar::<_, serde_json::Value>(&query)
        .bind(workflow_run_id)
        .bind(node_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}

/// Fetch resolved node inputs from ishikari_node_executions table.
/// Returns None if the job is not a workflow node job or if no inputs exist.
async fn fetch_node_inputs(
    pool: &PgPool,
    schema: Option<&str>,
    args: &serde_json::Value,
) -> Option<serde_json::Value> {
    // Extract workflow_run_id and node_id from args
    let workflow_run_id = args.get("workflow_run_id")?.as_i64()?;
    let node_id = args.get("node_id")?.as_str()?;

    let table = match schema {
        Some(s) => format!("{}.ishikari_node_executions", s),
        None => "ishikari_node_executions".to_string(),
    };

    // Check if table exists
    if !check_table_exists(pool, schema, "ishikari_node_executions").await {
        return None;
    }

    let query = format!(
        "SELECT inputs FROM {} WHERE workflow_run_id = $1 AND node_id = $2 AND inputs IS NOT NULL",
        table
    );

    sqlx::query_scalar::<_, serde_json::Value>(&query)
        .bind(workflow_run_id)
        .bind(node_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}

/// Check if a table exists in the database.
async fn check_table_exists(pool: &PgPool, schema: Option<&str>, table_name: &str) -> bool {
    let schema_name = schema.unwrap_or("public");

    let result: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        "#,
    )
    .bind(schema_name)
    .bind(table_name)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    result.unwrap_or(false)
}

/// Discard a job (mark it as discarded).
pub async fn discard(State(state): State<AppState>, Path(id): Path<i64>) -> Redirect {
    let table = match state.schema() {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        "UPDATE {} SET state = 'discarded', completed_at = NOW() WHERE id = $1 AND state NOT IN ('completed', 'discarded')",
        table
    );

    let _ = sqlx::query(&query)
        .bind(id)
        .execute(&state.pool)
        .await;

    Redirect::to(&format!("/jobs/{}", id))
}
