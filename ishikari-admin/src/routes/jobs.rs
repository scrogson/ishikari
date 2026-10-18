//! Enhanced job route handlers with dependency information.

#![allow(clippy::type_complexity)]

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use sqlx::PgPool;

use crate::routes::dashboard::{count_rows, jobs_table, state_type};
use crate::routes::sagas::SagaStepInfo;
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
    /// The page of jobs older than this id.
    pub before: Option<i64>,
    /// The page of jobs newer than this id.
    pub after: Option<i64>,
    pub per_page: Option<i64>,
}

impl EnhancedJobsQuery {
    /// The filters as a URL query, for links to the list's other pages.
    fn filters(&self) -> String {
        query_string(&[
            ("state", self.state.clone()),
            ("queue", self.queue.clone()),
            ("worker", self.worker.clone()),
            ("has_deps", self.has_deps.map(|b| b.to_string())),
            ("is_blocking", self.is_blocking.map(|b| b.to_string())),
            ("in_workflow", self.in_workflow.map(|b| b.to_string())),
            ("workflow_id", self.workflow_id.map(|id| id.to_string())),
            ("per_page", self.per_page.map(|n| n.to_string())),
        ])
    }

    /// Jobs per page: 50 unless asked, at most 100.
    pub(crate) fn limit(&self) -> i64 {
        self.per_page.unwrap_or(50).clamp(1, 100)
    }
}

/// `pairs` with a value as a URL query (without the `?`), the values percent-encoded.
pub(crate) fn query_string(pairs: &[(&str, Option<String>)]) -> String {
    let encode = |s: &str| -> String {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (b as char).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect()
    };
    pairs
        .iter()
        .filter_map(|(k, v)| v.as_deref().map(|v| format!("{k}={}", encode(v))))
        .collect::<Vec<_>>()
        .join("&")
}

/// Which page of a list of jobs, newest first: the jobs older (`before`) or newer (`after`)
/// than an id, or the newest.
///
/// Keyset pages: a page reads from an id down the primary key, so one deep in a list of
/// millions costs what the first does (an offset reads and drops every job before it).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Keyset {
    before: Option<i64>,
    after: Option<i64>,
}

impl Keyset {
    /// The page `before` or `after` an id (`before` when both), or the newest.
    pub(crate) fn new(before: Option<i64>, after: Option<i64>) -> Self {
        Keyset {
            before,
            after: after.filter(|_| before.is_none()),
        }
    }

    /// The condition on `{job}.id` for this page, if any, and the order to read it in.
    pub(crate) fn sql(&self, job: &str) -> (Option<String>, &'static str) {
        match (self.before, self.after) {
            (Some(id), _) => (Some(format!("{job}.id < {id}")), "DESC"),
            (None, Some(id)) => (Some(format!("{job}.id > {id}")), "ASC"),
            (None, None) => (None, "DESC"),
        }
    }

    /// True when the `rows` read after an id (of `limit + 1` asked) are all the newer jobs
    /// there are: then the newest page, a full one, is the one to show.
    pub(crate) fn reached_newest(&self, rows: usize, limit: i64) -> bool {
        self.after.is_some() && rows as i64 <= limit
    }

    /// This page of `rows` (`limit + 1` asked for, read in [`sql`](Self::sql)'s order) newest
    /// first, and the links to the pages around it, keeping `filters`.
    pub(crate) fn page<T>(
        &self,
        mut rows: Vec<T>,
        limit: i64,
        id: impl Fn(&T) -> i64,
        filters: String,
    ) -> (Vec<T>, Pager) {
        let more = rows.len() as i64 > limit;
        rows.truncate(limit.max(0) as usize);
        if self.after.is_some() {
            rows.reverse();
        }
        let newest = rows.first().map(&id);
        let oldest = rows.last().map(&id);
        // Paging away from a page, there's that page to come back to.
        let (newer, older) = match (self.before, self.after) {
            (Some(_), _) => (newest, oldest.filter(|_| more)),
            (None, Some(_)) => (newest.filter(|_| more), oldest),
            (None, None) => (None, oldest.filter(|_| more)),
        };
        let pager = Pager {
            newer,
            older,
            paged: self.before.is_some() || self.after.is_some(),
            filters,
        };
        (rows, pager)
    }
}

/// The links between the pages of a list of jobs, newest first (see [`Keyset`]).
#[derive(Debug, Default)]
pub struct Pager {
    /// The newest job's id on this page, when there are newer jobs.
    pub newer: Option<i64>,
    /// The oldest job's id on this page, when there are older jobs.
    pub older: Option<i64>,
    /// This page isn't the newest one.
    pub paged: bool,
    /// The list's filters as a URL query, kept by the links.
    pub filters: String,
}

impl Pager {
    /// There's another page to link to.
    pub fn is_shown(&self) -> bool {
        self.paged || self.newer.is_some() || self.older.is_some()
    }

    /// The URL query of the newest page.
    pub fn newest_query(&self) -> String {
        self.filters.clone()
    }

    /// The URL query of the page of newer jobs.
    pub fn newer_query(&self) -> Option<String> {
        self.newer.map(|id| self.query(format!("after={id}")))
    }

    /// The URL query of the page of older jobs.
    pub fn older_query(&self) -> Option<String> {
        self.older.map(|id| self.query(format!("before={id}")))
    }

    fn query(&self, cursor: String) -> String {
        if self.filters.is_empty() {
            cursor
        } else {
            format!("{cursor}&{}", self.filters)
        }
    }
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
    /// Saga step state, if this is a saga forward job.
    pub saga_step_state: Option<String>,
}

impl EnhancedJobInfo {
    pub fn saga_rollback_state(&self) -> Option<&str> {
        super::sagas::rollback_state(self.saga_step_state.as_deref())
    }

    pub fn saga_rollback_class(&self) -> &'static str {
        super::sagas::rollback_class(self.saga_step_state.as_deref())
    }

    /// Get CSS class for state badge.
    pub fn state_class(&self) -> &'static str {
        match self.state.as_str() {
            "available" => "ik-tone-primary",
            "pending" => "ik-tone-neutral",
            "scheduled" => "ik-tone-violet",
            "executing" => "ik-tone-info ik-live",
            "completed" => "ik-tone-success",
            "discarded" => "ik-tone-danger",
            "cancelled" => "ik-tone-neutral",
            "retryable" => "ik-tone-warning",
            _ => "ik-tone-neutral",
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
            "ik-tone-danger"
        } else if self.pending_deps > 0 {
            "ik-tone-warning"
        } else if self.dependency_count > 0 {
            "ik-tone-success"
        } else {
            "ik-tone-neutral"
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
    /// Saga step this job belongs to (as forward or compensation job)
    pub saga_step: Option<SagaStepInfo>,
    /// Total number of steps in the saga
    pub saga_step_count: i64,
}

impl EnhancedJobDetail {
    /// True if this job is the compensation job of its saga step.
    pub fn is_compensation_job(&self) -> bool {
        self.saga_step
            .as_ref()
            .is_some_and(|s| s.compensation_job_id == Some(self.id))
    }

    pub fn state_class(&self) -> &'static str {
        match self.state.as_str() {
            "available" => "ik-tone-primary",
            "pending" => "ik-tone-neutral",
            "scheduled" => "ik-tone-violet",
            "executing" => "ik-tone-info ik-live",
            "completed" => "ik-tone-success",
            "discarded" => "ik-tone-danger",
            "cancelled" => "ik-tone-neutral",
            "retryable" => "ik-tone-warning",
            _ => "ik-tone-neutral",
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
                } else if let Some(obj) = e.as_object() {
                    // An error record: {attempt, at, kind, error}.
                    let attempt = obj
                        .get("attempt")
                        .and_then(|a| a.as_i64())
                        .map(|a| a as i32)
                        .unwrap_or((i + 1) as i32);
                    let text = obj
                        .get("message")
                        .or_else(|| obj.get("error"))
                        .or_else(|| obj.get("reason"))
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| e.to_string());
                    let msg = match obj.get("kind").and_then(|k| k.as_str()) {
                        Some(kind) if kind != "error" => format!("{kind}: {text}"),
                        _ => text,
                    };
                    Some((attempt, msg))
                } else {
                    let msg = e
                        .as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| e.to_string());
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
        self.node_inputs
            .as_ref()
            .is_some_and(|v| !v.is_null() && v.as_object().is_some_and(|o| !o.is_empty()))
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
        self.raw_node_inputs
            .as_ref()
            .is_some_and(|v| !v.is_null() && v.as_object().is_some_and(|o| !o.is_empty()))
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
            "available" => "ik-tone-primary",
            "pending" => "ik-tone-neutral",
            "scheduled" => "ik-tone-violet",
            "executing" => "ik-tone-info ik-live",
            "completed" => "ik-tone-success",
            "discarded" => "ik-tone-danger",
            "cancelled" => "ik-tone-neutral",
            "retryable" => "ik-tone-warning",
            _ => "ik-tone-neutral",
        }
    }

    pub fn dep_state_class(&self) -> &'static str {
        match self.state.as_str() {
            "satisfied" => "ik-tone-success",
            "pending" => "ik-tone-warning",
            "failed" => "ik-tone-danger",
            _ => "ik-tone-neutral",
        }
    }
}

/// List enhanced jobs.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<EnhancedJobsQuery>,
) -> EnhancedJobsListTemplate {
    let (jobs, pager) = get_enhanced_jobs(&state.pool, state.schema(), &query).await;
    let total = if pager.is_shown() {
        count_enhanced_jobs(&state.pool, state.schema(), &query).await
    } else {
        jobs.len() as i64
    };

    let mut queues = super::queues::queue_names(&state.pool, state.schema()).await;
    let mut workers = worker_names(&state.pool, state.schema()).await;
    // The filter's options always have the one it's set to.
    for (names, current) in [(&mut queues, &query.queue), (&mut workers, &query.worker)] {
        if let Some(current) = current {
            if !names.contains(current) {
                names.push(current.clone());
                names.sort();
            }
        }
    }

    EnhancedJobsListTemplate {
        base_path: state.base_path.to_string(),
        nav_items: state.nav_items.to_vec(),
        jobs,
        queues,
        workers,
        current_state: query.state,
        current_queue: query.queue,
        current_worker: query.worker,
        has_deps_filter: query.has_deps,
        is_blocking_filter: query.is_blocking,
        in_workflow_filter: query.in_workflow,
        workflow_id_filter: query.workflow_id,
        pager,
        total,
    }
}

/// Worker names for the worker filter, without reading the whole table: the workers the
/// planner's statistics know of (the common ones, from the last `ANALYZE`) and those of the
/// latest jobs.
async fn worker_names(pool: &PgPool, schema: Option<&str>) -> Vec<String> {
    let query = format!(
        r#"
        WITH stats AS (
            SELECT most_common_vals::text::text[] AS common, histogram_bounds::text::text[] AS bounds
            FROM pg_stats
            WHERE schemaname = COALESCE($1, current_schema())
              AND tablename = 'ishikari_jobs' AND attname = 'worker'
        )
        SELECT unnest(common) FROM stats
        UNION SELECT unnest(bounds) FROM stats
        UNION SELECT worker FROM (SELECT worker FROM {jobs} ORDER BY id DESC LIMIT 1000) latest
        ORDER BY 1
        "#,
        jobs = jobs_table(schema)
    );
    sqlx::query_scalar(&query)
        .bind(schema)
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "listing workers");
            Vec::new()
        })
}

/// Query parameters for a job's page.
#[derive(Debug, Deserialize, Default)]
pub struct JobQuery {
    /// The action (`retry` or `cancel`) that just changed nothing.
    pub unchanged: Option<String>,
}

/// Show enhanced job detail.
pub async fn show(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Query(query): Query<JobQuery>,
) -> Result<EnhancedJobDetailTemplate, Response> {
    match get_enhanced_job(&state.pool, state.schema(), id).await {
        Ok(Some(job)) => Ok(EnhancedJobDetailTemplate {
            base_path: state.base_path.to_string(),
            nav_items: state.nav_items.to_vec(),
            job,
            unchanged: query.unchanged.filter(|a| a == "retry" || a == "cancel"),
        }),
        Ok(None) => Err((axum::http::StatusCode::NOT_FOUND, "Job not found").into_response()),
        Err(e) => Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Database error: {}", e),
        )
            .into_response()),
    }
}

/// The conditions on jobs `j` for `query`'s filters, and the values for their `$n`.
fn job_filters(schema: Option<&str>, query: &EnhancedJobsQuery) -> (Vec<String>, Vec<String>) {
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let mut conditions = Vec::new();
    let mut params: Vec<String> = Vec::new();

    match query.state.as_deref() {
        // Not a job state: completed jobs whose saga step was rolled back
        Some("compensated") => {
            conditions.push(format!(
                "j.state = 'completed' AND j.id IN (SELECT s.job_id FROM {} s WHERE s.state IN {})",
                saga_steps_table,
                super::sagas::ROLLBACK_STATES_SQL
            ));
        }
        Some(s) => {
            params.push(s.to_string());
            conditions.push(format!(
                "j.state = ${}::{}",
                params.len(),
                state_type(schema)
            ));
        }
        None => {}
    }
    if let Some(ref q) = query.queue {
        params.push(q.to_string());
        conditions.push(format!("j.queue = ${}", params.len()));
    }
    if let Some(ref w) = query.worker {
        params.push(w.to_string());
        conditions.push(format!("j.worker = ${}", params.len()));
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
        conditions.push(format!("j.workflow_id = ${}::bigint", params.len()));
    }

    (conditions, params)
}

/// How many jobs match `query`'s filters: exact up to
/// [`EXACT_COUNT_LIMIT`](super::dashboard::EXACT_COUNT_LIMIT), estimated past it.
pub(crate) async fn count_enhanced_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    query: &EnhancedJobsQuery,
) -> i64 {
    let (conditions, params) = job_filters(schema, query);
    let mut from = format!("FROM {} j", jobs_table(schema));
    if !conditions.is_empty() {
        from = format!("{from} WHERE {}", conditions.join(" AND "));
    }
    let binds: Vec<&str> = params.iter().map(String::as_str).collect();
    count_rows(pool, &from, &binds).await
}

/// A page of the jobs matching `query`'s filters, newest first, and the links to the others.
pub(crate) async fn get_enhanced_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    query: &EnhancedJobsQuery,
) -> (Vec<EnhancedJobInfo>, Pager) {
    let limit = query.limit();
    let mut keyset = Keyset::new(query.before, query.after);
    loop {
        let rows = read_enhanced_jobs(pool, schema, query, keyset, limit).await;
        if keyset.reached_newest(rows.len(), limit) {
            keyset = Keyset::default();
            continue;
        }
        return keyset.page(rows, limit, |j| j.id, query.filters());
    }
}

/// `limit + 1` jobs of `keyset`'s page, in its order.
async fn read_enhanced_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    query: &EnhancedJobsQuery,
    keyset: Keyset,
    limit: i64,
) -> Vec<EnhancedJobInfo> {
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let (mut conditions, params) = job_filters(schema, query);
    let (cursor, order) = keyset.sql("j");
    conditions.extend(cursor);
    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    // The page's jobs first, then what the list shows of each: dependency counts and saga
    // step state for those jobs only.
    let list_query = format!(
        r#"
        SELECT
            j.id, j.worker, j.queue, j.state::text, j.attempt, j.max_attempts,
            j.workflow_id, w.name as workflow_name, j.inserted_at,
            deps.total, blocking.total, deps.satisfied, deps.pending, deps.failed,
            saga.state as saga_step_state
        FROM (
            SELECT j.id, j.worker, j.queue, j.state, j.attempt, j.max_attempts,
                j.workflow_id, j.inserted_at
            FROM {jobs} j
            {where_clause}
            ORDER BY j.id {order}
            LIMIT {limit}
        ) j
        LEFT JOIN {workflows} w ON w.id = j.workflow_id
        CROSS JOIN LATERAL (
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE d.state = 'satisfied') AS satisfied,
                COUNT(*) FILTER (WHERE d.state = 'pending') AS pending,
                COUNT(*) FILTER (WHERE d.state = 'failed') AS failed
            FROM {deps} d WHERE d.job_id = j.id
        ) deps
        CROSS JOIN LATERAL (
            SELECT COUNT(*) AS total FROM {deps} d WHERE d.depends_on_job_id = j.id
        ) blocking
        LEFT JOIN LATERAL (
            SELECT s.state FROM {steps} s
            WHERE s.workflow_id = j.workflow_id AND s.job_id = j.id
            LIMIT 1
        ) saga ON true
        ORDER BY j.id {order}
        "#,
        deps = deps_table,
        jobs = jobs_table(schema),
        workflows = workflows_table,
        steps = saga_steps_table,
        limit = limit + 1,
    );

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
            Option<String>,
        ),
    >(&list_query);
    for p in &params {
        list_q = list_q.bind(p);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_else(|e| {
        tracing::error!(error = %e, "listing jobs");
        Vec::new()
    });

    rows.into_iter()
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
            saga_step_state: row.14,
        })
        .collect()
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

    // Check if workflows table exists (it might not if the workflow migrations haven't run)
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
    let node_id = args
        .get("node_id")
        .and_then(|v| v.as_str())
        .map(String::from);
    let node_type = args
        .get("node_type")
        .and_then(|v| v.as_str())
        .map(String::from);
    let raw_node_inputs = args.get("raw_inputs").cloned();

    // Fetch resolved inputs from node_executions table
    let node_inputs = fetch_node_inputs(pool, schema, args).await;

    let (saga_step, saga_step_count) = match job_row.8 {
        Some(_) if check_table_exists(pool, schema, "ishikari_saga_steps").await => {
            fetch_saga_step(pool, schema, id).await
        }
        _ => (None, 0),
    };

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
        saga_step,
        saga_step_count,
    }))
}

/// Fetch the saga step a job belongs to, either as forward or compensation job.
async fn fetch_saga_step(
    pool: &PgPool,
    schema: Option<&str>,
    job_id: i64,
) -> (Option<SagaStepInfo>, i64) {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            s.step_order,
            s.state,
            s.job_id,
            j.worker,
            j.state::text,
            s.compensation_job_id,
            cj.worker,
            cj.state::text,
            (SELECT COUNT(*) FROM {} s2 WHERE s2.workflow_id = s.workflow_id)
        FROM {} s
        JOIN {} j ON j.id = s.job_id
        LEFT JOIN {} cj ON cj.id = s.compensation_job_id
        WHERE s.job_id = $1 OR s.compensation_job_id = $1
        LIMIT 1
        "#,
        saga_steps_table, saga_steps_table, jobs_table, jobs_table
    );

    let row: Option<(
        i32,
        String,
        i64,
        String,
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
        i64,
    )> = sqlx::query_as(&query)
        .bind(job_id)
        .fetch_optional(pool)
        .await
        .unwrap_or_default();

    match row {
        Some(r) => (
            Some(SagaStepInfo {
                step_order: r.0,
                state: r.1,
                job_id: r.2,
                job_worker: r.3,
                job_state: r.4,
                compensation_job_id: r.5,
                compensation_worker: r.6,
                compensation_state: r.7,
            }),
            r.8,
        ),
        None => (None, 0),
    }
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
