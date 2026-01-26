//! Job route handlers.

#![allow(clippy::type_complexity)]

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Redirect, Response},
};
use serde::Deserialize;
use sqlx::PgPool;

use crate::templates::{JobDetailTemplate, JobsListTemplate};
use crate::AppState;

/// Query parameters for job listing.
#[derive(Debug, Deserialize, Default)]
pub struct JobsQuery {
    pub state: Option<String>,
    pub queue: Option<String>,
    pub worker: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// Job information for display.
#[allow(dead_code)]
#[derive(Debug)]
pub struct JobInfo {
    pub id: i64,
    pub queue: String,
    pub worker: String,
    pub state: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
    pub scheduled_at: chrono::DateTime<chrono::Utc>,
    pub attempted_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// Detailed job information.
#[derive(Debug)]
pub struct JobDetail {
    pub id: i64,
    pub queue: String,
    pub worker: String,
    pub state: String,
    pub args: serde_json::Value,
    pub errors: serde_json::Value,
    pub attempt: i32,
    pub max_attempts: i32,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
    pub scheduled_at: chrono::DateTime<chrono::Utc>,
    pub attempted_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl JobDetail {
    /// Check if the job has any errors.
    pub fn has_errors(&self) -> bool {
        self.errors
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false)
    }

    /// Format args as pretty JSON.
    pub fn args_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.args).unwrap_or_else(|_| self.args.to_string())
    }

    /// Format errors as pretty JSON.
    pub fn errors_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.errors).unwrap_or_else(|_| self.errors.to_string())
    }
}

/// List jobs page.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<JobsQuery>,
) -> JobsListTemplate {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(25).min(100);
    let offset = (page - 1) * per_page;

    let (jobs, total) = get_jobs(
        &state.pool,
        state.schema.as_deref(),
        query.state.as_deref(),
        query.queue.as_deref(),
        query.worker.as_deref(),
        per_page,
        offset,
    )
    .await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    JobsListTemplate {
        jobs,
        current_state: query.state,
        current_queue: query.queue,
        current_worker: query.worker,
        page,
        total,
        total_pages,
        nav_items: state.nav_items().to_vec(),
    }
}

/// Show single job.
pub async fn show(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<JobDetailTemplate, Response> {
    let job = get_job(&state.pool, state.schema.as_deref(), id)
        .await
        .ok_or_else(|| (axum::http::StatusCode::NOT_FOUND, "Job not found").into_response())?;

    Ok(JobDetailTemplate {
        job,
        nav_items: state.nav_items().to_vec(),
    })
}

/// Retry a failed job.
pub async fn retry(State(state): State<AppState>, Path(id): Path<i64>) -> Redirect {
    retry_job(&state.pool, state.schema.as_deref(), id).await;
    Redirect::to(&format!("/jobs/{}", id))
}

/// Cancel a job.
pub async fn cancel(State(state): State<AppState>, Path(id): Path<i64>) -> Redirect {
    cancel_job(&state.pool, state.schema.as_deref(), id).await;
    Redirect::to(&format!("/jobs/{}", id))
}

/// Discard a job.
pub async fn discard(State(state): State<AppState>, Path(id): Path<i64>) -> Redirect {
    discard_job(&state.pool, state.schema.as_deref(), id).await;
    Redirect::to(&format!("/jobs/{}", id))
}

/// Get jobs with filtering and pagination.
async fn get_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    state_filter: Option<&str>,
    queue_filter: Option<&str>,
    worker_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> (Vec<JobInfo>, i64) {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let mut conditions = Vec::new();
    let mut params: Vec<String> = Vec::new();

    if let Some(s) = state_filter {
        params.push(s.to_string());
        conditions.push(format!("state::text = ${}", params.len()));
    }
    if let Some(q) = queue_filter {
        params.push(q.to_string());
        conditions.push(format!("queue = ${}", params.len()));
    }
    if let Some(w) = worker_filter {
        params.push(format!("%{}%", w));
        conditions.push(format!("worker ILIKE ${}", params.len()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let count_query = format!("SELECT COUNT(*) FROM {} {}", table, where_clause);
    let list_query = format!(
        r#"
        SELECT id, queue, worker, state::text, attempt, max_attempts,
               inserted_at, scheduled_at, attempted_at, completed_at
        FROM {} {}
        ORDER BY id DESC
        LIMIT {} OFFSET {}
        "#,
        table, where_clause, limit, offset
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
            String,
            i32,
            i32,
            chrono::DateTime<chrono::Utc>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<chrono::DateTime<chrono::Utc>>,
        ),
    >(&list_query);
    for p in &params {
        list_q = list_q.bind(p);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let jobs = rows
        .into_iter()
        .map(|row| JobInfo {
            id: row.0,
            queue: row.1,
            worker: row.2,
            state: row.3,
            attempt: row.4,
            max_attempts: row.5,
            inserted_at: row.6,
            scheduled_at: row.7,
            attempted_at: row.8,
            completed_at: row.9,
        })
        .collect();

    (jobs, total)
}

/// Get a single job by ID.
async fn get_job(pool: &PgPool, schema: Option<&str>, id: i64) -> Option<JobDetail> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        r#"
        SELECT id, queue, worker, state::text, args, errors, attempt, max_attempts,
               inserted_at, scheduled_at, attempted_at, completed_at
        FROM {}
        WHERE id = $1
        "#,
        table
    );

    let row: Option<(
        i64,
        String,
        String,
        String,
        serde_json::Value,
        serde_json::Value,
        i32,
        i32,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()?;

    row.map(|r| JobDetail {
        id: r.0,
        queue: r.1,
        worker: r.2,
        state: r.3,
        args: r.4,
        errors: r.5,
        attempt: r.6,
        max_attempts: r.7,
        inserted_at: r.8,
        scheduled_at: r.9,
        attempted_at: r.10,
        completed_at: r.11,
    })
}

/// Retry a job by setting it back to available state.
async fn retry_job(pool: &PgPool, schema: Option<&str>, id: i64) {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let enum_type = match schema {
        Some(s) => format!("{}.ishikari_job_state", s),
        None => "ishikari_job_state".to_string(),
    };

    let query = format!(
        "UPDATE {} SET state = 'available'::{}, scheduled_at = now() WHERE id = $1",
        table, enum_type
    );

    let _ = sqlx::query(&query).bind(id).execute(pool).await;
}

/// Cancel a job.
async fn cancel_job(pool: &PgPool, schema: Option<&str>, id: i64) {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let enum_type = match schema {
        Some(s) => format!("{}.ishikari_job_state", s),
        None => "ishikari_job_state".to_string(),
    };

    let query = format!(
        "UPDATE {} SET state = 'cancelled'::{}, completed_at = now() WHERE id = $1",
        table, enum_type
    );

    let _ = sqlx::query(&query).bind(id).execute(pool).await;
}

/// Discard a job.
async fn discard_job(pool: &PgPool, schema: Option<&str>, id: i64) {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let enum_type = match schema {
        Some(s) => format!("{}.ishikari_job_state", s),
        None => "ishikari_job_state".to_string(),
    };

    let query = format!(
        "UPDATE {} SET state = 'discarded'::{}, completed_at = now() WHERE id = $1",
        table, enum_type
    );

    let _ = sqlx::query(&query).bind(id).execute(pool).await;
}
