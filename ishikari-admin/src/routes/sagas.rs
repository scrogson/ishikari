//! Saga route handlers.

#![allow(clippy::type_complexity)]

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use sqlx::PgPool;

use crate::templates::{SagaDetailTemplate, SagasListTemplate};
use crate::AppState;

/// Query parameters for saga listing.
#[derive(Debug, Deserialize, Default)]
pub struct SagasQuery {
    pub state: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// Saga workflow for list display.
#[derive(Debug)]
pub struct SagaInfo {
    pub id: i64,
    pub name: String,
    pub state: String,
    pub step_count: i64,
    pub completed_steps: i64,
    pub failed_steps: i64,
    pub compensating_steps: i64,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl SagaInfo {
    /// Calculate progress percentage.
    pub fn progress(&self) -> i64 {
        if self.step_count == 0 {
            0
        } else {
            (self.completed_steps * 100) / self.step_count
        }
    }

    /// Get CSS class for state badge.
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

/// Saga step detail.
#[derive(Debug)]
pub struct SagaStepInfo {
    pub step_order: i32,
    pub state: String,
    pub job_id: i64,
    pub job_worker: String,
    pub job_state: String,
    pub compensation_job_id: Option<i64>,
    pub compensation_worker: Option<String>,
    pub compensation_state: Option<String>,
}

impl SagaStepInfo {
    /// Get CSS class for step state.
    pub fn state_class(&self) -> &'static str {
        match self.state.as_str() {
            "pending" => "badge-secondary",
            "running" => "badge-info",
            "completed" => "badge-success",
            "failed" => "badge-error",
            "compensating" => "badge-warning",
            "compensated" => "badge-neutral",
            _ => "badge-ghost",
        }
    }

    /// Get CSS class for job state.
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

    /// Get CSS class for compensation state.
    pub fn compensation_state_class(&self) -> &'static str {
        match self.compensation_state.as_deref() {
            Some("available") => "badge-primary",
            Some("scheduled") => "badge-secondary",
            Some("executing") => "badge-info",
            Some("completed") => "badge-success",
            // Cancelled compensation = not needed (saga succeeded) - use neutral color
            Some("cancelled") => "badge-neutral",
            Some("discarded") => "badge-error",
            Some("retryable") => "badge-warning",
            _ => "badge-ghost",
        }
    }
}

/// Detailed saga information.
#[derive(Debug)]
pub struct SagaDetail {
    pub id: i64,
    pub name: String,
    pub state: String,
    pub metadata: serde_json::Value,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl SagaDetail {
    /// Format metadata as pretty JSON.
    pub fn metadata_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.metadata).unwrap_or_else(|_| self.metadata.to_string())
    }

    /// Get CSS class for state.
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

    /// Check if saga is in compensating state.
    pub fn is_compensating(&self) -> bool {
        self.state == "compensating"
    }
}

/// List sagas page.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<SagasQuery>,
) -> SagasListTemplate {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(25).min(100);
    let offset = (page - 1) * per_page;

    let (sagas, total) = get_sagas(
        &state.pool,
        state.schema(),
        query.state.as_deref(),
        per_page,
        offset,
    )
    .await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    SagasListTemplate {
        sagas,
        current_state: query.state,
        page,
        total,
        total_pages,
    }
}

/// Show single saga.
pub async fn show(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<SagaDetailTemplate, Response> {
    let saga = get_saga(&state.pool, state.schema(), id)
        .await
        .ok_or_else(|| (axum::http::StatusCode::NOT_FOUND, "Saga not found").into_response())?;

    let steps = get_saga_steps(&state.pool, state.schema(), id).await;

    Ok(SagaDetailTemplate { saga, steps })
}

/// Get sagas (workflows that have saga steps).
async fn get_sagas(
    pool: &PgPool,
    schema: Option<&str>,
    state_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> (Vec<SagaInfo>, i64) {
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let mut conditions =
        vec!["EXISTS (SELECT 1 FROM {} ss WHERE ss.workflow_id = w.id)".to_string()];
    let mut params: Vec<String> = Vec::new();

    if let Some(s) = state_filter {
        params.push(s.to_string());
        conditions.push(format!("w.state = ${}", params.len()));
    }

    // Replace the placeholder in the EXISTS condition
    conditions[0] = conditions[0].replace("{}", &saga_steps_table);

    let where_clause = format!("WHERE {}", conditions.join(" AND "));

    let count_query = format!(
        "SELECT COUNT(*) FROM {} w {}",
        workflows_table, where_clause
    );

    let list_query = format!(
        r#"
        SELECT
            w.id, w.name, w.state, w.inserted_at, w.completed_at,
            COUNT(s.step_order) as step_count,
            COUNT(s.step_order) FILTER (WHERE s.state = 'completed') as completed_steps,
            COUNT(s.step_order) FILTER (WHERE s.state = 'failed') as failed_steps,
            COUNT(s.step_order) FILTER (WHERE s.state IN ('compensating', 'compensated')) as compensating_steps
        FROM {} w
        JOIN {} s ON s.workflow_id = w.id
        {}
        GROUP BY w.id
        ORDER BY w.id DESC
        LIMIT {} OFFSET {}
        "#,
        workflows_table, saga_steps_table, where_clause, limit, offset
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
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
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

    let sagas = rows
        .into_iter()
        .map(|row| SagaInfo {
            id: row.0,
            name: row.1,
            state: row.2,
            inserted_at: row.3,
            completed_at: row.4,
            step_count: row.5,
            completed_steps: row.6,
            failed_steps: row.7,
            compensating_steps: row.8,
        })
        .collect();

    (sagas, total)
}

/// Get a saga by ID.
async fn get_saga(pool: &PgPool, schema: Option<&str>, id: i64) -> Option<SagaDetail> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    let query = format!(
        r#"
        SELECT id, name, state, metadata, inserted_at, completed_at
        FROM {}
        WHERE id = $1
        "#,
        table
    );

    let row: Option<(
        i64,
        String,
        String,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()?;

    row.map(|r| SagaDetail {
        id: r.0,
        name: r.1,
        state: r.2,
        metadata: r.3,
        inserted_at: r.4,
        completed_at: r.5,
    })
}

/// Get saga steps with job information.
async fn get_saga_steps(
    pool: &PgPool,
    schema: Option<&str>,
    workflow_id: i64,
) -> Vec<SagaStepInfo> {
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            s.step_order,
            s.state,
            s.job_id,
            j.worker as job_worker,
            j.state::text as job_state,
            s.compensation_job_id,
            cj.worker as compensation_worker,
            cj.state::text as compensation_state
        FROM {} s
        JOIN {} j ON j.id = s.job_id
        LEFT JOIN {} cj ON cj.id = s.compensation_job_id
        WHERE s.workflow_id = $1
        ORDER BY s.step_order
        "#,
        saga_steps_table, jobs_table, jobs_table
    );

    let rows: Vec<(
        i32,
        String,
        i64,
        String,
        String,
        Option<i64>,
        Option<String>,
        Option<String>,
    )> = sqlx::query_as(&query)
        .bind(workflow_id)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|row| SagaStepInfo {
            step_order: row.0,
            state: row.1,
            job_id: row.2,
            job_worker: row.3,
            job_state: row.4,
            compensation_job_id: row.5,
            compensation_worker: row.6,
            compensation_state: row.7,
        })
        .collect()
}
