//! Queue route handlers.

#![allow(clippy::type_complexity)]

use axum::extract::{Path, Query, State};
use serde::Deserialize;
use sqlx::PgPool;

use crate::templates::{QueueDetailTemplate, QueuesListTemplate};
use crate::AppState;

/// Job info for queue listings.
#[derive(Debug)]
pub struct QueueJobInfo {
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
    /// Saga step state, if this is a saga forward job.
    pub saga_step_state: Option<String>,
}

impl QueueJobInfo {
    pub fn saga_rollback_state(&self) -> Option<&str> {
        super::sagas::rollback_state(self.saga_step_state.as_deref())
    }

    pub fn saga_rollback_class(&self) -> &'static str {
        super::sagas::rollback_class(self.saga_step_state.as_deref())
    }

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
}

/// Query parameters for queue page.
#[derive(Debug, Deserialize, Default)]
pub struct QueueQuery {
    pub state: Option<String>,
    pub page: Option<i64>,
}

/// Queue information for display.
#[derive(Debug)]
pub struct QueueInfo {
    pub name: String,
    pub available: i64,
    pub scheduled: i64,
    pub executing: i64,
    pub retryable: i64,
    pub completed: i64,
    pub discarded: i64,
    pub cancelled: i64,
    pub total: i64,
}

/// List queues page.
pub async fn list(State(state): State<AppState>) -> QueuesListTemplate {
    let queues = get_queues(&state.pool, state.schema.as_deref()).await;
    QueuesListTemplate {
        base_path: state.base_path.to_string(),
        nav_items: state.nav_items.to_vec(),
        queues,
    }
}

/// Show single queue.
pub async fn show(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<QueueQuery>,
) -> QueueDetailTemplate {
    let stats = get_queue_stats(&state.pool, state.schema.as_deref(), &name)
        .await
        .unwrap_or_else(|| QueueInfo {
            name: name.clone(),
            available: 0,
            scheduled: 0,
            executing: 0,
            retryable: 0,
            completed: 0,
            discarded: 0,
            cancelled: 0,
            total: 0,
        });

    let page = query.page.unwrap_or(1).max(1);
    let per_page = 25i64;
    let offset = (page - 1) * per_page;

    let (jobs, total) = get_queue_jobs(
        &state.pool,
        state.schema.as_deref(),
        &name,
        query.state.as_deref(),
        per_page,
        offset,
    )
    .await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    QueueDetailTemplate {
        base_path: state.base_path.to_string(),
        nav_items: state.nav_items.to_vec(),
        queue_name: name,
        stats,
        jobs,
        current_state: query.state,
        page,
        total,
        total_pages,
    }
}

/// Get all queues with their statistics.
async fn get_queues(pool: &PgPool, schema: Option<&str>) -> Vec<QueueInfo> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            queue,
            COUNT(*) FILTER (WHERE state = 'available') as available,
            COUNT(*) FILTER (WHERE state = 'scheduled') as scheduled,
            COUNT(*) FILTER (WHERE state = 'executing') as executing,
            COUNT(*) FILTER (WHERE state = 'retryable') as retryable,
            COUNT(*) FILTER (WHERE state = 'completed') as completed,
            COUNT(*) FILTER (WHERE state = 'discarded') as discarded,
            COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled,
            COUNT(*) as total
        FROM {}
        GROUP BY queue
        ORDER BY queue
        "#,
        table
    );

    let rows: Vec<(String, i64, i64, i64, i64, i64, i64, i64, i64)> = sqlx::query_as(&query)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|row| QueueInfo {
            name: row.0,
            available: row.1,
            scheduled: row.2,
            executing: row.3,
            retryable: row.4,
            completed: row.5,
            discarded: row.6,
            cancelled: row.7,
            total: row.8,
        })
        .collect()
}

/// Get jobs for a specific queue.
async fn get_queue_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    queue_name: &str,
    state_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> (Vec<QueueJobInfo>, i64) {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    // "compensated" isn't a job state: completed jobs whose saga step was rolled back
    let compensated = state_filter == Some("compensated");
    let state_filter = state_filter.filter(|_| !compensated);

    let mut conditions = vec!["j.queue = $1".to_string()];
    let mut param_count = 1;

    if compensated {
        conditions.push(super::sagas::compensated_condition(&saga_steps_table, "j"));
    }
    if state_filter.is_some() {
        param_count += 1;
        conditions.push(format!("j.state::text = ${}", param_count));
    }

    let where_clause = format!("WHERE {}", conditions.join(" AND "));

    let count_query = format!("SELECT COUNT(*) FROM {} j {}", table, where_clause);
    let list_query = format!(
        r#"
        SELECT j.id, j.queue, j.worker, j.state::text, j.attempt, j.max_attempts,
               j.inserted_at, j.scheduled_at, j.attempted_at, j.completed_at,
               (SELECT s.state FROM {} s WHERE s.job_id = j.id LIMIT 1) as saga_step_state
        FROM {} j {}
        ORDER BY j.id DESC
        LIMIT {} OFFSET {}
        "#,
        saga_steps_table, table, where_clause, limit, offset
    );

    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query).bind(queue_name);
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
            Option<String>,
        ),
    >(&list_query)
    .bind(queue_name);

    if let Some(s) = state_filter {
        count_q = count_q.bind(s);
        list_q = list_q.bind(s);
    }

    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);
    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let jobs = rows
        .into_iter()
        .map(|row| QueueJobInfo {
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
            saga_step_state: row.10,
        })
        .collect();

    (jobs, total)
}

/// Get statistics for a single queue.
async fn get_queue_stats(
    pool: &PgPool,
    schema: Option<&str>,
    queue_name: &str,
) -> Option<QueueInfo> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            queue,
            COUNT(*) FILTER (WHERE state = 'available') as available,
            COUNT(*) FILTER (WHERE state = 'scheduled') as scheduled,
            COUNT(*) FILTER (WHERE state = 'executing') as executing,
            COUNT(*) FILTER (WHERE state = 'retryable') as retryable,
            COUNT(*) FILTER (WHERE state = 'completed') as completed,
            COUNT(*) FILTER (WHERE state = 'discarded') as discarded,
            COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled,
            COUNT(*) as total
        FROM {}
        WHERE queue = $1
        GROUP BY queue
        "#,
        table
    );

    let row: Option<(String, i64, i64, i64, i64, i64, i64, i64, i64)> = sqlx::query_as(&query)
        .bind(queue_name)
        .fetch_optional(pool)
        .await
        .ok()?;

    row.map(|r| QueueInfo {
        name: r.0,
        available: r.1,
        scheduled: r.2,
        executing: r.3,
        retryable: r.4,
        completed: r.5,
        discarded: r.6,
        cancelled: r.7,
        total: r.8,
    })
}
