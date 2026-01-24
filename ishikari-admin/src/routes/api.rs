//! API route handlers for htmx partial updates.

#![allow(clippy::type_complexity)]

use axum::extract::{Query, State};
use sqlx::PgPool;

use crate::routes::dashboard::JobStats;
use crate::routes::jobs::{JobInfo, JobsQuery};
use crate::templates::{JobsTablePartial, StatsPartial};
use crate::AppState;

/// Get jobs table partial for htmx updates.
pub async fn jobs_table(
    State(state): State<AppState>,
    Query(query): Query<JobsQuery>,
) -> JobsTablePartial {
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

    JobsTablePartial {
        jobs,
        page,
        total_pages,
    }
}

/// Get stats partial for htmx updates.
pub async fn stats(State(state): State<AppState>) -> StatsPartial {
    let stats = get_stats(&state.pool, state.schema.as_deref()).await;
    StatsPartial { stats }
}

/// Get job statistics.
async fn get_stats(pool: &PgPool, schema: Option<&str>) -> JobStats {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            COUNT(*) FILTER (WHERE state = 'available') as available,
            COUNT(*) FILTER (WHERE state = 'scheduled') as scheduled,
            COUNT(*) FILTER (WHERE state = 'executing') as executing,
            COUNT(*) FILTER (WHERE state = 'retryable') as retryable,
            COUNT(*) FILTER (WHERE state = 'completed') as completed,
            COUNT(*) FILTER (WHERE state = 'discarded') as discarded,
            COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled,
            COUNT(*) as total
        FROM {}
        "#,
        table
    );

    let row: Option<(i64, i64, i64, i64, i64, i64, i64, i64)> = sqlx::query_as(&query)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();

    match row {
        Some((
            available,
            scheduled,
            executing,
            retryable,
            completed,
            discarded,
            cancelled,
            total,
        )) => JobStats {
            available,
            scheduled,
            executing,
            retryable,
            completed,
            discarded,
            cancelled,
            total,
        },
        None => JobStats::default(),
    }
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

    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    for p in &params {
        count_q = count_q.bind(p);
    }
    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);

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
