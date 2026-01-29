//! Dashboard route handlers.

#![allow(clippy::type_complexity)]

use axum::extract::State;
use sqlx::PgPool;

use crate::templates::DashboardTemplate;
use crate::AppState;

// Re-export WorkflowStats from analytics for convenience
pub use super::analytics::WorkflowStats;

/// Dashboard index page.
pub async fn index(State(state): State<AppState>) -> DashboardTemplate {
    let stats = get_stats(&state.pool, state.schema.as_deref()).await;
    let workflow_stats = get_workflow_stats(&state.pool, state.schema.as_deref()).await;

    DashboardTemplate {
        stats,
        workflow_stats,
        recent_failures: get_recent_failures(&state.pool, state.schema.as_deref(), 5).await,
    }
}

/// Get workflow statistics for the dashboard.
async fn get_workflow_stats(pool: &PgPool, schema: Option<&str>) -> WorkflowStats {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    // Check if table exists first
    let table_exists: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'ishikari_workflows'
        )
        "#,
    )
    .bind(schema.unwrap_or("public"))
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    if !table_exists.unwrap_or(false) {
        return WorkflowStats::default();
    }

    let query = format!(
        r#"
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE state = 'running') as running,
            COUNT(*) FILTER (WHERE state = 'completed') as completed,
            COUNT(*) FILTER (WHERE state = 'failed') as failed,
            COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled
        FROM {}
        "#,
        table
    );

    let row: Option<(i64, i64, i64, i64, i64)> = sqlx::query_as(&query)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();

    match row {
        Some((total, running, completed, failed, cancelled)) => {
            let finished = completed + failed;
            let success_rate = if finished > 0 {
                completed as f64 / finished as f64
            } else {
                0.0
            };
            WorkflowStats {
                total,
                running,
                completed,
                failed,
                cancelled,
                success_rate,
            }
        }
        None => WorkflowStats::default(),
    }
}

/// Job statistics by state.
#[derive(Debug, Default, serde::Serialize)]
pub struct JobStats {
    pub available: i64,
    pub scheduled: i64,
    pub executing: i64,
    pub retryable: i64,
    pub completed: i64,
    pub discarded: i64,
    pub cancelled: i64,
    pub total: i64,
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

/// Recent job failure info.
#[derive(Debug)]
pub struct RecentFailure {
    pub id: i64,
    pub worker: String,
    pub queue: String,
    pub error: Option<String>,
    pub failed_at: chrono::DateTime<chrono::Utc>,
}

/// Get recent job failures.
async fn get_recent_failures(
    pool: &PgPool,
    schema: Option<&str>,
    limit: i64,
) -> Vec<RecentFailure> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        r#"
        SELECT id, worker, queue, errors->-1->>'message' as error, updated_at
        FROM {}
        WHERE state IN ('discarded', 'retryable')
        ORDER BY updated_at DESC
        LIMIT $1
        "#,
        table
    );

    let rows: Vec<(
        i64,
        String,
        String,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(&query)
        .bind(limit)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|(id, worker, queue, error, failed_at)| RecentFailure {
            id,
            worker,
            queue,
            error,
            failed_at,
        })
        .collect()
}
