//! Analytics dashboard handlers.

#![allow(clippy::type_complexity)]

use axum::extract::State;
use serde::Serialize;
use sqlx::PgPool;

use crate::templates::AnalyticsTemplate;
use crate::AppState;

/// Overall workflow statistics.
#[derive(Debug, Default, Serialize)]
pub struct WorkflowStats {
    pub total: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub cancelled: i64,
    pub success_rate: f64,
}

/// Statistics per workflow name/type.
#[derive(Debug, Serialize)]
pub struct WorkflowTypeStats {
    pub name: String,
    pub total: i64,
    pub completed: i64,
    pub failed: i64,
    pub success_rate: f64,
    pub avg_duration_secs: Option<f64>,
}

/// Overall job statistics.
#[derive(Debug, Default, Serialize)]
pub struct JobStats {
    pub total: i64,
    pub available: i64,
    pub executing: i64,
    pub completed: i64,
    pub discarded: i64,
    pub retryable: i64,
    pub success_rate: f64,
}

/// Statistics per worker type.
#[derive(Debug, Serialize)]
pub struct WorkerStats {
    pub worker: String,
    pub total: i64,
    pub completed: i64,
    pub discarded: i64,
    pub avg_attempts: f64,
    pub avg_duration_secs: Option<f64>,
}

/// Common failure patterns.
#[derive(Debug, Serialize)]
pub struct FailurePattern {
    pub worker: String,
    pub error_type: String,
    pub count: i64,
    pub last_seen: chrono::DateTime<chrono::Utc>,
}

/// Dependency resolution statistics.
#[derive(Debug, Default, Serialize)]
pub struct DependencyStats {
    pub total_deps: i64,
    pub pending: i64,
    pub satisfied: i64,
    pub failed: i64,
    pub blocked_jobs: i64,
}

/// Full analytics data.
#[derive(Debug, Default, Serialize)]
pub struct Analytics {
    pub workflow_stats: WorkflowStats,
    pub workflow_types: Vec<WorkflowTypeStats>,
    pub job_stats: JobStats,
    pub worker_stats: Vec<WorkerStats>,
    pub failure_patterns: Vec<FailurePattern>,
    pub dependency_stats: DependencyStats,
}

impl Analytics {
    fn state_class(&self, rate: f64) -> &'static str {
        if rate >= 0.95 {
            "badge-success"
        } else if rate >= 0.80 {
            "badge-warning"
        } else {
            "badge-error"
        }
    }

    pub fn workflow_success_class(&self) -> &'static str {
        self.state_class(self.workflow_stats.success_rate)
    }

    pub fn job_success_class(&self) -> &'static str {
        self.state_class(self.job_stats.success_rate)
    }
}

/// Analytics dashboard page.
pub async fn index(State(state): State<AppState>) -> AnalyticsTemplate {
    let analytics = fetch_analytics(&state.pool, state.schema()).await;
    AnalyticsTemplate { analytics }
}

async fn fetch_analytics(pool: &PgPool, schema: Option<&str>) -> Analytics {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };

    let mut analytics = Analytics::default();

    // Workflow stats
    if let Ok(stats) = fetch_workflow_stats(pool, &workflows_table).await {
        analytics.workflow_stats = stats;
    }

    // Workflow type breakdown
    if let Ok(types) = fetch_workflow_type_stats(pool, &workflows_table).await {
        analytics.workflow_types = types;
    }

    // Job stats
    if let Ok(stats) = fetch_job_stats(pool, &jobs_table).await {
        analytics.job_stats = stats;
    }

    // Worker stats
    if let Ok(workers) = fetch_worker_stats(pool, &jobs_table).await {
        analytics.worker_stats = workers;
    }

    // Failure patterns
    if let Ok(failures) = fetch_failure_patterns(pool, &jobs_table).await {
        analytics.failure_patterns = failures;
    }

    // Dependency stats
    if let Ok(deps) = fetch_dependency_stats(pool, &deps_table, &jobs_table).await {
        analytics.dependency_stats = deps;
    }

    analytics
}

async fn fetch_workflow_stats(pool: &PgPool, table: &str) -> Result<WorkflowStats, sqlx::Error> {
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

    let row: (i64, i64, i64, i64, i64) = sqlx::query_as(&query).fetch_one(pool).await?;

    let finished = row.2 + row.3; // completed + failed
    let success_rate = if finished > 0 {
        row.2 as f64 / finished as f64
    } else {
        0.0
    };

    Ok(WorkflowStats {
        total: row.0,
        running: row.1,
        completed: row.2,
        failed: row.3,
        cancelled: row.4,
        success_rate,
    })
}

async fn fetch_workflow_type_stats(
    pool: &PgPool,
    table: &str,
) -> Result<Vec<WorkflowTypeStats>, sqlx::Error> {
    let query = format!(
        r#"
        SELECT
            name,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE state = 'completed') as completed,
            COUNT(*) FILTER (WHERE state = 'failed') as failed,
            AVG(EXTRACT(EPOCH FROM (completed_at - inserted_at))) FILTER (WHERE completed_at IS NOT NULL) as avg_duration
        FROM {}
        GROUP BY name
        ORDER BY total DESC
        LIMIT 10
        "#,
        table
    );

    let rows: Vec<(String, i64, i64, i64, Option<f64>)> =
        sqlx::query_as(&query).fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|(name, total, completed, failed, avg_duration)| {
            let finished = completed + failed;
            let success_rate = if finished > 0 {
                completed as f64 / finished as f64
            } else {
                0.0
            };
            WorkflowTypeStats {
                name,
                total,
                completed,
                failed,
                success_rate,
                avg_duration_secs: avg_duration,
            }
        })
        .collect())
}

async fn fetch_job_stats(pool: &PgPool, table: &str) -> Result<JobStats, sqlx::Error> {
    let query = format!(
        r#"
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE state = 'available') as available,
            COUNT(*) FILTER (WHERE state = 'executing') as executing,
            COUNT(*) FILTER (WHERE state = 'completed') as completed,
            COUNT(*) FILTER (WHERE state = 'discarded') as discarded,
            COUNT(*) FILTER (WHERE state = 'retryable') as retryable
        FROM {}
        "#,
        table
    );

    let row: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(&query).fetch_one(pool).await?;

    let finished = row.3 + row.4; // completed + discarded
    let success_rate = if finished > 0 {
        row.3 as f64 / finished as f64
    } else {
        0.0
    };

    Ok(JobStats {
        total: row.0,
        available: row.1,
        executing: row.2,
        completed: row.3,
        discarded: row.4,
        retryable: row.5,
        success_rate,
    })
}

async fn fetch_worker_stats(pool: &PgPool, table: &str) -> Result<Vec<WorkerStats>, sqlx::Error> {
    let query = format!(
        r#"
        SELECT
            worker,
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE state = 'completed') as completed,
            COUNT(*) FILTER (WHERE state = 'discarded') as discarded,
            AVG(attempt) as avg_attempts,
            AVG(EXTRACT(EPOCH FROM (completed_at - inserted_at))) FILTER (WHERE completed_at IS NOT NULL) as avg_duration
        FROM {}
        GROUP BY worker
        ORDER BY total DESC
        LIMIT 20
        "#,
        table
    );

    let rows: Vec<(String, i64, i64, i64, Option<f64>, Option<f64>)> =
        sqlx::query_as(&query).fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(
            |(worker, total, completed, discarded, avg_attempts, avg_duration)| WorkerStats {
                worker,
                total,
                completed,
                discarded,
                avg_attempts: avg_attempts.unwrap_or(0.0),
                avg_duration_secs: avg_duration,
            },
        )
        .collect())
}

async fn fetch_failure_patterns(
    pool: &PgPool,
    table: &str,
) -> Result<Vec<FailurePattern>, sqlx::Error> {
    // Extract first error type from errors array for failed jobs
    let query = format!(
        r#"
        SELECT
            worker,
            COALESCE(errors->-1->>'kind', 'unknown') as error_type,
            COUNT(*) as count,
            MAX(inserted_at) as last_seen
        FROM {}
        WHERE state = 'discarded'
          AND jsonb_array_length(errors) > 0
        GROUP BY worker, error_type
        ORDER BY count DESC
        LIMIT 15
        "#,
        table
    );

    let rows: Vec<(String, String, i64, chrono::DateTime<chrono::Utc>)> =
        sqlx::query_as(&query).fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|(worker, error_type, count, last_seen)| FailurePattern {
            worker,
            error_type,
            count,
            last_seen,
        })
        .collect())
}

async fn fetch_dependency_stats(
    pool: &PgPool,
    deps_table: &str,
    jobs_table: &str,
) -> Result<DependencyStats, sqlx::Error> {
    let query = format!(
        r#"
        SELECT
            (SELECT COUNT(*) FROM {deps}),
            (SELECT COUNT(*) FROM {deps} WHERE state = 'pending'),
            (SELECT COUNT(*) FROM {deps} WHERE state = 'satisfied'),
            (SELECT COUNT(*) FROM {deps} WHERE state = 'failed'),
            (SELECT COUNT(DISTINCT j.id) FROM {jobs} j
             INNER JOIN {deps} d ON d.job_id = j.id
             WHERE j.state IN ('scheduled', 'available')
               AND d.state != 'satisfied')
        "#,
        deps = deps_table,
        jobs = jobs_table
    );

    let row: (i64, i64, i64, i64, i64) = sqlx::query_as(&query).fetch_one(pool).await?;

    Ok(DependencyStats {
        total_deps: row.0,
        pending: row.1,
        satisfied: row.2,
        failed: row.3,
        blocked_jobs: row.4,
    })
}
