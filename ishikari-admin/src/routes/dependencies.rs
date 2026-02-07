//! Job dependency route handlers.

#![allow(clippy::type_complexity)]

use axum::{
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use sqlx::PgPool;

use crate::templates::{DependenciesListTemplate, DependencyDetailTemplate};
use crate::AppState;

/// Query parameters for dependencies listing.
#[derive(Debug, Deserialize, Default)]
pub struct DependenciesQuery {
    /// Filter to show only blocked jobs
    pub blocked: Option<bool>,
    /// Filter by workflow
    pub workflow_id: Option<i64>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// Dependency relationship for display.
#[derive(Debug, Clone)]
pub struct DependencyInfo {
    pub job_id: i64,
    pub job_worker: String,
    pub job_state: String,
    pub depends_on_job_id: i64,
    pub depends_on_worker: String,
    pub depends_on_state: String,
    pub workflow_id: Option<i64>,
    pub workflow_name: Option<String>,
}

impl DependencyInfo {
    /// Check if this dependency is blocking the job.
    pub fn is_blocking(&self) -> bool {
        self.depends_on_state != "completed"
    }

    /// Get CSS class for the job state badge.
    pub fn job_state_class(&self) -> &'static str {
        state_to_class(&self.job_state)
    }

    /// Get CSS class for the dependency state badge.
    pub fn depends_on_state_class(&self) -> &'static str {
        state_to_class(&self.depends_on_state)
    }
}

fn state_to_class(state: &str) -> &'static str {
    match state {
        "available" => "badge-primary",
        "scheduled" => "badge-secondary",
        "executing" => "badge-info",
        "completed" => "badge-success",
        "discarded" | "cancelled" => "badge-error",
        "retryable" => "badge-warning",
        _ => "badge-ghost",
    }
}

/// Job with its dependencies for detailed view.
#[derive(Debug)]
pub struct JobWithDependencies {
    pub id: i64,
    pub worker: String,
    pub state: String,
    pub queue: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub workflow_id: Option<i64>,
    pub workflow_name: Option<String>,
    pub inserted_at: chrono::DateTime<chrono::Utc>,
    pub dependencies: Vec<DependencyTarget>,
    pub dependents: Vec<DependencyTarget>,
}

impl JobWithDependencies {
    pub fn state_class(&self) -> &'static str {
        state_to_class(&self.state)
    }

    /// Check if this job is blocked by any incomplete dependencies.
    pub fn is_blocked(&self) -> bool {
        self.dependencies.iter().any(|d| d.state != "completed")
    }

    /// Get blocking dependencies.
    pub fn blocking_dependencies(&self) -> Vec<&DependencyTarget> {
        self.dependencies
            .iter()
            .filter(|d| d.state != "completed")
            .collect()
    }
}

/// A dependency target (either upstream or downstream).
#[derive(Debug, Clone)]
pub struct DependencyTarget {
    pub job_id: i64,
    pub worker: String,
    pub state: String,
}

impl DependencyTarget {
    pub fn state_class(&self) -> &'static str {
        state_to_class(&self.state)
    }
}

/// Dependency graph statistics.
#[derive(Debug, Default)]
pub struct DependencyStats {
    pub total_dependencies: i64,
    pub blocked_jobs: i64,
    pub ready_jobs: i64,
}

/// List dependencies page.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<DependenciesQuery>,
) -> DependenciesListTemplate {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(50).min(100);
    let offset = (page - 1) * per_page;

    let (dependencies, total) = get_dependencies(
        &state.pool,
        state.schema(),
        query.blocked,
        query.workflow_id,
        per_page,
        offset,
    )
    .await;

    let stats = get_dependency_stats(&state.pool, state.schema()).await;
    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    DependenciesListTemplate {
        dependencies,
        stats,
        show_blocked_only: query.blocked.unwrap_or(false),
        workflow_id: query.workflow_id,
        page,
        total,
        total_pages,
    }
}

/// Show job with its dependency graph.
pub async fn show(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<DependencyDetailTemplate, Response> {
    let job = get_job_with_dependencies(&state.pool, state.schema(), id)
        .await
        .ok_or_else(|| (axum::http::StatusCode::NOT_FOUND, "Job not found").into_response())?;

    Ok(DependencyDetailTemplate { job })
}

/// Get all dependency relationships with filtering.
async fn get_dependencies(
    pool: &PgPool,
    schema: Option<&str>,
    blocked_only: Option<bool>,
    workflow_id: Option<i64>,
    limit: i64,
    offset: i64,
) -> (Vec<DependencyInfo>, i64) {
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    let mut conditions = Vec::new();
    let mut param_idx = 0;

    if blocked_only.unwrap_or(false) {
        conditions.push("dep_job.state != 'completed'".to_string());
    }

    if workflow_id.is_some() {
        param_idx += 1;
        conditions.push(format!("j.workflow_id = ${}", param_idx));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let count_query = format!(
        r#"
        SELECT COUNT(*)
        FROM {} d
        JOIN {} j ON d.job_id = j.id
        JOIN {} dep_job ON d.depends_on_job_id = dep_job.id
        LEFT JOIN {} w ON j.workflow_id = w.id
        {}
        "#,
        deps_table, jobs_table, jobs_table, workflows_table, where_clause
    );

    let list_query = format!(
        r#"
        SELECT
            d.job_id,
            j.worker as job_worker,
            j.state::text as job_state,
            d.depends_on_job_id,
            dep_job.worker as depends_on_worker,
            dep_job.state::text as depends_on_state,
            j.workflow_id,
            w.name as workflow_name
        FROM {} d
        JOIN {} j ON d.job_id = j.id
        JOIN {} dep_job ON d.depends_on_job_id = dep_job.id
        LEFT JOIN {} w ON j.workflow_id = w.id
        {}
        ORDER BY j.workflow_id NULLS LAST, d.job_id, d.depends_on_job_id
        LIMIT {} OFFSET {}
        "#,
        deps_table, jobs_table, jobs_table, workflows_table, where_clause, limit, offset
    );

    // Execute count query
    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    if let Some(wf_id) = workflow_id {
        count_q = count_q.bind(wf_id);
    }
    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);

    // Execute list query
    let mut list_q = sqlx::query_as::<
        _,
        (
            i64,
            String,
            String,
            i64,
            String,
            String,
            Option<i64>,
            Option<String>,
        ),
    >(&list_query);
    if let Some(wf_id) = workflow_id {
        list_q = list_q.bind(wf_id);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let dependencies = rows
        .into_iter()
        .map(|row| DependencyInfo {
            job_id: row.0,
            job_worker: row.1,
            job_state: row.2,
            depends_on_job_id: row.3,
            depends_on_worker: row.4,
            depends_on_state: row.5,
            workflow_id: row.6,
            workflow_name: row.7,
        })
        .collect();

    (dependencies, total)
}

/// Get dependency statistics.
async fn get_dependency_stats(pool: &PgPool, schema: Option<&str>) -> DependencyStats {
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            COUNT(*) as total_dependencies,
            COUNT(DISTINCT d.job_id) FILTER (
                WHERE dep_job.state != 'completed' AND j.state NOT IN ('completed', 'discarded', 'cancelled')
            ) as blocked_jobs,
            COUNT(DISTINCT d.job_id) FILTER (
                WHERE NOT EXISTS (
                    SELECT 1 FROM {} d2
                    JOIN {} dep2 ON d2.depends_on_job_id = dep2.id
                    WHERE d2.job_id = d.job_id AND dep2.state != 'completed'
                ) AND j.state NOT IN ('completed', 'discarded', 'cancelled')
            ) as ready_jobs
        FROM {} d
        JOIN {} j ON d.job_id = j.id
        JOIN {} dep_job ON d.depends_on_job_id = dep_job.id
        "#,
        deps_table, jobs_table, deps_table, jobs_table, jobs_table
    );

    let row: Option<(i64, i64, i64)> = sqlx::query_as(&query)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();

    match row {
        Some((total, blocked, ready)) => DependencyStats {
            total_dependencies: total,
            blocked_jobs: blocked,
            ready_jobs: ready,
        },
        None => DependencyStats::default(),
    }
}

/// Get a job with its full dependency information.
async fn get_job_with_dependencies(
    pool: &PgPool,
    schema: Option<&str>,
    job_id: i64,
) -> Option<JobWithDependencies> {
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

    // Get job info
    let job_query = format!(
        r#"
        SELECT
            j.id, j.worker, j.state::text, j.queue, j.attempt, j.max_attempts,
            j.workflow_id, w.name as workflow_name, j.inserted_at
        FROM {} j
        LEFT JOIN {} w ON j.workflow_id = w.id
        WHERE j.id = $1
        "#,
        jobs_table, workflows_table
    );

    let job_row: Option<(
        i64,
        String,
        String,
        String,
        i32,
        i32,
        Option<i64>,
        Option<String>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(&job_query)
        .bind(job_id)
        .fetch_optional(pool)
        .await
        .ok()?;

    let job_row = job_row?;

    // Get dependencies (jobs this job depends on)
    let deps_query = format!(
        r#"
        SELECT dep_job.id, dep_job.worker, dep_job.state::text
        FROM {} d
        JOIN {} dep_job ON d.depends_on_job_id = dep_job.id
        WHERE d.job_id = $1
        ORDER BY dep_job.id
        "#,
        deps_table, jobs_table
    );

    let dependencies: Vec<(i64, String, String)> = sqlx::query_as(&deps_query)
        .bind(job_id)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    // Get dependents (jobs that depend on this job)
    let dependents_query = format!(
        r#"
        SELECT j.id, j.worker, j.state::text
        FROM {} d
        JOIN {} j ON d.job_id = j.id
        WHERE d.depends_on_job_id = $1
        ORDER BY j.id
        "#,
        deps_table, jobs_table
    );

    let dependents: Vec<(i64, String, String)> = sqlx::query_as(&dependents_query)
        .bind(job_id)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    Some(JobWithDependencies {
        id: job_row.0,
        worker: job_row.1,
        state: job_row.2,
        queue: job_row.3,
        attempt: job_row.4,
        max_attempts: job_row.5,
        workflow_id: job_row.6,
        workflow_name: job_row.7,
        inserted_at: job_row.8,
        dependencies: dependencies
            .into_iter()
            .map(|(id, worker, state)| DependencyTarget {
                job_id: id,
                worker,
                state,
            })
            .collect(),
        dependents: dependents
            .into_iter()
            .map(|(id, worker, state)| DependencyTarget {
                job_id: id,
                worker,
                state,
            })
            .collect(),
    })
}
