//! Dependency resolver status route handlers.

#![allow(clippy::type_complexity)]

use axum::extract::State;
use sqlx::PgPool;

use crate::templates::ResolverStatusTemplate;
use crate::AppState;

/// Overall resolver statistics.
#[derive(Debug, Default)]
pub struct ResolverStats {
    /// Total number of dependencies.
    pub total_dependencies: i64,
    /// Dependencies waiting to be resolved.
    pub pending_dependencies: i64,
    /// Dependencies that have been satisfied.
    pub satisfied_dependencies: i64,
    /// Dependencies that have failed.
    pub failed_dependencies: i64,
}

/// Job waiting to be released.
#[derive(Debug)]
pub struct PendingRelease {
    pub job_id: i64,
    pub worker: String,
    pub workflow_id: Option<i64>,
    pub workflow_name: Option<String>,
    pub total_deps: i64,
    pub satisfied_deps: i64,
}

impl PendingRelease {
    pub fn progress(&self) -> i64 {
        if self.total_deps == 0 {
            100
        } else {
            (self.satisfied_deps * 100) / self.total_deps
        }
    }
}

/// Job blocked by failed dependencies.
#[derive(Debug)]
pub struct BlockedJob {
    pub job_id: i64,
    pub worker: String,
    pub workflow_id: Option<i64>,
    pub workflow_name: Option<String>,
    pub pending_deps: i64,
    pub failed_deps: i64,
}

impl BlockedJob {
    pub fn is_failed(&self) -> bool {
        self.failed_deps > 0
    }
}

/// Saga currently compensating.
#[derive(Debug)]
pub struct CompensatingSaga {
    pub workflow_id: i64,
    pub workflow_name: String,
    pub total_steps: i64,
    pub compensating_steps: i64,
    pub compensated_steps: i64,
}

/// A pending dependency with details.
#[derive(Debug)]
pub struct PendingDependency {
    pub job_id: i64,
    pub depends_on_job_id: i64,
    pub job_state: Option<String>,
    pub depends_on_state: Option<String>,
}

impl CompensatingSaga {
    pub fn progress(&self) -> i64 {
        let done = self.compensated_steps;
        let total = self.compensating_steps + self.compensated_steps;
        if total == 0 {
            0
        } else {
            (done * 100) / total
        }
    }
}

/// Resolver status page.
pub async fn status(State(state): State<AppState>) -> ResolverStatusTemplate {
    let stats = get_resolver_stats(&state.pool, state.schema()).await;
    let pending_releases = get_pending_releases(&state.pool, state.schema()).await;
    let blocked_jobs = get_blocked_jobs(&state.pool, state.schema()).await;
    let compensating_sagas = get_compensating_sagas(&state.pool, state.schema()).await;
    let pending_dependencies = get_pending_dependencies(&state.pool, state.schema()).await;

    ResolverStatusTemplate {
        stats,
        pending_releases,
        blocked_jobs,
        compensating_sagas,
        pending_dependencies,
    }
}

/// Get resolver statistics.
async fn get_resolver_stats(pool: &PgPool, schema: Option<&str>) -> ResolverStats {
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE state = 'pending') as pending,
            COUNT(*) FILTER (WHERE state = 'satisfied') as satisfied,
            COUNT(*) FILTER (WHERE state = 'failed') as failed
        FROM {}
        "#,
        deps_table
    );

    let row: Option<(i64, i64, i64, i64)> = sqlx::query_as(&query)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();

    match row {
        Some((total, pending, satisfied, failed)) => ResolverStats {
            total_dependencies: total,
            pending_dependencies: pending,
            satisfied_dependencies: satisfied,
            failed_dependencies: failed,
        },
        None => ResolverStats::default(),
    }
}

/// Get jobs that are ready to be released (all deps satisfied, still scheduled).
async fn get_pending_releases(pool: &PgPool, schema: Option<&str>) -> Vec<PendingRelease> {
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

    let query = format!(
        r#"
        SELECT
            j.id,
            j.worker,
            j.workflow_id,
            w.name as workflow_name,
            COUNT(d.id) as total_deps,
            COUNT(d.id) FILTER (WHERE d.state = 'satisfied') as satisfied_deps
        FROM {} j
        INNER JOIN {} d ON d.job_id = j.id
        LEFT JOIN {} w ON w.id = j.workflow_id
        WHERE j.state = 'scheduled'
        GROUP BY j.id, w.name
        HAVING COUNT(*) FILTER (WHERE d.state = 'pending') = 0
           AND COUNT(*) FILTER (WHERE d.state = 'failed') = 0
        ORDER BY j.id
        LIMIT 50
        "#,
        jobs_table, deps_table, workflows_table
    );

    let rows: Vec<(i64, String, Option<i64>, Option<String>, i64, i64)> = sqlx::query_as(&query)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|row| PendingRelease {
            job_id: row.0,
            worker: row.1,
            workflow_id: row.2,
            workflow_name: row.3,
            total_deps: row.4,
            satisfied_deps: row.5,
        })
        .collect()
}

/// Get jobs blocked by pending or failed dependencies.
async fn get_blocked_jobs(pool: &PgPool, schema: Option<&str>) -> Vec<BlockedJob> {
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

    let query = format!(
        r#"
        SELECT
            j.id,
            j.worker,
            j.workflow_id,
            w.name as workflow_name,
            COUNT(d.id) FILTER (WHERE d.state = 'pending') as pending_deps,
            COUNT(d.id) FILTER (WHERE d.state = 'failed') as failed_deps
        FROM {} j
        INNER JOIN {} d ON d.job_id = j.id
        LEFT JOIN {} w ON w.id = j.workflow_id
        WHERE j.state = 'scheduled'
        GROUP BY j.id, w.name
        HAVING COUNT(*) FILTER (WHERE d.state = 'pending') > 0
            OR COUNT(*) FILTER (WHERE d.state = 'failed') > 0
        ORDER BY failed_deps DESC, pending_deps DESC
        LIMIT 50
        "#,
        jobs_table, deps_table, workflows_table
    );

    let rows: Vec<(i64, String, Option<i64>, Option<String>, i64, i64)> = sqlx::query_as(&query)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|row| BlockedJob {
            job_id: row.0,
            worker: row.1,
            workflow_id: row.2,
            workflow_name: row.3,
            pending_deps: row.4,
            failed_deps: row.5,
        })
        .collect()
}

/// Get all pending dependencies with job state info.
async fn get_pending_dependencies(pool: &PgPool, schema: Option<&str>) -> Vec<PendingDependency> {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            d.job_id,
            d.depends_on_job_id,
            j1.state as job_state,
            j2.state as depends_on_state
        FROM {} d
        LEFT JOIN {} j1 ON j1.id = d.job_id
        LEFT JOIN {} j2 ON j2.id = d.depends_on_job_id
        WHERE d.state = 'pending'
        ORDER BY d.job_id
        LIMIT 50
        "#,
        deps_table, jobs_table, jobs_table
    );

    let rows: Vec<(i64, i64, Option<String>, Option<String>)> = sqlx::query_as(&query)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|row| PendingDependency {
            job_id: row.0,
            depends_on_job_id: row.1,
            job_state: row.2,
            depends_on_state: row.3,
        })
        .collect()
}

/// Get sagas that are currently compensating.
async fn get_compensating_sagas(pool: &PgPool, schema: Option<&str>) -> Vec<CompensatingSaga> {
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            w.id,
            w.name,
            COUNT(s.id) as total_steps,
            COUNT(s.id) FILTER (WHERE s.state = 'compensating') as compensating_steps,
            COUNT(s.id) FILTER (WHERE s.state = 'compensated') as compensated_steps
        FROM {} w
        INNER JOIN {} s ON s.workflow_id = w.id
        WHERE w.state = 'compensating'
        GROUP BY w.id
        ORDER BY w.id DESC
        "#,
        workflows_table, saga_steps_table
    );

    let rows: Vec<(i64, String, i64, i64, i64)> = sqlx::query_as(&query)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|row| CompensatingSaga {
            workflow_id: row.0,
            workflow_name: row.1,
            total_steps: row.2,
            compensating_steps: row.3,
            compensated_steps: row.4,
        })
        .collect()
}
