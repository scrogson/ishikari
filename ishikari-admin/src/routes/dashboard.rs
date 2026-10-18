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
        base_path: state.base_path.to_string(),
        nav_items: state.nav_items.to_vec(),
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

/// Job statistics by state. `total` counts every job, [`pending`](Self::pending) ones included.
///
/// A state's count is exact up to [`EXACT_COUNT_LIMIT`] jobs and the planner's estimate past it,
/// so that counting stays cheap on a table of millions of finished jobs.
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

impl JobStats {
    /// Workflow steps waiting on their upstreams: the jobs in none of the other states.
    ///
    /// Counted from `total` rather than kept as a field, so that the struct (which callers can
    /// build) keeps its 0.2.0 fields.
    pub fn pending(&self) -> i64 {
        let others = self.available
            + self.scheduled
            + self.executing
            + self.retryable
            + self.completed
            + self.discarded
            + self.cancelled;
        (self.total - others).max(0)
    }
}

/// Get job statistics, all from one snapshot (so `total` minus the rest is the pending jobs).
pub(crate) async fn get_stats(pool: &PgPool, schema: Option<&str>) -> JobStats {
    let mut stats = JobStats::default();
    for (state, count) in count_by_state(pool, schema, None).await {
        match state.as_str() {
            "available" => stats.available = count,
            "scheduled" => stats.scheduled = count,
            "executing" => stats.executing = count,
            "retryable" => stats.retryable = count,
            "completed" => stats.completed = count,
            "discarded" => stats.discarded = count,
            "cancelled" => stats.cancelled = count,
            _ => {}
        }
        stats.total += count;
    }
    stats
}

/// Up to this many jobs, a count is exact. Past it, it's the planner's estimate, shown as one
/// (see [`approx_count`]): counting more would read every job.
pub const EXACT_COUNT_LIMIT: i64 = 10_000;

/// A count as the pages show it: exact up to [`EXACT_COUNT_LIMIT`], else rounded and marked
/// as an estimate (`≈12k`, `≈1.2M`).
pub fn approx_count(n: i64) -> String {
    let f = n as f64;
    if n <= EXACT_COUNT_LIMIT {
        n.to_string()
    } else if f < 999_500.0 {
        format!("≈{:.0}k", f / 1e3)
    } else if f < 999_950_000.0 {
        format!("≈{:.1}M", f / 1e6)
    } else {
        format!("≈{:.1}B", f / 1e9)
    }
}

/// The jobs table in `schema`.
pub(crate) fn jobs_table(schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("{s}.ishikari_jobs"),
        None => "ishikari_jobs".to_string(),
    }
}

/// The job state type in `schema`. Comparing `state` with `$n::` it (not `state::text` with
/// `$n`) lets the state index serve the query.
pub(crate) fn state_type(schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("{s}.ishikari_job_state"),
        None => "ishikari_job_state".to_string(),
    }
}

/// The jobs in each state, of `queue` if given: exact up to [`EXACT_COUNT_LIMIT`], estimated
/// past it. Each count reads at most that many entries of the state index.
pub(crate) async fn count_by_state(
    pool: &PgPool,
    schema: Option<&str>,
    queue: Option<&str>,
) -> Vec<(String, i64)> {
    let jobs = jobs_table(schema);
    let state_type = state_type(schema);
    let in_queue = if queue.is_some() {
        " AND j.queue = $1"
    } else {
        ""
    };
    let query = format!(
        r#"
        SELECT s.state::text,
            (SELECT count(*) FROM (
                SELECT 1 FROM {jobs} j WHERE j.state = s.state{in_queue} LIMIT {limit}
            ) c)
        FROM unnest(enum_range(NULL::{state_type})) AS s(state)
        "#,
        limit = EXACT_COUNT_LIMIT + 1
    );
    let mut counts = sqlx::query_as::<_, (String, i64)>(&query);
    if let Some(queue) = queue {
        counts = counts.bind(queue);
    }
    let mut counts = counts.fetch_all(pool).await.unwrap_or_else(|e| {
        tracing::error!(error = %e, "counting jobs by state");
        Vec::new()
    });

    let in_queue = if queue.is_some() {
        " AND j.queue = $2"
    } else {
        ""
    };
    let select = format!("SELECT 1 FROM {jobs} j WHERE j.state = $1::{state_type}{in_queue}");
    for (state, count) in counts.iter_mut().filter(|(_, n)| *n > EXACT_COUNT_LIMIT) {
        let binds: Vec<&str> = std::iter::once(state.as_str()).chain(queue).collect();
        if let Some(rows) = estimate_rows(pool, &select, &binds).await {
            *count = rows.max(*count);
        }
    }
    counts
}

/// The rows of `from` (a `FROM ... WHERE ...` clause with `binds` for its `$n`): exact up to
/// [`EXACT_COUNT_LIMIT`], estimated past it.
pub(crate) async fn count_rows(pool: &PgPool, from: &str, binds: &[&str]) -> i64 {
    let query = format!(
        "SELECT count(*) FROM (SELECT 1 {from} LIMIT {}) c",
        EXACT_COUNT_LIMIT + 1
    );
    let mut count = sqlx::query_scalar::<_, i64>(&query);
    for b in binds {
        count = count.bind(*b);
    }
    let count = count.fetch_one(pool).await.unwrap_or_else(|e| {
        tracing::error!(error = %e, "counting jobs");
        0
    });
    if count <= EXACT_COUNT_LIMIT {
        return count;
    }
    let estimate = estimate_rows(pool, &format!("SELECT 1 {from}"), binds).await;
    estimate.map_or(count, |rows| rows.max(count))
}

/// The planner's estimate of the rows `select` returns, with `binds` for its `$n`.
pub(crate) async fn estimate_rows(pool: &PgPool, select: &str, binds: &[&str]) -> Option<i64> {
    let explain = format!("EXPLAIN (FORMAT JSON) {select}");
    let mut plan = sqlx::query_scalar::<_, serde_json::Value>(&explain);
    for b in binds {
        plan = plan.bind(*b);
    }
    let plan = plan
        .fetch_one(pool)
        .await
        .map_err(|e| tracing::error!(error = %e, "estimating a count"))
        .ok()?;
    plan[0]["Plan"]["Plan Rows"]
        .as_f64()
        .map(|rows| rows as i64)
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

/// How many of the newest discarded and of the newest retryable jobs the recent failures are
/// taken from.
const RECENT_FAILURES_SCAN: i64 = 1_000;

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

    // The latest failures of each state by id, not every one of them sorted by time: a job
    // fails after it is inserted, so the most recent failures are among the newest jobs.
    let latest = |state: &str| {
        format!(
            "SELECT id, worker, queue, errors, COALESCE(discarded_at, attempted_at, inserted_at) AS failed_at
             FROM {table} WHERE state = '{state}' ORDER BY id DESC LIMIT {RECENT_FAILURES_SCAN}"
        )
    };
    let query = format!(
        r#"
        SELECT id, worker, queue, errors[cardinality(errors)]->>'error' as error, failed_at
        FROM (({}) UNION ALL ({})) recent
        ORDER BY failed_at DESC
        LIMIT $1
        "#,
        latest("discarded"),
        latest("retryable")
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
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "reading recent failures");
            Vec::new()
        });

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
