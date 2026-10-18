//! Queue route handlers.

#![allow(clippy::type_complexity)]

use axum::extract::{Path, Query, State};
use serde::Deserialize;
use sqlx::PgPool;

use super::dashboard::{count_by_state, count_rows, jobs_table, state_type};
use super::jobs::{query_string, Keyset, Pager};
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
}

/// Query parameters for queue page.
#[derive(Debug, Deserialize, Default)]
pub struct QueueQuery {
    pub state: Option<String>,
    /// The page of jobs older than this id.
    pub before: Option<i64>,
    /// The page of jobs newer than this id.
    pub after: Option<i64>,
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

impl QueueInfo {
    /// The queue's jobs counted by state (exact up to
    /// [`EXACT_COUNT_LIMIT`](super::dashboard::EXACT_COUNT_LIMIT), estimated past it).
    fn from_counts(name: String, counts: Vec<(String, i64)>) -> Self {
        let mut info = QueueInfo {
            name,
            available: 0,
            scheduled: 0,
            executing: 0,
            retryable: 0,
            completed: 0,
            discarded: 0,
            cancelled: 0,
            total: 0,
        };
        for (state, count) in counts {
            match state.as_str() {
                "available" => info.available = count,
                "scheduled" => info.scheduled = count,
                "executing" => info.executing = count,
                "retryable" => info.retryable = count,
                "completed" => info.completed = count,
                "discarded" => info.discarded = count,
                "cancelled" => info.cancelled = count,
                _ => {}
            }
            info.total += count;
        }
        info
    }

    /// The jobs in `state`, when it's one of the counted states.
    fn count(&self, state: &str) -> Option<i64> {
        match state {
            "available" => Some(self.available),
            "scheduled" => Some(self.scheduled),
            "executing" => Some(self.executing),
            "retryable" => Some(self.retryable),
            "completed" => Some(self.completed),
            "discarded" => Some(self.discarded),
            "cancelled" => Some(self.cancelled),
            _ => None,
        }
    }
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
    let schema = state.schema.as_deref();
    let stats = get_queue_stats(&state.pool, schema, &name).await;
    let current_state = query.state.filter(|s| !s.is_empty());

    let (jobs, pager) = get_queue_jobs(
        &state.pool,
        schema,
        &name,
        current_state.as_deref(),
        Keyset::new(query.before, query.after),
    )
    .await;

    let total = match current_state.as_deref() {
        None => stats.total,
        Some(s) => match stats.count(s) {
            Some(count) => count,
            None => {
                let (from, binds) = queue_jobs_from(schema, Some(s));
                let binds: Vec<&str> = std::iter::once(name.as_str()).chain(binds).collect();
                count_rows(&state.pool, &from, &binds).await
            }
        },
    };

    QueueDetailTemplate {
        base_path: state.base_path.to_string(),
        nav_items: state.nav_items.to_vec(),
        queue_name: name,
        stats,
        jobs,
        current_state,
        pager,
        total,
    }
}

/// The names of the queues that have jobs, read from the state index (a skip over each
/// state's distinct queues) rather than from every job.
pub(crate) async fn queue_names(pool: &PgPool, schema: Option<&str>) -> Vec<String> {
    let query = format!(
        r#"
        WITH RECURSIVE pairs AS (
            (SELECT state, queue FROM {jobs} ORDER BY state, queue LIMIT 1)
            UNION ALL
            SELECT next.state, next.queue
            FROM pairs, LATERAL (
                SELECT j.state, j.queue FROM {jobs} j
                WHERE (j.state, j.queue) > (pairs.state, pairs.queue)
                ORDER BY j.state, j.queue
                LIMIT 1
            ) next
        )
        SELECT DISTINCT queue FROM pairs ORDER BY queue
        "#,
        jobs = jobs_table(schema)
    );
    sqlx::query_scalar(&query)
        .fetch_all(pool)
        .await
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "listing queues");
            Vec::new()
        })
}

/// Get all queues with their statistics.
async fn get_queues(pool: &PgPool, schema: Option<&str>) -> Vec<QueueInfo> {
    let mut queues = Vec::new();
    for name in queue_names(pool, schema).await {
        let counts = count_by_state(pool, schema, Some(&name)).await;
        queues.push(QueueInfo::from_counts(name, counts));
    }
    queues
}

/// The `FROM ... WHERE ...` of a queue's jobs (the queue is `$1`), in `state` if given, and the
/// values for its other `$n`.
fn queue_jobs_from<'a>(schema: Option<&str>, state: Option<&'a str>) -> (String, Vec<&'a str>) {
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };
    let mut from = format!("FROM {} j WHERE j.queue = $1", jobs_table(schema));
    let mut binds = Vec::new();
    match state {
        // Not a job state: completed jobs whose saga step was rolled back
        Some("compensated") => from.push_str(&format!(
            " AND j.state = 'completed' AND j.id IN (SELECT s.job_id FROM {} s WHERE s.state IN {})",
            saga_steps_table,
            super::sagas::ROLLBACK_STATES_SQL
        )),
        Some(s) => {
            from.push_str(&format!(" AND j.state = $2::{}", state_type(schema)));
            binds.push(s);
        }
        None => {}
    }
    (from, binds)
}

/// A page of a queue's jobs, newest first, and the links to the others.
async fn get_queue_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    queue_name: &str,
    state_filter: Option<&str>,
    mut keyset: Keyset,
) -> (Vec<QueueJobInfo>, Pager) {
    let limit = 25;
    let filters = query_string(&[("state", state_filter.map(str::to_string))]);
    loop {
        let rows = read_queue_jobs(pool, schema, queue_name, state_filter, keyset, limit).await;
        if keyset.reached_newest(rows.len(), limit) {
            keyset = Keyset::default();
            continue;
        }
        return keyset.page(rows, limit, |j| j.id, filters);
    }
}

/// `limit + 1` of a queue's jobs, of `keyset`'s page, in its order.
async fn read_queue_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    queue_name: &str,
    state_filter: Option<&str>,
    keyset: Keyset,
    limit: i64,
) -> Vec<QueueJobInfo> {
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let (mut from, binds) = queue_jobs_from(schema, state_filter);
    let (cursor, order) = keyset.sql("j");
    if let Some(cursor) = cursor {
        from = format!("{from} AND {cursor}");
    }
    let list_query = format!(
        r#"
        SELECT j.id, j.queue, j.worker, j.state::text, j.attempt, j.max_attempts,
               j.inserted_at, j.scheduled_at, j.attempted_at, j.completed_at,
               saga.state as saga_step_state
        FROM (
            SELECT j.id, j.queue, j.worker, j.state, j.attempt, j.max_attempts, j.inserted_at,
                j.scheduled_at, j.attempted_at, j.completed_at, j.workflow_id
            {from}
            ORDER BY j.id {order}
            LIMIT {limit}
        ) j
        LEFT JOIN LATERAL (
            SELECT s.state FROM {steps} s
            WHERE s.workflow_id = j.workflow_id AND s.job_id = j.id
            LIMIT 1
        ) saga ON true
        ORDER BY j.id {order}
        "#,
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
            chrono::DateTime<chrono::Utc>,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<chrono::DateTime<chrono::Utc>>,
            Option<String>,
        ),
    >(&list_query)
    .bind(queue_name);
    for b in binds {
        list_q = list_q.bind(b);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_else(|e| {
        tracing::error!(error = %e, "listing a queue's jobs");
        Vec::new()
    });

    rows.into_iter()
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
        .collect()
}

/// Get statistics for a single queue.
async fn get_queue_stats(pool: &PgPool, schema: Option<&str>, queue_name: &str) -> QueueInfo {
    let counts = count_by_state(pool, schema, Some(queue_name)).await;
    QueueInfo::from_counts(queue_name.to_string(), counts)
}
