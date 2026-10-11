//! Job storage in Postgres.
//!
//! Every statement goes through [`Postgres`], which knows the schema the jobs live in, so an
//! engine configured with a schema never writes anywhere else.
//!
//! Writes that record a job's outcome are fenced on the attempt they belong to:
//! `WHERE id = $1 AND state = 'executing' AND attempt = $2`. A run whose job was rescued,
//! cancelled or retried in the meantime finds no row to update, and its result is dropped
//! rather than overwriting the newer state.

use crate::Job;
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// Postgres-backed job storage, optionally in a schema other than the connection's default.
#[derive(Clone, Debug)]
pub struct Postgres {
    pub pool: Arc<sqlx::PgPool>,
    schema: Option<String>,
}

impl Postgres {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool: Arc::new(pool),
            schema: None,
        }
    }

    /// Keep jobs in `schema` rather than the connection's search path.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// The schema jobs are kept in, when one is set.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// A table or type name, qualified with the schema when one is set.
    pub(crate) fn name(&self, name: &str) -> String {
        qualified(self.schema.as_deref(), name)
    }

    /// Mark up to `limit` available jobs in `queue` as executing by `node` and return them.
    pub(crate) async fn claim(
        &self,
        queue: &str,
        node: &str,
        limit: i32,
    ) -> Result<Vec<Job>, sqlx::Error> {
        if limit <= 0 {
            return Ok(vec![]);
        }
        let jobs = self.name("ishikari_jobs");
        sqlx::query_as::<_, Job>(&format!(
            r#"
            WITH subset AS (
                SELECT id
                FROM {jobs}
                WHERE state = 'available'
                  AND queue = $1
                  AND attempt < max_attempts
                ORDER BY priority ASC, scheduled_at ASC, id ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {jobs} AS j
            SET state = 'executing',
                {release_executing},
                attempted_at = now(),
                attempted_by = array_append(coalesce(j.attempted_by, ARRAY[]::text[]), $3),
                attempt = j.attempt + 1
            FROM subset
            WHERE j.id = subset.id
            RETURNING j.*
            "#,
            release_executing = release("'executing'", "j."),
        ))
        .bind(queue)
        .bind(limit)
        .bind(node)
        .fetch_all(&*self.pool)
        .await
    }

    /// Record a successful run. False when the job had moved on.
    pub(crate) async fn complete(&self, job: &Job) -> Result<bool, sqlx::Error> {
        let jobs = self.name("ishikari_jobs");
        let done = sqlx::query(&format!(
            "UPDATE {jobs} SET state = 'completed', completed_at = now(), {release} \
             WHERE id = $1 AND state = 'executing' AND attempt = $2",
            release = release("'completed'", ""),
        ))
        .bind(job.id)
        .bind(job.attempt)
        .execute(&*self.pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Record a failed run: retryable at `retry_at`, or discarded when it was the last attempt.
    /// One statement, so a crash can't leave a job half-failed. False when the job had moved
    /// on.
    pub(crate) async fn error(
        &self,
        job: &Job,
        kind: ErrorKind,
        error: &str,
        retry_at: DateTime<Utc>,
    ) -> Result<bool, sqlx::Error> {
        let jobs = self.name("ishikari_jobs");
        let done = sqlx::query(&format!(
            r#"
            UPDATE {jobs}
            SET state = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END
                          ::{state},
                discarded_at = CASE WHEN attempt >= max_attempts THEN now() END,
                {release},
                errors = errors || $3::jsonb,
                scheduled_at = CASE WHEN attempt >= max_attempts THEN scheduled_at ELSE $4 END
            WHERE id = $1 AND state = 'executing' AND attempt = $2
            "#,
            state = self.name("ishikari_job_state"),
            release = release(
                "CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END",
                ""
            ),
        ))
        .bind(job.id)
        .bind(job.attempt)
        .bind(error_record(job.attempt, kind, error))
        .bind(retry_at)
        .execute(&*self.pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Record that the run cancelled its job. False when the job had moved on.
    pub(crate) async fn cancel(
        &self,
        job: &Job,
        reason: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        let jobs = self.name("ishikari_jobs");
        let reason: Vec<serde_json::Value> = reason
            .map(|r| error_record(job.attempt, ErrorKind::Cancelled, r))
            .into_iter()
            .collect();
        let done = sqlx::query(&format!(
            "UPDATE {jobs} SET state = 'cancelled', cancelled_at = now(), {release}, \
                 errors = errors || $3::jsonb[] \
             WHERE id = $1 AND state = 'executing' AND attempt = $2",
            release = release("'cancelled'", ""),
        ))
        .bind(job.id)
        .bind(job.attempt)
        .bind(reason)
        .execute(&*self.pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Put the job back to run again in `seconds`, without using up an attempt. False when the
    /// job had moved on.
    pub(crate) async fn snooze(&self, job: &Job, seconds: u64) -> Result<bool, sqlx::Error> {
        let jobs = self.name("ishikari_jobs");
        let done = sqlx::query(&format!(
            "UPDATE {jobs} SET state = 'scheduled', {release}, \
                 scheduled_at = now() + $3 * interval '1 second', max_attempts = max_attempts + 1 \
             WHERE id = $1 AND state = 'executing' AND attempt = $2",
            release = release("'scheduled'", ""),
        ))
        .bind(job.id)
        .bind(job.attempt)
        .bind(seconds as i64)
        .execute(&*self.pool)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    /// Make up to `limit` scheduled and retryable jobs that are due available. Returns how many.
    pub(crate) async fn stage(&self, limit: i32) -> Result<usize, sqlx::Error> {
        let jobs = self.name("ishikari_jobs");
        let staged = sqlx::query(&format!(
            r#"
            WITH due AS (
                SELECT id
                FROM {jobs}
                WHERE state IN ('scheduled', 'retryable')
                  AND scheduled_at <= now()
                ORDER BY scheduled_at, id
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {jobs} AS j
            SET state = 'available', {release}
            FROM due
            WHERE j.id = due.id
            "#,
            release = release("'available'", "j."),
        ))
        .bind(limit)
        .execute(&*self.pool)
        .await?;
        Ok(staged.rows_affected() as usize)
    }
}

impl Postgres {
    /// Record that `node` is alive and which queues it runs.
    pub(crate) async fn heartbeat(&self, node: &str, queues: &[String]) -> Result<(), sqlx::Error> {
        let nodes = self.name("ishikari_nodes");
        sqlx::query(&format!(
            "INSERT INTO {nodes} (name, queues) VALUES ($1, $2) \
             ON CONFLICT (name) DO UPDATE SET heartbeat_at = now(), queues = EXCLUDED.queues"
        ))
        .bind(node)
        .bind(queues)
        .execute(&*self.pool)
        .await?;
        Ok(())
    }

    /// Put interrupted jobs, given as (id, attempt), back to available with the attempt
    /// refunded. Jobs that moved on meanwhile are left alone. Returns how many were put back.
    pub(crate) async fn give_back(&self, jobs: &[(i64, i32)]) -> Result<usize, sqlx::Error> {
        let table = self.name("ishikari_jobs");
        let (ids, attempts): (Vec<i64>, Vec<i32>) = jobs.iter().copied().unzip();
        let done = sqlx::query(&format!(
            "UPDATE {table} AS j SET state = 'available', attempt = j.attempt - 1, {release} \
             FROM unnest($1::bigint[], $2::int[]) AS r(id, attempt) \
             WHERE j.id = r.id AND j.state = 'executing' AND j.attempt = r.attempt",
            release = release("'available'", "j."),
        ))
        .bind(ids)
        .bind(attempts)
        .execute(&*self.pool)
        .await?;
        Ok(done.rows_affected() as usize)
    }

    /// Remove `node`'s row (it is shutting down), and rows of nodes silent for a day.
    pub(crate) async fn forget_node(&self, node: &str) -> Result<(), sqlx::Error> {
        let nodes = self.name("ishikari_nodes");
        sqlx::query(&format!(
            "DELETE FROM {nodes} WHERE name = $1 OR heartbeat_at < now() - interval '1 day'"
        ))
        .bind(node)
        .execute(&*self.pool)
        .await?;
        Ok(())
    }

    /// Give back jobs whose node stopped: executing for longer than `node_timeout` on a node
    /// that hasn't heartbeated within it (or isn't known), or claimed by a node that didn't
    /// record itself (an older ishikari) more than `unowned_after` ago. Each becomes retryable
    /// with a "rescued" error, or discarded when it has no attempts left. Returns the rescued
    /// jobs' ids with the node they were taken from.
    pub(crate) async fn rescue(
        &self,
        node_timeout: std::time::Duration,
        unowned_after: std::time::Duration,
    ) -> Result<Vec<(i64, String)>, sqlx::Error> {
        let jobs = self.name("ishikari_jobs");
        let nodes = self.name("ishikari_nodes");
        sqlx::query_as(&format!(
            r#"
            WITH lost AS (
                SELECT j.id, coalesce(j.attempted_by[cardinality(j.attempted_by)], 'unknown') AS node
                FROM {jobs} j
                WHERE j.state = 'executing'
                  AND (
                    (j.attempted_by IS NOT NULL
                     AND j.attempted_at < now() - $1 * interval '1 millisecond'
                     AND NOT EXISTS (
                        SELECT 1 FROM {nodes} n
                        WHERE n.name = j.attempted_by[cardinality(j.attempted_by)]
                          AND n.heartbeat_at > now() - $1 * interval '1 millisecond'))
                    OR (j.attempted_by IS NULL
                        AND j.attempted_at < now() - $2 * interval '1 millisecond')
                  )
                FOR UPDATE OF j SKIP LOCKED
            )
            UPDATE {jobs} AS j
            SET state = CASE WHEN j.attempt >= j.max_attempts THEN 'discarded' ELSE 'retryable' END
                          ::{state},
                discarded_at = CASE WHEN j.attempt >= j.max_attempts THEN now() END,
                {release},
                scheduled_at = now(),
                errors = j.errors || jsonb_build_object(
                    'attempt', j.attempt,
                    'at', now(),
                    'kind', 'rescued',
                    'error', 'rescued: node ' || lost.node || ' stopped heartbeating')
            FROM lost
            WHERE j.id = lost.id
            RETURNING j.id, lost.node
            "#,
            state = self.name("ishikari_job_state"),
            release = release(
                "CASE WHEN j.attempt >= j.max_attempts THEN 'discarded' ELSE 'retryable' END",
                "j."
            ),
        ))
        .bind(node_timeout.as_millis() as i64)
        .bind(unowned_after.as_millis() as i64)
        .fetch_all(&*self.pool)
        .await
    }
}

/// Why an attempt ended without completing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// The worker returned an error.
    Error,
    /// The worker panicked.
    Panic,
    /// The worker ran past its timeout.
    Timeout,
    /// The job's node stopped and it was given back.
    Rescued,
    /// The worker cancelled the job.
    Cancelled,
}

impl ErrorKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorKind::Error => "error",
            ErrorKind::Panic => "panic",
            ErrorKind::Timeout => "timeout",
            ErrorKind::Rescued => "rescued",
            ErrorKind::Cancelled => "cancelled",
        }
    }
}

/// An entry of a job's `errors`: `{attempt, at, kind, error}`.
pub(crate) fn error_record(attempt: i32, kind: ErrorKind, error: &str) -> serde_json::Value {
    serde_json::json!({
        "attempt": attempt,
        "at": Utc::now(),
        "kind": kind.as_str(),
        "error": error,
    })
}

impl From<sqlx::PgPool> for Postgres {
    fn from(pool: sqlx::PgPool) -> Self {
        Self::new(pool)
    }
}

impl From<Arc<sqlx::PgPool>> for Postgres {
    fn from(pool: Arc<sqlx::PgPool>) -> Self {
        Self { pool, schema: None }
    }
}

/// `unique_key = …`: keep the job's unique key when `new_state` (an SQL text expression, using
/// the row's old values) is one of its unique states, release it otherwise. `p` qualifies the
/// columns (`"j."` in an aliased update).
fn release(new_state: &str, p: &str) -> String {
    format!(
        "unique_key = CASE WHEN ({new_state})::text = ANY({p}unique_states::text[]) \
         THEN {p}unique_key END"
    )
}

/// `schema.name`, or `name` when no schema is set.
pub(crate) fn qualified(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    }
}
