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

    /// Record a successful run, and in the same transaction release the jobs that were waiting
    /// only on it (and complete its workflow when it was the last step). False when the job had
    /// moved on.
    pub(crate) async fn complete(&self, job: &Job) -> Result<bool, sqlx::Error> {
        let jobs = self.name("ishikari_jobs");
        let mut tx = self.pool.begin().await?;
        let done: Option<Option<i64>> = sqlx::query_scalar(&format!(
            "UPDATE {jobs} SET state = 'completed', completed_at = now(), {release} \
             WHERE id = $1 AND state = 'executing' AND attempt = $2 RETURNING workflow_id",
            release = release("'completed'", ""),
        ))
        .bind(job.id)
        .bind(job.attempt)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(workflow_id) = done else {
            return Ok(false);
        };
        self.after_completed(&mut tx, job.id, workflow_id).await?;
        tx.commit().await?;
        Ok(true)
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
        let mut tx = self.pool.begin().await?;
        let done: Option<(String, Option<i64>)> = sqlx::query_as(&format!(
            r#"
            UPDATE {jobs}
            SET state = CASE WHEN attempt >= max_attempts THEN 'discarded' ELSE 'retryable' END
                          ::{state},
                discarded_at = CASE WHEN attempt >= max_attempts THEN now() END,
                {release},
                errors = errors || $3::jsonb,
                scheduled_at = CASE WHEN attempt >= max_attempts THEN scheduled_at ELSE $4 END
            WHERE id = $1 AND state = 'executing' AND attempt = $2
            RETURNING state::text, workflow_id
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
        .fetch_optional(&mut *tx)
        .await?;
        let Some((state, workflow_id)) = done else {
            return Ok(false);
        };
        if state == "discarded" {
            self.after_failed(&mut tx, job.id, workflow_id).await?;
        }
        tx.commit().await?;
        Ok(true)
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
        let mut tx = self.pool.begin().await?;
        let done: Option<Option<i64>> = sqlx::query_scalar(&format!(
            "UPDATE {jobs} SET state = 'cancelled', cancelled_at = now(), {release}, \
                 errors = errors || $3::jsonb[] \
             WHERE id = $1 AND state = 'executing' AND attempt = $2 RETURNING workflow_id",
            release = release("'cancelled'", ""),
        ))
        .bind(job.id)
        .bind(job.attempt)
        .bind(reason)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(workflow_id) = done else {
            return Ok(false);
        };
        self.after_failed(&mut tx, job.id, workflow_id).await?;
        tx.commit().await?;
        Ok(true)
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

    /// After `id` completed: mark what depended on it satisfied, make available the dependents
    /// with nothing left to wait for, and complete the workflow when every job in it has. The
    /// dependents are locked first, so two upstreams finishing at once can't both leave the
    /// shared dependent waiting; the workflow is locked before its check for the same reason.
    async fn after_completed(
        &self,
        tx: &mut sqlx::PgConnection,
        id: i64,
        workflow_id: Option<i64>,
    ) -> Result<(), sqlx::Error> {
        let (jobs, deps) = (
            self.name("ishikari_jobs"),
            self.name("ishikari_job_dependencies"),
        );
        let dependents: Vec<i64> = sqlx::query_scalar(&format!(
            "UPDATE {deps} SET state = 'satisfied' \
             WHERE depends_on_job_id = $1 AND state = 'pending' RETURNING job_id"
        ))
        .bind(id)
        .fetch_all(&mut *tx)
        .await?;
        if !dependents.is_empty() {
            sqlx::query(&format!(
                "SELECT id FROM {jobs} WHERE id = ANY($1) ORDER BY id FOR UPDATE"
            ))
            .bind(&dependents)
            .execute(&mut *tx)
            .await?;
            let released = sqlx::query(&format!(
                "UPDATE {jobs} AS j SET state = 'available', scheduled_at = now(), {release} \
                 WHERE j.id = ANY($1) AND j.state IN ('pending', 'scheduled') \
                   AND NOT EXISTS (SELECT 1 FROM {deps} d \
                                   WHERE d.job_id = j.id AND d.state <> 'satisfied')",
                release = release("'available'", "j."),
            ))
            .bind(&dependents)
            .execute(&mut *tx)
            .await?;
            if released.rows_affected() > 0 {
                tracing::debug!(
                    id,
                    released = released.rows_affected(),
                    "released dependents"
                );
            }
        }
        if let Some(workflow_id) = workflow_id {
            let workflows = self.name("ishikari_workflows");
            // Taken last, after the job rows. The last steps' completions wait here for each
            // other, so the last one's check sees every other step completed.
            sqlx::query(&format!(
                "SELECT 1 FROM {workflows} WHERE id = $1 FOR NO KEY UPDATE"
            ))
            .bind(workflow_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query(&format!(
                "UPDATE {workflows} SET state = 'completed', completed_at = now() \
                 WHERE id = $1 AND state = 'running' \
                   AND NOT EXISTS (SELECT 1 FROM {jobs} WHERE workflow_id = $1 AND state <> 'completed')"
            ))
            .bind(workflow_id)
            .execute(&mut *tx)
            .await?;
        }
        Ok(())
    }

    /// After `id` was discarded or cancelled: everything that depends on it, directly or not,
    /// can never run, so its dependencies are marked failed and those jobs cancelled. A workflow
    /// that isn't a saga is marked failed (a saga's compensation is the resolver's).
    async fn after_failed(
        &self,
        tx: &mut sqlx::PgConnection,
        id: i64,
        workflow_id: Option<i64>,
    ) -> Result<(), sqlx::Error> {
        let (jobs, deps) = (
            self.name("ishikari_jobs"),
            self.name("ishikari_job_dependencies"),
        );
        let downstream: Vec<i64> = sqlx::query_scalar(&format!(
            r#"
            WITH RECURSIVE down(id) AS (
                SELECT job_id FROM {deps} WHERE depends_on_job_id = $1
                UNION
                SELECT d.job_id FROM {deps} d JOIN down ON d.depends_on_job_id = down.id
            )
            SELECT id FROM down ORDER BY id
            "#
        ))
        .bind(id)
        .fetch_all(&mut *tx)
        .await?;
        if !downstream.is_empty() {
            let mut failed = downstream.clone();
            failed.push(id);
            sqlx::query(&format!(
                "UPDATE {deps} SET state = 'failed' \
                 WHERE depends_on_job_id = ANY($1) AND state = 'pending'"
            ))
            .bind(&failed)
            .execute(&mut *tx)
            .await?;
            sqlx::query(&format!(
                "UPDATE {jobs} SET state = 'cancelled', cancelled_at = now(), {release} \
                 WHERE id = ANY($1) AND state IN ('pending', 'scheduled')",
                release = release("'cancelled'", ""),
            ))
            .bind(&downstream)
            .execute(&mut *tx)
            .await?;
        }
        if let Some(workflow_id) = workflow_id {
            sqlx::query(&format!(
                "UPDATE {workflows} SET state = 'failed', completed_at = now() \
                 WHERE id = $1 AND state = 'running' \
                   AND NOT EXISTS (SELECT 1 FROM {saga} WHERE workflow_id = $1)",
                workflows = self.name("ishikari_workflows"),
                saga = self.name("ishikari_saga_steps"),
            ))
            .bind(workflow_id)
            .execute(&mut *tx)
            .await?;
        }
        Ok(())
    }

    /// Run a job again as soon as it can: a discarded, cancelled or retryable job starts over
    /// (its attempts reset, its errors kept), an executing one is given back (the interrupted
    /// attempt refunded; if its run is still going, its result is dropped), and a scheduled one
    /// runs now. A workflow step whose upstreams haven't all completed waits for them again,
    /// the steps its failure cancelled wait again too, and its workflow is running again.
    /// Returns false when the job is pending, available, completed or missing.
    pub async fn retry_job(&self, id: i64) -> Result<bool, sqlx::Error> {
        let (jobs, deps) = (
            self.name("ishikari_jobs"),
            self.name("ishikari_job_dependencies"),
        );
        let mut tx = self.pool.begin().await?;
        let found: Option<Option<i64>> = sqlx::query_scalar(&format!(
            r#"
            UPDATE {jobs} AS j
            SET state = CASE WHEN EXISTS (SELECT 1 FROM {deps} d
                                          WHERE d.job_id = j.id AND d.state <> 'satisfied')
                             THEN 'pending' ELSE 'available' END::{state},
                attempt = CASE j.state WHEN 'executing' THEN j.attempt - 1
                                       WHEN 'scheduled' THEN j.attempt
                                       ELSE 0 END,
                scheduled_at = now(),
                discarded_at = NULL,
                cancelled_at = NULL
            WHERE j.id = $1 AND j.state IN ('discarded', 'cancelled', 'retryable', 'executing', 'scheduled')
            RETURNING j.workflow_id
            "#,
            state = self.name("ishikari_job_state"),
        ))
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(workflow_id) = found else {
            return Ok(false);
        };
        // What its failure cancelled waits again.
        let downstream: Vec<i64> = sqlx::query_scalar(&format!(
            r#"
            WITH RECURSIVE down(id) AS (
                SELECT job_id FROM {deps} WHERE depends_on_job_id = $1
                UNION
                SELECT d.job_id FROM {deps} d JOIN down ON d.depends_on_job_id = down.id
            )
            SELECT id FROM down
            "#
        ))
        .bind(id)
        .fetch_all(&mut *tx)
        .await?;
        if !downstream.is_empty() {
            let mut upstreams = downstream.clone();
            upstreams.push(id);
            sqlx::query(&format!(
                "UPDATE {deps} SET state = 'pending' \
                 WHERE depends_on_job_id = ANY($1) AND state = 'failed'"
            ))
            .bind(&upstreams)
            .execute(&mut *tx)
            .await?;
            sqlx::query(&format!(
                "UPDATE {jobs} SET state = 'pending', cancelled_at = NULL \
                 WHERE id = ANY($1) AND state = 'cancelled'"
            ))
            .bind(&downstream)
            .execute(&mut *tx)
            .await?;
        }
        if let Some(workflow_id) = workflow_id {
            sqlx::query(&format!(
                "UPDATE {workflows} SET state = 'running', completed_at = NULL \
                 WHERE id = $1 AND state IN ('failed', 'cancelled')",
                workflows = self.name("ishikari_workflows"),
            ))
            .bind(workflow_id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(true)
    }

    /// Cancel a job that hasn't finished (waiting, available, scheduled, retryable or
    /// executing; an executing run's result is dropped), and everything that depends on it.
    /// Returns false when the job had already finished or is missing.
    pub async fn cancel_job(&self, id: i64, reason: Option<&str>) -> Result<bool, sqlx::Error> {
        let jobs = self.name("ishikari_jobs");
        let mut tx = self.pool.begin().await?;
        let found: Option<(i32, Option<i64>)> = sqlx::query_as(&format!(
            "SELECT attempt, workflow_id FROM {jobs} WHERE id = $1 \
               AND state IN ('pending', 'available', 'scheduled', 'retryable', 'executing') \
             FOR UPDATE"
        ))
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((attempt, workflow_id)) = found else {
            return Ok(false);
        };
        let reason: Vec<serde_json::Value> = reason
            .map(|r| error_record(attempt, ErrorKind::Cancelled, r))
            .into_iter()
            .collect();
        sqlx::query(&format!(
            "UPDATE {jobs} SET state = 'cancelled', cancelled_at = now(), {release}, \
                 errors = errors || $2::jsonb[] \
             WHERE id = $1",
            release = release("'cancelled'", ""),
        ))
        .bind(id)
        .bind(reason)
        .execute(&mut *tx)
        .await?;
        self.after_failed(&mut tx, id, workflow_id).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Delete up to `limit` finished jobs (completed, cancelled or discarded) older than
    /// `older_than`, keeping jobs whose workflow hasn't finished, and remove their dependency
    /// rows. Then delete finished workflows left without jobs. Returns how many jobs were
    /// deleted.
    pub(crate) async fn prune(
        &self,
        older_than: std::time::Duration,
        limit: i64,
    ) -> Result<usize, sqlx::Error> {
        let (jobs, deps, workflows) = (
            self.name("ishikari_jobs"),
            self.name("ishikari_job_dependencies"),
            self.name("ishikari_workflows"),
        );
        let mut tx = self.pool.begin().await?;
        let deleted: Vec<i64> = sqlx::query_scalar(&format!(
            r#"
            WITH doomed AS (
                SELECT j.id FROM {jobs} j
                WHERE j.state IN ('completed', 'cancelled', 'discarded')
                  AND coalesce(j.completed_at, j.cancelled_at, j.discarded_at, j.inserted_at)
                      < now() - $1 * interval '1 millisecond'
                  AND (j.workflow_id IS NULL OR EXISTS (
                        SELECT 1 FROM {workflows} w
                        WHERE w.id = j.workflow_id
                          AND w.state IN ('completed', 'failed', 'cancelled', 'compensated')))
                ORDER BY j.id
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            DELETE FROM {jobs} WHERE id IN (SELECT id FROM doomed) RETURNING id
            "#
        ))
        .bind(older_than.as_millis() as i64)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await?;
        if !deleted.is_empty() {
            sqlx::query(&format!(
                "DELETE FROM {deps} WHERE job_id = ANY($1) OR depends_on_job_id = ANY($1)"
            ))
            .bind(&deleted)
            .execute(&mut *tx)
            .await?;
            sqlx::query(&format!(
                "DELETE FROM {workflows} w \
                 WHERE w.state IN ('completed', 'failed', 'cancelled', 'compensated') \
                   AND NOT EXISTS (SELECT 1 FROM {jobs} j WHERE j.workflow_id = w.id)"
            ))
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(deleted.len())
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
                  -- A workflow step from before the pending state waits for its dependencies.
                  AND NOT EXISTS (
                      SELECT 1 FROM {deps} d
                      WHERE d.job_id = {jobs}.id AND d.state <> 'satisfied')
                ORDER BY scheduled_at, id
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE {jobs} AS j
            SET state = 'available', {release}
            FROM due
            WHERE j.id = due.id
            "#,
            deps = self.name("ishikari_job_dependencies"),
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
