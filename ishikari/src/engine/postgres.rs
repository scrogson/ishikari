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

    /// Mark up to `limit` available jobs in `queue` as executing and return them.
    pub(crate) async fn claim(&self, queue: &str, limit: i32) -> Result<Vec<Job>, sqlx::Error> {
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
                attempted_at = now(),
                attempt = j.attempt + 1
            FROM subset
            WHERE j.id = subset.id
            RETURNING j.*
            "#
        ))
        .bind(queue)
        .bind(limit)
        .fetch_all(&*self.pool)
        .await
    }

    /// Record a successful run. False when the job had moved on.
    pub(crate) async fn complete(&self, job: &Job) -> Result<bool, sqlx::Error> {
        let jobs = self.name("ishikari_jobs");
        let done = sqlx::query(&format!(
            "UPDATE {jobs} SET state = 'completed', completed_at = now() \
             WHERE id = $1 AND state = 'executing' AND attempt = $2"
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
                errors = errors || $3::jsonb,
                scheduled_at = CASE WHEN attempt >= max_attempts THEN scheduled_at ELSE $4 END
            WHERE id = $1 AND state = 'executing' AND attempt = $2
            "#,
            state = self.name("ishikari_job_state"),
        ))
        .bind(job.id)
        .bind(job.attempt)
        .bind(serde_json::Value::String(error.to_string()))
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
            .map(|r| serde_json::Value::String(r.to_string()))
            .into_iter()
            .collect();
        let done = sqlx::query(&format!(
            "UPDATE {jobs} SET state = 'cancelled', cancelled_at = now(), errors = errors || $3::jsonb[] \
             WHERE id = $1 AND state = 'executing' AND attempt = $2"
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
            "UPDATE {jobs} SET state = 'scheduled', \
                 scheduled_at = now() + $3 * interval '1 second', max_attempts = max_attempts + 1 \
             WHERE id = $1 AND state = 'executing' AND attempt = $2"
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
            SET state = 'available'
            FROM due
            WHERE j.id = due.id
            "#
        ))
        .bind(limit)
        .execute(&*self.pool)
        .await?;
        Ok(staged.rows_affected() as usize)
    }
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

/// `schema.name`, or `name` when no schema is set.
pub(crate) fn qualified(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    }
}
