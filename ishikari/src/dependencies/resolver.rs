//! Dependency resolution service.
//!
//! The `DependencyResolver` runs as a background service alongside the ishikari
//! stager. Jobs release their dependents, fail them, and move their sagas on in the
//! transaction that ends them; the resolver's sweep is the safety net for what a job
//! left behind (one rescued and discarded, or written by an older ishikari).
//!
//! # How it works
//!
//! 1. Polls for jobs that recently completed (state = 'completed')
//! 2. For each completed job, finds all dependencies where `depends_on_job_id` matches
//! 3. Marks those dependencies as 'satisfied'
//! 4. For jobs with all dependencies satisfied, updates them to 'available' state
//! 5. Also handles failed/cancelled/discarded jobs by marking dependencies as 'failed'
//! 6. Finishes workflows whose jobs have all finished, and moves on sagas whose jobs
//!    ended without moving them (see [`crate::workflows::Saga`])

use crate::workflows::{Result, Workflow};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, warn};

/// A background service that resolves job dependencies.
///
/// When a job completes, the resolver updates any jobs that were waiting on it,
/// potentially allowing them to start executing.
#[derive(Debug)]
pub struct DependencyResolver {
    pool: Arc<PgPool>,
    interval: Duration,
    schema: Option<String>,
}

impl DependencyResolver {
    /// Create a new dependency resolver.
    ///
    /// # Arguments
    ///
    /// * `pool` - The database connection pool
    /// * `interval` - How often to check for completed jobs
    ///
    /// Panics if `interval` is zero.
    #[track_caller]
    pub fn new(pool: impl Into<Arc<PgPool>>, interval: Duration) -> Self {
        Self {
            pool: pool.into(),
            interval: crate::nonzero(interval, "the resolver's interval"),
            schema: None,
        }
    }

    /// Set the schema for multi-tenant support.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Start the resolver as a background task.
    ///
    /// Returns a handle that can be used to await or abort the task.
    pub fn start(self) -> JoinHandle<()> {
        info!(interval = ?self.interval, "starting dependency resolver");
        tokio::spawn(async move {
            self.run().await;
        })
    }

    /// Run the resolver loop. Each tick holds an advisory lock (per schema) while it resolves,
    /// so with several nodes only one sweeps at a time; the others skip that tick.
    #[instrument(skip(self))]
    async fn run(self) {
        let mut interval = tokio::time::interval(self.interval);
        let key = format!(
            "ishikari:resolver:{}",
            self.schema.as_deref().unwrap_or("public")
        );

        loop {
            interval.tick().await;
            let mut lock = match self.pool.acquire().await {
                Ok(conn) => conn,
                Err(e) => {
                    error!(error = %e, "dependency resolver couldn't get a connection");
                    continue;
                }
            };
            let held: std::result::Result<bool, sqlx::Error> =
                sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext($1))")
                    .bind(&key)
                    .fetch_one(&mut *lock)
                    .await;
            match held {
                Ok(true) => {
                    if let Err(e) = self.resolve_once().await {
                        error!(error = %e, "dependency resolution failed");
                    }
                    let _ = sqlx::query("SELECT pg_advisory_unlock(hashtext($1))")
                        .bind(&key)
                        .execute(&mut *lock)
                        .await;
                }
                Ok(false) => debug!("another node is resolving dependencies"),
                Err(e) => error!(error = %e, "dependency resolver couldn't take its lock"),
            }
        }
    }

    /// Run a single resolution cycle (public for testing).
    pub async fn resolve_once_public(&self) -> Result<()> {
        self.resolve_once().await
    }

    /// Run a single resolution cycle.
    async fn resolve_once(&self) -> Result<()> {
        let schema = self.schema.as_deref();
        let jobs_table = table_name("ishikari_jobs", schema);
        let deps_table = table_name("ishikari_job_dependencies", schema);

        // Find completed jobs that have pending dependents
        let query = format!(
            r#"
            SELECT DISTINCT d.depends_on_job_id
            FROM {} d
            INNER JOIN {} j ON j.id = d.depends_on_job_id
            WHERE d.state = 'pending'
              AND j.state = 'completed'
            "#,
            deps_table, jobs_table
        );

        let completed_jobs: Vec<(i64,)> =
            sqlx::query_as(&query).fetch_all(self.pool.as_ref()).await?;

        for (completed_job_id,) in completed_jobs {
            debug!(
                job_id = completed_job_id,
                "processing completed job dependencies"
            );

            // Mark all dependencies on this job as satisfied
            let update_query = format!(
                "UPDATE {} SET state = 'satisfied' WHERE depends_on_job_id = $1 AND state = 'pending'",
                deps_table
            );

            sqlx::query(&update_query)
                .bind(completed_job_id)
                .execute(self.pool.as_ref())
                .await?;
        }

        // Find failed/cancelled/discarded jobs that have pending dependents
        let failed_query = format!(
            r#"
            SELECT DISTINCT d.depends_on_job_id
            FROM {} d
            INNER JOIN {} j ON j.id = d.depends_on_job_id
            WHERE d.state = 'pending'
              AND j.state IN ('discarded', 'cancelled')
            "#,
            deps_table, jobs_table
        );

        let failed_jobs: Vec<(i64,)> = sqlx::query_as(&failed_query)
            .fetch_all(self.pool.as_ref())
            .await?;

        for (failed_job_id,) in failed_jobs {
            debug!(job_id = failed_job_id, "processing failed job dependencies");

            // Mark all dependencies on this job as failed
            let update_query = format!(
                "UPDATE {} SET state = 'failed' WHERE depends_on_job_id = $1 AND state = 'pending'",
                deps_table
            );

            sqlx::query(&update_query)
                .bind(failed_job_id)
                .execute(self.pool.as_ref())
                .await?;
        }

        // Now find jobs that have all dependencies satisfied and are still in 'scheduled' state
        // These jobs should be moved to 'available' state
        let ready_jobs_query = format!(
            r#"
            SELECT j.id
            FROM {} j
            INNER JOIN {} d ON d.job_id = j.id
            WHERE j.state IN ('pending', 'scheduled')
            GROUP BY j.id
            HAVING COUNT(*) FILTER (WHERE d.state = 'pending') = 0
               AND COUNT(*) FILTER (WHERE d.state = 'failed') = 0
            "#,
            jobs_table, deps_table
        );

        let ready_jobs: Vec<(i64,)> = sqlx::query_as(&ready_jobs_query)
            .fetch_all(self.pool.as_ref())
            .await?;

        for (job_id,) in ready_jobs {
            info!(job_id = job_id, "releasing job with satisfied dependencies");

            let release_query = format!(
                "UPDATE {} SET state = 'available', scheduled_at = now() WHERE id = $1 AND state IN ('pending', 'scheduled')",
                jobs_table
            );

            sqlx::query(&release_query)
                .bind(job_id)
                .execute(self.pool.as_ref())
                .await?;
        }

        // Find jobs with failed dependencies - they should be cancelled
        let failed_dep_jobs_query = format!(
            r#"
            SELECT DISTINCT j.id, j.workflow_id
            FROM {} j
            INNER JOIN {} d ON d.job_id = j.id
            WHERE j.state IN ('pending', 'scheduled')
              AND d.state = 'failed'
            "#,
            jobs_table, deps_table
        );

        let failed_dep_jobs: Vec<(i64, Option<i64>)> = sqlx::query_as(&failed_dep_jobs_query)
            .fetch_all(self.pool.as_ref())
            .await?;

        for (job_id, workflow_id) in failed_dep_jobs {
            info!(job_id = job_id, "cancelling job with failed dependencies");

            let cancel_query = format!(
                "UPDATE {} SET state = 'cancelled', cancelled_at = now() WHERE id = $1 AND state IN ('pending', 'scheduled')",
                jobs_table
            );

            sqlx::query(&cancel_query)
                .bind(job_id)
                .execute(self.pool.as_ref())
                .await?;

            // If this job is part of a workflow, check if we need to fail the workflow
            if let Some(wf_id) = workflow_id {
                self.check_workflow_status(wf_id).await?;
            }
        }

        // Check for completed workflows
        self.check_completed_workflows().await?;

        // Move on sagas their jobs left behind
        self.sweep_sagas().await?;

        Ok(())
    }

    /// Move on the sagas a job left behind: one that ended without recording its step or
    /// compensation, a running saga whose steps all completed or one of which failed, and a
    /// rollback with nothing running. A saga whose step or compensation is running is left
    /// alone. Each moves on in a transaction of its own, as its job's would have moved it.
    async fn sweep_sagas(&self) -> Result<()> {
        let schema = self.schema.as_deref();
        let (jobs, steps, workflows) = (
            table_name("ishikari_jobs", schema),
            table_name("ishikari_saga_steps", schema),
            table_name("ishikari_workflows", schema),
        );
        let behind: Vec<i64> = sqlx::query_scalar(&format!(
            r#"
            SELECT w.id FROM {workflows} w
            WHERE w.state IN ('running', 'compensating')
              AND EXISTS (SELECT 1 FROM {steps} s WHERE s.workflow_id = w.id)
              AND (
                EXISTS (SELECT 1 FROM {steps} s JOIN {jobs} j ON j.id = s.job_id
                        WHERE s.workflow_id = w.id AND s.state = 'pending'
                          AND j.state IN ('completed', 'discarded', 'cancelled'))
                OR EXISTS (SELECT 1 FROM {steps} s JOIN {jobs} j ON j.id = s.compensation_job_id
                           WHERE s.workflow_id = w.id AND s.state = 'compensating'
                             AND j.state IN ('completed', 'discarded', 'cancelled'))
                OR (w.state = 'running'
                    AND NOT EXISTS (SELECT 1 FROM {steps} s
                                    WHERE s.workflow_id = w.id AND s.state <> 'completed'))
                OR (w.state = 'running'
                    AND EXISTS (SELECT 1 FROM {steps} s
                                WHERE s.workflow_id = w.id AND s.state = 'failed'))
                OR (w.state = 'compensating'
                    AND NOT EXISTS (SELECT 1 FROM {steps} s
                                    WHERE s.workflow_id = w.id
                                      AND s.state IN ('pending', 'compensating')))
              )
            ORDER BY w.id
            "#
        ))
        .fetch_all(self.pool.as_ref())
        .await?;
        if behind.is_empty() {
            return Ok(());
        }
        let storage = crate::Postgres::from(self.pool.clone());
        let storage = match schema {
            Some(schema) => storage.with_schema(schema),
            None => storage,
        };
        for workflow in behind {
            warn!(
                workflow_id = workflow,
                "moving on a saga its jobs left behind"
            );
            storage.sweep_saga(workflow).await?;
        }
        Ok(())
    }

    /// Check if a workflow should be marked as failed.
    /// Sagas are not: a failed step rolls them back (see `sweep_sagas`).
    async fn check_workflow_status(&self, workflow_id: i64) -> Result<()> {
        let schema = self.schema.as_deref();
        let jobs_table = table_name("ishikari_jobs", schema);
        let saga_steps_table = table_name("ishikari_saga_steps", schema);

        // Check if this is a saga (has saga steps)
        let is_saga_query = format!(
            "SELECT EXISTS(SELECT 1 FROM {} WHERE workflow_id = $1)",
            saga_steps_table
        );
        let is_saga: (bool,) = sqlx::query_as(&is_saga_query)
            .bind(workflow_id)
            .fetch_one(self.pool.as_ref())
            .await?;

        // Sagas roll back instead, as they move on
        if is_saga.0 {
            return Ok(());
        }

        // Check if any job in the workflow failed/cancelled/discarded
        let query = format!(
            r#"
            SELECT COUNT(*)
            FROM {}
            WHERE workflow_id = $1
              AND state IN ('discarded', 'cancelled')
            "#,
            jobs_table
        );

        let count: (i64,) = sqlx::query_as(&query)
            .bind(workflow_id)
            .fetch_one(self.pool.as_ref())
            .await?;

        if count.0 > 0 {
            info!(workflow_id = workflow_id, "marking workflow as failed");
            Workflow::fail(self.pool.as_ref(), workflow_id, schema).await?;
        }

        Ok(())
    }

    /// Finish running workflows whose jobs have all finished: failed when one was discarded or
    /// cancelled, completed otherwise. Sagas finish as they move on (see `sweep_sagas`).
    async fn check_completed_workflows(&self) -> Result<()> {
        let schema = self.schema.as_deref();
        let jobs_table = table_name("ishikari_jobs", schema);
        let workflows_table = table_name("ishikari_workflows", schema);
        let saga_steps_table = table_name("ishikari_saga_steps", schema);

        let query = format!(
            r#"
            SELECT w.id,
                   EXISTS (SELECT 1 FROM {jobs} j
                           WHERE j.workflow_id = w.id AND j.state IN ('discarded', 'cancelled'))
            FROM {workflows} w
            WHERE w.state = 'running'
              AND NOT EXISTS (SELECT 1 FROM {steps} s WHERE s.workflow_id = w.id)
              AND NOT EXISTS (
                  SELECT 1 FROM {jobs} j
                  WHERE j.workflow_id = w.id
                    AND j.state NOT IN ('completed', 'cancelled', 'discarded')
              )
              AND EXISTS (SELECT 1 FROM {jobs} j WHERE j.workflow_id = w.id)
            "#,
            jobs = jobs_table,
            workflows = workflows_table,
            steps = saga_steps_table,
        );

        let finished: Vec<(i64, bool)> =
            sqlx::query_as(&query).fetch_all(self.pool.as_ref()).await?;

        for (workflow_id, failed) in finished {
            if failed {
                info!(workflow_id = workflow_id, "marking workflow as failed");
                Workflow::fail(self.pool.as_ref(), workflow_id, schema).await?;
            } else {
                info!(workflow_id = workflow_id, "marking workflow as completed");
                Workflow::complete(self.pool.as_ref(), workflow_id, schema).await?;
            }
        }

        Ok(())
    }
}

/// Helper to build schema-qualified table names.
fn table_name(table: &str, schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("{}.{}", s, table),
        None => table.to_string(),
    }
}
