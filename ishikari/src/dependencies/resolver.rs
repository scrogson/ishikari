//! Dependency resolution service.
//!
//! The `DependencyResolver` runs as a background service alongside the ishikari
//! stager. It monitors for completed jobs and updates the dependency states
//! of any jobs that were waiting on them.
//!
//! # How it works
//!
//! 1. Polls for jobs that recently completed (state = 'completed')
//! 2. For each completed job, finds all dependencies where `depends_on_job_id` matches
//! 3. Marks those dependencies as 'satisfied'
//! 4. For jobs with all dependencies satisfied, updates them to 'available' state
//! 5. Also handles failed/cancelled/discarded jobs by marking dependencies as 'failed'

use crate::workflows::{Result, Workflow, WorkflowState};
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
    pub fn new(pool: impl Into<Arc<PgPool>>, interval: Duration) -> Self {
        Self {
            pool: pool.into(),
            interval,
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

    /// Run the resolver loop.
    #[instrument(skip(self))]
    async fn run(self) {
        let mut interval = tokio::time::interval(self.interval);

        loop {
            interval.tick().await;
            if let Err(e) = self.resolve_once().await {
                error!(error = %e, "dependency resolution failed");
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
            WHERE j.state = 'scheduled'
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

            // Reset scheduled_at to now() in case this is a compensation job
            // that was scheduled far in the future (year 3000).
            let release_query = format!(
                "UPDATE {} SET state = 'available', scheduled_at = now() WHERE id = $1 AND state = 'scheduled'",
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
            WHERE j.state = 'scheduled'
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
                "UPDATE {} SET state = 'cancelled', cancelled_at = now() WHERE id = $1 AND state = 'scheduled'",
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

        // Update saga step states for completed forward jobs
        self.update_saga_step_states().await?;

        // Check for completed workflows
        self.check_completed_workflows().await?;

        // Handle saga compensation
        self.process_saga_compensation().await?;

        Ok(())
    }

    /// Update saga step states when forward jobs complete.
    async fn update_saga_step_states(&self) -> Result<()> {
        let schema = self.schema.as_deref();
        let jobs_table = table_name("ishikari_jobs", schema);
        let saga_steps_table = table_name("ishikari_saga_steps", schema);

        // Find saga steps where the forward job completed but step is still pending
        let query = format!(
            r#"
            UPDATE {} s
            SET state = 'completed'
            FROM {} j
            WHERE s.job_id = j.id
              AND s.state = 'pending'
              AND j.state = 'completed'
            "#,
            saga_steps_table, jobs_table
        );

        let result = sqlx::query(&query).execute(self.pool.as_ref()).await?;

        if result.rows_affected() > 0 {
            debug!(
                count = result.rows_affected(),
                "updated saga step states to completed"
            );
        }

        Ok(())
    }

    /// Process saga compensation for failed workflows.
    async fn process_saga_compensation(&self) -> Result<()> {
        let schema = self.schema.as_deref();
        let jobs_table = table_name("ishikari_jobs", schema);
        let saga_steps_table = table_name("ishikari_saga_steps", schema);
        let workflows_table = table_name("ishikari_workflows", schema);

        // Find sagas that need to start compensation:
        // - Workflow is 'running' (not yet compensating)
        // - Has saga steps
        // - Has at least one failed/discarded/cancelled forward job
        // Find sagas that need compensation:
        // - Workflow is 'running'
        // - Has at least one failed forward job
        // - ALL forward jobs are in terminal states (no jobs still executing)
        let needs_compensation_query = format!(
            r#"
            SELECT DISTINCT s.workflow_id
            FROM {} s
            INNER JOIN {} j ON j.id = s.job_id
            INNER JOIN {} w ON w.id = s.workflow_id
            WHERE w.state = 'running'
              AND j.state IN ('discarded', 'cancelled')
              AND s.state = 'pending'
              AND NOT EXISTS (
                  SELECT 1 FROM {} s2
                  INNER JOIN {} j2 ON j2.id = s2.job_id
                  WHERE s2.workflow_id = s.workflow_id
                    AND j2.state NOT IN ('completed', 'cancelled', 'discarded')
              )
            "#,
            saga_steps_table, jobs_table, workflows_table, saga_steps_table, jobs_table
        );

        let workflows_to_compensate: Vec<(i64,)> = sqlx::query_as(&needs_compensation_query)
            .fetch_all(self.pool.as_ref())
            .await?;

        for (workflow_id,) in workflows_to_compensate {
            info!(workflow_id = workflow_id, "starting saga compensation");

            // Mark workflow as compensating
            Workflow::set_state(
                self.pool.as_ref(),
                workflow_id,
                WorkflowState::Compensating,
                schema,
            )
            .await?;

            // Mark saga steps as completed if their forward job completed successfully
            let mark_completed_steps = format!(
                r#"
                UPDATE {} s
                SET state = 'completed'
                FROM {} j
                WHERE s.job_id = j.id
                  AND s.workflow_id = $1
                  AND s.state = 'pending'
                  AND j.state = 'completed'
                "#,
                saga_steps_table, jobs_table
            );
            sqlx::query(&mark_completed_steps)
                .bind(workflow_id)
                .execute(self.pool.as_ref())
                .await?;

            // Mark saga steps as failed if their forward job failed or was cancelled
            let mark_failed_steps = format!(
                r#"
                UPDATE {} s
                SET state = 'failed'
                FROM {} j
                WHERE s.job_id = j.id
                  AND s.workflow_id = $1
                  AND s.state = 'pending'
                  AND j.state IN ('cancelled', 'discarded')
                "#,
                saga_steps_table, jobs_table
            );
            sqlx::query(&mark_failed_steps)
                .bind(workflow_id)
                .execute(self.pool.as_ref())
                .await?;

            // Mark remaining pending saga steps as failed ONLY if their forward job
            // is in a terminal non-success state OR is still scheduled (will never run).
            // Do NOT mark steps as failed if their forward job is still executing or available.
            let mark_remaining_pending_failed = format!(
                r#"
                UPDATE {} s
                SET state = 'failed'
                FROM {} j
                WHERE s.job_id = j.id
                  AND s.workflow_id = $1
                  AND s.state = 'pending'
                  AND j.state IN ('scheduled', 'cancelled', 'discarded')
                "#,
                saga_steps_table, jobs_table
            );
            sqlx::query(&mark_remaining_pending_failed)
                .bind(workflow_id)
                .execute(self.pool.as_ref())
                .await?;

            // Log step states before cancellation
            let step_states_query = format!(
                r#"
                SELECT s.step_order, s.state, s.job_id, s.compensation_job_id, j.state::text as job_state, j.worker
                FROM {} s
                INNER JOIN {} j ON j.id = s.job_id
                WHERE s.workflow_id = $1
                ORDER BY s.step_order
                "#,
                saga_steps_table, jobs_table
            );
            let step_states: Vec<(i32, String, i64, Option<i64>, String, String)> =
                sqlx::query_as(&step_states_query)
                    .bind(workflow_id)
                    .fetch_all(self.pool.as_ref())
                    .await?;

            for (step_order, step_state, job_id, comp_job_id, job_state, worker) in &step_states {
                info!(
                    workflow_id = workflow_id,
                    step_order = step_order,
                    step_state = %step_state,
                    job_id = job_id,
                    job_state = %job_state,
                    compensation_job_id = ?comp_job_id,
                    worker = %worker,
                    "saga step state"
                );
            }

            // Cancel compensation jobs for failed steps (nothing to compensate)
            let cancel_failed_comp_jobs = format!(
                r#"
                UPDATE {} j
                SET state = 'cancelled', cancelled_at = now()
                FROM {} s
                WHERE s.compensation_job_id = j.id
                  AND s.workflow_id = $1
                  AND s.state = 'failed'
                  AND j.state = 'scheduled'
                "#,
                jobs_table, saga_steps_table
            );
            let cancelled = sqlx::query(&cancel_failed_comp_jobs)
                .bind(workflow_id)
                .execute(self.pool.as_ref())
                .await?;

            if cancelled.rows_affected() > 0 {
                info!(
                    workflow_id = workflow_id,
                    count = cancelled.rows_affected(),
                    "cancelled compensation jobs for FAILED steps (step never completed)"
                );
            }

            // Find completed saga steps that have compensation jobs
            // Order by step_order DESC to run compensations in reverse
            let completed_steps_query = format!(
                r#"
                SELECT s.id, s.compensation_job_id, s.step_order
                FROM {} s
                INNER JOIN {} j ON j.id = s.job_id
                WHERE s.workflow_id = $1
                  AND s.compensation_job_id IS NOT NULL
                  AND j.state = 'completed'
                ORDER BY s.step_order DESC
                "#,
                saga_steps_table, jobs_table
            );

            let completed_steps: Vec<(i64, i64, i32)> = sqlx::query_as(&completed_steps_query)
                .bind(workflow_id)
                .fetch_all(self.pool.as_ref())
                .await?;

            info!(
                workflow_id = workflow_id,
                completed_steps_count = completed_steps.len(),
                "found completed steps needing compensation"
            );

            for (step_id, comp_job_id, step_order) in &completed_steps {
                info!(
                    workflow_id = workflow_id,
                    step_id = step_id,
                    step_order = step_order,
                    compensation_job_id = comp_job_id,
                    "completed step to compensate"
                );
            }

            // Chain compensation jobs: each waits for the previous compensation
            let mut prev_comp_job_id: Option<i64> = None;

            for (step_id, comp_job_id, step_order) in completed_steps {
                info!(
                    workflow_id = workflow_id,
                    step_order = step_order,
                    compensation_job_id = comp_job_id,
                    "scheduling compensation job"
                );

                // Mark saga step as compensating
                let mark_compensating = format!(
                    "UPDATE {} SET state = 'compensating' WHERE id = $1",
                    saga_steps_table
                );
                sqlx::query(&mark_compensating)
                    .bind(step_id)
                    .execute(self.pool.as_ref())
                    .await?;

                if let Some(prev_id) = prev_comp_job_id {
                    // Add dependency on previous compensation job
                    // Use ON CONFLICT DO NOTHING to handle re-runs safely
                    let deps_table = table_name("ishikari_job_dependencies", schema);
                    let add_dep = format!(
                        "INSERT INTO {} (job_id, depends_on_job_id, state) VALUES ($1, $2, 'pending') ON CONFLICT DO NOTHING",
                        deps_table
                    );
                    sqlx::query(&add_dep)
                        .bind(comp_job_id)
                        .bind(prev_id)
                        .execute(self.pool.as_ref())
                        .await?;
                } else {
                    // First compensation job - release it immediately.
                    // Reset scheduled_at since compensation jobs are scheduled to year 3000.
                    info!(
                        workflow_id = workflow_id,
                        compensation_job_id = comp_job_id,
                        step_order = step_order,
                        "releasing first compensation job immediately"
                    );
                    let release = format!(
                        "UPDATE {} SET state = 'available', scheduled_at = now() WHERE id = $1 AND state = 'scheduled'",
                        jobs_table
                    );
                    let result = sqlx::query(&release)
                        .bind(comp_job_id)
                        .execute(self.pool.as_ref())
                        .await?;
                    info!(
                        workflow_id = workflow_id,
                        compensation_job_id = comp_job_id,
                        rows_affected = result.rows_affected(),
                        "released first compensation job"
                    );
                }

                prev_comp_job_id = Some(comp_job_id);
            }

            // If no completed steps have compensation, mark workflow as compensated
            if prev_comp_job_id.is_none() {
                info!(
                    workflow_id = workflow_id,
                    "no compensation needed, marking as compensated"
                );
                Workflow::set_state(
                    self.pool.as_ref(),
                    workflow_id,
                    WorkflowState::Compensated,
                    schema,
                )
                .await?;
            }
        }

        // Handle stuck compensating workflows (compensation was never properly set up)
        self.recover_stuck_compensation().await?;

        // Check for compensating workflows where all compensation is done
        self.check_compensation_complete().await?;

        Ok(())
    }

    /// Recover stuck compensating workflows where compensation jobs were never released.
    ///
    /// This handles the case where a workflow was marked as 'compensating' but the
    /// compensation job release failed or never happened.
    async fn recover_stuck_compensation(&self) -> Result<()> {
        let schema = self.schema.as_deref();
        let jobs_table = table_name("ishikari_jobs", schema);
        let saga_steps_table = table_name("ishikari_saga_steps", schema);
        let workflows_table = table_name("ishikari_workflows", schema);

        // Find compensating workflows where:
        // - There are completed steps (forward job completed)
        // - Those steps have compensation jobs still in 'scheduled' state
        // This handles cases where:
        // 1. The step is 'completed' or 'pending' (compensation setup never ran)
        // 2. The step is 'compensating' but the job release failed
        let stuck_query = format!(
            r#"
            SELECT DISTINCT w.id
            FROM {} w
            INNER JOIN {} s ON s.workflow_id = w.id
            INNER JOIN {} fj ON fj.id = s.job_id
            INNER JOIN {} cj ON cj.id = s.compensation_job_id
            WHERE w.state = 'compensating'
              AND fj.state = 'completed'
              AND cj.state = 'scheduled'
              AND s.state IN ('completed', 'pending', 'compensating')
            "#,
            workflows_table, saga_steps_table, jobs_table, jobs_table
        );

        let stuck_workflows: Vec<(i64,)> = sqlx::query_as(&stuck_query)
            .fetch_all(self.pool.as_ref())
            .await?;

        for (workflow_id,) in stuck_workflows {
            warn!(
                workflow_id = workflow_id,
                "recovering stuck compensating workflow - releasing compensation jobs"
            );

            // Find completed steps that need compensation (same logic as process_saga_compensation)
            let completed_steps_query = format!(
                r#"
                SELECT s.id, s.compensation_job_id, s.step_order
                FROM {} s
                INNER JOIN {} j ON j.id = s.job_id
                WHERE s.workflow_id = $1
                  AND s.compensation_job_id IS NOT NULL
                  AND j.state = 'completed'
                ORDER BY s.step_order DESC
                "#,
                saga_steps_table, jobs_table
            );

            let completed_steps: Vec<(i64, i64, i32)> = sqlx::query_as(&completed_steps_query)
                .bind(workflow_id)
                .fetch_all(self.pool.as_ref())
                .await?;

            let mut prev_comp_job_id: Option<i64> = None;

            for (step_id, comp_job_id, step_order) in completed_steps {
                // Mark saga step as compensating (if not already)
                let mark_compensating = format!(
                    "UPDATE {} SET state = 'compensating' WHERE id = $1 AND state IN ('completed', 'pending')",
                    saga_steps_table
                );
                sqlx::query(&mark_compensating)
                    .bind(step_id)
                    .execute(self.pool.as_ref())
                    .await?;

                if let Some(prev_id) = prev_comp_job_id {
                    // Add dependency on previous compensation job
                    let deps_table = table_name("ishikari_job_dependencies", schema);
                    let add_dep = format!(
                        "INSERT INTO {} (job_id, depends_on_job_id, state) VALUES ($1, $2, 'pending') ON CONFLICT DO NOTHING",
                        deps_table
                    );
                    sqlx::query(&add_dep)
                        .bind(comp_job_id)
                        .bind(prev_id)
                        .execute(self.pool.as_ref())
                        .await?;
                } else {
                    // First compensation job - release it immediately
                    info!(
                        workflow_id = workflow_id,
                        compensation_job_id = comp_job_id,
                        step_order = step_order,
                        "releasing stuck compensation job"
                    );
                    let release = format!(
                        "UPDATE {} SET state = 'available', scheduled_at = now() WHERE id = $1 AND state = 'scheduled'",
                        jobs_table
                    );
                    sqlx::query(&release)
                        .bind(comp_job_id)
                        .execute(self.pool.as_ref())
                        .await?;
                }

                prev_comp_job_id = Some(comp_job_id);
            }
        }

        Ok(())
    }

    /// Check if compensation is complete for compensating workflows.
    async fn check_compensation_complete(&self) -> Result<()> {
        let schema = self.schema.as_deref();
        let jobs_table = table_name("ishikari_jobs", schema);
        let saga_steps_table = table_name("ishikari_saga_steps", schema);
        let workflows_table = table_name("ishikari_workflows", schema);

        // Find compensating workflows where all compensation jobs are done
        let query = format!(
            r#"
            SELECT w.id
            FROM {} w
            WHERE w.state = 'compensating'
              AND NOT EXISTS (
                  SELECT 1 FROM {} s
                  INNER JOIN {} j ON j.id = s.compensation_job_id
                  WHERE s.workflow_id = w.id
                    AND s.state = 'compensating'
                    AND j.state NOT IN ('completed', 'cancelled', 'discarded')
              )
            "#,
            workflows_table, saga_steps_table, jobs_table
        );

        let done_workflows: Vec<(i64,)> =
            sqlx::query_as(&query).fetch_all(self.pool.as_ref()).await?;

        for (workflow_id,) in done_workflows {
            // Update saga step states
            let update_steps = format!(
                r#"
                UPDATE {} s
                SET state = CASE
                    WHEN j.state = 'completed' THEN 'compensated'
                    ELSE 'compensation_failed'
                END
                FROM {} j
                WHERE s.compensation_job_id = j.id
                  AND s.workflow_id = $1
                  AND s.state = 'compensating'
                "#,
                saga_steps_table, jobs_table
            );
            sqlx::query(&update_steps)
                .bind(workflow_id)
                .execute(self.pool.as_ref())
                .await?;

            // Check if any compensation failed
            let failed_query = format!(
                "SELECT COUNT(*) FROM {} WHERE workflow_id = $1 AND state = 'compensation_failed'",
                saga_steps_table
            );
            let failed_count: (i64,) = sqlx::query_as(&failed_query)
                .bind(workflow_id)
                .fetch_one(self.pool.as_ref())
                .await?;

            if failed_count.0 > 0 {
                warn!(workflow_id = workflow_id, "saga compensation failed");
                // Mark as failed - compensation could not complete successfully
                Workflow::set_state(
                    self.pool.as_ref(),
                    workflow_id,
                    WorkflowState::Failed,
                    schema,
                )
                .await?;
            } else {
                info!(workflow_id = workflow_id, "saga compensation complete");
                Workflow::set_state(
                    self.pool.as_ref(),
                    workflow_id,
                    WorkflowState::Compensated,
                    schema,
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Check if a workflow should be marked as failed.
    /// Note: Sagas are NOT marked as failed here - they are handled by
    /// process_saga_compensation which will mark them as 'compensating'.
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

        // Sagas are handled by process_saga_compensation, not here
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

    /// Check for workflows where all jobs have completed.
    async fn check_completed_workflows(&self) -> Result<()> {
        let schema = self.schema.as_deref();
        let jobs_table = table_name("ishikari_jobs", schema);
        let workflows_table = table_name("ishikari_workflows", schema);
        let saga_steps_table = table_name("ishikari_saga_steps", schema);

        // First, check for successful sagas that need their compensation jobs cancelled.
        // A saga is successful if all forward jobs completed and no failures occurred.
        self.cancel_unused_compensation_jobs().await?;

        // Find running workflows where all jobs are completed
        let query = format!(
            r#"
            SELECT w.id
            FROM {} w
            WHERE w.state = 'running'
              AND NOT EXISTS (
                  SELECT 1 FROM {} j
                  WHERE j.workflow_id = w.id
                    AND j.state NOT IN ('completed', 'cancelled', 'discarded')
              )
              AND EXISTS (
                  SELECT 1 FROM {} j
                  WHERE j.workflow_id = w.id
              )
            "#,
            workflows_table, jobs_table, jobs_table
        );

        let completed_workflows: Vec<(i64,)> =
            sqlx::query_as(&query).fetch_all(self.pool.as_ref()).await?;

        for (workflow_id,) in completed_workflows {
            // Check if this is a saga (has saga steps)
            let is_saga_query = format!(
                "SELECT EXISTS(SELECT 1 FROM {} WHERE workflow_id = $1)",
                saga_steps_table
            );
            let is_saga: (bool,) = sqlx::query_as(&is_saga_query)
                .bind(workflow_id)
                .fetch_one(self.pool.as_ref())
                .await?;

            // Check if any forward jobs failed (not counting cancelled compensation jobs)
            let failed_forward_query = format!(
                r#"
                SELECT COUNT(*)
                FROM {} j
                WHERE j.workflow_id = $1
                  AND j.state IN ('discarded')
                  AND NOT EXISTS (
                      SELECT 1 FROM {} s
                      WHERE s.compensation_job_id = j.id
                  )
                "#,
                jobs_table, saga_steps_table
            );

            let failed_count: (i64,) = sqlx::query_as(&failed_forward_query)
                .bind(workflow_id)
                .fetch_one(self.pool.as_ref())
                .await?;

            if failed_count.0 > 0 {
                info!(workflow_id = workflow_id, "marking workflow as failed");
                Workflow::fail(self.pool.as_ref(), workflow_id, schema).await?;
            } else {
                info!(
                    workflow_id = workflow_id,
                    is_saga = is_saga.0,
                    "marking workflow as completed"
                );
                Workflow::complete(self.pool.as_ref(), workflow_id, schema).await?;
            }
        }

        Ok(())
    }

    /// Cancel unused compensation jobs for successful sagas.
    ///
    /// When all forward jobs in a saga complete successfully, the compensation
    /// jobs are no longer needed and should be cancelled.
    async fn cancel_unused_compensation_jobs(&self) -> Result<()> {
        let schema = self.schema.as_deref();
        let jobs_table = table_name("ishikari_jobs", schema);
        let saga_steps_table = table_name("ishikari_saga_steps", schema);
        let workflows_table = table_name("ishikari_workflows", schema);

        // Find sagas where all forward jobs completed successfully
        // and compensation jobs are still scheduled (not yet cancelled)
        let query = format!(
            r#"
            SELECT DISTINCT s.workflow_id
            FROM {} s
            INNER JOIN {} w ON w.id = s.workflow_id
            WHERE w.state = 'running'
              AND s.compensation_job_id IS NOT NULL
              AND EXISTS (
                  SELECT 1 FROM {} cj
                  WHERE cj.id = s.compensation_job_id
                    AND cj.state = 'scheduled'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM {} s2
                  INNER JOIN {} j ON j.id = s2.job_id
                  WHERE s2.workflow_id = s.workflow_id
                    AND j.state NOT IN ('completed')
              )
            "#,
            saga_steps_table, workflows_table, jobs_table, saga_steps_table, jobs_table
        );

        let successful_sagas: Vec<(i64,)> =
            sqlx::query_as(&query).fetch_all(self.pool.as_ref()).await?;

        for (workflow_id,) in successful_sagas {
            warn!(
                workflow_id = workflow_id,
                "cancelling unused compensation jobs for SUCCESSFUL saga (all forward jobs completed)"
            );

            // First, log which jobs would be cancelled
            let preview_query = format!(
                r#"
                SELECT j.id, j.worker
                FROM {} j
                INNER JOIN {} s ON s.compensation_job_id = j.id
                WHERE s.workflow_id = $1
                  AND j.state = 'scheduled'
                "#,
                jobs_table, saga_steps_table
            );

            let jobs_to_cancel: Vec<(i64, String)> = sqlx::query_as(&preview_query)
                .bind(workflow_id)
                .fetch_all(self.pool.as_ref())
                .await?;

            for (job_id, worker) in &jobs_to_cancel {
                warn!(
                    workflow_id = workflow_id,
                    job_id = job_id,
                    worker = %worker,
                    "about to cancel compensation job (saga completed successfully)"
                );
            }

            // Cancel all scheduled compensation jobs for this saga
            let cancel_query = format!(
                r#"
                UPDATE {} j
                SET state = 'cancelled', cancelled_at = now()
                FROM {} s
                WHERE s.compensation_job_id = j.id
                  AND s.workflow_id = $1
                  AND j.state = 'scheduled'
                "#,
                jobs_table, saga_steps_table
            );

            let result = sqlx::query(&cancel_query)
                .bind(workflow_id)
                .execute(self.pool.as_ref())
                .await?;

            if result.rows_affected() > 0 {
                warn!(
                    workflow_id = workflow_id,
                    count = result.rows_affected(),
                    "cancelled unused compensation jobs for successful saga"
                );
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
