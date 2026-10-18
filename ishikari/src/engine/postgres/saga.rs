//! Saga rollback, in the transaction that ends a saga's job.
//!
//! A saga's steps run one after another. When a step is discarded or cancelled, or the saga is
//! cancelled ([`Postgres::cancel_saga`]), the steps that haven't started are cancelled and the
//! saga is `compensating`. Once no step is running, the completed steps are undone in reverse
//! order, one compensation at a time. Whatever a compensation's outcome, its end releases the
//! next: one that is discarded or cancelled leaves its step `compensation_failed` and the rest
//! still run. The rollback then ends the saga `compensated`, or `failed` when a compensation
//! failed. A saga whose steps all complete is `completed`, its compensations cancelled unused.
//!
//! Compensations wait parked in `pending`, with nothing to wait for, so neither the stager nor
//! a retry can run one before its rollback releases it.
//!
//! Every move is made by [`Postgres::advance_saga`] with the saga's row locked, in the
//! transaction of the job whose end caused it; the resolver's sweep calls it too, for a saga a
//! job left behind (one rescued and discarded).

use super::{release, Postgres};
use sqlx::PgConnection;

/// A saga job's state once it has ended.
const ENDED: &str = "('completed', 'discarded', 'cancelled')";

impl Postgres {
    /// Move the saga on from where its jobs are, with its row locked: steps and compensations
    /// that ended are recorded, a saga whose steps all completed is completed, one with a failed
    /// step rolls back, and a rollback with nothing running releases its next compensation or
    /// ends. Does nothing to a workflow that isn't a saga or has finished.
    pub(crate) async fn advance_saga(
        &self,
        tx: &mut PgConnection,
        workflow: i64,
    ) -> Result<(), sqlx::Error> {
        let (jobs, steps, workflows) = (
            self.name("ishikari_jobs"),
            self.name("ishikari_saga_steps"),
            self.name("ishikari_workflows"),
        );
        let state: Option<String> = sqlx::query_scalar(&format!(
            "SELECT state FROM {workflows} w WHERE id = $1 \
               AND EXISTS (SELECT 1 FROM {steps} WHERE workflow_id = w.id) \
             FOR NO KEY UPDATE"
        ))
        .bind(workflow)
        .fetch_optional(&mut *tx)
        .await?;
        match state.as_deref() {
            Some("running") => {
                self.settle_saga(tx, workflow).await?;
                let (failed, waiting): (Option<bool>, Option<bool>) = sqlx::query_as(&format!(
                    "SELECT bool_or(state = 'failed'), bool_or(state = 'pending') \
                     FROM {steps} WHERE workflow_id = $1"
                ))
                .bind(workflow)
                .fetch_one(&mut *tx)
                .await?;
                if failed == Some(true) {
                    self.roll_back_saga(tx, workflow, None).await?;
                } else if waiting != Some(true) {
                    self.drop_compensations(tx, workflow).await?;
                    sqlx::query(&format!(
                        "UPDATE {workflows} SET state = 'completed', completed_at = now() \
                         WHERE id = $1"
                    ))
                    .bind(workflow)
                    .execute(&mut *tx)
                    .await?;
                    return Ok(());
                } else {
                    return Ok(());
                }
            }
            Some("compensating") => {}
            _ => return Ok(()),
        }

        self.settle_saga(tx, workflow).await?;
        loop {
            // A step still running, or a compensation: the rollback waits for it.
            let busy: bool = sqlx::query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM {steps} \
                                WHERE workflow_id = $1 AND state IN ('pending', 'compensating'))"
            ))
            .bind(workflow)
            .fetch_one(&mut *tx)
            .await?;
            if busy {
                return Ok(());
            }
            // The last completed step with something to undo.
            let next: Option<(i64, i64)> = sqlx::query_as(&format!(
                "SELECT id, compensation_job_id FROM {steps} \
                 WHERE workflow_id = $1 AND state = 'completed' \
                   AND compensation_job_id IS NOT NULL \
                 ORDER BY step_order DESC LIMIT 1"
            ))
            .bind(workflow)
            .fetch_optional(&mut *tx)
            .await?;
            let Some((step, compensation)) = next else {
                break;
            };
            // Parked compensations are `pending` (`scheduled` far ahead, from before v07).
            let released = sqlx::query(&format!(
                "UPDATE {jobs} SET state = 'available', scheduled_at = now(), {release} \
                 WHERE id = $1 AND state IN ('pending', 'scheduled')",
                release = release("'available'", ""),
            ))
            .bind(compensation)
            .execute(&mut *tx)
            .await?;
            let job: Option<String> = if released.rows_affected() == 1 {
                Some("available".into())
            } else {
                sqlx::query_scalar(&format!("SELECT state::text FROM {jobs} WHERE id = $1"))
                    .bind(compensation)
                    .fetch_optional(&mut *tx)
                    .await?
            };
            // One cancelled by hand before its turn can't undo its step.
            let undo = match job.as_deref() {
                Some("completed") => "compensated",
                Some("discarded" | "cancelled") | None => "compensation_failed",
                Some(_) => "compensating",
            };
            sqlx::query(&format!("UPDATE {steps} SET state = $2 WHERE id = $1"))
                .bind(step)
                .bind(undo)
                .execute(&mut *tx)
                .await?;
            if undo == "compensating" {
                tracing::debug!(workflow, compensation, "released a compensation");
                return Ok(());
            }
        }

        // Rolled back as far as it goes.
        self.drop_compensations(tx, workflow).await?;
        sqlx::query(&format!(
            "UPDATE {workflows} SET completed_at = now(), \
                 state = CASE WHEN EXISTS (SELECT 1 FROM {steps} \
                                           WHERE workflow_id = $1 AND state = 'compensation_failed') \
                              THEN 'failed' ELSE 'compensated' END \
             WHERE id = $1"
        ))
        .bind(workflow)
        .execute(&mut *tx)
        .await?;
        Ok(())
    }

    /// Record the saga's steps and compensations whose jobs have ended.
    async fn settle_saga(&self, tx: &mut PgConnection, workflow: i64) -> Result<(), sqlx::Error> {
        let (jobs, steps) = (self.name("ishikari_jobs"), self.name("ishikari_saga_steps"));
        sqlx::query(&format!(
            "UPDATE {steps} s \
             SET state = CASE WHEN j.state = 'completed' THEN 'completed' ELSE 'failed' END \
             FROM {jobs} j \
             WHERE j.id = s.job_id AND s.workflow_id = $1 AND s.state = 'pending' \
               AND j.state IN {ENDED}"
        ))
        .bind(workflow)
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "UPDATE {steps} s \
             SET state = CASE WHEN j.state = 'completed' THEN 'compensated' \
                              ELSE 'compensation_failed' END \
             FROM {jobs} j \
             WHERE j.id = s.compensation_job_id AND s.workflow_id = $1 \
               AND s.state = 'compensating' AND j.state IN {ENDED}"
        ))
        .bind(workflow)
        .execute(&mut *tx)
        .await?;
        Ok(())
    }

    /// Start the saga's rollback: its steps that haven't started (or are waiting to retry) are
    /// cancelled, with `reason` recorded when given, and it is compensating. A running step is
    /// left to finish.
    async fn roll_back_saga(
        &self,
        tx: &mut PgConnection,
        workflow: i64,
        reason: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let (jobs, deps, steps) = (
            self.name("ishikari_jobs"),
            self.name("ishikari_job_dependencies"),
            self.name("ishikari_saga_steps"),
        );
        let cancelled: Vec<i64> = sqlx::query_scalar(&format!(
            "UPDATE {jobs} AS j SET state = 'cancelled', cancelled_at = now(), {release}, \
                 errors = CASE WHEN $2::text IS NULL THEN j.errors \
                               ELSE j.errors || jsonb_build_object('attempt', j.attempt, \
                                    'at', now(), 'kind', 'cancelled', 'error', $2::text) END \
             FROM {steps} s \
             WHERE s.job_id = j.id AND s.workflow_id = $1 \
               AND j.state IN ('pending', 'available', 'scheduled', 'retryable') \
             RETURNING j.id",
            release = release("'cancelled'", "j."),
        ))
        .bind(workflow)
        .bind(reason)
        .fetch_all(&mut *tx)
        .await?;
        if !cancelled.is_empty() {
            sqlx::query(&format!(
                "UPDATE {deps} SET state = 'failed' \
                 WHERE depends_on_job_id = ANY($1) AND state = 'pending'"
            ))
            .bind(&cancelled)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(&format!(
            "UPDATE {workflows} SET state = 'compensating' WHERE id = $1 AND state = 'running'",
            workflows = self.name("ishikari_workflows"),
        ))
        .bind(workflow)
        .execute(&mut *tx)
        .await?;
        tracing::debug!(workflow, "rolling back a saga");
        self.settle_saga(tx, workflow).await
    }

    /// Cancel the saga's compensations still parked: nothing will release them.
    async fn drop_compensations(
        &self,
        tx: &mut PgConnection,
        workflow: i64,
    ) -> Result<(), sqlx::Error> {
        let (jobs, steps) = (self.name("ishikari_jobs"), self.name("ishikari_saga_steps"));
        sqlx::query(&format!(
            "UPDATE {jobs} AS j SET state = 'cancelled', cancelled_at = now(), {release} \
             FROM {steps} s \
             WHERE s.compensation_job_id = j.id AND s.workflow_id = $1 \
               AND s.state <> 'compensating' AND j.state IN ('pending', 'scheduled')",
            release = release("'cancelled'", "j."),
        ))
        .bind(workflow)
        .execute(&mut *tx)
        .await?;
        Ok(())
    }

    /// Cancel a running saga: its steps that haven't started are cancelled (with `reason`
    /// recorded), a running step is left to finish, and then its completed steps are undone as
    /// when a step fails. Returns false when there is no such saga or it isn't running.
    pub async fn cancel_saga(&self, id: i64, reason: Option<&str>) -> Result<bool, sqlx::Error> {
        let (jobs, steps) = (self.name("ishikari_jobs"), self.name("ishikari_saga_steps"));
        let mut tx = self.pool.begin().await?;
        // The steps it cancels are locked before the saga's row, the order a step's completion
        // takes them in (its dependents, then its workflow).
        sqlx::query(&format!(
            "SELECT j.id FROM {jobs} j JOIN {steps} s ON s.job_id = j.id \
             WHERE s.workflow_id = $1 \
               AND j.state IN ('pending', 'available', 'scheduled', 'retryable') \
             ORDER BY j.id FOR UPDATE OF j"
        ))
        .bind(id)
        .execute(&mut *tx)
        .await?;
        let running: Option<bool> = sqlx::query_scalar(&format!(
            "SELECT state = 'running' FROM {workflows} w WHERE id = $1 \
               AND EXISTS (SELECT 1 FROM {steps} WHERE workflow_id = w.id) \
             FOR NO KEY UPDATE",
            workflows = self.name("ishikari_workflows"),
        ))
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?;
        if running != Some(true) {
            return Ok(false);
        }
        self.roll_back_saga(&mut tx, id, reason).await?;
        self.advance_saga(&mut tx, id).await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Move a saga on in a transaction of its own; the resolver's sweep, for a saga a job left
    /// behind.
    pub(crate) async fn sweep_saga(&self, workflow: i64) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        self.advance_saga(&mut tx, workflow).await?;
        tx.commit().await
    }
}
