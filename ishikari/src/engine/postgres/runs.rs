//! Definition runs, in the transaction that ends a node's job.
//!
//! Each node of a definition's run is a [`NodeJob`], whose runner records the node's execution
//! and finishes the run, failing it when the node fails its last attempt. A node's job can also
//! end where its runner doesn't see it: timed out, panicking, cancelled, or rescued from a
//! stopped node and discarded. Its end records the node failed, with the job's error, and fails
//! the run while it is still running. A failure the runner recorded for the same attempt is
//! kept, so the two never disagree. The resolver's sweep does the same for a job that ended
//! without it (one rescued and discarded). Retrying the job runs its failed run again.

use super::Postgres;
use crate::execution::NodeJob;
use sqlx::PgConnection;

impl Postgres {
    /// After the job `id` was discarded or cancelled: when it runs a node of a definition's run,
    /// record the node failed with the job's error from its last attempt (or the state it
    /// ended in), unless the runner recorded that attempt failing, and fail the run if it is
    /// still running. Does nothing for any other job.
    pub(crate) async fn fail_node_run(
        &self,
        tx: &mut PgConnection,
        id: i64,
    ) -> Result<(), sqlx::Error> {
        let (jobs, runs, nodes) = (
            self.name("ishikari_jobs"),
            self.name("ishikari_workflow_runs"),
            self.name("ishikari_node_executions"),
        );
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
            WITH dead AS (
                SELECT r.id AS run,
                       j.args->>'node_id' AS node,
                       j.args->>'node_type' AS node_type,
                       greatest(j.attempt - 1, 0) AS retry_count,
                       coalesce(
                           CASE WHEN (last.error->>'attempt')::int = j.attempt
                                THEN last.error->>'error' END,
                           j.state::text
                       ) AS error,
                       j.attempted_at AS started,
                       coalesce(j.discarded_at, j.cancelled_at, now()) AS ended
                FROM {jobs} j
                CROSS JOIN LATERAL (SELECT j.errors[cardinality(j.errors)] AS error) last
                JOIN {runs} r ON r.workflow_id = j.workflow_id
                             AND r.id::text = j.args->>'workflow_run_id'
                WHERE j.id = $1 AND j.worker = $2 AND j.state IN ('discarded', 'cancelled')
            ),
            recorded AS (
                INSERT INTO {nodes} AS n (
                    workflow_run_id, node_id, node_type, status, error, retry_count, duration_ms,
                    started_at, completed_at
                )
                SELECT run, node, node_type, 'failed', error, retry_count,
                       (extract(epoch FROM ended - started) * 1000)::bigint,
                       coalesce(started, ended), ended
                FROM dead
                ON CONFLICT (workflow_run_id, node_id) DO UPDATE SET
                    node_type = EXCLUDED.node_type,
                    status = EXCLUDED.status,
                    inputs = NULL,
                    output = NULL,
                    error = EXCLUDED.error,
                    skip_reason = NULL,
                    retry_count = EXCLUDED.retry_count,
                    duration_ms = EXCLUDED.duration_ms,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at
                WHERE NOT (n.status = 'failed' AND n.retry_count = EXCLUDED.retry_count)
            )
            UPDATE {runs} r
            SET status = 'failed',
                error = format('Node ''%s'' failed: %s', dead.node, dead.error),
                completed_at = now()
            FROM dead
            WHERE r.id = dead.run AND r.status = 'running'
            "#
        )))
        .bind(id)
        .bind(NodeJob::WORKER)
        .execute(&mut *tx)
        .await?;
        Ok(())
    }

    /// Fail a run for the node's job `id` in a transaction of its own; the resolver's sweep,
    /// for a job that ended without its end doing it (one rescued and discarded).
    pub(crate) async fn sweep_node_run(&self, id: i64) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        self.fail_node_run(&mut tx, id).await?;
        tx.commit().await
    }

    /// After the job `id` was retried: when it runs a node of a definition's run that failed,
    /// the run is running again.
    pub(crate) async fn revive_node_run(
        &self,
        tx: &mut PgConnection,
        id: i64,
    ) -> Result<(), sqlx::Error> {
        let (jobs, runs) = (
            self.name("ishikari_jobs"),
            self.name("ishikari_workflow_runs"),
        );
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"
            UPDATE {runs} r
            SET status = 'running', error = NULL, outputs = NULL, completed_at = NULL
            FROM {jobs} j
            WHERE j.id = $1 AND j.worker = $2
              AND r.workflow_id = j.workflow_id AND r.id::text = j.args->>'workflow_run_id'
              AND r.status = 'failed'
            "#
        )))
        .bind(id)
        .bind(NodeJob::WORKER)
        .execute(&mut *tx)
        .await?;
        Ok(())
    }
}
