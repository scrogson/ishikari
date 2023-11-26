//! Ishikari Queue

use crate::worker::Status::*;
use crate::Job;
use buildstructor::buildstructor;
use futures::TryStreamExt;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub struct Queue {
    name: String,
    pool: Arc<PgPool>,
}

#[buildstructor]
impl Queue {
    #[builder]
    pub fn new(name: String, pool: Arc<PgPool>) -> Self {
        Self { name, pool }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        info!("Starting queue {}", self.name);

        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen_all(vec!["insert", "signal"]).await?;
        let mut stream = listener.into_stream();

        let mut interval = pin!(tokio::time::interval(Duration::from_secs(2)));

        loop {
            tokio::select! {
                res = stream.try_next() => {
                    if let Some(notification) = res? {
                        info!("Queue {} received notification: {notification:?}", self.name);
                        info!("Fetching jobs due to notification");
                        execute_jobs(&self.pool, &self.name, 10).await;
                    }
                },
                _ = interval.tick() => {
                    info!("Queue {}: checking for jobs", self.name);
                    execute_jobs(&self.pool, &self.name, 10).await;
                }
            }
        }
    }
}

async fn execute_jobs(pool: &PgPool, queue_name: &str, limit: i64) {
    match fetch_and_update_jobs(pool, queue_name, limit).await {
        Ok(jobs) => {
            info!("Queue: executing {} jobs", jobs.len());
            for job in jobs {
                let job_pool = pool.clone();
                tokio::spawn(async move {
                    let worker = job.worker().unwrap();

                    match worker.perform().await {
                        Ok(result) => match result {
                            Complete(complete) => {
                                info!("Job completed id={} result={:?}", job.id, complete.0);
                                complete_job(&job_pool, job.id).await;
                            }
                            Discard(discard) => {
                                info!("Job discarded id={} reason={:?}", job.id, discard.0);
                                discard_job(&job_pool, job.id).await;
                            }
                            Cancel(cancel) => {
                                info!("Job cancelled id={} reason={:?}", job.id, cancel.0);
                                cancel_job(&job_pool, job.id).await;
                            }
                            Snooze(snooze) => {
                                info!("Job snoozed id={} snooze={}", job.id, snooze.0);
                                snooze_job(&job_pool, job.id, snooze.0).await;
                            }
                        },
                        Err(e) => {
                            error!(
                                "Job failed id={} attempt={} error={:?}",
                                job.id,
                                job.attempt,
                                e.to_string()
                            );
                            error_job(&job_pool, job.id, &e.to_string()).await;
                        }
                    }
                });
            }
        }
        Err(e) => error!("Error fetching or updating jobs: {}", e),
    }
}

async fn snooze_job(pool: &sqlx::PgPool, id: i64, snooze: u64) {
    sqlx::query(r#"UPDATE jobs SET state = 'scheduled', scheduled_at = (now() + $1 * interval '1 second'), max_attempts = max_attempts + 1 WHERE id = $2"#)
        .bind(snooze as i64)
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

async fn cancel_job(pool: &sqlx::PgPool, id: i64) {
    sqlx::query(r#"UPDATE jobs SET state = 'cancelled', cancelled_at = now() WHERE id = $1"#)
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

async fn discard_job(pool: &sqlx::PgPool, id: i64) {
    sqlx::query(r#"UPDATE jobs SET state = 'discarded', discarded_at = now() WHERE id = $1"#)
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

async fn complete_job(pool: &sqlx::PgPool, id: i64) {
    sqlx::query(r#"UPDATE jobs SET state = 'completed', completed_at = now() WHERE id = $1 "#)
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

async fn error_job(pool: &sqlx::PgPool, id: i64, error_message: &str) {
    let result = sqlx::query(
        r#"
        UPDATE jobs
        SET state = 'retryable', errors = errors || $2::jsonb
        WHERE id = $1
    "#,
    )
    .bind(id)
    .bind(serde_json::to_value(error_message).unwrap())
    .execute(pool)
    .await;

    if let Err(e) = result {
        error!("Failed to update job error: {}", e);
    }
}

async fn fetch_and_update_jobs(
    pool: &sqlx::PgPool,
    queue: &str,
    demand: i64,
) -> Result<Vec<Job>, sqlx::Error> {
    info!("Fetching jobs");
    let mut tx = pool.begin().await?;

    let jobs = sqlx::query_as::<_, Job>(
        r#"
        WITH subset AS (
            SELECT id
            FROM jobs
            WHERE state = 'available'
              AND queue = $1
              AND attempt < max_attempts
            ORDER BY priority ASC, scheduled_at ASC, id ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE jobs
        SET state = 'executing',
            attempted_at = now(),
            -- attempted_by = ARRAY[$3, $4],
            attempt = attempt + 1
        FROM subset
        WHERE jobs.id = subset.id
        RETURNING jobs.*
        "#,
    )
    .bind(queue)
    .bind(demand)
    .fetch_all(&mut *tx)
    .await?;

    tx.commit().await?;

    info!("Fetched {} jobs", jobs.len());

    Ok(jobs.into())
}
