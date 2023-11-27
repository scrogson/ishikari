//! Ishikari Queue

use crate::{Context, Job, Status::*};
use buildstructor::buildstructor;
use futures::TryStreamExt;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::any::Any;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub struct Queue {
    pub context: Arc<dyn std::any::Any + Send + Sync>,
    pub name: String,
    pub pool: Arc<PgPool>,
}

#[derive(Debug, serde::Deserialize)]
struct QueueNotification {
    queue: String,
}

#[buildstructor]
impl Queue {
    #[builder]
    pub fn new(
        name: String,
        pool: Arc<PgPool>,
        context: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Self {
        let context = context.unwrap_or(Arc::new(()));

        Self {
            context,
            name,
            pool,
        }
    }

    pub fn start(self: Arc<Self>) {
        let queue = self.clone();

        tokio::spawn(async move {
            queue.run().await.unwrap();
        });
    }

    async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        info!("Starting queue '{}'", self.name);

        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen_all(vec!["insert", "signal"]).await?;
        let mut stream = listener.into_stream();

        // TODO: make this configurable
        let mut interval = pin!(tokio::time::interval(Duration::from_secs(2)));

        loop {
            tokio::select! {
                res = stream.try_next() => {
                    match res {
                        Ok(Some(notification)) => {
                            handle_notification(self.clone(), &notification).await;
                        }
                        Ok(None) => {
                            info!("Queue '{}' received EOF", self.name);
                            break Ok(());
                        }
                        Err(e) => {
                            error!("Queue '{}' received error: {}", self.name, e);
                            break Ok(());
                        }
                    }
                },
                _ = interval.tick() => {
                    info!("Queue {}: checking for jobs", self.name);
                    execute_jobs(self.clone()).await;
                }
            }
        }
    }
}

async fn handle_notification(queue: Arc<Queue>, notification: &sqlx::postgres::PgNotification) {
    match notification.channel() {
        "insert" => match serde_json::from_str::<QueueNotification>(notification.payload()) {
            Ok(QueueNotification { queue: name }) => {
                if name == queue.name {
                    info!("Queue '{}': insert notification", queue.name);
                    execute_jobs(queue.clone()).await;
                }
            }
            Err(e) => {
                error!(
                    "Queue '{}': received invalid insert notification: {}",
                    queue.name, e
                );
            }
        },
        "signal" => {
            info!("Queue '{}': received signal notification", queue.name);
        }
        _ => {}
    }
}

async fn execute_jobs(queue: Arc<Queue>) {
    match fetch_and_update_jobs(queue.pool.clone(), &queue.name, 10).await {
        Ok(jobs) => {
            info!("Queue: executing {} jobs", jobs.len());
            for job in jobs {
                let job_clone = job.clone();
                let pool = queue.pool.clone();

                let queue_clone = Arc::clone(&queue);

                tokio::spawn(async move {
                    let worker = &job_clone.worker().unwrap();
                    let context = Context::new(Arc::new(job_clone), queue_clone);

                    match worker.perform(context).await {
                        Ok(result) => match result {
                            Complete(complete) => {
                                info!("Job completed id={} result={:?}", job.id, complete.0);
                                complete_job(&pool, job.id).await;
                            }
                            Discard(discard) => {
                                info!("Job discarded id={} reason={:?}", job.id, discard.0);
                                discard_job(&pool, job.id).await;
                            }
                            Cancel(cancel) => {
                                info!("Job cancelled id={} reason={:?}", job.id, cancel.0);
                                cancel_job(&pool, job.id).await;
                            }
                            Snooze(snooze) => {
                                info!("Job snoozed id={} snooze={}", job.id, snooze.0);
                                snooze_job(&pool, job.id, snooze.0).await;
                            }
                        },
                        Err(e) => {
                            error!(
                                "Job failed id={} attempt={} error={:?}",
                                job.id,
                                job.attempt,
                                e.to_string()
                            );
                            error_job(&pool, job.id, &e.to_string()).await;
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
    pool: Arc<sqlx::PgPool>,
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

    Ok(jobs)
}
