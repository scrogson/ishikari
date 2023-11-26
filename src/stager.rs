//! Ishikari Stager
//!
//! The Stager is responsible for moving tasks from scheduled/retryable to available.

use futures::TryStreamExt;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

pub struct Stager {
    pool: Arc<PgPool>,
}

impl Stager {
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        info!("Starting stager");

        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen_all(vec!["stager"]).await?;
        let mut stream = listener.into_stream();

        let mut interval = pin!(tokio::time::interval(Duration::from_secs(2)));

        loop {
            tokio::select! {
                res = stream.try_next() => {
                    if let Some(notification) = res? {
                        info!("{notification:?}");
                        let count = stage_jobs(&self.pool, 10).await?;
                        info!("Staged {} jobs", count);
                    }
                },
                _ = interval.tick() => {
                    let count = stage_jobs(&self.pool, 10).await?;
                    info!("Staged {} jobs", count);
                }
            }
        }
    }
}

async fn stage_jobs(pool: &PgPool, limit: i64) -> Result<usize, sqlx::Error> {
    info!("Staging jobs");

    let ids = sqlx::query(
        r#"
        WITH subquery AS (
            SELECT id, state
            FROM jobs
            WHERE state IN ('scheduled', 'retryable')
              AND queue IS NOT NULL
              AND priority IN (0, 1, 2, 3)
              AND scheduled_at <= now()
            LIMIT $1
        )
        UPDATE jobs
        SET state = 'available'
        FROM subquery
        WHERE jobs.id = subquery.id
        RETURNING jobs.id
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;

    Ok(ids.len())
}
