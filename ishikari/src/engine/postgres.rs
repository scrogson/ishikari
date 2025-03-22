use crate::engine::Storage;
use crate::Job;
use chrono::{DateTime, Utc};
use std::sync::Arc;

pub struct Postgres {
    pub pool: Arc<sqlx::PgPool>,
}

impl Postgres {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool: Arc::new(pool),
        }
    }
}

impl From<sqlx::PgPool> for Postgres {
    fn from(pool: sqlx::PgPool) -> Self {
        Self::new(pool)
    }
}

impl From<Arc<sqlx::PgPool>> for Postgres {
    fn from(pool: Arc<sqlx::PgPool>) -> Self {
        Self { pool: pool.clone() }
    }
}

#[async_trait::async_trait]
impl Storage for Postgres {
    type Error = sqlx::Error;

    async fn cancel_job(&self, id: i64) -> Result<(), Self::Error> {
        sqlx::query(r#"UPDATE jobs SET state = 'cancelled', cancelled_at = now() WHERE id = $1"#)
            .bind(id)
            .execute(&*self.pool)
            .await
            .map(|_| ())
    }

    async fn complete_job(&self, id: i64) -> Result<(), Self::Error> {
        sqlx::query(r#"UPDATE jobs SET state = 'completed', completed_at = now() WHERE id = $1"#)
            .bind(id)
            .execute(&*self.pool)
            .await
            .map(|_| ())
    }

    async fn discard_job(&self, id: i64) -> Result<(), Self::Error> {
        sqlx::query(r#"UPDATE jobs SET state = 'discarded', discarded_at = now() WHERE id = $1"#)
            .bind(id)
            .execute(&*self.pool)
            .await
            .map(|_| ())
    }

    async fn error_job(
        &self,
        id: i64,
        error_message: &str,
        schedule_at: DateTime<Utc>,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            UPDATE jobs
            SET
                state = 'retryable',
                errors = errors || $2::jsonb,
                scheduled_at = $3
            WHERE id = $1
        "#,
        )
        .bind(id)
        .bind(serde_json::to_value(error_message).unwrap())
        .bind(schedule_at)
        .execute(&*self.pool)
        .await
        .map(|_| ())
    }

    async fn retry_job(&self, id: i64) -> Result<(), Self::Error> {
        sqlx::query(
            r#"UPDATE jobs SET state = 'available', max_attempts = max_attempts + 1 WHERE id = $1"#,
        )
        .bind(id)
        .execute(&*self.pool)
        .await
        .map(|_| ())
    }

    async fn snooze_job(&self, id: i64, snooze: u64) -> Result<(), Self::Error> {
        sqlx::query(r#"UPDATE jobs SET state = 'scheduled', scheduled_at = (now() + $1 * interval '1 second'), max_attempts = max_attempts + 1 WHERE id = $2"#)
            .bind(snooze as i64)
            .bind(id)
            .execute(&*self.pool)
            .await
            .map(|_| ())
    }

    async fn fetch_jobs(&self) -> Result<Vec<Job>, Self::Error> {
        let jobs = sqlx::query_as::<_, Job>(r#"SELECT * FROM jobs WHERE state = 'available' ORDER BY priority DESC, inserted_at ASC LIMIT 10"#)
            .fetch_all(&*self.pool)
            .await?;
        Ok(jobs)
    }

    async fn prune_jobs(&self) -> Result<Vec<Job>, Self::Error> {
        let jobs = sqlx::query_as::<_, Job>(r#"DELETE FROM jobs WHERE state = 'completed' OR state = 'cancelled' OR state = 'discarded' RETURNING *"#)
            .fetch_all(&*self.pool)
            .await?;
        Ok(jobs)
    }

    async fn stage_jobs(&self, limit: i32) -> Result<usize, Self::Error> {
        let ids = sqlx::query(
            r#"
            WITH subquery AS (
                SELECT id, state
                FROM jobs
                WHERE state IN ('scheduled', 'retryable')
                  AND queue IS NOT NULL
                AND priority IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
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
        .fetch_all(&*self.pool)
        .await?;

        Ok(ids.len())
    }

    async fn fetch_and_execute_jobs(
        &self,
        queue: &str,
        demand: i32,
    ) -> Result<Vec<Job>, Self::Error> {
        let mut tx = self.pool.begin().await?;

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

        Ok(jobs)
    }
}
