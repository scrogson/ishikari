//! Shared helpers for integration tests against a real Postgres.
//!
//! Each test gets its own schema, migrated to the current version, and a pool whose
//! `search_path` points at it, so tests never see each other's jobs. Set
//! `ISHIKARI_TEST_DATABASE_URL` (or `DATABASE_URL`) to a database the tests may create schemas
//! in. `ISHIKARI_SKIP_DB_TESTS=1` skips them; otherwise a missing URL fails loudly, so database
//! tests are never skipped by accident.

#![allow(dead_code)]

use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};
use std::future::Future;
use std::time::{Duration, Instant};

pub struct TestDb {
    /// A pool whose connections use the test's schema.
    pub pool: PgPool,
    /// The test's schema.
    pub schema: String,
    /// The database URL.
    pub url: String,
}

impl TestDb {
    /// A fresh, migrated schema, or `None` when database tests are switched off.
    pub async fn new() -> Option<TestDb> {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return None;
        }
        let url = std::env::var("ISHIKARI_TEST_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .expect(
                "set ISHIKARI_TEST_DATABASE_URL (or DATABASE_URL), or ISHIKARI_SKIP_DB_TESTS=1",
            );
        let schema = format!(
            "ishikari_t_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );

        let admin = PgPool::connect(&url).await.expect("connect");
        ishikari::migrations::migrate(&admin, &schema)
            .await
            .expect("migrate the test schema");
        admin.close().await;

        Some(TestDb {
            pool: TestDb::pool_for(&url, &schema).await,
            schema,
            url,
        })
    }

    /// Another pool on the same schema, as a second app instance would have.
    pub async fn another_pool(&self) -> PgPool {
        TestDb::pool_for(&self.url, &self.schema).await
    }

    /// A pool on the database's default `search_path`, for tests of explicit schemas.
    pub async fn plain_pool(&self) -> PgPool {
        PgPool::connect(&self.url).await.expect("connect")
    }

    /// A pool on `url` whose connections use `schema`.
    pub async fn pool_for(url: &str, schema: &str) -> PgPool {
        let path = format!("SET search_path TO {schema}");
        PgPoolOptions::new()
            .max_connections(10)
            .after_connect(move |conn, _| {
                let path = path.clone();
                Box::pin(async move {
                    conn.execute(path.as_str()).await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .expect("connect")
    }

    /// Drop the schema. Call at the end of a test; a failed test leaves its schema behind
    /// (`ishikari_t_*`) for inspection. Engines still polling the schema can deadlock with the
    /// drop, so it is retried.
    pub async fn finish(self) {
        self.pool.close().await;
        let admin = PgPool::connect(&self.url).await.expect("connect");
        for attempt in 1.. {
            match admin
                .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
                .await
            {
                Ok(_) => break,
                Err(sqlx::Error::Database(e))
                    if e.code().as_deref() == Some("40P01") && attempt < 10 =>
                {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => panic!("drop the test schema: {e}"),
            }
        }
    }

    /// A job's state, as text.
    pub async fn state(&self, id: i64) -> String {
        sqlx::query_scalar("SELECT state::text FROM ishikari_jobs WHERE id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .expect("job state")
    }
}

/// Poll `check` every 20 ms until it returns true, panicking after `timeout`.
pub async fn eventually<F, Fut>(what: &str, timeout: Duration, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let start = Instant::now();
    while !check().await {
        if start.elapsed() > timeout {
            panic!("timed out after {timeout:?} waiting for: {what}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
