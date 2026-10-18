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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
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

/// What is logged on this thread, at every level and without colors, while the guard lives. A
/// test on the default (current-thread) runtime sees its engine's logs too.
pub fn capture_logs() -> (tracing::subscriber::DefaultGuard, Logs) {
    let logs = Logs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(logs.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    (tracing::subscriber::set_default(subscriber), logs)
}

#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<u8>>>);

impl Logs {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

impl std::io::Write for Logs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Logs {
    type Writer = Logs;

    fn make_writer(&'a self) -> Logs {
        self.clone()
    }
}

/// Once armed, panic in the next event logged on this thread with `message`, as a bug in the
/// code logging it would. Disarms itself.
pub fn panic_on(message: &'static str) -> (tracing::subscriber::DefaultGuard, Arc<AtomicBool>) {
    use tracing_subscriber::layer::SubscriberExt;
    let armed = Arc::new(AtomicBool::new(false));
    let layer = PanicOn {
        message,
        armed: armed.clone(),
    };
    let subscriber = tracing_subscriber::registry().with(layer);
    (tracing::subscriber::set_default(subscriber), armed)
}

struct PanicOn {
    message: &'static str,
    armed: Arc<AtomicBool>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for PanicOn {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Message(Option<String>);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = Some(format!("{value:?}"));
                }
            }
        }
        let mut message = Message(None);
        event.record(&mut message);
        if message.0.as_deref() == Some(self.message) && self.armed.swap(false, Ordering::SeqCst) {
            panic!("injected panic logging {:?}", self.message);
        }
    }
}
