//! A migrated schema of a test's own and requests against the admin.
//!
//! Uses `ISHIKARI_TEST_DATABASE_URL`, else `DATABASE_URL`; `ISHIKARI_SKIP_DB_TESTS=1` skips.

#![allow(dead_code)]

use axum::body::Body;
use axum::http::Request;
use axum::response::Response;
use axum::Router;
use sqlx::PgPool;
use std::future::Future;
use std::time::{Duration, Instant};
use tower::ServiceExt;

pub struct TestDb {
    pub pool: PgPool,
    pub schema: String,
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
        let pool = PgPool::connect(&url).await.expect("connect");
        let schema = format!(
            "ishikari_admin_t_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        ishikari::migrations::migrate(&pool, &schema)
            .await
            .expect("migrate the test schema");
        Some(TestDb { pool, schema })
    }

    /// A job of `worker` in `state`, written directly.
    pub async fn job(&self, worker: &str, state: &str) {
        sqlx::query(&format!(
            "INSERT INTO {s}.ishikari_jobs (worker, state) VALUES ($1, $2::{s}.ishikari_job_state)",
            s = self.schema
        ))
        .bind(worker)
        .bind(state)
        .execute(&self.pool)
        .await
        .expect("insert a job");
    }

    /// A job of `worker` that failed for good, with `error` its last attempt's error record.
    pub async fn discarded(&self, worker: &str, error: &str) -> i64 {
        let record = serde_json::json!({
            "attempt": 1, "at": "2026-10-11T12:00:00Z", "kind": "error", "error": error
        });
        sqlx::query_scalar(&format!(
            "INSERT INTO {s}.ishikari_jobs (worker, state, attempt, max_attempts, errors, attempted_at, discarded_at)
             VALUES ($1, 'discarded', 1, 1, ARRAY[$2::jsonb], now(), now()) RETURNING id",
            s = self.schema
        ))
        .bind(worker)
        .bind(record)
        .fetch_one(&self.pool)
        .await
        .expect("insert a discarded job")
    }

    pub async fn drop(self) {
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.pool)
            .await
            .expect("drop the test schema");
    }
}

/// GET `path` with these headers.
pub async fn get(app: &Router, path: &str, headers: &[(&str, &str)]) -> Response {
    let mut req = Request::get(path);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    app.clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// POST to `path`, with no body.
pub async fn post(app: Router, path: String) -> Response {
    app.oneshot(Request::post(path).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// POST a form (`application/x-www-form-urlencoded`) to `path`.
pub async fn post_form(app: &Router, path: &str, form: &[(&str, &str)]) -> Response {
    let body = form
        .iter()
        .map(|(k, v)| format!("{k}={}", urlencode(v)))
        .collect::<Vec<_>>()
        .join("&");
    app.clone()
        .oneshot(
            Request::post(path)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
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

pub async fn body(res: Response) -> String {
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}
