//! Timeouts, and stopping an engine without leaving jobs behind.

mod common;

use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Engine, Postgres, Queue, Shutdown};
use std::time::{Duration, Instant};

#[ishikari::job]
struct Sleeps {
    millis: u64,
}

#[ishikari::worker]
impl Worker for Sleeps {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        Complete::default().into()
    }
}

#[ishikari::job]
struct Hangs;

#[ishikari::worker(max_attempts = 1, timeout = "200ms")]
impl Worker for Hangs {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Complete::default().into()
    }
}

fn engine(db: &TestDb) -> Engine {
    Engine::builder("test")
        .node_name("stopping")
        .stager_interval(Duration::from_millis(20))
        .with_queue(Queue::builder("default").interval(Duration::from_millis(20)))
        .start(Postgres::new(db.pool.clone()))
}

async fn started(db: &TestDb, id: i64) {
    eventually("the job starts", Duration::from_secs(5), || async {
        db.state(id).await == "executing"
    })
    .await;
}

async fn nodes(db: &TestDb) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM ishikari_nodes")
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn an_attempt_past_its_timeout_fails() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(Hangs, &db.pool).await.unwrap();
    let _engine = engine(&db);

    eventually("the job is discarded", Duration::from_secs(5), || async {
        db.state(job.id).await == "discarded"
    })
    .await;
    let errors: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT errors FROM ishikari_jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(errors[0]["kind"], "timeout");
    assert_eq!(errors[0]["error"], "timed out after 200ms");

    db.finish().await;
}

#[tokio::test]
async fn shutdown_waits_for_running_jobs() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(Sleeps { millis: 200 }, &db.pool)
        .await
        .unwrap();
    let engine = engine(&db);
    started(&db, job.id).await;
    assert_eq!(nodes(&db).await, 1);

    let report = engine.shutdown(Duration::from_secs(5)).await;
    assert_eq!(
        report,
        Shutdown {
            finished: 1,
            returned: 0
        }
    );
    assert_eq!(db.state(job.id).await, "completed");
    assert_eq!(nodes(&db).await, 0, "the node's row is removed");

    db.finish().await;
}

#[tokio::test]
async fn shutdown_gives_back_what_outlasts_the_grace() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(Sleeps { millis: 10_000 }, &db.pool)
        .await
        .unwrap();
    let engine = engine(&db);
    started(&db, job.id).await;

    let start = Instant::now();
    let report = engine.shutdown(Duration::from_millis(200)).await;
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "it didn't wait for the job"
    );
    assert_eq!(
        report,
        Shutdown {
            finished: 0,
            returned: 1
        }
    );
    let (state, attempt): (String, i32) =
        sqlx::query_as("SELECT state::text, attempt FROM ishikari_jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(
        (state.as_str(), attempt),
        ("available", 0),
        "back, attempt refunded"
    );

    // Nothing of the engine is left running: a new job isn't picked up.
    let later = ishikari::insert(Sleeps { millis: 1 }, &db.pool)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(db.state(later.id).await, "available");

    db.finish().await;
}
