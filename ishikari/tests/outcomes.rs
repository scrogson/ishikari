//! How a run's result is written back: only to the attempt it belongs to, and a last failed
//! attempt goes straight to discarded.

mod common;

use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Engine, Postgres, Queue};
use std::time::Duration;

#[ishikari::job]
struct Slow;

#[ishikari::worker]
impl Worker for Slow {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        tokio::time::sleep(Duration::from_millis(400)).await;
        Complete::default().into()
    }
}

#[ishikari::job]
struct AlwaysFails;

#[ishikari::worker(max_attempts = 1)]
impl Worker for AlwaysFails {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Err("nope".into())
    }
}

fn engine(db: &TestDb) -> Engine {
    Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .with_queue(Queue::builder("default").interval(Duration::from_millis(20)))
        .start(Postgres::new(db.pool.clone()))
}

#[tokio::test]
async fn a_job_changed_while_it_runs_keeps_its_new_state() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(Slow, &db.pool).await.unwrap();
    let _engine = engine(&db);

    eventually("the job starts", Duration::from_secs(5), || async {
        db.state(job.id).await == "executing"
    })
    .await;
    // Someone cancels it from the admin while the run is still going.
    sqlx::query("UPDATE ishikari_jobs SET state = 'cancelled', cancelled_at = now() WHERE id = $1")
        .bind(job.id)
        .execute(&db.pool)
        .await
        .unwrap();

    // The run finishes ~400 ms after starting; its completion must not land.
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(db.state(job.id).await, "cancelled");

    db.finish().await;
}

#[tokio::test]
async fn the_last_failed_attempt_is_discarded_with_its_error() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(AlwaysFails, &db.pool).await.unwrap();
    let _engine = engine(&db);

    eventually("the job is discarded", Duration::from_secs(5), || async {
        db.state(job.id).await == "discarded"
    })
    .await;
    let (attempt, errors, discarded): (i32, Vec<serde_json::Value>, bool) = sqlx::query_as(
        "SELECT attempt, errors, discarded_at IS NOT NULL FROM ishikari_jobs WHERE id = $1",
    )
    .bind(job.id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(attempt, 1);
    assert_eq!(errors, vec![serde_json::json!("nope")]);
    assert!(discarded);

    db.finish().await;
}
