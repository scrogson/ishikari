//! How a run's result is written back: only to the attempt it belongs to, a last failed
//! attempt goes straight to discarded, and neither a worker's panicking hooks nor a write that
//! fails for a while leave the job executing.

mod common;

use chrono::{DateTime, Utc};
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

#[ishikari::job]
struct Quick;

#[ishikari::worker]
impl Worker for Quick {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[ishikari::job]
struct BadBackoff;

#[ishikari::worker(max_attempts = 2)]
impl Worker for BadBackoff {
    fn backoff(&self, _attempt: i32) -> DateTime<Utc> {
        panic!("no backoff")
    }

    async fn perform(&self, _ctx: Context) -> PerformResult {
        Err("nope".into())
    }
}

#[ishikari::job]
struct BadTimeout;

#[ishikari::worker(max_attempts = 2)]
impl Worker for BadTimeout {
    fn timeout(&self) -> Option<Duration> {
        panic!("no timeout")
    }

    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
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
    assert_eq!(errors.len(), 1);
    assert_eq!(
        (errors[0]["kind"].as_str(), errors[0]["error"].as_str()),
        (Some("error"), Some("nope"))
    );
    assert!(discarded);

    db.finish().await;
}

#[tokio::test]
async fn a_worker_whose_backoff_or_timeout_panics_is_still_recorded() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let backoff = ishikari::insert(BadBackoff, &db.pool).await.unwrap();
    let timeout = ishikari::insert(BadTimeout, &db.pool).await.unwrap();
    let _engine = engine(&db);

    for (job, kind) in [(backoff.id, "error"), (timeout.id, "panic")] {
        eventually(
            "the failure is recorded",
            Duration::from_secs(5),
            || async { db.state(job).await == "retryable" },
        )
        .await;
        let errors: Vec<serde_json::Value> =
            sqlx::query_scalar("SELECT errors FROM ishikari_jobs WHERE id = $1")
                .bind(job)
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(errors[0]["kind"].as_str(), Some(kind));
    }

    db.finish().await;
}

#[tokio::test]
async fn an_outcome_write_that_fails_for_a_while_is_retried() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // The database refuses to complete jobs for a while.
    sqlx::query(
        "ALTER TABLE ishikari_jobs ADD CONSTRAINT no_completing \
         CHECK (state <> 'completed') NOT VALID",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let job = ishikari::insert(Quick, &db.pool).await.unwrap();
    let _engine = engine(&db);
    eventually("the job runs", Duration::from_secs(5), || async {
        db.state(job.id).await == "executing"
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(db.state(job.id).await, "executing");

    sqlx::query("ALTER TABLE ishikari_jobs DROP CONSTRAINT no_completing")
        .execute(&db.pool)
        .await
        .unwrap();
    eventually(
        "the completion is written",
        Duration::from_secs(10),
        || async { db.state(job.id).await == "completed" },
    )
    .await;

    db.finish().await;
}
