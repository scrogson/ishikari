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

#[ishikari::job]
struct Counts {
    n: u32,
}

#[ishikari::worker]
impl Worker for Counts {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[ishikari::job]
struct FailsDeep;

#[ishikari::worker(max_attempts = 1)]
impl Worker for FailsDeep {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        let root = anyhow::anyhow!("connection refused");
        Err(root
            .context("fetching the feed")
            .context("syncing town 7")
            .into())
    }
}

/// Insert a job row as another node's code would, with `args` as given.
async fn insert_raw(db: &TestDb, args: serde_json::Value) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO ishikari_jobs (queue, worker, args) VALUES ('default', $1, $2) RETURNING id",
    )
    .bind(args["ishikari_worker"].as_str().unwrap().to_string())
    .bind(args)
    .fetch_one(&db.pool)
    .await
    .unwrap()
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

#[tokio::test]
async fn a_job_of_a_worker_this_node_doesnt_know_waits_without_using_an_attempt() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // During a rolling deploy, a new node inserts a job of a worker the old nodes don't have.
    let id = insert_raw(
        &db,
        serde_json::json!({"ishikari_worker": "AddedInTheNextDeploy"}),
    )
    .await;
    let _engine = engine(&db);

    eventually("the job is put back", Duration::from_secs(5), || async {
        db.state(id).await == "scheduled"
    })
    .await;
    let (attempt, max_attempts, errors, later): (i32, i32, i64, bool) = sqlx::query_as(
        "SELECT attempt, max_attempts, cardinality(errors)::bigint, scheduled_at > now() \
         FROM ishikari_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(max_attempts - attempt, 20, "no attempt used");
    assert_eq!(errors, 0, "no error recorded");
    assert!(later, "it waits a while before it is tried again");

    db.finish().await;
}

#[tokio::test]
async fn a_job_whose_args_dont_fit_its_worker_fails() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let id = insert_raw(
        &db,
        serde_json::json!({"ishikari_worker": "Counts", "n": "many"}),
    )
    .await;
    let _engine = engine(&db);

    eventually(
        "the failure is recorded",
        Duration::from_secs(5),
        || async { db.state(id).await == "retryable" },
    )
    .await;
    let errors: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT errors FROM ishikari_jobs WHERE id = $1")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["kind"], "error");
    let error = errors[0]["error"].as_str().unwrap();
    assert!(error.contains("invalid type"), "{error}");

    db.finish().await;
}

#[tokio::test]
async fn an_error_is_recorded_with_its_sources() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(FailsDeep, &db.pool).await.unwrap();
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
    assert_eq!(
        errors[0]["error"],
        "syncing town 7: fetching the feed: connection refused"
    );

    db.finish().await;
}
