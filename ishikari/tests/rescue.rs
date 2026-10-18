//! A job whose node stops is given back to its queue; a job on a live node is left alone, however
//! long it runs.

mod common;

use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Engine, Postgres, Queue};
use std::sync::Arc;
use std::time::Duration;

/// Hangs on its first attempt, as if its node died mid-run; succeeds after.
#[ishikari::job]
struct Stalls;

#[ishikari::worker]
impl Worker for Stalls {
    async fn perform(&self, ctx: Context) -> PerformResult {
        if ctx.job().attempt == 1 {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        Complete::default().into()
    }
}

#[ishikari::job]
struct Long;

#[ishikari::worker]
impl Worker for Long {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        tokio::time::sleep(Duration::from_millis(800)).await;
        Complete::default().into()
    }
}

fn fast(name: &str) -> ishikari::EngineBuilder {
    Engine::builder(name)
        .stager_interval(Duration::from_millis(20))
        .heartbeat_interval(Duration::from_millis(50))
        .node_timeout(Duration::from_millis(300))
        .rescue_interval(Duration::from_millis(50))
        .with_queue(Queue::builder("default").interval(Duration::from_millis(20)))
}

#[tokio::test]
async fn a_job_left_by_a_stopped_node_runs_again_elsewhere() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(Stalls, &db.pool).await.unwrap();

    // Node A, in a runtime of its own, takes the job, then dies with it.
    let crash = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (url, schema) = (db.url.clone(), db.schema.clone());
    crash.spawn(async move {
        let pool = TestDb::pool_for(&url, &schema).await;
        let _a = fast("a").node_name("node-a").start(Postgres::new(pool));
        std::future::pending::<()>().await;
    });
    eventually("node A runs the job", Duration::from_secs(5), || async {
        db.state(job.id).await == "executing"
    })
    .await;
    crash.shutdown_background();

    // Node B notices A's silence and runs the job again.
    let _b = fast("b")
        .node_name("node-b")
        .start(Postgres::new(db.pool.clone()));
    eventually(
        "the job completes on node B",
        Duration::from_secs(10),
        || async { db.state(job.id).await == "completed" },
    )
    .await;

    let (attempt, by, errors): (i32, Vec<String>, Vec<serde_json::Value>) =
        sqlx::query_as("SELECT attempt, attempted_by, errors FROM ishikari_jobs WHERE id = $1")
            .bind(job.id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(attempt, 2);
    assert_eq!(by, vec!["node-a", "node-b"]);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["kind"], "rescued");
    assert_eq!(errors[0]["attempt"], 1);
    assert_eq!(
        errors[0]["error"],
        "rescued: node node-a stopped heartbeating"
    );

    db.finish().await;
}

#[tokio::test]
async fn a_long_job_on_a_live_node_is_not_rescued() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(Long, &db.pool).await.unwrap();
    // Runs for 800 ms against a 300 ms node timeout; the heartbeat keeps it owned.
    let _engine = fast("live").start(Postgres::new(db.pool.clone()));

    eventually("the job completes", Duration::from_secs(5), || async {
        db.state(job.id).await == "completed"
    })
    .await;
    let (attempt, errors): (i32, i64) = sqlx::query_as(
        "SELECT attempt, cardinality(errors)::bigint FROM ishikari_jobs WHERE id = $1",
    )
    .bind(job.id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!((attempt, errors), (1, 0));

    db.finish().await;
}

#[tokio::test]
async fn a_long_job_on_a_standalone_queue_is_not_rescued() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(Long, &db.pool).await.unwrap();
    // An engine that only rescues, with a node timeout well under the job's 800 ms.
    let _rescuer = Engine::builder("rescuer")
        .heartbeat_interval(Duration::from_millis(50))
        .node_timeout(Duration::from_millis(300))
        .rescue_interval(Duration::from_millis(50))
        .start(Postgres::new(db.pool.clone()));
    Queue::builder("default")
        .interval(Duration::from_millis(20))
        .build(Arc::new(Postgres::new(db.pool.clone())), Arc::new(()))
        .heartbeat_interval(Duration::from_millis(50))
        .start();

    eventually("the job completes", Duration::from_secs(5), || async {
        db.state(job.id).await == "completed"
    })
    .await;
    let (attempt, errors, by): (i32, i64, Vec<String>) = sqlx::query_as(
        "SELECT attempt, cardinality(errors)::bigint, attempted_by FROM ishikari_jobs WHERE id = $1",
    )
    .bind(job.id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!((attempt, errors), (1, 0), "run once, never rescued");
    let queues: Vec<String> =
        sqlx::query_scalar("SELECT queues FROM ishikari_nodes WHERE name = $1")
            .bind(&by[0])
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(queues, vec!["default"], "the queue's node heartbeats");

    db.finish().await;
}
