//! Workflows: steps run after the steps they depend on, failures cancel what depends on them,
//! a workflow is inserted atomically, and its progress can be read.

mod common;

use chrono::{DateTime, Utc};
use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::workflows::{Dag, Pipeline, Saga, Workflow, WorkflowState};
use ishikari::{Engine, Postgres, Queue};
use std::time::Duration;

#[ishikari::job]
#[derive(Clone)]
struct Step {
    millis: u64,
}

#[ishikari::worker]
impl Worker for Step {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        tokio::time::sleep(Duration::from_millis(self.millis)).await;
        Complete::default().into()
    }
}

#[ishikari::job]
#[derive(Clone)]
struct Fails;

#[ishikari::worker(max_attempts = 1)]
impl Worker for Fails {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Err("step failed".into())
    }
}

fn engine(name: &str, pool: sqlx::PgPool) -> Engine {
    Engine::builder(name)
        .stager_interval(Duration::from_millis(20))
        .resolver_interval(Duration::from_millis(100))
        .with_queue(
            Queue::builder("default")
                .concurrency(4)
                .interval(Duration::from_millis(20)),
        )
        .start(Postgres::new(pool))
}

/// A step's (state, attempt, attempted_at, completed_at).
type StepRow = (String, i32, Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// Each step's row, by step name.
async fn steps(db: &TestDb, workflow: i64) -> std::collections::HashMap<String, StepRow> {
    type Named = (
        String,
        String,
        i32,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
    );
    let rows: Vec<Named> = sqlx::query_as(
        "SELECT meta->>'ishikari_step', state::text, attempt, attempted_at, completed_at \
             FROM ishikari_jobs WHERE workflow_id = $1",
    )
    .bind(workflow)
    .fetch_all(&db.pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(name, state, attempt, at, done)| (name, (state, attempt, at, done)))
        .collect()
}

async fn workflow_state(db: &TestDb, id: i64) -> String {
    sqlx::query_scalar("SELECT state FROM ishikari_workflows WHERE id = $1")
        .bind(id)
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn dag_steps_wait_for_their_upstreams() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Dag::new("diamond")
        .add("a", Step { millis: 300 })
        .add_after("b", Step { millis: 50 }, ["a"])
        .add_after("c", Step { millis: 50 }, ["a"])
        .add_after("d", Step { millis: 0 }, ["b", "c"])
        .run(&db.pool)
        .await
        .unwrap();
    let s = steps(&db, wf.id).await;
    assert_eq!(s["b"].0, "pending", "waiting on a");
    assert_eq!(s["a"].0, "available");

    // No dependency resolver is started by hand: completion releases the next steps.
    let _engine = engine("test", db.pool.clone());
    eventually(
        "the workflow completes",
        Duration::from_secs(10),
        || async { workflow_state(&db, wf.id).await == "completed" },
    )
    .await;

    let s = steps(&db, wf.id).await;
    let started = |n: &str| s[n].2.unwrap();
    let done = |n: &str| s[n].3.unwrap();
    assert!(
        started("b") >= done("a") && started("c") >= done("a"),
        "b and c after a"
    );
    assert!(started("d") >= done("b").max(done("c")), "d after b and c");
    assert!(s
        .values()
        .all(|(state, attempt, ..)| state == "completed" && *attempt == 1));

    db.finish().await;
}

#[tokio::test]
async fn a_failed_step_cancels_what_depends_on_it() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Dag::new("breaks")
        .add("a", Step { millis: 0 })
        .add_after("b", Fails, ["a"])
        .add_after("c", Step { millis: 0 }, ["b"])
        .add_after("d", Step { millis: 0 }, ["c"])
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine("test", db.pool.clone());

    eventually("the workflow fails", Duration::from_secs(10), || async {
        workflow_state(&db, wf.id).await == "failed"
    })
    .await;
    let s = steps(&db, wf.id).await;
    assert_eq!(s["a"].0, "completed");
    assert_eq!(s["b"].0, "discarded");
    assert_eq!(
        (s["c"].0.as_str(), s["d"].0.as_str()),
        ("cancelled", "cancelled")
    );

    db.finish().await;
}

#[tokio::test]
async fn two_engines_release_a_step_once() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // Two upstreams finishing at about the same time on different engines.
    let wf = Dag::new("join")
        .add("a", Step { millis: 100 })
        .add("b", Step { millis: 100 })
        .add_after("c", Step { millis: 0 }, ["a", "b"])
        .run(&db.pool)
        .await
        .unwrap();
    let _one = engine("one", db.pool.clone());
    let _two = engine("two", db.another_pool().await);

    eventually(
        "the workflow completes",
        Duration::from_secs(10),
        || async { workflow_state(&db, wf.id).await == "completed" },
    )
    .await;
    let s = steps(&db, wf.id).await;
    assert_eq!(
        (s["c"].0.as_str(), s["c"].1),
        ("completed", 1),
        "c ran once"
    );

    db.finish().await;
}

#[tokio::test]
async fn a_workflow_inserted_in_a_rolled_back_transaction_leaves_nothing() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let mut tx = db.pool.begin().await.unwrap();
    Pipeline::new("rolled-back")
        .step(Step { millis: 0 })
        .step(Step { millis: 0 })
        .run(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let (workflows, jobs, deps): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM ishikari_workflows), (SELECT count(*) FROM ishikari_jobs), \
                (SELECT count(*) FROM ishikari_job_dependencies)",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!((workflows, jobs, deps), (0, 0, 0));

    db.finish().await;
}

#[tokio::test]
async fn progress_shows_each_step() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Dag::new("prepare-town")
        .add("directory", Step { millis: 0 })
        .add_after("sources", Fails, ["directory"])
        .add_after("engine", Step { millis: 0 }, ["sources"])
        .add("hero", Step { millis: 0 })
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine("test", db.pool.clone());
    eventually("the workflow fails", Duration::from_secs(10), || async {
        workflow_state(&db, wf.id).await == "failed"
    })
    .await;

    let progress = Workflow::progress(&db.pool, wf.id, None)
        .await
        .unwrap()
        .expect("the workflow exists");
    assert_eq!(progress.workflow.name, "prepare-town");
    assert_eq!(progress.workflow.state, WorkflowState::Failed);
    let names: Vec<&str> = progress.steps.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["directory", "sources", "engine", "hero"],
        "in insert order"
    );
    let sources = &progress.steps[1];
    assert_eq!(sources.state, ishikari::JobState::Discarded);
    assert_eq!(sources.depends_on, vec!["directory".to_string()]);
    assert_eq!(sources.errors[0]["error"], "step failed");
    assert_eq!(progress.steps[2].state, ishikari::JobState::Cancelled);
    assert_eq!(progress.steps[3].state, ishikari::JobState::Completed);

    db.finish().await;
}

#[tokio::test]
async fn a_failed_saga_step_runs_the_compensations_of_the_steps_before_it() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Saga::new("book-trip")
        .step(Step { millis: 0 })
        .compensate(Step { millis: 0 })
        .step(Fails)
        .no_compensate()
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine("test", db.pool.clone());

    eventually(
        "the saga is compensated",
        Duration::from_secs(10),
        || async { workflow_state(&db, wf.id).await == "compensated" },
    )
    .await;
    let s = steps(&db, wf.id).await;
    assert_eq!(s["1"].0, "completed");
    assert_eq!(s["2"].0, "discarded");
    assert_eq!(s["compensate-1"].0, "completed", "step 1 was undone");

    db.finish().await;
}

#[ishikari::job]
#[derive(Clone)]
struct FailsUntilFixed;

#[ishikari::worker(max_attempts = 1)]
impl Worker for FailsUntilFixed {
    async fn perform(&self, ctx: Context) -> PerformResult {
        // Fails until the test "fixes" it by setting meta.fixed.
        if ctx
            .job()
            .meta
            .as_ref()
            .and_then(|m| m.get("fixed"))
            .is_none()
        {
            return Err("not yet".into());
        }
        Complete::default().into()
    }
}

#[tokio::test]
async fn retrying_a_failed_step_revives_what_it_cancelled() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Dag::new("retry")
        .add("a", FailsUntilFixed)
        .add_after("b", Step { millis: 0 }, ["a"])
        .add_after("c", Step { millis: 0 }, ["b"])
        .run(&db.pool)
        .await
        .unwrap();
    let engine = engine("test", db.pool.clone());
    eventually("the workflow fails", Duration::from_secs(10), || async {
        workflow_state(&db, wf.id).await == "failed"
    })
    .await;
    assert_eq!(steps(&db, wf.id).await["c"].0, "cancelled");

    let a: i64 = sqlx::query_scalar(
        "UPDATE ishikari_jobs SET meta = meta || '{\"fixed\": true}' \
         WHERE workflow_id = $1 AND meta->>'ishikari_step' = 'a' RETURNING id",
    )
    .bind(wf.id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(engine.retry_job(a).await.unwrap());
    eventually(
        "the workflow completes",
        Duration::from_secs(10),
        || async { workflow_state(&db, wf.id).await == "completed" },
    )
    .await;
    let s = steps(&db, wf.id).await;
    assert!(s.values().all(|(state, ..)| state == "completed"));

    db.finish().await;
}
