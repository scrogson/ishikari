//! Sagas: a saga that completes drops its compensations, a failed or cancelled step rolls the
//! completed steps back in reverse order in the transaction that ends it, a failed compensation
//! doesn't stop the rest, and parked compensations can't be run by hand.

mod common;

use chrono::{DateTime, Utc};
use common::{eventually, TestDb};
use ishikari::dependencies::DependencyResolver;
use ishikari::prelude::*;
use ishikari::workflows::Saga;
use ishikari::{Engine, Postgres, Queue};
use std::collections::HashMap;
use std::time::Duration;

#[ishikari::job]
#[derive(Clone)]
struct Book {
    millis: u64,
}

#[ishikari::worker]
impl Worker for Book {
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

#[ishikari::job]
#[derive(Clone)]
struct CancelsItself;

#[ishikari::worker]
impl Worker for CancelsItself {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Cancel::default().message("not today").into()
    }
}

/// Never runs: no engine in these tests works its queue.
#[ishikari::job]
#[derive(Clone)]
struct Elsewhere;

#[ishikari::worker(queue = "elsewhere")]
impl Worker for Elsewhere {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
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

/// An engine whose resolver sweeps once, at start: only the job statements themselves move a
/// saga on.
fn engine(pool: sqlx::PgPool) -> Engine {
    engine_sweeping(pool, Duration::from_secs(3600))
}

/// An engine whose resolver sweeps every `resolver`.
fn engine_sweeping(pool: sqlx::PgPool, resolver: Duration) -> Engine {
    Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .resolver_interval(resolver)
        .with_queue(
            Queue::builder("default")
                .concurrency(4)
                .interval(Duration::from_millis(20)),
        )
        .start(Postgres::new(pool))
}

/// A step's job: (id, state, attempt, attempted_at, completed_at, errors).
type JobRow = (
    i64,
    String,
    i32,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    Vec<serde_json::Value>,
);

/// Each job of the saga by step name ("1", "2", …, "compensate-1", …).
async fn jobs(db: &TestDb, workflow: i64) -> HashMap<String, JobRow> {
    type Named = (
        String,
        i64,
        String,
        i32,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        Vec<serde_json::Value>,
    );
    let rows: Vec<Named> = sqlx::query_as(
        "SELECT meta->>'ishikari_step', id, state::text, attempt, attempted_at, completed_at, \
                errors \
         FROM ishikari_jobs WHERE workflow_id = $1",
    )
    .bind(workflow)
    .fetch_all(&db.pool)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(name, id, state, attempt, at, done, errors)| {
            (name, (id, state, attempt, at, done, errors))
        })
        .collect()
}

/// The saga's step states, first step first.
async fn step_states(db: &TestDb, workflow: i64) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT state FROM ishikari_saga_steps WHERE workflow_id = $1 ORDER BY step_order",
    )
    .bind(workflow)
    .fetch_all(&db.pool)
    .await
    .unwrap()
}

async fn workflow_state(db: &TestDb, id: i64) -> String {
    sqlx::query_scalar("SELECT state FROM ishikari_workflows WHERE id = $1")
        .bind(id)
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

/// The saga's state once it has finished.
async fn finished(db: &TestDb, id: i64) -> String {
    eventually("the saga finishes", Duration::from_secs(10), || async {
        !matches!(
            workflow_state(db, id).await.as_str(),
            "running" | "compensating"
        )
    })
    .await;
    workflow_state(db, id).await
}

#[tokio::test]
async fn a_saga_whose_steps_all_complete_completes_and_drops_its_compensations() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Book { millis: 0 })
        .no_compensate()
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine(db.pool.clone());

    assert_eq!(finished(&db, wf.id).await, "completed");
    let j = jobs(&db, wf.id).await;
    for step in ["1", "2", "3"] {
        assert_eq!(j[step].1, "completed", "step {step}");
    }
    for comp in ["compensate-1", "compensate-2"] {
        assert_eq!(
            (j[comp].1.as_str(), j[comp].2),
            ("cancelled", 0),
            "{comp} never ran"
        );
    }
    assert_eq!(step_states(&db, wf.id).await, ["completed"; 3]);

    db.finish().await;
}

#[tokio::test]
async fn a_failed_step_rolls_the_saga_back_at_once_in_reverse_order() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(Book { millis: 50 })
        .step(Book { millis: 0 })
        .compensate(Book { millis: 50 })
        .step(Fails)
        .compensate(Book { millis: 0 })
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine(db.pool.clone());

    assert_eq!(finished(&db, wf.id).await, "compensated");
    let j = jobs(&db, wf.id).await;
    assert_eq!(j["3"].1, "discarded");
    assert_eq!(j["compensate-2"].1, "completed");
    assert_eq!(j["compensate-1"].1, "completed");
    assert!(
        j["compensate-1"].3.unwrap() >= j["compensate-2"].4.unwrap(),
        "step 2 is undone before step 1"
    );
    assert_eq!(
        (j["compensate-3"].1.as_str(), j["compensate-3"].2),
        ("cancelled", 0),
        "the failed step has nothing to undo"
    );
    assert_eq!(
        step_states(&db, wf.id).await,
        ["compensated", "compensated", "failed"]
    );

    db.finish().await;
}

#[tokio::test]
async fn a_parked_or_dropped_compensation_cannot_be_retried() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let storage = Postgres::new(db.pool.clone());
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .run(&db.pool)
        .await
        .unwrap();
    let comp = jobs(&db, wf.id).await["compensate-1"].0;

    // Before the saga runs, its compensation is parked.
    assert!(!storage.retry_job(comp).await.unwrap());
    assert_eq!(db.state(comp).await, "pending", "still parked");

    // Once it has completed, its compensation is dropped and stays so.
    let _engine = engine(db.pool.clone());
    assert_eq!(finished(&db, wf.id).await, "completed");
    assert!(!storage.retry_job(comp).await.unwrap());
    assert_eq!(db.state(comp).await, "cancelled");

    db.finish().await;
}

#[tokio::test]
async fn a_rolled_back_saga_cannot_be_retried_forward() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let storage = Postgres::new(db.pool.clone());
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Fails)
        .no_compensate()
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine_sweeping(db.pool.clone(), Duration::from_millis(100));
    assert_eq!(finished(&db, wf.id).await, "compensated");

    let failed = jobs(&db, wf.id).await["2"].0;
    assert!(!storage.retry_job(failed).await.unwrap());
    assert_eq!(db.state(failed).await, "discarded");
    assert_eq!(workflow_state(&db, wf.id).await, "compensated");

    db.finish().await;
}

#[tokio::test]
async fn a_failed_compensation_does_not_stop_the_rest() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Book { millis: 0 })
        .compensate(Fails)
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Fails)
        .no_compensate()
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine_sweeping(db.pool.clone(), Duration::from_millis(100));

    assert_eq!(
        finished(&db, wf.id).await,
        "failed",
        "the rollback is partial"
    );
    let j = jobs(&db, wf.id).await;
    assert_eq!(j["compensate-3"].1, "completed");
    assert_eq!(j["compensate-2"].1, "discarded");
    assert_eq!(j["compensate-1"].1, "completed", "step 1 is still undone");
    assert_eq!(
        step_states(&db, wf.id).await,
        [
            "compensated",
            "compensation_failed",
            "compensated",
            "failed"
        ]
    );

    db.finish().await;
}

#[tokio::test]
async fn retrying_a_failed_compensation_finishes_the_rollback() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let storage = Postgres::new(db.pool.clone());
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(FailsUntilFixed)
        .step(Fails)
        .no_compensate()
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine_sweeping(db.pool.clone(), Duration::from_millis(100));
    assert_eq!(finished(&db, wf.id).await, "failed");

    let comp: i64 = sqlx::query_scalar(
        "UPDATE ishikari_jobs SET meta = meta || '{\"fixed\": true}' \
         WHERE workflow_id = $1 AND meta->>'ishikari_step' = 'compensate-1' RETURNING id",
    )
    .bind(wf.id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(storage.retry_job(comp).await.unwrap());
    assert_eq!(finished(&db, wf.id).await, "compensated");
    assert_eq!(step_states(&db, wf.id).await, ["compensated", "failed"]);
    assert_eq!(jobs(&db, wf.id).await["2"].1, "discarded", "not run again");

    db.finish().await;
}

#[tokio::test]
async fn a_saga_without_compensations_whose_step_is_cancelled_is_rolled_back() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Saga::new("notify")
        .step(Book { millis: 0 })
        .no_compensate()
        .step(CancelsItself)
        .no_compensate()
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine_sweeping(db.pool.clone(), Duration::from_millis(100));

    assert_eq!(finished(&db, wf.id).await, "compensated");
    assert_eq!(step_states(&db, wf.id).await, ["completed", "failed"]);

    db.finish().await;
}

#[tokio::test]
async fn cancelling_a_saga_undoes_its_completed_steps() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let storage = Postgres::new(db.pool.clone());
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Elsewhere)
        .compensate(Book { millis: 0 })
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine_sweeping(db.pool.clone(), Duration::from_millis(100));
    eventually("step 1 completes", Duration::from_secs(10), || async {
        jobs(&db, wf.id).await["1"].1 == "completed"
    })
    .await;

    assert!(storage
        .cancel_saga(wf.id, Some("changed my mind"))
        .await
        .unwrap());
    assert_eq!(finished(&db, wf.id).await, "compensated");
    let j = jobs(&db, wf.id).await;
    assert_eq!(j["2"].1, "cancelled");
    assert_eq!(j["2"].5[0]["error"], "changed my mind");
    assert_eq!(j["compensate-1"].1, "completed", "step 1 is undone");
    assert_eq!(
        (j["compensate-2"].1.as_str(), j["compensate-2"].2),
        ("cancelled", 0),
        "step 2 never ran"
    );
    assert!(
        !storage.cancel_saga(wf.id, None).await.unwrap(),
        "a finished saga isn't cancelled"
    );

    db.finish().await;
}

#[tokio::test]
async fn cancelling_a_saga_waits_for_its_running_step_then_undoes_it() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let storage = Postgres::new(db.pool.clone());
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Book { millis: 500 })
        .compensate(Book { millis: 0 })
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine(db.pool.clone());
    eventually("step 2 runs", Duration::from_secs(10), || async {
        jobs(&db, wf.id).await["2"].1 == "executing"
    })
    .await;

    assert!(storage.cancel_saga(wf.id, None).await.unwrap());
    assert_eq!(workflow_state(&db, wf.id).await, "compensating");
    assert_eq!(finished(&db, wf.id).await, "compensated");
    let j = jobs(&db, wf.id).await;
    assert_eq!(j["2"].1, "completed", "the running step finished");
    assert_eq!((j["3"].1.as_str(), j["3"].2), ("cancelled", 0));
    assert_eq!(j["compensate-2"].1, "completed", "and was undone");
    assert!(j["compensate-1"].3.unwrap() >= j["compensate-2"].4.unwrap());
    assert_eq!(
        step_states(&db, wf.id).await,
        ["compensated", "compensated", "failed"]
    );

    db.finish().await;
}

#[tokio::test]
async fn a_sweep_during_a_rollback_leaves_it_alone() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Book { millis: 0 })
        .compensate(Book { millis: 1000 })
        .step(Fails)
        .no_compensate()
        .run(&db.pool)
        .await
        .unwrap();
    let _engine = engine_sweeping(db.pool.clone(), Duration::from_millis(100));
    eventually(
        "step 3's compensation runs",
        Duration::from_secs(10),
        || async { jobs(&db, wf.id).await["compensate-3"].1 == "executing" },
    )
    .await;

    // The steps before it wait their turn.
    let before = (step_states(&db, wf.id).await, jobs(&db, wf.id).await);
    assert_eq!(
        before.0,
        ["completed", "completed", "compensating", "failed"]
    );
    assert_eq!(before.1["compensate-2"].1, "pending");
    DependencyResolver::new(db.pool.clone(), Duration::from_secs(1))
        .resolve_once_public()
        .await
        .unwrap();
    assert_eq!(
        (step_states(&db, wf.id).await, jobs(&db, wf.id).await),
        before,
        "the sweep changed nothing"
    );

    assert_eq!(finished(&db, wf.id).await, "compensated");

    db.finish().await;
}

#[tokio::test]
async fn a_rollback_from_before_v07_carries_on_after_it() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Saga::new("book-trip")
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Book { millis: 0 })
        .compensate(Fails)
        .step(Book { millis: 0 })
        .compensate(Book { millis: 0 })
        .step(Book { millis: 0 })
        .no_compensate()
        .run(&db.pool)
        .await
        .unwrap();
    // Where an older ishikari left it: step 4 failed, every completed step marked compensating,
    // step 3's compensation released and the others parked in 3000, chained to run after it.
    let j = jobs(&db, wf.id).await;
    for (step, state) in [
        ("1", "completed"),
        ("2", "completed"),
        ("3", "completed"),
        ("4", "discarded"),
        ("compensate-3", "available"),
        ("compensate-2", "scheduled"),
        ("compensate-1", "scheduled"),
    ] {
        sqlx::query(
            "UPDATE ishikari_jobs SET state = $2::ishikari_job_state, \
                 scheduled_at = CASE WHEN $2 = 'scheduled' THEN '3000-01-01' ELSE now() END \
             WHERE id = $1",
        )
        .bind(j[step].0)
        .bind(state)
        .execute(&db.pool)
        .await
        .unwrap();
    }
    sqlx::query("UPDATE ishikari_job_dependencies SET state = 'satisfied'")
        .execute(&db.pool)
        .await
        .unwrap();
    for (comp, after) in [
        ("compensate-2", "compensate-3"),
        ("compensate-1", "compensate-2"),
    ] {
        sqlx::query(
            "INSERT INTO ishikari_job_dependencies (job_id, depends_on_job_id) VALUES ($1, $2)",
        )
        .bind(j[comp].0)
        .bind(j[after].0)
        .execute(&db.pool)
        .await
        .unwrap();
    }
    sqlx::query(
        "UPDATE ishikari_saga_steps SET state = CASE WHEN step_order < 3 THEN 'compensating' \
                                                    ELSE 'failed' END \
         WHERE workflow_id = $1",
    )
    .bind(wf.id)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE ishikari_workflows SET state = 'compensating' WHERE id = $1")
        .bind(wf.id)
        .execute(&db.pool)
        .await
        .unwrap();

    // v07 brings it in line.
    sqlx::raw_sql(&ishikari::migrations::sql(&db.schema, 6, 7))
        .execute(&db.pool)
        .await
        .unwrap();
    let _engine = engine(db.pool.clone());

    assert_eq!(
        finished(&db, wf.id).await,
        "failed",
        "the rollback is partial"
    );
    let j = jobs(&db, wf.id).await;
    assert_eq!(j["compensate-3"].1, "completed");
    assert_eq!(j["compensate-2"].1, "discarded");
    assert_eq!(j["compensate-1"].1, "completed", "step 1 is still undone");
    assert_eq!(
        step_states(&db, wf.id).await,
        [
            "compensated",
            "compensation_failed",
            "compensated",
            "failed"
        ]
    );

    db.finish().await;
}
