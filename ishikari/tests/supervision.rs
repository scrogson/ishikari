//! An engine's background tasks outlive their panics: each is restarted, and a queue keeps the
//! jobs it was running.

mod common;

use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Cron, Engine, Postgres, Queue};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[ishikari::job]
struct Tick;

#[ishikari::worker]
impl Worker for Tick {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[ishikari::job]
struct Slow;

#[ishikari::worker]
impl Worker for Slow {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        tokio::time::sleep(Duration::from_millis(400)).await;
        Complete::default().into()
    }
}

async fn finished_once(db: &TestDb, id: i64) -> bool {
    let (state, attempt, errors): (String, i32, i64) = sqlx::query_as(
        "SELECT state::text, attempt, cardinality(errors)::bigint FROM ishikari_jobs WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(
        attempt <= 1 && errors == 0,
        "the job ran again: {attempt} attempts"
    );
    state == "completed"
}

#[tokio::test]
async fn a_queue_that_panics_is_restarted_and_keeps_its_running_jobs() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (_logs, armed) = common::panic_on("polling jobs");
    let job = ishikari::insert(Slow, &db.pool).await.unwrap();
    let _engine = Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .with_queue(Queue::builder("default").interval(Duration::from_millis(20)))
        .start(Postgres::new(db.pool.clone()));
    eventually("the job starts", Duration::from_secs(5), || async {
        db.state(job.id).await == "executing"
    })
    .await;

    // The queue's loop panics at its next poll, while the job runs.
    armed.store(true, Ordering::SeqCst);
    eventually("the queue panics", Duration::from_secs(5), || async {
        !armed.load(Ordering::SeqCst)
    })
    .await;
    eventually("the running job finishes", Duration::from_secs(5), || {
        finished_once(&db, job.id)
    })
    .await;
    let later = ishikari::insert(Slow, &db.pool).await.unwrap();
    eventually("the queue runs new jobs", Duration::from_secs(10), || {
        finished_once(&db, later.id)
    })
    .await;

    db.finish().await;
}

static PANICKED: AtomicBool = AtomicBool::new(false);

#[tokio::test]
async fn a_cron_task_that_panics_is_restarted() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // Making the first slot's job panics.
    let cron = Cron::new("flaky", "* * * * * *", || {
        if !PANICKED.swap(true, Ordering::SeqCst) {
            panic!("making the job panicked");
        }
        Tick
    })
    .unwrap();
    let _engine = Engine::builder("test")
        .cron_interval(Duration::from_millis(100))
        .cron_catch_up(Duration::from_secs(3))
        .cron(cron)
        .start(Postgres::new(db.pool.clone()));

    eventually("a slot is inserted", Duration::from_secs(10), || async {
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM ishikari_jobs WHERE meta->>'cron' = 'flaky'",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap()
            > 0
    })
    .await;
    assert!(PANICKED.load(Ordering::SeqCst));

    db.finish().await;
}
