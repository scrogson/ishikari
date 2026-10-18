//! Jobs start as soon as there is room for them, not at a queue's next poll: a slot is refilled
//! when its job finishes, and queues are woken when jobs become available.

mod common;

use chrono::Utc;
use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Engine, InsertOpts, Postgres, Queue};
use std::time::Duration;

#[ishikari::job]
struct Quick;

#[ishikari::worker]
impl Worker for Quick {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

/// Longer than any test waits, so a job that runs was not found by polling.
const NEVER: Duration = Duration::from_secs(60);
const SOON: Duration = Duration::from_secs(5);

async fn completed(db: &TestDb) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM ishikari_jobs WHERE state = 'completed'")
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

/// Wait until an engine on `db` listens for jobs becoming available.
async fn listening(db: &TestDb) {
    eventually("the engine listens", SOON, || async {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
             WHERE query LIKE 'LISTEN %' AND position($1 in query) > 0)",
        )
        .bind(format!("{}.ishikari_insert", db.schema))
        .fetch_one(&db.pool)
        .await
        .unwrap()
    })
    .await;
}

#[tokio::test]
async fn a_freed_slot_is_refilled_without_waiting_for_the_poll() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    for _ in 0..5 {
        ishikari::insert(Quick, &db.pool).await.unwrap();
    }
    // The first poll is at start; with one slot, the rest must be claimed as each finishes.
    let _engine = Engine::builder("test")
        .with_queue(Queue::builder("default").concurrency(1).interval(NEVER))
        .start(Postgres::new(db.pool.clone()));

    eventually("all five jobs ran", SOON, || async {
        completed(&db).await == 5
    })
    .await;

    db.finish().await;
}

#[tokio::test]
async fn an_inserted_job_runs_without_waiting_for_the_poll() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let _engine = Engine::builder("test")
        .with_queue(Queue::builder("default").interval(NEVER))
        .start(Postgres::new(db.pool.clone()));
    listening(&db).await;

    ishikari::insert(Quick, &db.pool).await.unwrap();
    eventually("the job ran", SOON, || async { completed(&db).await == 1 }).await;

    db.finish().await;
}

#[tokio::test]
async fn a_staged_job_runs_without_waiting_for_the_poll() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let _engine = Engine::builder("test")
        .stager_interval(Duration::from_millis(50))
        .with_queue(Queue::builder("default").interval(NEVER))
        .start(Postgres::new(db.pool.clone()));
    listening(&db).await;

    let at = Utc::now() + chrono::Duration::milliseconds(200);
    ishikari::insert_with(Quick, InsertOpts::new().schedule_at(at), &db.pool)
        .await
        .unwrap();
    eventually("the job ran", SOON, || async { completed(&db).await == 1 }).await;

    db.finish().await;
}

#[tokio::test]
async fn without_listening_queues_find_jobs_by_polling() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let _engine = Engine::builder("test")
        .listen(false)
        .with_queue(Queue::builder("default").interval(Duration::from_millis(50)))
        .start(Postgres::new(db.pool.clone()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    let listeners: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
         WHERE query LIKE 'LISTEN %' AND position($1 in query) > 0)",
    )
    .bind(format!("{}.ishikari_insert", db.schema))
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(!listeners, "the engine doesn't listen");

    ishikari::insert(Quick, &db.pool).await.unwrap();
    eventually("the job ran", SOON, || async { completed(&db).await == 1 }).await;

    db.finish().await;
}

#[tokio::test]
async fn a_schema_upgraded_from_v5_wakes_queues() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let schema = format!("{}_v5", db.schema);
    let admin = db.plain_pool().await;
    sqlx::raw_sql(&ishikari::migrations::sql(&schema, 0, 5))
        .execute(&admin)
        .await
        .unwrap();
    assert_eq!(
        ishikari::migrations::migrate(&admin, &schema)
            .await
            .unwrap(),
        5
    );

    let engine = Engine::builder("test")
        .schema(&schema)
        .stager_interval(Duration::from_millis(50))
        .with_queue(Queue::builder("default").interval(NEVER))
        .start(Postgres::new(admin.clone()));
    eventually("the engine listens", SOON, || async {
        sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
             WHERE query LIKE 'LISTEN %' AND position($1 in query) > 0)",
        )
        .bind(format!("{schema}.ishikari_insert"))
        .fetch_one(&admin)
        .await
        .unwrap()
    })
    .await;

    let opts = || InsertOpts::new().schema(&schema);
    ishikari::insert_with(Quick, opts(), &admin).await.unwrap();
    let at = Utc::now() + chrono::Duration::milliseconds(200);
    ishikari::insert_with(Quick, opts().schedule_at(at), &admin)
        .await
        .unwrap();
    eventually("both jobs ran", SOON, || async {
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {schema}.ishikari_jobs WHERE state = 'completed'"
        ))
        .fetch_one(&admin)
        .await
        .unwrap()
            == 2
    })
    .await;

    engine.shutdown(Duration::ZERO).await;
    sqlx::raw_sql(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    db.finish().await;
}
