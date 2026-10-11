//! A queue never runs more jobs at once than its concurrency, and the stager outlives errors.

mod common;

use chrono::Utc;
use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Engine, Postgres, Queue, Stager};
use std::sync::Arc;
use std::time::Duration;

#[ishikari::job]
struct Nap;

#[ishikari::worker]
impl Worker for Nap {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        tokio::time::sleep(Duration::from_millis(150)).await;
        Complete::default().into()
    }
}

async fn count(db: &TestDb, state: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM ishikari_jobs WHERE state::text = $1")
        .bind(state)
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_queue_runs_at_most_its_concurrency() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    for _ in 0..10 {
        ishikari::insert(Nap, &db.pool).await.unwrap();
    }
    let _engine = Engine::builder("test")
        .with_queue(
            Queue::builder("default")
                .concurrency(2)
                .interval(Duration::from_millis(10)),
        )
        .start(Postgres::new(db.pool.clone()));

    let mut most = 0;
    while count(&db, "completed").await < 10 {
        most = most.max(count(&db, "executing").await);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(most <= 2, "{most} jobs ran at once with concurrency 2");
    assert_eq!(most, 2, "both slots were used");

    db.finish().await;
}

#[tokio::test]
async fn the_stager_keeps_going_after_errors() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // A schema that doesn't exist yet: every staging call fails until it does.
    let missing = format!("{}_later", db.schema);
    let storage = Arc::new(Postgres::new(db.plain_pool().await).with_schema(missing.clone()));
    let _stager = Stager::new(storage, Duration::from_millis(20), 10).start();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let admin = db.plain_pool().await;
    ishikari::migrations::migrate(&admin, &missing)
        .await
        .unwrap();
    let id: i64 = sqlx::query_scalar(&format!(
        "INSERT INTO {missing}.ishikari_jobs (worker, state, scheduled_at) \
         VALUES ('w', 'scheduled', $1) RETURNING id"
    ))
    .bind(Utc::now())
    .fetch_one(&admin)
    .await
    .unwrap();

    eventually(
        "the stager makes it available",
        Duration::from_secs(5),
        || {
            let admin = admin.clone();
            let missing = missing.clone();
            async move {
                let state: String = sqlx::query_scalar(&format!(
                    "SELECT state::text FROM {missing}.ishikari_jobs WHERE id = $1"
                ))
                .bind(id)
                .fetch_one(&admin)
                .await
                .unwrap();
                state == "available"
            }
        },
    )
    .await;

    sqlx::query(&format!("DROP SCHEMA {missing} CASCADE"))
        .execute(&admin)
        .await
        .unwrap();
    db.finish().await;
}
