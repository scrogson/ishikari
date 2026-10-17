//! Cron slots run once across nodes, and a starting node catches up on recent slots.

mod common;

use chrono::{Duration as ChronoDuration, Utc};
use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Cron, Engine, Postgres, Queue};
use std::sync::Arc;
use std::time::Duration;

#[ishikari::job]
struct Tick;

#[ishikari::worker]
impl Worker for Tick {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

async fn slots(db: &TestDb) -> (i64, i64) {
    sqlx::query_as(
        "SELECT count(*), count(DISTINCT meta->>'slot') FROM ishikari_jobs WHERE meta->>'cron' = 'tick'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn two_nodes_insert_each_slot_once() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let cron = Cron::new("tick", "* * * * *", || Tick).unwrap();
    let storage = Arc::new(Postgres::new(db.pool.clone()));
    let now = Utc::now();
    let due = cron.due(now - ChronoDuration::minutes(3), now);
    assert_eq!(due.len(), 3);
    let mut new = 0;
    for slot in due {
        // Both nodes see the slot; only one job is inserted.
        for _node in 0..2 {
            if !cron
                .insert_slot(storage.clone(), slot)
                .await
                .unwrap()
                .is_duplicate()
            {
                new += 1;
            }
        }
    }
    assert_eq!(new, 3);
    assert_eq!(slots(&db).await, (3, 3));

    db.finish().await;
}

#[tokio::test]
async fn engines_run_a_seconds_schedule_without_doubles_and_catch_up() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let engine = |name: &str, pool| {
        Engine::builder(name)
            .stager_interval(Duration::from_millis(50))
            .cron_interval(Duration::from_millis(100))
            .cron_catch_up(Duration::from_secs(3))
            .cron(Cron::new("tick", "* * * * * *", || Tick).unwrap())
            .with_queue(Queue::builder("default").interval(Duration::from_millis(50)))
            .start(Postgres::new(pool))
    };
    let a = engine("a", db.pool.clone());
    let b = engine("b", db.another_pool().await);

    // The last 3 seconds' slots are caught up, once between the two engines.
    eventually(
        "the recent slots are caught up",
        Duration::from_secs(5),
        || async { slots(&db).await.0 >= 3 },
    )
    .await;
    let (caught_up, distinct) = slots(&db).await;
    assert_eq!(caught_up, distinct, "no slot twice");

    // Then about one per second, still never twice.
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let (count, distinct) = slots(&db).await;
    assert_eq!(count, distinct, "no slot twice");
    assert!(
        (caught_up + 1..=caught_up + 3).contains(&count),
        "about 2 more slots in 2 s: {caught_up} then {count}"
    );

    a.shutdown(Duration::from_secs(1)).await;
    b.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// The `tick` slots inserted: how many, and how many seconds they span (first to last, both
/// counted), so a gap shows as fewer slots than seconds.
async fn span(db: &TestDb) -> (i64, i64) {
    sqlx::query_as(
        "SELECT count(DISTINCT meta->>'slot'), \
             coalesce(extract(epoch FROM max((meta->>'slot')::timestamptz) \
                                     - min((meta->>'slot')::timestamptz))::bigint + 1, 0) \
         FROM ishikari_jobs WHERE meta->>'cron' = 'tick'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

fn every_second(db: &TestDb, catch_up: Duration) -> Engine {
    Engine::builder("cron")
        .cron_interval(Duration::from_millis(100))
        .cron_catch_up(catch_up)
        .cron(Cron::new("tick", "* * * * * *", || Tick).unwrap())
        .start(Postgres::new(db.pool.clone()))
}

#[tokio::test]
async fn more_than_a_ticks_worth_of_slots_are_all_caught_up() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // 150 slots are due at once; a tick inserts at most 100, and the next goes on from there.
    let engine = every_second(&db, Duration::from_secs(150));

    eventually(
        "150 slots are caught up",
        Duration::from_secs(5),
        || async { span(&db).await.0 >= 150 },
    )
    .await;
    let (slots, seconds) = span(&db).await;
    assert_eq!(slots, seconds, "no slot skipped");

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

#[tokio::test]
async fn slots_that_failed_to_insert_are_inserted_on_a_later_tick() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let engine = every_second(&db, Duration::from_secs(2));
    eventually("the first slots", Duration::from_secs(5), || async {
        span(&db).await.0 >= 2
    })
    .await;

    // The database refuses cron jobs for a while.
    sqlx::query(
        "ALTER TABLE ishikari_jobs ADD CONSTRAINT no_cron CHECK (meta->>'cron' IS NULL) NOT VALID",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let refused_until = Utc::now();
    sqlx::query("ALTER TABLE ishikari_jobs DROP CONSTRAINT no_cron")
        .execute(&db.pool)
        .await
        .unwrap();

    // The refused slots are inserted once it accepts them again.
    eventually(
        "every slot through the outage is inserted",
        Duration::from_secs(5),
        || async {
            let (slots, seconds) = span(&db).await;
            let last: chrono::DateTime<Utc> = sqlx::query_scalar(
                "SELECT max((meta->>'slot')::timestamptz) FROM ishikari_jobs \
                 WHERE meta->>'cron' = 'tick'",
            )
            .fetch_one(&db.pool)
            .await
            .unwrap();
            slots == seconds && last > refused_until
        },
    )
    .await;

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}
