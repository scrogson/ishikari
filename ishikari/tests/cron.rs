//! Cron slots run once across nodes, and a starting node catches up on recent slots. Entries
//! kept in the database are picked up, changed and dropped while engines run.

mod common;

use chrono::{Duration as ChronoDuration, Utc};
use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{CatchUp, Cron, CronEntry, Crons, Engine, Postgres, Queue};
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
        Duration::from_secs(30),
        || async { slots(&db).await.0 >= 3 },
    )
    .await;
    let (caught_up, distinct) = slots(&db).await;
    assert_eq!(caught_up, distinct, "no slot twice");

    // Then about one per second, still never twice. One more, when the first count was taken
    // before the last of the 3 seconds' slots (which may be 4) was in.
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    let (count, distinct) = slots(&db).await;
    assert_eq!(count, distinct, "no slot twice");
    assert!(
        (caught_up + 1..=caught_up + 4).contains(&count),
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
        Duration::from_secs(30),
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
    eventually("the first slots", Duration::from_secs(30), || async {
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
        Duration::from_secs(30),
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

// Entries kept in the database (`Crons`), created, changed and deleted while engines run.

#[ishikari::job]
struct Ping {
    n: i32,
}

#[ishikari::worker]
impl Worker for Ping {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

/// The slots inserted for the entry `name`, in order, with the `n` each job was given.
async fn pings(db: &TestDb, name: &str) -> Vec<(chrono::DateTime<Utc>, i32)> {
    sqlx::query_as(
        "SELECT (meta->>'slot')::timestamptz, (args->>'n')::int FROM ishikari_jobs \
         WHERE meta->>'cron' = $1 ORDER BY 1",
    )
    .bind(name)
    .fetch_all(&db.pool)
    .await
    .unwrap()
}

fn engine_with(name: &str, pool: sqlx::PgPool, statics: Vec<Cron>) -> Engine {
    statics
        .into_iter()
        .fold(
            Engine::builder(name)
                .cron_interval(Duration::from_millis(100))
                .cron_catch_up(Duration::from_secs(5)),
            |builder, cron| builder.cron(cron),
        )
        .start(Postgres::new(pool))
}

fn every_second_ping(name: &str, n: i32) -> CronEntry {
    CronEntry::new(name, "* * * * * *", &Ping { n }).unwrap()
}

#[tokio::test]
async fn an_entry_put_at_runtime_runs_without_a_restart_and_without_back_filling() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // No cron given to the engine at all.
    let engine = engine_with("dyn", db.pool.clone(), vec![]);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let crons = Crons::new(db.pool.clone());
    let entry = crons
        .put(&every_second_ping("agent-ping", 1))
        .await
        .unwrap();
    assert_eq!(entry.name(), "agent-ping");
    assert_eq!(entry.worker(), std::any::type_name::<Ping>());

    eventually("the entry's slots", Duration::from_secs(30), || async {
        pings(&db, "agent-ping").await.len() >= 2
    })
    .await;
    let slots = pings(&db, "agent-ping").await;
    // Within the catch-up window, but nothing from before the entry was put.
    assert!(
        slots.iter().all(|(slot, _)| *slot > entry.updated_at()),
        "no slot before the entry was put: {slots:?}, put at {}",
        entry.updated_at()
    );
    assert!(slots.iter().all(|(_, n)| *n == 1));

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

#[tokio::test]
async fn a_deleted_entry_inserts_no_more_slots() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let engine = engine_with("dyn", db.pool.clone(), vec![]);
    let crons = Crons::new(db.pool.clone());
    crons.put(&every_second_ping("gone", 1)).await.unwrap();
    eventually("the first slots", Duration::from_secs(30), || async {
        pings(&db, "gone").await.len() >= 2
    })
    .await;

    assert!(crons.delete("gone").await.unwrap());
    // No job is inserted once the delete has returned (not even of a slot already due).
    let at_delete = pings(&db, "gone").await;
    assert!(!crons.delete("gone").await.unwrap(), "already gone");
    assert!(crons.get("gone").await.unwrap().is_none());

    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(
        pings(&db, "gone").await,
        at_delete,
        "no job after the delete"
    );

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

#[tokio::test]
async fn two_engines_insert_an_entrys_slots_once() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let a = engine_with("a", db.pool.clone(), vec![]);
    let b = engine_with("b", db.another_pool().await, vec![]);
    Crons::new(db.pool.clone())
        .put(&every_second_ping("shared", 1))
        .await
        .unwrap();

    eventually("a few slots", Duration::from_secs(30), || async {
        pings(&db, "shared").await.len() >= 3
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let slots = pings(&db, "shared").await;
    let mut distinct = slots.clone();
    distinct.dedup_by_key(|(slot, _)| *slot);
    assert_eq!(slots.len(), distinct.len(), "no slot twice: {slots:?}");

    a.shutdown(Duration::from_secs(1)).await;
    b.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

#[tokio::test]
async fn putting_an_entry_again_changes_it_from_then_on() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let engine = engine_with("dyn", db.pool.clone(), vec![]);
    let crons = Crons::new(db.pool.clone());
    let first = crons.put(&every_second_ping("changing", 1)).await.unwrap();
    eventually("the first job's slots", Duration::from_secs(30), || async {
        pings(&db, "changing").await.len() >= 2
    })
    .await;

    // Putting the same entry again changes nothing, its time included.
    let same = crons.put(&every_second_ping("changing", 1)).await.unwrap();
    assert_eq!(same.updated_at(), first.updated_at());
    assert_eq!(same.inserted_at(), first.inserted_at());

    let second = crons.put(&every_second_ping("changing", 2)).await.unwrap();
    assert!(second.updated_at() > first.updated_at());
    assert_eq!(second.inserted_at(), first.inserted_at());
    eventually("the new job's slots", Duration::from_secs(30), || async {
        pings(&db, "changing")
            .await
            .iter()
            .filter(|(_, n)| *n == 2)
            .count()
            >= 2
    })
    .await;
    let slots = pings(&db, "changing").await;
    assert!(
        slots
            .iter()
            .all(|(slot, n)| (*n == 1) == (*slot <= second.updated_at())),
        "the old job before the change, the new one after: {slots:?} (changed at {})",
        second.updated_at()
    );

    // A pattern that won't fall due for months stops the slots.
    let yearly = CronEntry::new("changing", "0 0 1 1 *", &Ping { n: 3 }).unwrap();
    let third = crons.put(&yearly).await.unwrap();
    tokio::time::sleep(Duration::from_millis(2_000)).await;
    assert!(pings(&db, "changing")
        .await
        .iter()
        .all(|(slot, _)| *slot <= third.updated_at()));
    assert_eq!(
        crons.get("changing").await.unwrap().unwrap().pattern(),
        "0 0 1 1 *"
    );

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

#[tokio::test]
async fn entries_are_listed_with_what_they_insert() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let crons = Crons::new(db.pool.clone());
    let chicago = CronEntry::new("b-daily", "0 7 * * *", &Ping { n: 7 })
        .unwrap()
        .timezone(ishikari::chrono_tz::America::Chicago)
        .queue("reports")
        .priority(2)
        .max_attempts(3)
        .tag("agent");
    crons.put(&chicago).await.unwrap();
    crons.put(&every_second_ping("a-often", 1)).await.unwrap();

    let all = crons.list().await.unwrap();
    let names: Vec<_> = all.iter().map(|e| e.name()).collect();
    assert_eq!(names, ["a-often", "b-daily"]);
    let daily = &all[1];
    assert_eq!(daily.pattern(), "0 7 * * *");
    assert_eq!(daily.timezone(), ishikari::chrono_tz::America::Chicago);
    assert_eq!(daily.queue(), "reports");
    assert_eq!(daily.priority(), 2);
    assert_eq!(daily.max_attempts(), 3);
    assert_eq!(daily.tags(), ["agent"]);
    assert_eq!(
        daily.args(),
        &serde_json::json!({"ishikari_worker": "Ping", "n": 7})
    );
    // The job's own queue and attempts, unless the entry sets them.
    let often = &all[0];
    assert_eq!(often.queue(), "default");
    assert_eq!(often.max_attempts(), 20);

    db.finish().await;
}

#[test]
fn an_entry_with_a_bad_pattern_is_refused() {
    let err = CronEntry::new("x", "every day", &Ping { n: 1 }).unwrap_err();
    assert_eq!(err.pattern, "every day");
    assert!(CronEntry::new("", "* * * * *", &Ping { n: 1 }).is_err());
}

#[tokio::test]
async fn a_row_that_cannot_be_read_does_not_stop_the_others() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    sqlx::query(
        "INSERT INTO ishikari_crons (name, pattern, timezone, worker, args, queue, max_attempts) \
         VALUES ('broken', 'not a pattern', 'UTC', 'x', '{}', 'default', 1), \
                ('lost', '* * * * * *', 'Mars/Olympus', 'x', '{}', 'default', 1)",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let engine = engine_with("dyn", db.pool.clone(), vec![]);
    Crons::new(db.pool.clone())
        .put(&every_second_ping("fine", 1))
        .await
        .unwrap();
    eventually(
        "the good entry's slots",
        Duration::from_secs(30),
        || async { pings(&db, "fine").await.len() >= 2 },
    )
    .await;
    assert!(pings(&db, "lost").await.is_empty());

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

#[tokio::test]
async fn an_entry_named_as_a_static_cron_is_ignored() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let statik = Cron::new("same", "* * * * * *", || Ping { n: 1 }).unwrap();
    let engine = engine_with("dyn", db.pool.clone(), vec![statik]);
    Crons::new(db.pool.clone())
        .put(&every_second_ping("same", 2))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let slots = pings(&db, "same").await;
    assert!(slots.len() >= 2);
    assert!(
        slots.iter().all(|(_, n)| *n == 1),
        "only the engine's own cron: {slots:?}"
    );

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

#[tokio::test]
async fn two_engine_crons_of_one_name_both_run() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let yearly = Cron::new("dup", "0 0 1 1 *", || Ping { n: 1 }).unwrap();
    let often = Cron::new("dup", "* * * * * *", || Ping { n: 2 }).unwrap();
    let engine = engine_with("dyn", db.pool.clone(), vec![yearly, often]);
    eventually(
        "the second entry's slots",
        Duration::from_secs(30),
        || async { pings(&db, "dup").await.len() >= 2 },
    )
    .await;

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

#[tokio::test]
async fn a_null_tag_is_refused() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let refused = sqlx::query(
        "INSERT INTO ishikari_crons (name, pattern, worker, args, queue, max_attempts, tags) \
         VALUES ('nulltag', '* * * * *', 'x', '{}', 'default', 1, '{NULL}')",
    )
    .execute(&db.pool)
    .await;
    assert!(refused.is_err(), "a tag can't be NULL");
    // Nor longer than a job's tag may be.
    let long = CronEntry::new("long", "* * * * *", &Ping { n: 1 })
        .unwrap()
        .tag("t".repeat(256));
    assert!(Crons::new(db.pool.clone()).put(&long).await.is_err());

    db.finish().await;
}

#[tokio::test]
async fn unreadable_entries_are_listed_apart_so_they_can_be_deleted() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    sqlx::query(
        "INSERT INTO ishikari_crons (name, pattern, timezone, worker, args, queue, max_attempts) \
         VALUES ('broken', 'not a pattern', 'UTC', 'x', '{}', 'default', 1)",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let crons = Crons::new(db.pool.clone());
    crons.put(&every_second_ping("fine", 1)).await.unwrap();

    let all = crons.list_all().await.unwrap();
    assert_eq!(all.len(), 2);
    let broken = all[0].as_ref().unwrap_err();
    assert_eq!(broken.name(), "broken");
    assert_eq!(broken.error().pattern, "not a pattern");
    assert_eq!(all[1].as_ref().unwrap().name(), "fine");
    assert!(crons.delete("broken").await.unwrap());

    db.finish().await;
}

#[tokio::test]
async fn an_entry_is_put_and_deleted_with_the_callers_transaction() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let crons = Crons::new(db.pool.clone());
    let entry = every_second_ping("owned", 1);

    // Rolled back with the caller's own rows: not kept.
    let mut tx = db.pool.begin().await.unwrap();
    crons.put_in(&mut tx, &entry).await.unwrap();
    assert!(
        crons.get("owned").await.unwrap().is_none(),
        "not seen before the commit"
    );
    tx.rollback().await.unwrap();
    assert!(crons.get("owned").await.unwrap().is_none());

    let mut tx = db.pool.begin().await.unwrap();
    let put = crons.put_in(&mut tx, &entry).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        crons.get("owned").await.unwrap().unwrap().updated_at(),
        put.updated_at()
    );

    let mut tx = db.pool.begin().await.unwrap();
    assert!(crons.delete_in(&mut tx, "owned").await.unwrap());
    tx.rollback().await.unwrap();
    assert!(
        crons.get("owned").await.unwrap().is_some(),
        "a rolled back delete keeps it"
    );
    let mut tx = db.pool.begin().await.unwrap();
    assert!(crons.delete_in(&mut tx, "owned").await.unwrap());
    tx.commit().await.unwrap();
    assert!(crons.get("owned").await.unwrap().is_none());

    db.finish().await;
}

#[tokio::test]
async fn a_paused_entry_inserts_nothing_until_resumed_and_back_fills_nothing() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let engine = engine_with("dyn", db.pool.clone(), vec![]);
    let crons = Crons::new(db.pool.clone());
    crons.put(&every_second_ping("napping", 1)).await.unwrap();
    eventually("the first slots", Duration::from_secs(30), || async {
        !pings(&db, "napping").await.is_empty()
    })
    .await;

    assert!(crons.pause("napping").await.unwrap());
    // Final, as a delete is: nothing once it has returned.
    let at_pause = pings(&db, "napping").await;
    assert!(crons.pause("napping").await.unwrap(), "already paused");
    let paused = crons.get("napping").await.unwrap().unwrap();
    assert!(paused.paused(), "kept, and listed as paused");
    assert_eq!(crons.list().await.unwrap().len(), 1);
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert_eq!(pings(&db, "napping").await, at_pause, "no job while paused");

    // Putting it again changes it, and keeps it paused.
    let put = crons.put(&every_second_ping("napping", 2)).await.unwrap();
    assert!(put.paused());

    assert!(crons.resume("napping").await.unwrap());
    let resumed = crons.get("napping").await.unwrap().unwrap();
    assert!(!resumed.paused());
    assert!(crons.resume("napping").await.unwrap(), "already running");
    assert_eq!(
        crons.get("napping").await.unwrap().unwrap().updated_at(),
        resumed.updated_at(),
        "resuming a running entry moves nothing"
    );
    eventually(
        "slots after the resume",
        Duration::from_secs(30),
        || async { pings(&db, "napping").await.len() > at_pause.len() },
    )
    .await;
    let after: Vec<_> = pings(&db, "napping").await[at_pause.len()..].to_vec();
    assert!(
        after
            .iter()
            .all(|(slot, n)| *slot > resumed.updated_at() && *n == 2),
        "none from the pause, and the job as put: {after:?}"
    );

    assert!(!crons.pause("never-was").await.unwrap());
    assert!(!crons.resume("never-was").await.unwrap());

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

#[tokio::test]
async fn an_entrys_catch_up_is_kept_and_a_change_to_it_is_a_change() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let crons = Crons::new(db.pool.clone());
    let all = crons.put(&every_second_ping("sync", 1)).await.unwrap();
    assert_eq!(all.catch_up(), CatchUp::All, "the default");
    let latest = crons
        .put(&every_second_ping("sync", 1).catch_up(CatchUp::Latest))
        .await
        .unwrap();
    assert_eq!(latest.catch_up(), CatchUp::Latest);
    assert!(latest.updated_at() > all.updated_at());
    assert_eq!(
        crons.get("sync").await.unwrap().unwrap().catch_up(),
        CatchUp::Latest
    );

    db.finish().await;
}

#[tokio::test]
async fn an_entry_is_run_now_as_its_own_job() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let crons = Crons::new(db.pool.clone());
    let yearly = CronEntry::new("yearly", "0 0 1 1 *", &Ping { n: 7 })
        .unwrap()
        .priority(2)
        .tag("agent");
    crons.put(&yearly).await.unwrap();
    crons.pause("yearly").await.unwrap();

    let Some(ishikari::Inserted::New(job)) = crons.run_now("yearly").await.unwrap() else {
        panic!("a new job");
    };
    assert_eq!(job.worker, std::any::type_name::<Ping>());
    assert_eq!(job.args["n"], 7);
    assert_eq!(job.priority, 2);
    assert_eq!(job.tags, ["agent"]);
    let meta = job.meta.clone().unwrap();
    assert_eq!(meta["cron"], "yearly");
    assert_eq!(meta["run_now"], true);
    assert!(job.scheduled_at <= Utc::now());
    assert!(crons.run_now("never-was").await.unwrap().is_none());

    // In the caller's transaction: rolled back with it.
    let mut tx = db.pool.begin().await.unwrap();
    crons.run_now_in(&mut tx, "yearly").await.unwrap().unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(pings(&db, "yearly").await.len(), 1);

    db.finish().await;
}
