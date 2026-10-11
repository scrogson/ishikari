//! Insert options, inserts inside a transaction, batches and unique jobs.

mod common;

use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Engine, InsertOpts, Postgres, Queue, Unique};
use std::time::Duration;

#[ishikari::job]
struct Sync {
    town: i64,
    page: i64,
}

#[ishikari::worker]
impl Worker for Sync {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

/// Unique by default, through the worker.
#[ishikari::job]
struct Digest {
    town: i64,
}

#[ishikari::worker]
impl Worker for Digest {
    fn unique(&self) -> Option<Unique> {
        Some(Unique::new())
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
async fn options_are_stored_and_a_scheduled_job_waits() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let opts = InsertOpts::new()
        .priority(2)
        .max_attempts(4)
        .tag("feeds")
        .meta(serde_json::json!({"by": "test"}))
        .schedule_in(Duration::from_millis(400));
    let job = ishikari::insert_with(Sync { town: 1, page: 1 }, opts, &db.pool)
        .await
        .unwrap()
        .into_job();
    assert_eq!(
        (job.priority, job.max_attempts, job.tags.clone()),
        (2, 4, vec!["feeds".to_string()])
    );
    assert_eq!(job.meta, Some(serde_json::json!({"by": "test"})));
    assert_eq!(db.state(job.id).await, "scheduled");

    let _engine = engine(&db);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(db.state(job.id).await, "scheduled", "not before its time");
    eventually("it runs after its time", Duration::from_secs(5), || async {
        db.state(job.id).await == "completed"
    })
    .await;

    db.finish().await;
}

#[tokio::test]
async fn a_job_inserted_in_a_rolled_back_transaction_never_exists() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let mut tx = db.pool.begin().await.unwrap();
    let job = ishikari::insert(Sync { town: 1, page: 1 }, &mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM ishikari_jobs WHERE id = $1")
        .bind(job.id)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);

    db.finish().await;
}

#[tokio::test]
async fn a_unique_job_is_inserted_once_while_it_is_incomplete() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let by_town = || InsertOpts::new().unique(Unique::new().args(["town"]));
    let first = ishikari::insert_with(Sync { town: 7, page: 1 }, by_town(), &db.pool)
        .await
        .unwrap();
    assert!(!first.is_duplicate());
    // Same town, another page: the same job, since only `town` counts.
    let again = ishikari::insert_with(Sync { town: 7, page: 2 }, by_town(), &db.pool)
        .await
        .unwrap();
    assert!(again.is_duplicate());
    assert_eq!(again.job().id, first.job().id);
    // Another town is another job.
    let other = ishikari::insert_with(Sync { town: 8, page: 1 }, by_town(), &db.pool)
        .await
        .unwrap();
    assert!(!other.is_duplicate());

    // The worker's own uniqueness applies to a plain insert.
    let d1 = ishikari::insert(Digest { town: 1 }, &db.pool)
        .await
        .unwrap();
    let d2 = ishikari::insert(Digest { town: 1 }, &db.pool)
        .await
        .unwrap();
    assert_eq!(d1.id, d2.id);

    // Once the job completes, its key is free again.
    let _engine = engine(&db);
    let id = first.job().id;
    eventually(
        "the first job completes",
        Duration::from_secs(5),
        || async { db.state(id).await == "completed" },
    )
    .await;
    let after = ishikari::insert_with(Sync { town: 7, page: 3 }, by_town(), &db.pool)
        .await
        .unwrap();
    assert!(
        !after.is_duplicate(),
        "a completed job no longer holds the key"
    );

    db.finish().await;
}

#[tokio::test]
async fn once_per_period_holds_after_completion() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let hourly =
        || InsertOpts::new().unique(Unique::new().period(Duration::from_secs(3600)).all_states());
    let first = ishikari::insert_with(Sync { town: 1, page: 1 }, hourly(), &db.pool)
        .await
        .unwrap();
    let _engine = engine(&db);
    let id = first.job().id;
    eventually("it completes", Duration::from_secs(5), || async {
        db.state(id).await == "completed"
    })
    .await;
    let again = ishikari::insert_with(Sync { town: 1, page: 1 }, hourly(), &db.pool)
        .await
        .unwrap();
    assert!(again.is_duplicate(), "still this hour's job");

    db.finish().await;
}

#[tokio::test]
async fn a_batch_skips_unique_duplicates() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let opts = InsertOpts::new()
        .queue("bulk")
        .unique(Unique::new().args(["town"]));
    ishikari::insert_with(Sync { town: 2, page: 1 }, opts.clone(), &db.pool)
        .await
        .unwrap();
    let jobs = (1..=3).map(|town| Sync { town, page: 1 });
    let inserted = ishikari::insert_all(jobs, opts, &db.pool).await.unwrap();
    let towns: Vec<i64> = inserted
        .iter()
        .map(|j| j.args["town"].as_i64().unwrap())
        .collect();
    assert_eq!(towns, vec![1, 3], "town 2 was already queued");
    assert!(inserted.iter().all(|j| j.queue == "bulk"));

    db.finish().await;
}

#[tokio::test]
async fn an_engine_inserts_into_its_schema() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let pool = db.plain_pool().await;
    let engine = Engine::builder("test")
        .schema(db.schema.clone())
        .start(Postgres::new(pool.clone()));
    let inserted = engine
        .insert_with(
            Sync { town: 1, page: 1 },
            InsertOpts::new().unique(Unique::new()),
            &pool,
        )
        .await
        .unwrap();
    let again = engine
        .insert_with(
            Sync { town: 1, page: 1 },
            InsertOpts::new().unique(Unique::new()),
            &pool,
        )
        .await
        .unwrap();
    assert_eq!(again.job().id, inserted.job().id);
    assert!(again.is_duplicate());

    pool.close().await;
    db.finish().await;
}
