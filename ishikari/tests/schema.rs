//! An engine configured with a schema keeps every job write in that schema, even when the
//! connection's search_path doesn't include it.

mod common;

use chrono::{DateTime, Utc};
use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Engine, Postgres, Queue};
use sqlx::PgPool;
use std::time::Duration;

#[ishikari::job]
struct Succeeds;

#[ishikari::worker]
impl Worker for Succeeds {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[ishikari::job]
struct FailsOnce;

#[ishikari::worker]
impl Worker for FailsOnce {
    fn backoff(&self, _attempt: i32) -> DateTime<Utc> {
        Utc::now()
    }

    async fn perform(&self, ctx: Context) -> PerformResult {
        if ctx.job().attempt == 1 {
            return Err("first attempt fails".into());
        }
        Complete::default().into()
    }
}

#[ishikari::job]
struct SnoozesOnce;

#[ishikari::worker]
impl Worker for SnoozesOnce {
    async fn perform(&self, ctx: Context) -> PerformResult {
        if ctx.job().attempt == 1 {
            return Snooze(0).into();
        }
        Complete::default().into()
    }
}

#[ishikari::job]
struct Cancels;

#[ishikari::worker]
impl Worker for Cancels {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Cancel::default().message("not needed").into()
    }
}

async fn row(pool: &PgPool, schema: &str, id: i64) -> (String, i32, i64) {
    sqlx::query_as(&format!(
        "SELECT state::text, attempt, cardinality(errors)::bigint FROM {schema}.ishikari_jobs WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn every_outcome_is_written_to_the_engines_schema() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // The default search_path (public) has no ishikari tables, so any write that ignores the
    // schema fails.
    let pool = db.plain_pool().await;
    let engine = Engine::builder("test")
        .schema(db.schema.clone())
        .stager_interval(Duration::from_millis(20))
        .with_queue(Queue::builder("default").interval(Duration::from_millis(20)))
        .start(Postgres::new(pool.clone()));

    let ok = engine.insert(Succeeds).await.unwrap();
    let retried = engine.insert(FailsOnce).await.unwrap();
    let snoozed = engine.insert(SnoozesOnce).await.unwrap();
    let cancelled = engine.insert(Cancels).await.unwrap();

    for (id, state) in [
        (ok.id, "completed"),
        (retried.id, "completed"),
        (snoozed.id, "completed"),
        (cancelled.id, "cancelled"),
    ] {
        eventually(
            &format!("job {id} is {state}"),
            Duration::from_secs(10),
            || {
                let pool = pool.clone();
                let schema = db.schema.clone();
                async move { row(&pool, &schema, id).await.0 == state }
            },
        )
        .await;
    }
    let (_, attempt, errors) = row(&pool, &db.schema, retried.id).await;
    assert_eq!(
        (attempt, errors),
        (2, 1),
        "one failed attempt, then success"
    );

    pool.close().await;
    db.finish().await;
}
