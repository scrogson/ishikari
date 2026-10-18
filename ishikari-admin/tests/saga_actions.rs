//! The workflow actions on a saga: cancelling one rolls back its completed steps, and retrying
//! one whose rollback failed runs only its failed compensations again.

mod common;

use axum::http::StatusCode;
use common::{eventually, post, TestDb};
use ishikari::prelude::*;
use ishikari::workflows::Saga;
use ishikari::{Engine, Postgres, Queue};
use ishikari_admin::AppState;
use std::time::Duration;

#[ishikari::job]
#[derive(Clone)]
struct Book;

#[ishikari::worker]
impl Worker for Book {
    async fn perform(&self, _ctx: Context) -> PerformResult {
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

/// Never runs: the test's engine doesn't work its queue.
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

fn engine(db: &TestDb) -> Engine {
    Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .resolver_interval(Duration::from_millis(100))
        .with_queue(
            Queue::builder("default")
                .concurrency(4)
                .interval(Duration::from_millis(20)),
        )
        .start(Postgres::new(db.pool.clone()).with_schema(&db.schema))
}

/// The (id, state, attempt) of the saga's job for `step`.
async fn job(db: &TestDb, workflow: i64, step: &str) -> (i64, String, i32) {
    sqlx::query_as(&format!(
        "SELECT id, state::text, attempt FROM {}.ishikari_jobs \
         WHERE workflow_id = $1 AND meta->>'ishikari_step' = $2",
        db.schema
    ))
    .bind(workflow)
    .bind(step)
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

async fn workflow_state(db: &TestDb, id: i64) -> String {
    sqlx::query_scalar(&format!(
        "SELECT state FROM {}.ishikari_workflows WHERE id = $1",
        db.schema
    ))
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

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
async fn cancelling_a_saga_rolls_back_its_completed_steps() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let wf = Saga::new("book-trip")
        .schema(&db.schema)
        .step(Book)
        .compensate(Book)
        .step(Elsewhere)
        .compensate(Book)
        .run(&db.pool)
        .await
        .unwrap();
    let engine = engine(&db);
    eventually("step 1 completes", Duration::from_secs(10), || async {
        job(&db, wf.id, "1").await.1 == "completed"
    })
    .await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let res = post(app.clone(), format!("/workflows/{}/cancel", wf.id)).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert_eq!(finished(&db, wf.id).await, "compensated");
    assert_eq!(job(&db, wf.id, "2").await.1, "cancelled");
    assert_eq!(
        job(&db, wf.id, "compensate-1").await.1,
        "completed",
        "step 1 is undone"
    );
    assert_eq!(job(&db, wf.id, "compensate-2").await.2, 0, "never ran");

    let res = post(app, format!("/workflows/{}/cancel", wf.id)).await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST, "already finished");

    engine.shutdown(Duration::from_secs(1)).await;
    db.drop().await;
}

#[tokio::test]
async fn retrying_a_saga_runs_only_its_failed_compensations_again() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let s = &db.schema;
    let wf = Saga::new("book-trip")
        .schema(s)
        .step(Book)
        .compensate(FailsUntilFixed)
        .step(Book)
        .compensate(Book)
        .step(Fails)
        .no_compensate()
        .run(&db.pool)
        .await
        .unwrap();
    let engine = engine(&db);
    assert_eq!(finished(&db, wf.id).await, "failed");
    sqlx::query(&format!(
        "UPDATE {s}.ishikari_jobs SET meta = meta || '{{\"fixed\": true}}' \
         WHERE workflow_id = $1 AND meta->>'ishikari_step' = 'compensate-1'"
    ))
    .bind(wf.id)
    .execute(&db.pool)
    .await
    .unwrap();
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), s));

    let res = post(app, format!("/workflows/{}/retry", wf.id)).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert_eq!(finished(&db, wf.id).await, "compensated");
    assert_eq!(job(&db, wf.id, "compensate-1").await.1, "completed");
    let (_, state, attempt) = job(&db, wf.id, "3").await;
    assert_eq!(
        (state.as_str(), attempt),
        ("discarded", 1),
        "the failed step isn't run again"
    );
    assert_eq!(
        job(&db, wf.id, "compensate-2").await.2,
        1,
        "nor step 2's undo"
    );

    engine.shutdown(Duration::from_secs(1)).await;
    db.drop().await;
}
