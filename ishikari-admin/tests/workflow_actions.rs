//! The workflow actions: cancel and retry, against steps finishing at the same time.

mod common;

use axum::http::StatusCode;
use common::{eventually, post, TestDb};
use ishikari::prelude::*;
use ishikari::workflows::Pipeline;
use ishikari::{Engine, Postgres, Queue};
use ishikari_admin::AppState;
use std::time::Duration;

#[ishikari::job]
#[derive(Clone)]
struct Step;

#[ishikari::worker]
impl Worker for Step {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

/// An engine on the test's schema whose resolver won't sweep during a test, so only the job
/// statements themselves can finish a workflow.
fn engine(db: &TestDb) -> Engine {
    Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .resolver_interval(Duration::from_secs(3600))
        .with_queue(
            Queue::builder("default")
                .concurrency(4)
                .interval(Duration::from_millis(20)),
        )
        .start(Postgres::new(db.pool.clone()).with_schema(&db.schema))
}

/// A one-step workflow, with the step and the workflow then set as given.
async fn workflow(db: &TestDb, step: &str, workflow: &str) -> (i64, i64) {
    let s = &db.schema;
    let wf = Pipeline::new("one-step")
        .schema(s)
        .step(Step)
        .run(&db.pool)
        .await
        .unwrap();
    let step: i64 = sqlx::query_scalar(&format!(
        "UPDATE {s}.ishikari_jobs SET state = $2::{s}.ishikari_job_state, attempt = 1, \
             attempted_at = now(), \
             discarded_at = CASE WHEN $2 = 'discarded' THEN now() END \
         WHERE workflow_id = $1 RETURNING id"
    ))
    .bind(wf.id)
    .bind(step)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "UPDATE {s}.ishikari_workflows SET state = $2, \
             completed_at = CASE WHEN $2 = 'running' THEN NULL ELSE now() END \
         WHERE id = $1"
    ))
    .bind(wf.id)
    .bind(workflow)
    .execute(&db.pool)
    .await
    .unwrap();
    (wf.id, step)
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

async fn job_state(db: &TestDb, id: i64) -> String {
    sqlx::query_scalar(&format!(
        "SELECT state::text FROM {}.ishikari_jobs WHERE id = $1",
        db.schema
    ))
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

/// Whether a statement on the test's `table` is waiting for a row lock.
async fn waiting_on(db: &TestDb, table: &str) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                        WHERE wait_event_type = 'Lock' AND query LIKE '%' || $1 || '%')",
    )
    .bind(format!("{}.{table}", db.schema))
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

/// A retried step that runs and completes before the retry has set its workflow running again
/// must still complete the workflow, not leave it running until the resolver's next sweep.
#[tokio::test]
async fn a_retried_workflow_completes_when_its_step_finishes_during_the_retry() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (wf, step) = workflow(&db, "discarded", "failed").await;
    let engine = engine(&db);
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    // Hold the workflow's row, so the retry stops at its update of the workflow.
    let mut holder = db.pool.begin().await.unwrap();
    sqlx::query(&format!(
        "SELECT 1 FROM {}.ishikari_workflows WHERE id = $1 FOR UPDATE",
        db.schema
    ))
    .bind(wf)
    .execute(&mut *holder)
    .await
    .unwrap();
    let retry = tokio::spawn(post(app, format!("/workflows/{wf}/retry")));
    eventually(
        "the retry waits on the workflow's row",
        Duration::from_secs(5),
        || waiting_on(&db, "ishikari_workflows"),
    )
    .await;
    // Whatever the retry has already let run, runs to the end meanwhile.
    if job_state(&db, step).await != "discarded" {
        eventually("the step completes", Duration::from_secs(5), || async {
            job_state(&db, step).await == "completed"
        })
        .await;
    }
    holder.commit().await.unwrap();
    assert_eq!(retry.await.unwrap().status(), StatusCode::SEE_OTHER);

    eventually("the workflow completes", Duration::from_secs(5), || async {
        workflow_state(&db, wf).await == "completed"
    })
    .await;

    engine.shutdown(Duration::from_secs(1)).await;
    db.drop().await;
}

/// A cancel that races the last step's completion must not turn the completed workflow into a
/// cancelled one.
#[tokio::test]
async fn cancelling_a_workflow_as_its_last_step_completes_leaves_it_completed() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let s = &db.schema;
    let (wf, step) = workflow(&db, "executing", "running").await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), s));

    // The last step's completion, in flight: the step and the workflow completed, uncommitted.
    let mut completion = db.pool.begin().await.unwrap();
    sqlx::query(&format!(
        "UPDATE {s}.ishikari_jobs SET state = 'completed', completed_at = now() WHERE id = $1"
    ))
    .bind(step)
    .execute(&mut *completion)
    .await
    .unwrap();
    sqlx::query(&format!(
        "UPDATE {s}.ishikari_workflows SET state = 'completed', completed_at = now() \
         WHERE id = $1"
    ))
    .bind(wf)
    .execute(&mut *completion)
    .await
    .unwrap();

    let cancel = tokio::spawn(post(app, format!("/workflows/{wf}/cancel")));
    eventually(
        "the cancel waits on the step's row",
        Duration::from_secs(5),
        || waiting_on(&db, "ishikari_jobs"),
    )
    .await;
    completion.commit().await.unwrap();

    let res = cancel.await.unwrap();
    assert_eq!(workflow_state(&db, wf).await, "completed");
    assert_eq!(job_state(&db, step).await, "completed");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);

    db.drop().await;
}

/// Cancelling a workflow cancels its steps as cancelling each one would: their unique keys are
/// released, and what depends on them elsewhere can never run, so it is cancelled too.
#[tokio::test]
async fn cancelling_a_workflow_releases_unique_keys_and_cancels_dependents() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let s = &db.schema;
    let (wf, step) = workflow(&db, "available", "running").await;
    sqlx::query(&format!(
        "UPDATE {s}.ishikari_jobs SET unique_key = 'only-one', \
             unique_states = '{{available,scheduled,executing,retryable}}' \
         WHERE id = $1"
    ))
    .bind(step)
    .execute(&db.pool)
    .await
    .unwrap();
    // A job outside the workflow, waiting on its step.
    let outside: i64 = sqlx::query_scalar(&format!(
        "INSERT INTO {s}.ishikari_jobs (worker, state) VALUES ('Step', 'pending') RETURNING id"
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {s}.ishikari_job_dependencies (job_id, depends_on_job_id) VALUES ($1, $2)"
    ))
    .bind(outside)
    .bind(step)
    .execute(&db.pool)
    .await
    .unwrap();
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), s));

    let res = post(app, format!("/workflows/{wf}/cancel")).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);

    assert_eq!(workflow_state(&db, wf).await, "cancelled");
    assert_eq!(job_state(&db, step).await, "cancelled");
    let key: Option<String> = sqlx::query_scalar(&format!(
        "SELECT unique_key FROM {s}.ishikari_jobs WHERE id = $1"
    ))
    .bind(step)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(key, None, "the step's unique key is released");
    assert_eq!(job_state(&db, outside).await, "cancelled");
    let dep: String = sqlx::query_scalar(&format!(
        "SELECT state FROM {s}.ishikari_job_dependencies WHERE job_id = $1"
    ))
    .bind(outside)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(dep, "failed");

    db.drop().await;
}

/// A job's errors, and whether its discarded_at and cancelled_at are set.
async fn errors_and_ends(db: &TestDb, id: i64) -> (Vec<serde_json::Value>, bool, bool) {
    sqlx::query_as(&format!(
        "SELECT errors, discarded_at IS NOT NULL, cancelled_at IS NOT NULL \
         FROM {}.ishikari_jobs WHERE id = $1",
        db.schema
    ))
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

/// Retrying a workflow starts its failed steps over as retrying each one would: their errors
/// are kept, as the history of their earlier runs, and they are no longer discarded.
#[tokio::test]
async fn retrying_a_workflow_keeps_its_steps_errors() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let s = &db.schema;
    let (wf, step) = workflow(&db, "discarded", "failed").await;
    let record = serde_json::json!({
        "attempt": 1, "at": "2026-10-17T12:00:00Z", "kind": "error", "error": "boom"
    });
    sqlx::query(&format!(
        "UPDATE {s}.ishikari_jobs SET errors = ARRAY[$2::jsonb] WHERE id = $1"
    ))
    .bind(step)
    .bind(&record)
    .execute(&db.pool)
    .await
    .unwrap();
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), s));

    let res = post(app, format!("/workflows/{wf}/retry")).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);

    assert_eq!(job_state(&db, step).await, "available");
    assert_eq!(workflow_state(&db, wf).await, "running");
    let (errors, discarded, cancelled) = errors_and_ends(&db, step).await;
    assert_eq!(errors, vec![record], "the earlier run's error is kept");
    assert!(!discarded && !cancelled, "no longer discarded");

    db.drop().await;
}

/// Cancelling a workflow records why on each step it cancels, as cancelling a job does.
#[tokio::test]
async fn cancelling_a_workflow_records_why_on_its_steps() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (wf, step) = workflow(&db, "executing", "running").await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let res = post(app, format!("/workflows/{wf}/cancel")).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);

    let (errors, _, cancelled) = errors_and_ends(&db, step).await;
    assert!(cancelled);
    assert_eq!(errors.len(), 1, "one record: {errors:?}");
    assert_eq!(errors[0]["attempt"], 1);
    assert_eq!(errors[0]["kind"], "cancelled");
    assert_eq!(errors[0]["error"], "workflow cancelled from the admin");
    assert!(errors[0]["at"].is_string());

    db.drop().await;
}
