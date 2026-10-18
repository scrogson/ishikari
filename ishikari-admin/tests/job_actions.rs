//! A job's actions: retry and cancel.

mod common;

use axum::http::{header, StatusCode};
use common::{body, get, post, TestDb};
use ishikari_admin::AppState;

async fn job(db: &TestDb, state: &str) -> i64 {
    sqlx::query_scalar(&format!(
        "INSERT INTO {s}.ishikari_jobs (worker, state) VALUES ('app::Step', $1::{s}.ishikari_job_state) \
         RETURNING id",
        s = db.schema
    ))
    .bind(state)
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

async fn state(db: &TestDb, id: i64) -> String {
    sqlx::query_scalar(&format!(
        "SELECT state::text FROM {}.ishikari_jobs WHERE id = $1",
        db.schema
    ))
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

/// An action that changes nothing (cancelling a finished job, retrying one that's about to run)
/// comes back to the job's page saying so.
#[tokio::test]
async fn an_action_that_changes_nothing_says_so() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    for (job_state, action, notice) in [
        ("completed", "cancel", "Not cancelled"),
        ("available", "retry", "Not retried"),
    ] {
        let id = job(&db, job_state).await;
        let res = post(app.clone(), format!("/jobs/{id}/{action}")).await;
        assert_eq!(res.status(), StatusCode::SEE_OTHER);
        let location = res.headers()[header::LOCATION]
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(location, format!("/jobs/{id}?unchanged={action}"));
        let html = body(get(&app, &location, &[]).await).await;
        assert!(html.contains(notice), "{action}: {html}");
        assert_eq!(state(&db, id).await, job_state);
    }

    // One that changes something says nothing.
    let id = job(&db, "available").await;
    let res = post(app.clone(), format!("/jobs/{id}/cancel")).await;
    assert_eq!(
        res.headers()[header::LOCATION],
        format!("/jobs/{id}").as_str()
    );
    assert_eq!(state(&db, id).await, "cancelled");
    let html = body(get(&app, &format!("/jobs/{id}"), &[]).await).await;
    assert!(!html.contains("Not cancelled"));

    db.drop().await;
}

/// There is no discard action: it set `completed_at` rather than `discarded_at`, outside the
/// library, and nothing used it. Cancel stops a job.
#[tokio::test]
async fn there_is_no_discard() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));
    let id = job(&db, "available").await;

    let res = post(app, format!("/jobs/{id}/discard")).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert_eq!(state(&db, id).await, "available");

    db.drop().await;
}
