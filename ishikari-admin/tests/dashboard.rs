//! The dashboard's job counts.

mod common;

use axum::http::{header, StatusCode};
use common::{body, get, TestDb};
use ishikari_admin::AppState;

/// The dashboard refreshes its counts with htmx every few seconds, swapping in what
/// `/api/stats` returns: that must be the counts' HTML, not the JSON other clients get.
#[tokio::test]
async fn the_dashboards_refresh_gets_the_counts_as_html() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let res = get(&app, "/api/stats", &[("hx-request", "true")]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let ct = res.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .to_string();
    assert!(ct.starts_with("text/html"), "got {ct}");
    let html = body(res).await;
    assert!(html.contains("Available"), "the counts: {html}");
    assert!(!html.contains("<html"), "a fragment, not a page");

    let res = get(&app, "/api/stats", &[]).await;
    let ct = res.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .to_string();
    assert!(ct.starts_with("application/json"), "API clients keep JSON");

    db.drop().await;
}

/// Workflow steps waiting on their upstreams are counted on their own.
#[tokio::test]
async fn the_dashboard_counts_pending_jobs() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    db.job("app::workers::Step", "pending").await;
    db.job("app::workers::Step", "pending").await;
    db.job("app::workers::Step", "available").await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let html = body(get(&app, "/", &[]).await).await;
    let pending = html
        .split(">Pending</div>")
        .nth(1)
        .expect("a Pending count");
    let count = pending.split("</div>").next().unwrap();
    assert!(count.trim_end().ends_with(">2"), "{count}");

    let html = body(get(&app, "/api/stats", &[("hx-request", "true")]).await).await;
    let pending = html
        .split(">Pending</div>")
        .nth(1)
        .expect("a Pending count");
    let count = pending.split("</div>").next().unwrap();
    assert!(count.trim_end().ends_with(">2"), "refreshed: {count}");

    db.drop().await;
}

#[tokio::test]
async fn the_dashboard_lists_recent_failures_with_their_errors() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let id = db
        .discarded("app::workers::SendEmail", "smtp: connection refused")
        .await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let html = body(get(&app, "/", &[]).await).await;
    assert!(!html.contains("No recent failures"), "{html}");
    assert!(html.contains(&format!("href=\"/jobs/{id}\"")));
    assert!(html.contains("smtp: connection refused"));

    db.drop().await;
}

#[tokio::test]
async fn analytics_groups_failures_by_worker_and_kind() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    db.discarded("app::workers::SendEmail", "smtp: connection refused")
        .await;
    db.discarded("app::workers::SendEmail", "smtp: timed out")
        .await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let html = body(get(&app, "/analytics", &[]).await).await;
    let patterns = html
        .split("Failure Patterns")
        .nth(1)
        .expect("the failure patterns, with discarded jobs");
    assert!(patterns.contains(">SendEmail</span>"), "{patterns}");
    assert!(
        patterns.contains(r#"badge-error tabular-nums">2</span>"#),
        "{patterns}"
    );

    db.drop().await;
}
