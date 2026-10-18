//! Actions a browser sends from another site are refused (CSRF).

mod common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::response::Response;
use axum::Router;
use common::TestDb;
use ishikari_admin::AppState;
use tower::ServiceExt;

async fn send(app: &Router, method: Method, path: &str, headers: &[(&str, &str)]) -> Response {
    let mut req = Request::builder().method(method).uri(path);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    app.clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn available_job(db: &TestDb) -> i64 {
    sqlx::query_scalar(&format!(
        "INSERT INTO {}.ishikari_jobs (worker, state) VALUES ('app::Step', 'available') RETURNING id",
        db.schema
    ))
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

/// A browser says where a request comes from in `Sec-Fetch-Site`, or else in `Origin`: one
/// from another site (or a sandboxed page's `null`) is refused and changes nothing.
#[tokio::test]
async fn an_action_from_another_site_is_refused() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));
    let id = available_job(&db).await;
    let cancel = format!("/jobs/{id}/cancel");

    for headers in [
        &[("sec-fetch-site", "cross-site")][..],
        &[("sec-fetch-site", "same-site")],
        &[
            ("host", "admin.example"),
            ("origin", "https://evil.example"),
        ],
        &[("host", "admin.example"), ("origin", "null")],
        &[
            ("host", "admin.example"),
            ("origin", "https://admin.example.evil.example"),
        ],
    ] {
        let res = send(&app, Method::POST, &cancel, headers).await;
        assert_eq!(res.status(), StatusCode::FORBIDDEN, "{headers:?}");
    }
    assert_eq!(state(&db, id).await, "available");

    // The builder saves with PUT.
    let res = send(
        &app,
        Method::PUT,
        "/api/definitions/1",
        &[("sec-fetch-site", "cross-site")],
    )
    .await;
    assert_eq!(res.status(), StatusCode::FORBIDDEN);

    db.drop().await;
}

/// The admin's own pages, and clients that aren't browsers (neither header), act; and anyone
/// may read.
#[tokio::test]
async fn an_action_from_the_admin_itself_goes_through() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    for headers in [
        &[
            ("sec-fetch-site", "same-origin"),
            ("origin", "https://evil.example"),
        ][..],
        &[
            ("host", "admin.example:8080"),
            ("origin", "https://admin.example:8080"),
        ],
        &[],
    ] {
        let id = available_job(&db).await;
        let res = send(&app, Method::POST, &format!("/jobs/{id}/cancel"), headers).await;
        assert_eq!(res.status(), StatusCode::SEE_OTHER, "{headers:?}");
        assert_eq!(state(&db, id).await, "cancelled", "{headers:?}");
    }

    let res = send(
        &app,
        Method::GET,
        "/jobs",
        &[("sec-fetch-site", "cross-site")],
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);

    db.drop().await;
}
