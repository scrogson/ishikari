//! The workflow builder ships in the crate.

#![cfg(feature = "embed-frontend")]

mod common;

use axum::http::{header, StatusCode};
use axum::Router;
use common::get;
use ishikari_admin::AppState;

fn mounted_at_admin() -> Router {
    let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
    Router::new().nest(
        "/admin",
        ishikari_admin::app(AppState::new(pool).with_base_path("/admin")),
    )
}

/// The builder's script and stylesheet, which the definition editor loads, are embedded
/// (committed, so in the published crate) and served under the admin's base path.
#[tokio::test]
async fn the_builders_assets_are_served_under_the_base_path() {
    let app = mounted_at_admin();
    for (path, content_type) in [
        ("/admin/builder/assets/index.js", "javascript"),
        ("/admin/builder/assets/index.css", "text/css"),
    ] {
        let res = get(&app, path, &[]).await;
        assert_eq!(res.status(), StatusCode::OK, "{path}");
        let ct = res.headers()[header::CONTENT_TYPE].to_str().unwrap();
        assert!(ct.contains(content_type), "{path}: {ct}");
    }
}

/// The builder runs inside the definition editor, which gives it its configuration: anything
/// under `/builder` other than its assets goes there, under the base path.
#[tokio::test]
async fn the_builders_pages_go_to_the_editor_under_the_base_path() {
    for path in ["/admin/builder", "/admin/builder/definitions/new"] {
        let res = get(&mounted_at_admin(), path, &[]).await;
        assert_eq!(res.status(), StatusCode::SEE_OTHER, "{path}");
        assert_eq!(res.headers()[header::LOCATION], "/admin/definitions/new");
    }
    let res = get(&mounted_at_admin(), "/admin/builder/assets/missing.js", &[]).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}
