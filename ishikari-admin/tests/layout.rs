//! The dashboard inside a host's own layout, and on its own.

mod common;

use axum::body::Body;
use axum::http::{header, request::Parts, Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Response};
use axum::Router;
use common::{body, get, TestDb};
use ishikari_admin::{AppState, Page};
use tower::ServiceExt;

/// Who the host's auth let in.
#[derive(Clone)]
struct User(&'static str);

async fn sign_in(mut req: Request<Body>, next: Next) -> Response {
    req.extensions_mut().insert(User("ada"));
    next.run(req).await
}

/// The host's layout: its own chrome and theme, and the user its auth found.
fn host_layout(parts: &Parts, page: Page) -> Response {
    let Some(User(user)) = parts.extensions.get::<User>() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let active = page
        .active_item()
        .map(|i| i.label.clone())
        .unwrap_or_default();
    Html(format!(
        r#"<!doctype html><html data-theme="host"><head><title>{title} · Host</title>
<link rel="stylesheet" href="{css}"></head>
<body><header id="host-chrome" data-user="{user}" data-active="{active}"></header>
{nav}<main id="host-main">{body}</main></body></html>"#,
        title = page.title,
        css = ishikari_admin::embed_stylesheet_href(&page.base_path),
        nav = page.nav_html(),
        body = page.body,
    ))
    .into_response()
}

fn host(db: &TestDb) -> Router {
    let admin = ishikari_admin::app_with_layout(
        AppState::with_schema(db.pool.clone(), &db.schema).with_base_path("/jobs"),
        host_layout,
    );
    Router::new()
        .nest("/jobs", admin)
        .layer(middleware::from_fn(sign_in))
}

#[tokio::test]
async fn a_host_layout_wraps_each_page() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = host(&db);

    let res = get(&app, "/jobs", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let html = body(res).await;
    assert!(html.contains("<title>Dashboard · Host</title>"), "{html}");
    assert!(
        html.contains(r#"data-user="ada""#),
        "the layout sees the host's request"
    );
    assert!(html.contains(r#"data-active="Dashboard""#));
    assert!(html.contains(r#"href="/jobs/assets/admin-embed.css?v="#));
    let main = html.split(r#"<main id="host-main">"#).nth(1).unwrap();
    assert!(main.contains(r#"src="/jobs/assets/admin.js?v="#));
    assert!(
        main.contains("Recent Failures"),
        "the page's content, in the host's main"
    );
    assert!(main.contains(r#"class="ishikari-admin"#));
    assert!(!html.contains("sidebar-nav"), "not the standalone sidebar");
    assert!(!html.contains(r#"data-theme="dark""#), "nor its theme");
    assert!(!html.contains("/assets/admin.css"), "nor its stylesheet");

    let html = body(get(&app, "/jobs/jobs?state=pending", &[]).await).await;
    assert!(html.contains("<title>Jobs · Host</title>"));
    assert!(html.contains(r#"data-active="Jobs""#));
    assert!(html.contains(r#"href="/jobs/jobs" class="menu-active" aria-current="page""#));
    assert!(!html.contains("sidebar-nav"));

    db.drop().await;
}

#[tokio::test]
async fn a_form_shown_again_comes_inside_the_layout_too() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let res = host(&db)
        .oneshot(
            Request::post("/jobs/definitions/import")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from("yaml=%3A%20not%20yaml%20%5B"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let html = body(res).await;
    assert!(html.contains("host-chrome"));
    assert!(html.contains("<title>Import Definition · Host</title>"));
    let main = html.split(r#"<main id="host-main">"#).nth(1).unwrap();
    assert!(
        main.contains(r#"role="alert" class="alert alert-error"#),
        "{html}"
    );

    db.drop().await;
}

#[tokio::test]
async fn htmx_fragments_are_not_wrapped() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    db.job("app::workers::Step", "pending").await;
    let app = host(&db);

    for path in ["/jobs/api/stats", "/jobs/api/jobs"] {
        let res = get(&app, path, &[("hx-request", "true")]).await;
        assert_eq!(res.status(), StatusCode::OK, "{path}");
        let html = body(res).await;
        assert!(!html.contains("host-chrome"), "{path}: {html}");
        assert!(!html.contains("<html"), "{path}");
    }
    let stats = body(get(&app, "/jobs/api/stats", &[("hx-request", "true")]).await).await;
    assert!(stats.contains("Pending"));

    db.drop().await;
}

#[tokio::test]
async fn the_embed_stylesheet_leaves_the_theme_to_the_host() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = host(&db);

    let res = get(&app, "/jobs/assets/admin-embed.css", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    assert!(res.headers()[header::CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/css"));
    let css = body(res).await;
    assert!(css.contains(".btn") && css.contains(".badge") && css.contains(".table"));
    assert!(css.contains(".ishikari-admin .code-block"));
    for theme in [
        "[data-theme",
        "--color-base-100:",
        "--color-primary:",
        "color-scheme",
    ] {
        assert!(!css.contains(theme), "declares {theme}");
    }

    // The standalone stylesheet has the themes the embed one leaves out.
    let css = body(get(&app, "/jobs/assets/admin.css", &[]).await).await;
    assert!(css.contains("[data-theme=dark]") && css.contains("--color-base-100:"));

    let res = get(&app, "/jobs/assets/admin.js", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);

    db.drop().await;
}

#[tokio::test]
async fn without_a_layout_pages_are_standalone() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let admin = ishikari_admin::app(
        AppState::with_schema(db.pool.clone(), &db.schema).with_base_path("/jobs"),
    );
    let app = Router::new().nest("/jobs", admin);

    let html = body(get(&app, "/jobs/jobs", &[]).await).await;
    assert!(
        html.contains("<title>Jobs - Ishikari Admin</title>"),
        "{html}"
    );
    assert!(html.contains(r#"<html lang="en" data-theme="dark">"#));
    assert!(html.contains(r#"id="sidebar-nav""#));
    assert!(html.contains(r#"href="/jobs/assets/admin.css?v="#));
    assert!(html.contains(r#"src="/jobs/assets/htmx.min.js?v="#));
    assert!(html.contains(r#"src="/jobs/assets/admin.js?v="#));
    assert!(!html.contains("admin-embed.css"));

    db.drop().await;
}
