//! The admin mounted inside a host app, under a prefix, behind the host's own auth.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use ishikari_admin::AppState;
use sqlx::PgPool;
use tower::ServiceExt;

/// The host's auth: only requests carrying its session header get through.
async fn require_session(req: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    match req.headers().get("x-session") {
        Some(v) if v == "admin" => Ok(next.run(req).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}

async fn body(res: Response) -> String {
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

#[tokio::test]
async fn the_admin_runs_under_a_prefix_behind_the_hosts_auth() {
    if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
        return;
    }
    let url = std::env::var("ISHIKARI_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .expect("set ISHIKARI_TEST_DATABASE_URL (or DATABASE_URL), or ISHIKARI_SKIP_DB_TESTS=1");
    let pool = PgPool::connect(&url).await.unwrap();
    let schema = format!("ishikari_admin_t_{}", std::process::id());
    ishikari::migrations::migrate(&pool, &schema).await.unwrap();

    let admin =
        ishikari_admin::app(AppState::with_schema(pool.clone(), &schema).with_base_path("/jobs"));
    let host = Router::new()
        .nest("/jobs", admin)
        .layer(middleware::from_fn(require_session));

    let get = |path: &str, session: Option<&str>| {
        let mut req = Request::get(path);
        if let Some(s) = session {
            req = req.header("x-session", s);
        }
        host.clone().oneshot(req.body(Body::empty()).unwrap())
    };

    assert_eq!(
        get("/jobs", None).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
    let res = get("/jobs", Some("admin")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let html = body(res).await;
    assert!(
        html.contains("href=\"/jobs/jobs\""),
        "links carry the prefix"
    );
    assert!(
        html.contains("/jobs/assets/"),
        "assets come from the admin itself"
    );
    let css = get("/jobs/assets/admin.css", Some("admin")).await.unwrap();
    assert_eq!(css.status(), StatusCode::OK);

    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .unwrap();
}
