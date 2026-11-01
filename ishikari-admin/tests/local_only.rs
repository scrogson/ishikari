//! A server on the loopback interface answers only requests for it by name (DNS rebinding).

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::routing::get;
use axum::Router;
use tower::ServiceExt;

async fn status(app: &Router, host: Option<&str>, uri: &str) -> StatusCode {
    let mut req = Request::builder().uri(uri);
    if let Some(host) = host {
        req = req.header(header::HOST, host);
    }
    app.clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
        .status()
}

#[tokio::test]
async fn only_requests_for_the_loopback_interface_are_answered() {
    let app = ishikari_admin::local_only(Router::new().route("/", get(|| async { "ok" })));
    for host in [
        "localhost",
        "localhost:3000",
        "LocalHost:3000",
        "127.0.0.1",
        "127.0.0.1:3000",
        "[::1]",
        "[::1]:3000",
    ] {
        assert_eq!(
            status(&app, Some(host), "/").await,
            StatusCode::OK,
            "{host}"
        );
    }
    for host in [
        "attacker.example",
        "attacker.example:3000",
        "localhost.attacker.example",
        "127.0.0.1.nip.io:3000",
        "localhost:3000@attacker.example",
        "localhost:",
        "localhost:3000x",
        "::1",
        "",
    ] {
        assert_eq!(
            status(&app, Some(host), "/").await,
            StatusCode::FORBIDDEN,
            "{host}"
        );
    }
    // Without a Host, an HTTP/2 request's authority is its host.
    assert_eq!(
        status(&app, None, "http://localhost:3000/").await,
        StatusCode::OK
    );
    assert_eq!(
        status(&app, None, "http://attacker.example/").await,
        StatusCode::FORBIDDEN
    );
    assert_eq!(status(&app, None, "/").await, StatusCode::FORBIDDEN);
}
