//! Answering only requests for the loopback interface by name, for a server listening on it.

use axum::{
    extract::Request,
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Router,
};

/// `app`, answering only requests whose host is `localhost`, `127.0.0.1` or `[::1]` (with any
/// port), for a server on the loopback interface with no auth in front of it, as the
/// standalone admin is. A site the user visits can point its own name at 127.0.0.1 (DNS
/// rebinding), and its pages' requests then reach the server as their own origin's, with
/// `Host` still its name.
pub fn local_only(app: Router) -> Router {
    app.layer(axum::middleware::from_fn(check))
}

async fn check(req: Request, next: Next) -> Response {
    // HTTP/2 has the host in the URI rather than a header.
    let host = match req.headers().get(header::HOST) {
        Some(host) => host.to_str().ok(),
        None => req.uri().authority().map(|a| a.as_str()),
    };
    if !host.is_some_and(is_loopback) {
        return (StatusCode::FORBIDDEN, "Only localhost is served").into_response();
    }
    next.run(req).await
}

fn is_loopback(host: &str) -> bool {
    let name = match host.rsplit_once(':') {
        // `[::1]` has colons of its own, but no port.
        Some((name, port)) if !host.ends_with(']') => {
            if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
                return false;
            }
            name
        }
        _ => host,
    };
    name.eq_ignore_ascii_case("localhost") || name == "127.0.0.1" || name == "[::1]"
}
