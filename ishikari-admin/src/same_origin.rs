//! Refusing actions a browser sends from another site (CSRF), and pages framed by one.

use axum::{
    extract::Request,
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

/// Refuse a request that changes something (anything but GET, HEAD and OPTIONS) when a browser
/// sent it from another origin, so another site can't have an admin's browser retry, cancel or
/// run jobs. A browser says where a request comes from in `Sec-Fetch-Site`, or else (older
/// browsers) in `Origin`, compared with `Host`. A request with neither isn't a browser's, and
/// passes: the host's auth guards those.
///
/// A page may be framed by its own origin only (`X-Frame-Options`, unless the host set it):
/// another site framing it could have the user click its buttons, sent as the page's own.
pub(crate) async fn check(req: Request, next: Next) -> Response {
    if is_cross_origin(&req) {
        return (StatusCode::FORBIDDEN, "Cross-origin request refused").into_response();
    }
    let mut res = next.run(req).await;
    res.headers_mut()
        .entry(header::X_FRAME_OPTIONS)
        .or_insert(HeaderValue::from_static("SAMEORIGIN"));
    res
}

fn is_cross_origin(req: &Request) -> bool {
    if matches!(*req.method(), Method::GET | Method::HEAD | Method::OPTIONS) {
        return false;
    }
    let headers = req.headers();
    if let Some(site) = headers.get("sec-fetch-site") {
        // `none`: the user's own doing (typed, bookmarked), not a page's.
        return !matches!(site.as_bytes(), b"same-origin" | b"none");
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return false;
    };
    let origin_host = origin
        .to_str()
        .ok()
        .and_then(|o| o.split_once("://"))
        .map(|(_, host)| host);
    // HTTP/2 has the host in the URI rather than a header.
    let host = headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .or_else(|| req.uri().authority().map(|a| a.as_str()));
    origin_host.is_none() || origin_host != host
}
