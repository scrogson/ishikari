//! Embedded static assets for the server-rendered admin UI.
//!
//! `admin.css` is compiled from `styles/admin.css` (Tailwind + daisyUI);
//! rebuild it with `mise run admin:css` after changing templates.

use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};

const ADMIN_CSS: &str = include_str!("../../assets/admin.css");
const HTMX_JS: &str = include_str!("../../assets/htmx.min.js");

/// Serve an embedded asset by file name.
pub async fn serve(Path(file): Path<String>) -> Response {
    let (content_type, body) = match file.as_str() {
        "admin.css" => ("text/css; charset=utf-8", ADMIN_CSS),
        "htmx.min.js" => ("text/javascript; charset=utf-8", HTMX_JS),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };

    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        body,
    )
        .into_response()
}
