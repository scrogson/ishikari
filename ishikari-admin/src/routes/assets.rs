//! Embedded static assets for the server-rendered admin UI.
//!
//! `admin.css` (the standalone pages, light and dark) and `admin-embed.css` (for a host's
//! layout: no theme, colored by the host's variables) are compiled from `styles/` (Tailwind);
//! rebuild them with `mise run admin:css` after changing templates. `builder.js` is the
//! workflow builder, which the definition editor loads.

use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};

const ADMIN_CSS: &str = include_str!("../../assets/admin.css");
const ADMIN_EMBED_CSS: &str = include_str!("../../assets/admin-embed.css");
const ADMIN_JS: &str = include_str!("../../assets/admin.js");
const BUILDER_JS: &str = include_str!("../../assets/builder.js");
const HTMX_JS: &str = include_str!("../../assets/htmx.min.js");

/// Serve an embedded asset by file name.
pub async fn serve(Path(file): Path<String>) -> Response {
    let (content_type, body) = match file.as_str() {
        "admin.css" => ("text/css; charset=utf-8", ADMIN_CSS),
        "admin-embed.css" => ("text/css; charset=utf-8", ADMIN_EMBED_CSS),
        "admin.js" => ("text/javascript; charset=utf-8", ADMIN_JS),
        "builder.js" => ("text/javascript; charset=utf-8", BUILDER_JS),
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
