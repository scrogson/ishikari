//! Frontend serving routes for the visual workflow builder.
//!
//! This module handles:
//! - Serving the embedded React SPA assets (when `embed-frontend` feature is enabled)
//! - Sending the builder's other paths to the definition editor, which runs it

use axum::{
    body::Body,
    http::{header, Response, StatusCode, Uri},
    response::IntoResponse,
};
#[cfg(feature = "embed-frontend")]
use axum::{extract::State, response::Redirect};

#[cfg(feature = "embed-frontend")]
use crate::AppState;

#[cfg(feature = "embed-frontend")]
use rust_embed::Embed;

#[cfg(feature = "embed-frontend")]
#[derive(Embed)]
#[folder = "frontend/dist"]
#[prefix = ""]
struct FrontendAssets;

/// Serve the workflow builder's embedded assets (`frontend/dist`).
///
/// The builder runs inside the definition editor, which gives it its configuration (the API's
/// base path), so anything else under `/builder` goes to the editor.
#[cfg(feature = "embed-frontend")]
pub async fn serve_builder(State(state): State<AppState>, uri: Uri) -> Response<Body> {
    let path = uri.path().trim_start_matches("/builder/");
    if !path.starts_with("assets/") {
        return Redirect::to(&format!("{}/definitions/new", state.base_path)).into_response();
    }

    match FrontendAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime.as_ref())
                .body(Body::from(content.data.into_owned()))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Not found"))
            .unwrap(),
    }
}

/// Placeholder when embed-frontend feature is disabled.
/// Returns 404 for assets, instructions page for other requests.
#[cfg(not(feature = "embed-frontend"))]
pub async fn serve_builder(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches("/builder/");

    // For asset requests, return 404 - they should use Vite dev server
    if path.starts_with("assets/") {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header(header::CONTENT_TYPE, "text/plain")
            .body(Body::from(
                "Assets not embedded. Run with --features embed-frontend or use Vite dev server.",
            ))
            .unwrap();
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html")
        .body(Body::from(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Workflow Builder - Development Mode</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background-color: #0f172a;
            color: #f8fafc;
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            margin: 0;
            padding: 2rem;
        }
        .container {
            max-width: 600px;
            text-align: center;
        }
        h1 { color: #3b82f6; margin-bottom: 1rem; }
        p { color: #94a3b8; line-height: 1.6; }
        code {
            background: #1e293b;
            padding: 0.25rem 0.5rem;
            border-radius: 0.25rem;
            font-size: 0.875rem;
        }
        .steps {
            text-align: left;
            background: #1e293b;
            padding: 1.5rem;
            border-radius: 0.5rem;
            margin-top: 1.5rem;
        }
        .steps li { margin-bottom: 0.75rem; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Workflow Builder</h1>
        <p>The frontend is not embedded. For development, run the Vite dev server:</p>
        <ol class="steps">
            <li>Navigate to the frontend directory: <code>cd frontend</code></li>
            <li>Install dependencies: <code>npm install</code></li>
            <li>Start the dev server: <code>npm run dev</code></li>
            <li>Open <code>http://localhost:5173</code></li>
        </ol>
        <p style="margin-top: 1.5rem;">
            For production, build with: <code>cargo build --features embed-frontend</code>
        </p>
    </div>
</body>
</html>"#,
        ))
        .unwrap()
}

/// Check if frontend assets are available.
#[cfg(feature = "embed-frontend")]
pub fn has_frontend_assets() -> bool {
    FrontendAssets::get("assets/index.js").is_some()
}

#[cfg(not(feature = "embed-frontend"))]
pub fn has_frontend_assets() -> bool {
    false
}
