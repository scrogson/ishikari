//! Frontend serving routes for the visual workflow builder.
//!
//! This module handles:
//! - Serving the embedded React SPA assets (when `embed-frontend` feature is enabled)
//! - SPA routing fallback to index.html

use axum::{
    body::Body,
    http::{header, Response, StatusCode, Uri},
    response::IntoResponse,
};

#[cfg(feature = "embed-frontend")]
use rust_embed::Embed;

#[cfg(feature = "embed-frontend")]
#[derive(Embed)]
#[folder = "frontend/dist"]
#[prefix = ""]
struct FrontendAssets;

/// Serve embedded static assets for the workflow builder.
///
/// When the `embed-frontend` feature is enabled, this serves files from the
/// embedded `frontend/dist` directory. For SPA routing, requests to unknown
/// paths return `index.html`.
#[cfg(feature = "embed-frontend")]
pub async fn serve_builder(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches("/builder/");

    // Try to serve the exact file
    if let Some(content) = FrontendAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, mime.as_ref())
            .body(Body::from(content.data.into_owned()))
            .unwrap();
    }

    // For assets directory, return 404
    if path.starts_with("assets/") {
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Not found"))
            .unwrap();
    }

    // SPA fallback: serve index.html for all other routes
    if let Some(content) = FrontendAssets::get("index.html") {
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html")
            .body(Body::from(content.data.into_owned()))
            .unwrap();
    }

    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(
            "Frontend not found. Run `npm run build` in the frontend directory.",
        ))
        .unwrap()
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
    FrontendAssets::get("index.html").is_some()
}

#[cfg(not(feature = "embed-frontend"))]
pub fn has_frontend_assets() -> bool {
    false
}
