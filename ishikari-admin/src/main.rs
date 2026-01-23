//! Ishikari Admin - Job queue dashboard
//!
//! A web-based admin interface for managing ishikari jobs.

use axum::{
    extract::FromRef,
    routing::get,
    Router,
};
use sqlx::PgPool;
use std::net::SocketAddr;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod routes;
mod templates;

/// Application state shared across all routes.
#[derive(Clone, FromRef)]
pub struct AppState {
    pub pool: PgPool,
    pub schema: Option<String>,
}

impl AppState {
    pub fn new(pool: PgPool) -> Self {
        Self { pool, schema: None }
    }

    pub fn with_schema(pool: PgPool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: Some(schema.into()),
        }
    }
}

/// Build the application router.
pub fn app(state: AppState) -> Router {
    Router::new()
        // Dashboard
        .route("/", get(routes::dashboard::index))
        // Jobs
        .route("/jobs", get(routes::jobs::list))
        .route("/jobs/{id}", get(routes::jobs::show))
        .route("/jobs/{id}/retry", axum::routing::post(routes::jobs::retry))
        .route("/jobs/{id}/cancel", axum::routing::post(routes::jobs::cancel))
        .route("/jobs/{id}/discard", axum::routing::post(routes::jobs::discard))
        // Queues
        .route("/queues", get(routes::queues::list))
        .route("/queues/{name}", get(routes::queues::show))
        // API endpoints for htmx
        .route("/api/jobs", get(routes::api::jobs_table))
        .route("/api/stats", get(routes::api::stats))
        // Static files
        .nest_service("/static", ServeDir::new("static"))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ishikari_admin=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Get database URL from environment
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");

    // Create database pool
    let pool = PgPool::connect(&database_url).await?;

    // Create app state
    let state = AppState::new(pool);

    // Build the app
    let app = app(state);

    // Run the server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("Ishikari Admin listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
