//! Ishikari Admin - Job queue dashboard
//!
//! A web-based admin interface for managing ishikari jobs.
//!
//! # Usage
//!
//! ```rust,no_run
//! use ishikari_admin::{app, AppState};
//! use sqlx::PgPool;
//!
//! #[tokio::main]
//! async fn main() {
//!     let pool = PgPool::connect("postgres://...").await.unwrap();
//!     let state = AppState::new(pool);
//!     let app = app(state);
//!     // ... serve the app
//! }
//! ```
//!
//! # Composing with Pro Features
//!
//! If you have access to ishikari-pro-admin, you can merge the routes:
//!
//! ```rust,ignore
//! use ishikari_admin::{app, AppState};
//! use ishikari_pro_admin;
//!
//! let state = AppState::new(pool);
//! let app = app(state.clone())
//!     .merge(ishikari_pro_admin::routes(state));
//! ```

use axum::{extract::FromRef, routing::get, Router};
use sqlx::PgPool;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

pub mod routes;
pub mod templates;

// Re-export commonly used types
pub use routes::dashboard::{JobStats, RecentFailure};
pub use routes::jobs::{JobDetail, JobInfo};
pub use routes::queues::QueueInfo;

/// Application state shared across all routes.
#[derive(Clone, FromRef)]
pub struct AppState {
    pub pool: PgPool,
    pub schema: Option<String>,
}

impl AppState {
    /// Create new app state with the default public schema.
    pub fn new(pool: PgPool) -> Self {
        Self { pool, schema: None }
    }

    /// Create app state with a custom schema for multi-tenant support.
    pub fn with_schema(pool: PgPool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: Some(schema.into()),
        }
    }

    /// Get the schema name, if any.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }
}

/// Build the application router with all admin routes.
///
/// This returns a router that can be extended with additional routes
/// or mounted at a different path.
pub fn app(state: AppState) -> Router {
    Router::new()
        // Dashboard
        .route("/", get(routes::dashboard::index))
        // Jobs
        .route("/jobs", get(routes::jobs::list))
        .route("/jobs/{id}", get(routes::jobs::show))
        .route("/jobs/{id}/retry", axum::routing::post(routes::jobs::retry))
        .route(
            "/jobs/{id}/cancel",
            axum::routing::post(routes::jobs::cancel),
        )
        .route(
            "/jobs/{id}/discard",
            axum::routing::post(routes::jobs::discard),
        )
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

/// Build the application router mounted at a specific path.
///
/// Use this when mounting the admin at a subpath like "/admin".
pub fn app_at(path: &str, state: AppState) -> Router {
    Router::new().nest(path, app(state))
}
