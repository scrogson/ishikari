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
use std::sync::Arc;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

pub mod routes;
pub mod templates;

// Re-export commonly used types
pub use routes::dashboard::{JobStats, RecentFailure};
pub use routes::jobs::{JobDetail, JobInfo};
pub use routes::queues::QueueInfo;

/// A navigation item for the sidebar.
#[derive(Clone, Debug)]
pub struct NavItem {
    pub label: String,
    pub href: String,
    pub icon: Option<String>,
    pub is_pro: bool,
}

impl NavItem {
    /// Create a new navigation item.
    pub fn new(label: impl Into<String>, href: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            href: href.into(),
            icon: None,
            is_pro: false,
        }
    }

    /// Create a pro navigation item.
    pub fn pro(label: impl Into<String>, href: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            href: href.into(),
            icon: None,
            is_pro: true,
        }
    }

    /// Set the icon for this navigation item (SVG path data).
    pub fn icon(mut self, icon: impl Into<String>) -> Self {
        self.icon = Some(icon.into());
        self
    }
}

/// Application state shared across all routes.
#[derive(Clone, FromRef)]
pub struct AppState {
    pub pool: PgPool,
    pub schema: Option<String>,
    pub nav_items: Arc<Vec<NavItem>>,
}

impl AppState {
    /// Create new app state with the default public schema.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: None,
            nav_items: Arc::new(Self::default_nav_items()),
        }
    }

    /// Create app state with a custom schema for multi-tenant support.
    pub fn with_schema(pool: PgPool, schema: impl Into<String>) -> Self {
        Self {
            pool,
            schema: Some(schema.into()),
            nav_items: Arc::new(Self::default_nav_items()),
        }
    }

    /// Add additional navigation items (for pro/extension features).
    pub fn with_nav_items(mut self, items: Vec<NavItem>) -> Self {
        let mut all_items = Self::default_nav_items();
        all_items.extend(items);
        self.nav_items = Arc::new(all_items);
        self
    }

    /// Get the schema name, if any.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Get navigation items for the sidebar.
    pub fn nav_items(&self) -> &[NavItem] {
        &self.nav_items
    }

    fn default_nav_items() -> Vec<NavItem> {
        vec![
            NavItem::new("Dashboard", "/").icon("M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"),
            NavItem::new("Jobs", "/jobs").icon("M21 13.255A23.931 23.931 0 0112 15c-3.183 0-6.22-.62-9-1.745M16 6V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v2m4 6h.01M5 20h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"),
            NavItem::new("Queues", "/queues").icon("M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"),
        ]
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
