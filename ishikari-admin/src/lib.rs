//! Ishikari Admin - Complete job and workflow dashboard
//!
//! A web-based admin interface for managing ishikari jobs, workflows, and more.
//!
//! # Features
//!
//! - Dashboard with job statistics
//! - Job listing with dependency information
//! - Queue management
//! - Workflow listing and details
//! - Workflow definition builder (React + xyflow)
//! - Pipeline/DAG/Saga visualization
//! - Job dependency graphs
//! - Analytics dashboard
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

use axum::{
    extract::FromRef,
    routing::{get, post},
    Router,
};
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
            NavItem::new("Definitions", "/definitions").icon("M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"),
            NavItem::new("Workflows", "/workflows").icon("M2.25 7.125C2.25 6.504 2.754 6 3.375 6h6c.621 0 1.125.504 1.125 1.125v3.75c0 .621-.504 1.125-1.125 1.125h-6a1.125 1.125 0 01-1.125-1.125v-3.75zM14.25 8.625c0-.621.504-1.125 1.125-1.125h5.25c.621 0 1.125.504 1.125 1.125v8.25c0 .621-.504 1.125-1.125 1.125h-5.25a1.125 1.125 0 01-1.125-1.125v-8.25zM3.75 16.125c0-.621.504-1.125 1.125-1.125h5.25c.621 0 1.125.504 1.125 1.125v2.25c0 .621-.504 1.125-1.125 1.125h-5.25a1.125 1.125 0 01-1.125-1.125v-2.25z"),
            NavItem::new("Sagas", "/sagas").icon("M19.5 12c0-1.232-.046-2.453-.138-3.662a4.006 4.006 0 00-3.7-3.7 48.678 48.678 0 00-7.324 0 4.006 4.006 0 00-3.7 3.7c-.017.22-.032.441-.046.662M19.5 12l3-3m-3 3l-3-3m-12 3c0 1.232.046 2.453.138 3.662a4.006 4.006 0 003.7 3.7 48.656 48.656 0 007.324 0 4.006 4.006 0 003.7-3.7c.017-.22.032-.441.046-.662M4.5 12l3 3m-3-3l-3 3"),
            NavItem::new("Dependencies", "/dependencies").icon("M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244"),
            NavItem::new("Analytics", "/analytics").icon("M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z"),
            NavItem::new("Resolver", "/resolver").icon("M4.5 12a7.5 7.5 0 0015 0m-15 0a7.5 7.5 0 1115 0m-15 0H3m16.5 0H21m-1.5 0H12m-8.457 3.077l1.41-.513m14.095-5.13l1.41-.513M5.106 17.785l1.15-.964m11.49-9.642l1.149-.964M7.501 19.795l.75-1.3m7.5-12.99l.75-1.3m-6.063 16.658l.26-1.477m2.605-14.772l.26-1.477m0 17.726l-.26-1.477M10.698 4.614l-.26-1.477M16.5 19.794l-.75-1.299M7.5 4.205L12 12m6.894 5.785l-1.149-.964M6.256 7.178l-1.15-.964m15.352 8.864l-1.41-.513M4.954 9.435l-1.41-.514M12.002 12l-3.75 6.495"),
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
        // Jobs (enhanced with dependency info, workflows, etc.)
        .route("/jobs", get(routes::jobs::list))
        .route("/jobs/{id}", get(routes::jobs::show))
        .route("/jobs/{id}/retry", post(routes::actions::retry_job))
        .route("/jobs/{id}/cancel", post(routes::actions::cancel_job))
        .route("/jobs/{id}/discard", post(routes::jobs::discard))
        // Queues
        .route("/queues", get(routes::queues::list))
        .route("/queues/{name}", get(routes::queues::show))
        // Analytics
        .route("/analytics", get(routes::analytics::index))
        // Dependencies
        .route("/dependencies", get(routes::dependencies::list))
        .route("/dependencies/{id}", get(routes::dependencies::show))
        // Resolver
        .route("/resolver", get(routes::resolver::status))
        // Sagas
        .route("/sagas", get(routes::sagas::list))
        .route("/sagas/{id}", get(routes::sagas::show))
        // Workflows
        .route("/workflows", get(routes::workflows::list))
        .route("/workflows/{id}", get(routes::workflows::show))
        .route("/workflows/{id}/cancel", post(routes::actions::cancel_workflow))
        .route("/workflows/{id}/retry", post(routes::actions::retry_workflow))
        .route("/workflows/{id}/clone", post(routes::actions::clone_workflow))
        // Workflow Definitions
        .route("/definitions", get(routes::definitions::list))
        .route("/definitions/new", get(routes::definitions::new_definition))
        .route(
            "/definitions/import",
            get(routes::definitions::import_form).post(routes::definitions::import_yaml),
        )
        .route("/definitions/{id}", get(routes::definitions::show))
        .route("/definitions/{id}/edit", get(routes::definitions::edit))
        .route("/definitions/{id}/export", get(routes::definitions::export_yaml))
        .route("/definitions/{id}/delete", post(routes::definitions::delete))
        .route(
            "/definitions/{id}/run",
            get(routes::definitions::run_form).post(routes::definitions::run),
        )
        // API endpoints for htmx
        .route("/api/jobs", get(routes::api::enhanced_jobs_table))
        .route("/api/stats", get(routes::api::stats))
        .route("/api/dependencies", get(routes::api::dependencies_table))
        .route("/api/sagas", get(routes::api::sagas_table))
        .route("/api/workflows", get(routes::api::workflows_table))
        .route("/api/workflows/{id}/jobs", get(routes::api::workflow_jobs))
        .route("/api/definitions", get(routes::api::definitions_table).post(routes::api::create_definition))
        // API endpoints for workflow builder (JSON)
        .route("/api/node-types", get(routes::api::node_types))
        .route(
            "/api/definitions/{id}",
            get(routes::api::get_definition).put(routes::api::update_definition),
        )
        .route("/api/definitions/validate", post(routes::api::validate_definition))
        // Visual workflow builder (React SPA)
        .route("/builder/{*path}", get(routes::frontend::serve_builder))
        .route("/builder", get(routes::frontend::serve_builder))
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
