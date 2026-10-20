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
//! - Workflow definition builder (a canvas of HTML nodes and SVG edges, no build step)
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
//!
//! Each page is a standalone page. To show them inside the host app's own admin layout and
//! theme, see [`app_with_layout`] and [`Layout`].

// Handlers return `Result<_, axum::response::Response>`; boxing the error
// to satisfy clippy (1.99+) would just add an allocation per error response.
#![allow(clippy::result_large_err)]

use axum::{
    extract::{DefaultBodyLimit, FromRef},
    routing::{get, post},
    Router,
};
use ishikari::execution::{ModuleError, NodeModules, NodeSpec};
use sqlx::PgPool;
use std::sync::Arc;
use tower_http::trace::TraceLayer;

pub mod layout;
pub mod routes;
mod same_origin;
pub mod templates;

pub use layout::{embed_stylesheet_href, Layout, Page};

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
}

impl NavItem {
    /// Create a new navigation item.
    pub fn new(label: impl Into<String>, href: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            href: href.into(),
            icon: None,
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
    pub base_path: Arc<String>,
    pub nav_items: Arc<Vec<NavItem>>,
    pub node_types: Arc<Vec<NodeSpec>>,
    pub node_modules: Option<Arc<dyn NodeModules>>,
}

impl AppState {
    /// Create new app state with the default public schema.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: None,
            base_path: Arc::new(String::new()),
            nav_items: Arc::new(Self::default_nav_items("")),
            node_types: Arc::new(Vec::new()),
            node_modules: None,
        }
    }

    /// Create app state with a custom schema for multi-tenant support.
    pub fn with_schema(pool: PgPool, schema: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            ..Self::new(pool)
        }
    }

    /// The node types the workflow builder offers: your
    /// [`NodeTypes`](ishikari::execution::NodeTypes)' catalog. Once you register them, a
    /// definition with a node of another type isn't saved. With none, the builder offers none,
    /// and takes any type you name.
    ///
    /// ```rust,no_run
    /// # use ishikari::execution::NodeTypes;
    /// # use ishikari_admin::AppState;
    /// # fn f(pool: sqlx::PgPool, types: NodeTypes) {
    /// let state = AppState::new(pool).with_node_types(types.catalog());
    /// # }
    /// ```
    pub fn with_node_types(mut self, node_types: Vec<NodeSpec>) -> Self {
        self.node_types = Arc::new(node_types);
        self
    }

    /// Node types your operators upload, beyond those you register in Rust: `ishikari-wasm`'s
    /// `WasmModules`, which you give your [`NodeTypes`](ishikari::execution::NodeTypes) too so
    /// they run. The Node types page lists and uploads them, and the builder offers them after
    /// your registered types (one named as a registered type is not offered: it wouldn't run).
    ///
    /// ```rust,ignore
    /// let modules = Arc::new(ishikari_wasm::WasmModules::new(pool.clone()));
    /// let types = NodeTypes::new(pool.clone()).modules(modules.clone());
    /// let state = AppState::new(pool)
    ///     .with_node_types(types.catalog())
    ///     .with_node_modules(modules);
    /// ```
    pub fn with_node_modules(mut self, modules: Arc<dyn NodeModules>) -> Self {
        self.node_modules = Some(modules);
        self.nav_items = Arc::new(Self::with_node_types_item(
            self.nav_items.to_vec(),
            &self.base_path,
        ));
        self
    }

    /// The node types the builder offers: those registered, then the latest version of each
    /// uploaded module not named as one of them.
    pub async fn catalog(&self) -> Result<Vec<NodeSpec>, ModuleError> {
        let mut catalog = self.node_types.to_vec();
        if let Some(modules) = &self.node_modules {
            for module in modules.modules().await? {
                if !self.node_types.iter().any(|t| t.name == module.spec.name) {
                    catalog.push(module.spec);
                }
            }
        }
        Ok(catalog)
    }

    /// Set the base path for when the admin is mounted at a subpath.
    ///
    /// For example, if mounted at "/admin", all links will be prefixed with "/admin".
    pub fn with_base_path(mut self, path: impl Into<String>) -> Self {
        let path = path.into();
        let path = path.trim_end_matches('/').to_string();
        self.nav_items = Arc::new(self.nav_items_at(&path));
        self.base_path = Arc::new(path);
        self
    }

    /// Add additional navigation items (for extension features).
    pub fn with_nav_items(mut self, items: Vec<NavItem>) -> Self {
        let base = self.base_path.as_str();
        let mut all_items = self.nav_items_at(base);
        // Prefix additional items with base path
        for mut item in items {
            if !item.href.starts_with("http") {
                item.href = format!("{}{}", base, item.href);
            }
            all_items.push(item);
        }
        self.nav_items = Arc::new(all_items);
        self
    }

    /// Get the schema name, if any.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Get the base path for URL prefixing.
    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    /// Get navigation items for the sidebar.
    pub fn nav_items(&self) -> &[NavItem] {
        &self.nav_items
    }

    /// The default navigation at `base`, with Node types when there are modules to upload.
    fn nav_items_at(&self, base: &str) -> Vec<NavItem> {
        let items = Self::default_nav_items(base);
        match self.node_modules {
            Some(_) => Self::with_node_types_item(items, base),
            None => items,
        }
    }

    /// `items` with Node types after Definitions (or last).
    fn with_node_types_item(mut items: Vec<NavItem>, base: &str) -> Vec<NavItem> {
        let href = format!("{base}/node-types");
        if items.iter().any(|item| item.href == href) {
            return items;
        }
        let definitions = format!("{base}/definitions");
        let at = items
            .iter()
            .position(|item| item.href == definitions)
            .map_or(items.len(), |i| i + 1);
        items.insert(
            at,
            NavItem::new("Node types", href).icon("M21 7.5l-9-5.25L3 7.5m18 0l-9 5.25m9-5.25v9l-9 5.25M3 7.5l9 5.25M3 7.5v9l9 5.25m0-9v9"),
        );
        items
    }

    fn default_nav_items(base: &str) -> Vec<NavItem> {
        let p = |path: &str| {
            if base.is_empty() {
                path.to_string()
            } else if path == "/" {
                base.to_string()
            } else {
                format!("{}{}", base, path)
            }
        };

        vec![
            NavItem::new("Dashboard", p("/")).icon("M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6"),
            NavItem::new("Jobs", p("/jobs")).icon("M21 13.255A23.931 23.931 0 0112 15c-3.183 0-6.22-.62-9-1.745M16 6V4a2 2 0 00-2-2h-4a2 2 0 00-2 2v2m4 6h.01M5 20h14a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"),
            NavItem::new("Queues", p("/queues")).icon("M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10"),
            NavItem::new("Definitions", p("/definitions")).icon("M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"),
            NavItem::new("Workflows", p("/workflows")).icon("M2.25 7.125C2.25 6.504 2.754 6 3.375 6h6c.621 0 1.125.504 1.125 1.125v3.75c0 .621-.504 1.125-1.125 1.125h-6a1.125 1.125 0 01-1.125-1.125v-3.75zM14.25 8.625c0-.621.504-1.125 1.125-1.125h5.25c.621 0 1.125.504 1.125 1.125v8.25c0 .621-.504 1.125-1.125 1.125h-5.25a1.125 1.125 0 01-1.125-1.125v-8.25zM3.75 16.125c0-.621.504-1.125 1.125-1.125h5.25c.621 0 1.125.504 1.125 1.125v2.25c0 .621-.504 1.125-1.125 1.125h-5.25a1.125 1.125 0 01-1.125-1.125v-2.25z"),
            NavItem::new("Sagas", p("/sagas")).icon("M19.5 12c0-1.232-.046-2.453-.138-3.662a4.006 4.006 0 00-3.7-3.7 48.678 48.678 0 00-7.324 0 4.006 4.006 0 00-3.7 3.7c-.017.22-.032.441-.046.662M19.5 12l3-3m-3 3l-3-3m-12 3c0 1.232.046 2.453.138 3.662a4.006 4.006 0 003.7 3.7 48.656 48.656 0 007.324 0 4.006 4.006 0 003.7-3.7c.017-.22.032-.441.046-.662M4.5 12l3 3m-3-3l-3 3"),
            NavItem::new("Dependencies", p("/dependencies")).icon("M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244"),
            NavItem::new("Analytics", p("/analytics")).icon("M3 13.125C3 12.504 3.504 12 4.125 12h2.25c.621 0 1.125.504 1.125 1.125v6.75C7.5 20.496 6.996 21 6.375 21h-2.25A1.125 1.125 0 013 19.875v-6.75zM9.75 8.625c0-.621.504-1.125 1.125-1.125h2.25c.621 0 1.125.504 1.125 1.125v11.25c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V8.625zM16.5 4.125c0-.621.504-1.125 1.125-1.125h2.25C20.496 3 21 3.504 21 4.125v15.75c0 .621-.504 1.125-1.125 1.125h-2.25a1.125 1.125 0 01-1.125-1.125V4.125z"),
            NavItem::new("Resolver", p("/resolver")).icon("M4.5 12a7.5 7.5 0 0015 0m-15 0a7.5 7.5 0 1115 0m-15 0H3m16.5 0H21m-1.5 0H12m-8.457 3.077l1.41-.513m14.095-5.13l1.41-.513M5.106 17.785l1.15-.964m11.49-9.642l1.149-.964M7.501 19.795l.75-1.3m7.5-12.99l.75-1.3m-6.063 16.658l.26-1.477m2.605-14.772l.26-1.477m0 17.726l-.26-1.477M10.698 4.614l-.26-1.477M16.5 19.794l-.75-1.299M7.5 4.205L12 12m6.894 5.785l-1.149-.964M6.256 7.178l-1.15-.964m15.352 8.864l-1.41-.513M4.954 9.435l-1.41-.514M12.002 12l-3.75 6.495"),
        ]
    }
}

/// Build the application router with all admin routes.
///
/// This returns a router that can be extended with additional routes
/// or mounted at a different path. Each page is a standalone page of its own; see
/// [`app_with_layout`] to show them inside the host's layout.
pub fn app(state: AppState) -> Router {
    routes().layer(TraceLayer::new_for_http()).with_state(state)
}

/// [`app`], each page inside the host's [`Layout`] (its own header, navigation and theme)
/// rather than the dashboard's standalone page. htmx fragments are never wrapped.
///
/// ```rust,no_run
/// # use axum::{http::request::Parts, response::{Html, IntoResponse, Response}, Router};
/// # use ishikari_admin::{AppState, Page};
/// # async fn f(pool: sqlx::PgPool) {
/// let layout = |_parts: &Parts, page: Page| -> Response {
///     Html(format!(
///         r#"<html data-theme="mine"><head><link rel="stylesheet" href="{}"></head>
///            <body><main>{}{}</main></body></html>"#,
///         ishikari_admin::embed_stylesheet_href(&page.base_path),
///         page.nav_html(),
///         page.body,
///     ))
///     .into_response()
/// };
/// let admin = ishikari_admin::app_with_layout(
///     AppState::new(pool).with_base_path("/jobs"),
///     layout,
/// );
/// let app: Router = Router::new().nest("/jobs", admin);
/// # }
/// ```
pub fn app_with_layout(state: AppState, layout: impl Layout + 'static) -> Router {
    let layout: Arc<dyn Layout> = Arc::new(layout);
    routes()
        .layer(axum::middleware::from_fn(move |req, next| {
            layout::wrap(layout.clone(), req, next)
        }))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn routes() -> Router<AppState> {
    Router::new()
        // Dashboard
        .route("/", get(routes::dashboard::index))
        // Jobs (enhanced with dependency info, workflows, etc.)
        .route("/jobs", get(routes::jobs::list))
        .route("/jobs/{id}", get(routes::jobs::show))
        .route("/jobs/{id}/retry", post(routes::actions::retry_job))
        .route("/jobs/{id}/cancel", post(routes::actions::cancel_job))
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
        .route(
            "/workflows/{id}/cancel",
            post(routes::actions::cancel_workflow),
        )
        .route(
            "/workflows/{id}/retry",
            post(routes::actions::retry_workflow),
        )
        .route(
            "/workflows/{id}/clone",
            post(routes::actions::clone_workflow),
        )
        // Workflow Definitions
        .route("/definitions", get(routes::definitions::list))
        .route("/definitions/new", get(routes::definitions::new_definition))
        .route(
            "/definitions/import",
            get(routes::definitions::import_form).post(routes::definitions::import_yaml),
        )
        .route("/definitions/{id}", get(routes::definitions::show))
        .route("/definitions/{id}/edit", get(routes::definitions::edit))
        .route(
            "/definitions/{id}/export",
            get(routes::definitions::export_yaml),
        )
        .route(
            "/definitions/{id}/delete",
            post(routes::definitions::delete),
        )
        .route(
            "/definitions/{id}/run",
            get(routes::definitions::run_form).post(routes::definitions::run),
        )
        // Node types: those registered, and modules uploaded
        .route(
            "/node-types",
            get(routes::node_types::list)
                .post(routes::node_types::upload)
                .layer(DefaultBodyLimit::max(routes::node_types::MAX_UPLOAD_BYTES)),
        )
        .route(
            "/node-types/script",
            get(routes::node_types::editor)
                .post(routes::node_types::script)
                .layer(DefaultBodyLimit::max(routes::node_types::MAX_UPLOAD_BYTES)),
        )
        .route(
            "/node-types/job",
            get(routes::node_types::job_form).post(routes::node_types::insert_job),
        )
        // API endpoints for htmx
        .route("/api/jobs", get(routes::api::enhanced_jobs_table))
        .route("/api/stats", get(routes::api::stats_or_counts))
        .route("/api/dependencies", get(routes::api::dependencies_table))
        .route("/api/sagas", get(routes::api::sagas_table))
        .route("/api/workflows", get(routes::api::workflows_table))
        .route("/api/workflows/{id}/jobs", get(routes::api::workflow_jobs))
        .route(
            "/api/definitions",
            get(routes::api::definitions_table).post(routes::api::create_definition),
        )
        // API endpoints for workflow builder (JSON)
        .route("/api/node-types", get(routes::api::node_types))
        .route(
            "/api/definitions/{id}",
            get(routes::api::get_definition).put(routes::api::update_definition),
        )
        .route(
            "/api/definitions/validate",
            post(routes::api::validate_definition),
        )
        // The workflow builder's old paths go to the definition editor
        .route("/builder/{*path}", get(routes::builder::redirect))
        .route("/builder", get(routes::builder::redirect))
        // Embedded CSS/JS for the admin UI
        .route("/assets/{file}", get(routes::assets::serve))
        .layer(axum::middleware::from_fn(same_origin::check))
}

/// Build the application router mounted at a specific path.
///
/// Use this when mounting the admin at a subpath like "/admin".
pub fn app_at(path: &str, state: AppState) -> Router {
    Router::new().nest(path, app(state))
}
