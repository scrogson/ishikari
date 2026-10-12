//! Askama templates for the admin interface.

use askama::Template;
use askama_web::WebTemplate;
use axum::response::{IntoResponse, Response};

use crate::layout::{respond, Content};

use crate::routes::analytics::Analytics;
use crate::routes::dashboard::{JobStats, RecentFailure, WorkflowStats};
use crate::routes::definitions::{DefinitionDetail, DefinitionInfo, WorkflowRunInfo};
use crate::routes::dependencies::{DependencyInfo, DependencyStats, JobWithDependencies};
use crate::routes::jobs::{EnhancedJobDetail, EnhancedJobInfo};
use crate::routes::queues::{QueueInfo, QueueJobInfo};
use crate::routes::resolver::{
    BlockedJob, CompensatingSaga, PendingDependency, PendingRelease, ResolverStats,
};
use crate::routes::sagas::{SagaDetail, SagaInfo, SagaStepInfo};
use crate::routes::workflows::{WorkflowDetail, WorkflowInfo, WorkflowJobInfo};
use crate::NavItem;

/// Custom askama filters available to all admin templates.
mod filters {
    use std::fmt::Display;

    /// Last `::` segment of a path, e.g. `app::workers::SendEmail` -> `SendEmail`.
    pub fn short_name<T: Display>(s: T, _: &dyn askama::Values) -> askama::Result<String> {
        let s = s.to_string();
        Ok(s.rsplit("::").next().unwrap_or(&s).to_string())
    }
}

/// Pages: the standalone page as their response, their title and content blocks for a host's
/// layout.
macro_rules! pages {
    ($($page:ty),* $(,)?) => {$(
        impl Content for $page {
            fn title(&self) -> askama::Result<String> {
                self.as_title().render()
            }
            fn content(&self) -> askama::Result<String> {
                self.as_content().render()
            }
            fn base_path(&self) -> &str {
                &self.base_path
            }
            fn nav_items(&self) -> &[NavItem] {
                &self.nav_items
            }
        }

        impl IntoResponse for $page {
            fn into_response(self) -> Response {
                respond(self)
            }
        }
    )*};
}

pages!(
    DashboardTemplate,
    QueuesListTemplate,
    QueueDetailTemplate,
    WorkflowsListTemplate,
    WorkflowDetailTemplate,
    DependenciesListTemplate,
    DependencyDetailTemplate,
    SagasListTemplate,
    SagaDetailTemplate,
    ResolverStatusTemplate,
    EnhancedJobsListTemplate,
    EnhancedJobDetailTemplate,
    AnalyticsTemplate,
    DefinitionsListTemplate,
    DefinitionDetailTemplate,
    DefinitionImportTemplate,
    DefinitionRunTemplate,
    DefinitionEditTemplate,
);

/// Dashboard page template.
#[derive(Template)]
#[template(path = "dashboard.html", blocks = ["title", "content"])]
pub struct DashboardTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub stats: JobStats,
    pub workflow_stats: WorkflowStats,
    pub recent_failures: Vec<RecentFailure>,
}

/// The dashboard's job counts, refreshed in place.
#[derive(Template, WebTemplate)]
#[template(path = "partials/stats.html")]
pub struct StatsPartial {
    pub stats: JobStats,
}

/// Queues list page template.
#[derive(Template)]
#[template(path = "queues/list.html", blocks = ["title", "content"])]
pub struct QueuesListTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub queues: Vec<QueueInfo>,
}

/// Queue detail page template.
#[derive(Template)]
#[template(path = "queues/show.html", blocks = ["title", "content"])]
pub struct QueueDetailTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub queue_name: String,
    pub stats: QueueInfo,
    pub jobs: Vec<QueueJobInfo>,
    pub current_state: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Workflows list page template.
#[derive(Template)]
#[template(path = "workflows/list.html", blocks = ["title", "content"])]
pub struct WorkflowsListTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub workflows: Vec<WorkflowInfo>,
    pub current_state: Option<String>,
    pub current_name: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Workflow detail page template.
#[derive(Template)]
#[template(path = "workflows/show.html", blocks = ["title", "content"])]
pub struct WorkflowDetailTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub workflow: WorkflowDetail,
    pub jobs: Vec<WorkflowJobInfo>,
}

/// Workflows table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/workflows_table.html")]
pub struct WorkflowsTablePartial {
    pub base_path: String,
    pub workflows: Vec<WorkflowInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Workflow jobs partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/workflow_jobs.html")]
pub struct WorkflowJobsPartial {
    pub base_path: String,
    pub jobs: Vec<WorkflowJobInfo>,
}

/// Dependencies list page template.
#[derive(Template)]
#[template(path = "dependencies/list.html", blocks = ["title", "content"])]
pub struct DependenciesListTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub dependencies: Vec<DependencyInfo>,
    pub stats: DependencyStats,
    pub show_blocked_only: bool,
    pub workflow_id: Option<i64>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Dependency detail page template (job with its dependencies).
#[derive(Template)]
#[template(path = "dependencies/show.html", blocks = ["title", "content"])]
pub struct DependencyDetailTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub job: JobWithDependencies,
}

/// Dependencies table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/dependencies_table.html")]
pub struct DependenciesTablePartial {
    pub base_path: String,
    pub dependencies: Vec<DependencyInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Sagas list page template.
#[derive(Template)]
#[template(path = "sagas/list.html", blocks = ["title", "content"])]
pub struct SagasListTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub sagas: Vec<SagaInfo>,
    pub current_state: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Saga detail page template.
#[derive(Template)]
#[template(path = "sagas/show.html", blocks = ["title", "content"])]
pub struct SagaDetailTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub saga: SagaDetail,
    pub steps: Vec<SagaStepInfo>,
}

/// Sagas table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/sagas_table.html")]
pub struct SagasTablePartial {
    pub base_path: String,
    pub sagas: Vec<SagaInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Resolver status page template.
#[derive(Template)]
#[template(path = "resolver/status.html", blocks = ["title", "content"])]
pub struct ResolverStatusTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub stats: ResolverStats,
    pub pending_releases: Vec<PendingRelease>,
    pub blocked_jobs: Vec<BlockedJob>,
    pub compensating_sagas: Vec<CompensatingSaga>,
    pub pending_dependencies: Vec<PendingDependency>,
}

/// Enhanced jobs list page template.
#[derive(Template)]
#[template(path = "jobs/list.html", blocks = ["title", "content"])]
pub struct EnhancedJobsListTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub jobs: Vec<EnhancedJobInfo>,
    /// Distinct queue names, for the queue filter.
    pub queues: Vec<String>,
    /// Distinct worker names, for the worker filter.
    pub workers: Vec<String>,
    pub current_state: Option<String>,
    pub current_queue: Option<String>,
    pub current_worker: Option<String>,
    pub has_deps_filter: Option<bool>,
    pub is_blocking_filter: Option<bool>,
    pub in_workflow_filter: Option<bool>,
    pub workflow_id_filter: Option<i64>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Enhanced job detail page template.
#[derive(Template)]
#[template(path = "jobs/show.html", blocks = ["title", "content"])]
pub struct EnhancedJobDetailTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub job: EnhancedJobDetail,
}

/// Enhanced jobs table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/enhanced_jobs_table.html")]
pub struct EnhancedJobsTablePartial {
    pub base_path: String,
    pub jobs: Vec<EnhancedJobInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Analytics dashboard template.
#[derive(Template)]
#[template(path = "analytics/index.html", blocks = ["title", "content"])]
pub struct AnalyticsTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub analytics: Analytics,
}

/// Workflow definitions list page template.
#[derive(Template)]
#[template(path = "definitions/list.html", blocks = ["title", "content"])]
pub struct DefinitionsListTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub definitions: Vec<DefinitionInfo>,
    pub current_name: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
    pub error: Option<String>,
}

/// Workflow definition detail page template.
#[derive(Template)]
#[template(path = "definitions/show.html", blocks = ["title", "content"])]
pub struct DefinitionDetailTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub definition: DefinitionDetail,
}

/// Workflow definition import form template.
#[derive(Template)]
#[template(path = "definitions/import.html", blocks = ["title", "content"])]
pub struct DefinitionImportTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub error: Option<String>,
    pub yaml: String,
}

/// Workflow definition run form template.
#[derive(Template)]
#[template(path = "definitions/run.html", blocks = ["title", "content"])]
pub struct DefinitionRunTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub definition: DefinitionDetail,
    pub runs: Vec<WorkflowRunInfo>,
    pub error: Option<String>,
}

/// Definitions table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/definitions_table.html")]
pub struct DefinitionsTablePartial {
    pub base_path: String,
    pub definitions: Vec<DefinitionInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Workflow definition editor (React SPA wrapper).
#[derive(Template)]
#[template(path = "definitions/edit.html", blocks = ["title", "content"])]
pub struct DefinitionEditTemplate {
    pub base_path: String,
    pub nav_items: Vec<NavItem>,
    pub definition_id: Option<i64>,
    pub name: String,
    pub is_new: bool,
}
