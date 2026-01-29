//! Askama templates for the pro admin interface.

use askama::Template;
use askama_web::WebTemplate;

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

/// Dashboard page template.
#[derive(Template, WebTemplate)]
#[template(path = "dashboard.html")]
pub struct DashboardTemplate {
    pub stats: JobStats,
    pub workflow_stats: WorkflowStats,
    pub recent_failures: Vec<RecentFailure>,
}

/// Queues list page template.
#[derive(Template, WebTemplate)]
#[template(path = "queues/list.html")]
pub struct QueuesListTemplate {
    pub queues: Vec<QueueInfo>,
}

/// Queue detail page template.
#[derive(Template, WebTemplate)]
#[template(path = "queues/show.html")]
pub struct QueueDetailTemplate {
    pub queue_name: String,
    pub stats: QueueInfo,
    pub jobs: Vec<QueueJobInfo>,
    pub current_state: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Workflows list page template.
#[derive(Template, WebTemplate)]
#[template(path = "workflows/list.html")]
pub struct WorkflowsListTemplate {
    pub workflows: Vec<WorkflowInfo>,
    pub current_state: Option<String>,
    pub current_name: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Workflow detail page template.
#[derive(Template, WebTemplate)]
#[template(path = "workflows/show.html")]
pub struct WorkflowDetailTemplate {
    pub workflow: WorkflowDetail,
    pub jobs: Vec<WorkflowJobInfo>,
}

/// Workflows table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/workflows_table.html")]
pub struct WorkflowsTablePartial {
    pub workflows: Vec<WorkflowInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Workflow jobs partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/workflow_jobs.html")]
pub struct WorkflowJobsPartial {
    pub jobs: Vec<WorkflowJobInfo>,
}

/// Dependencies list page template.
#[derive(Template, WebTemplate)]
#[template(path = "dependencies/list.html")]
pub struct DependenciesListTemplate {
    pub dependencies: Vec<DependencyInfo>,
    pub stats: DependencyStats,
    pub show_blocked_only: bool,
    pub workflow_id: Option<i64>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Dependency detail page template (job with its dependencies).
#[derive(Template, WebTemplate)]
#[template(path = "dependencies/show.html")]
pub struct DependencyDetailTemplate {
    pub job: JobWithDependencies,
}

/// Dependencies table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/dependencies_table.html")]
pub struct DependenciesTablePartial {
    pub dependencies: Vec<DependencyInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Sagas list page template.
#[derive(Template, WebTemplate)]
#[template(path = "sagas/list.html")]
pub struct SagasListTemplate {
    pub sagas: Vec<SagaInfo>,
    pub current_state: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Saga detail page template.
#[derive(Template, WebTemplate)]
#[template(path = "sagas/show.html")]
pub struct SagaDetailTemplate {
    pub saga: SagaDetail,
    pub steps: Vec<SagaStepInfo>,
}

/// Sagas table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/sagas_table.html")]
pub struct SagasTablePartial {
    pub sagas: Vec<SagaInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Resolver status page template.
#[derive(Template, WebTemplate)]
#[template(path = "resolver/status.html")]
pub struct ResolverStatusTemplate {
    pub stats: ResolverStats,
    pub pending_releases: Vec<PendingRelease>,
    pub blocked_jobs: Vec<BlockedJob>,
    pub compensating_sagas: Vec<CompensatingSaga>,
    pub pending_dependencies: Vec<PendingDependency>,
}

/// Enhanced jobs list page template.
#[derive(Template, WebTemplate)]
#[template(path = "jobs/list.html")]
pub struct EnhancedJobsListTemplate {
    pub jobs: Vec<EnhancedJobInfo>,
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
#[derive(Template, WebTemplate)]
#[template(path = "jobs/show.html")]
pub struct EnhancedJobDetailTemplate {
    pub job: EnhancedJobDetail,
}

/// Enhanced jobs table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/enhanced_jobs_table.html")]
pub struct EnhancedJobsTablePartial {
    pub jobs: Vec<EnhancedJobInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Analytics dashboard template.
#[derive(Template, WebTemplate)]
#[template(path = "analytics/index.html")]
pub struct AnalyticsTemplate {
    pub analytics: Analytics,
}

/// Workflow definitions list page template.
#[derive(Template, WebTemplate)]
#[template(path = "definitions/list.html")]
pub struct DefinitionsListTemplate {
    pub definitions: Vec<DefinitionInfo>,
    pub current_name: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
    pub error: Option<String>,
}

/// Workflow definition detail page template.
#[derive(Template, WebTemplate)]
#[template(path = "definitions/show.html")]
pub struct DefinitionDetailTemplate {
    pub definition: DefinitionDetail,
}

/// Workflow definition import form template.
#[derive(Template, WebTemplate)]
#[template(path = "definitions/import.html")]
pub struct DefinitionImportTemplate {
    pub error: Option<String>,
    pub yaml: String,
}

/// Workflow definition run form template.
#[derive(Template, WebTemplate)]
#[template(path = "definitions/run.html")]
pub struct DefinitionRunTemplate {
    pub definition: DefinitionDetail,
    pub runs: Vec<WorkflowRunInfo>,
    pub error: Option<String>,
}

/// Definitions table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/definitions_table.html")]
pub struct DefinitionsTablePartial {
    pub definitions: Vec<DefinitionInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Workflow definition editor (React SPA wrapper).
#[derive(Template, WebTemplate)]
#[template(path = "definitions/edit.html")]
pub struct DefinitionEditTemplate {
    pub definition_id: Option<i64>,
    pub name: String,
    pub is_new: bool,
}
