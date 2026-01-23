//! Askama templates for the admin interface.

use askama::Template;
use askama_web::WebTemplate;

use crate::routes::dashboard::{JobStats, RecentFailure};
use crate::routes::jobs::{JobDetail, JobInfo};
use crate::routes::queues::QueueInfo;

/// Dashboard page template.
#[derive(Template, WebTemplate)]
#[template(path = "dashboard.html")]
pub struct DashboardTemplate {
    pub stats: JobStats,
    pub recent_failures: Vec<RecentFailure>,
}

/// Jobs list page template.
#[derive(Template, WebTemplate)]
#[template(path = "jobs/list.html")]
pub struct JobsListTemplate {
    pub jobs: Vec<JobInfo>,
    pub current_state: Option<String>,
    pub current_queue: Option<String>,
    pub current_worker: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Job detail page template.
#[derive(Template, WebTemplate)]
#[template(path = "jobs/show.html")]
pub struct JobDetailTemplate {
    pub job: JobDetail,
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
    pub jobs: Vec<JobInfo>,
    pub current_state: Option<String>,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
}

/// Jobs table partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/jobs_table.html")]
pub struct JobsTablePartial {
    pub jobs: Vec<JobInfo>,
    pub page: i64,
    pub total_pages: i64,
}

/// Stats partial for htmx updates.
#[derive(Template, WebTemplate)]
#[template(path = "partials/stats.html")]
pub struct StatsPartial {
    pub stats: JobStats,
}
