//! Route handlers for the admin interface.

pub mod actions;
pub mod analytics;
pub mod api;
pub mod assets;
pub mod builder;
pub mod dashboard;
pub mod definitions;
pub mod dependencies;
pub mod jobs;
pub mod node_types;
pub mod queues;
pub mod resolver;
pub mod sagas;
pub mod secrets;
pub mod workflows;

/// The highest page a list may be asked for.
const MAX_PAGE: i64 = 1_000_000;

/// The largest page a list may be asked for.
const MAX_PER_PAGE: i64 = 100;

/// A list's page (from 1), its size (`default` unless asked, from 1 to 100) and how many rows
/// come before it, from the `page` and `per_page` asked for.
pub(crate) fn paged(page: Option<i64>, per_page: Option<i64>, default: i64) -> (i64, i64, i64) {
    let page = page.unwrap_or(1).clamp(1, MAX_PAGE);
    let per_page = per_page.unwrap_or(default).clamp(1, MAX_PER_PAGE);
    (page, per_page, (page - 1).saturating_mul(per_page))
}
