//! API route handlers for htmx updates and workflow builder JSON API.

#![allow(clippy::type_complexity)]

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Serialize;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;

use super::definitions::{
    DefinitionDetail, DefinitionInfo, DefinitionsQuery, NodeDefinition, SchemaField,
    WorkflowDefinition,
};
use super::dependencies::{DependenciesQuery, DependencyInfo};
use super::jobs::{EnhancedJobInfo, EnhancedJobsQuery};
use super::sagas::{SagaInfo, SagasQuery};
use super::workflows::{WorkflowInfo, WorkflowJobInfo, WorkflowsQuery};
use crate::templates::{
    DefinitionsTablePartial, DependenciesTablePartial, EnhancedJobsTablePartial, SagasTablePartial,
    WorkflowJobsPartial, WorkflowsTablePartial,
};
use crate::AppState;

/// Workflows table partial for htmx updates.
pub async fn workflows_table(
    State(state): State<AppState>,
    Query(query): Query<WorkflowsQuery>,
) -> WorkflowsTablePartial {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(25).min(100);
    let offset = (page - 1) * per_page;

    let (workflows, total) = get_workflows(
        &state.pool,
        state.schema(),
        query.state.as_deref(),
        query.name.as_deref(),
        per_page,
        offset,
    )
    .await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    WorkflowsTablePartial {
        workflows,
        page,
        total_pages,
    }
}

/// Workflow jobs partial for htmx updates.
pub async fn workflow_jobs(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> WorkflowJobsPartial {
    let jobs = get_workflow_jobs(&state.pool, state.schema(), id).await;
    WorkflowJobsPartial { jobs }
}

/// Get workflows (copied from workflows.rs to avoid circular deps)
async fn get_workflows(
    pool: &PgPool,
    schema: Option<&str>,
    state_filter: Option<&str>,
    name_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> (Vec<WorkflowInfo>, i64) {
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let mut conditions = Vec::new();
    let mut params: Vec<String> = Vec::new();

    if let Some(s) = state_filter {
        params.push(s.to_string());
        conditions.push(format!("w.state = ${}", params.len()));
    }
    if let Some(n) = name_filter {
        params.push(format!("%{}%", n));
        conditions.push(format!("w.name ILIKE ${}", params.len()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let count_query = format!(
        "SELECT COUNT(*) FROM {} w {}",
        workflows_table, where_clause
    );
    let list_query = format!(
        r#"
        SELECT
            w.id, w.name, w.state, w.inserted_at, w.completed_at,
            COUNT(j.id) as job_count,
            COUNT(j.id) FILTER (WHERE j.state = 'completed') as completed_jobs,
            COUNT(j.id) FILTER (WHERE j.state IN ('discarded', 'cancelled')) as failed_jobs
        FROM {} w
        LEFT JOIN {} j ON j.workflow_id = w.id
        {}
        GROUP BY w.id
        ORDER BY w.id DESC
        LIMIT {} OFFSET {}
        "#,
        workflows_table, jobs_table, where_clause, limit, offset
    );

    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    for p in &params {
        count_q = count_q.bind(p);
    }
    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);

    let mut list_q = sqlx::query_as::<
        _,
        (
            i64,
            String,
            String,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            i64,
            i64,
            i64,
        ),
    >(&list_query);
    for p in &params {
        list_q = list_q.bind(p);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let workflows = rows
        .into_iter()
        .map(|row| WorkflowInfo {
            id: row.0,
            name: row.1,
            state: row.2,
            inserted_at: row.3,
            completed_at: row.4,
            job_count: row.5,
            completed_jobs: row.6,
            failed_jobs: row.7,
        })
        .collect();

    (workflows, total)
}

/// Get workflow jobs (copied from workflows.rs)
async fn get_workflow_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    workflow_id: i64,
) -> Vec<WorkflowJobInfo> {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let query = format!(
        r#"
        SELECT
            j.id, j.worker, j.state::text, j.attempt, j.max_attempts,
            j.inserted_at, j.completed_at,
            COALESCE(array_agg(d.depends_on_job_id) FILTER (WHERE d.depends_on_job_id IS NOT NULL), '{{}}') as dependencies,
            EXISTS(SELECT 1 FROM {} s WHERE s.compensation_job_id = j.id) as is_compensation
        FROM {} j
        LEFT JOIN {} d ON d.job_id = j.id
        WHERE j.workflow_id = $1
        GROUP BY j.id
        ORDER BY j.id
        "#,
        saga_steps_table, jobs_table, deps_table
    );

    let rows: Vec<(
        i64,
        String,
        String,
        i32,
        i32,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
        Vec<i64>,
        bool,
    )> = sqlx::query_as(&query)
        .bind(workflow_id)
        .fetch_all(pool)
        .await
        .unwrap_or_default();

    rows.into_iter()
        .map(|row| WorkflowJobInfo {
            id: row.0,
            worker: row.1,
            state: row.2,
            attempt: row.3,
            max_attempts: row.4,
            inserted_at: row.5,
            completed_at: row.6,
            dependencies: row.7,
            is_compensation: row.8,
        })
        .collect()
}

/// Dependencies table partial for htmx updates.
pub async fn dependencies_table(
    State(state): State<AppState>,
    Query(query): Query<DependenciesQuery>,
) -> DependenciesTablePartial {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(50).min(100);
    let offset = (page - 1) * per_page;

    let (dependencies, total) = get_dependencies(
        &state.pool,
        state.schema(),
        query.blocked,
        query.workflow_id,
        per_page,
        offset,
    )
    .await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    DependenciesTablePartial {
        dependencies,
        page,
        total_pages,
    }
}

/// Get dependencies (copied from dependencies.rs for htmx partial)
async fn get_dependencies(
    pool: &PgPool,
    schema: Option<&str>,
    blocked_only: Option<bool>,
    workflow_id: Option<i64>,
    limit: i64,
    offset: i64,
) -> (Vec<DependencyInfo>, i64) {
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    let mut conditions = Vec::new();
    let mut param_idx = 0;

    if blocked_only.unwrap_or(false) {
        conditions.push("dep_job.state != 'completed'".to_string());
    }

    if workflow_id.is_some() {
        param_idx += 1;
        conditions.push(format!("j.workflow_id = ${}", param_idx));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let count_query = format!(
        r#"
        SELECT COUNT(*)
        FROM {} d
        JOIN {} j ON d.job_id = j.id
        JOIN {} dep_job ON d.depends_on_job_id = dep_job.id
        LEFT JOIN {} w ON j.workflow_id = w.id
        {}
        "#,
        deps_table, jobs_table, jobs_table, workflows_table, where_clause
    );

    let list_query = format!(
        r#"
        SELECT
            d.job_id,
            j.worker as job_worker,
            j.state::text as job_state,
            d.depends_on_job_id,
            dep_job.worker as depends_on_worker,
            dep_job.state::text as depends_on_state,
            j.workflow_id,
            w.name as workflow_name
        FROM {} d
        JOIN {} j ON d.job_id = j.id
        JOIN {} dep_job ON d.depends_on_job_id = dep_job.id
        LEFT JOIN {} w ON j.workflow_id = w.id
        {}
        ORDER BY j.workflow_id NULLS LAST, d.job_id, d.depends_on_job_id
        LIMIT {} OFFSET {}
        "#,
        deps_table, jobs_table, jobs_table, workflows_table, where_clause, limit, offset
    );

    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    if let Some(wf_id) = workflow_id {
        count_q = count_q.bind(wf_id);
    }
    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);

    let mut list_q = sqlx::query_as::<
        _,
        (
            i64,
            String,
            String,
            i64,
            String,
            String,
            Option<i64>,
            Option<String>,
        ),
    >(&list_query);
    if let Some(wf_id) = workflow_id {
        list_q = list_q.bind(wf_id);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let dependencies = rows
        .into_iter()
        .map(|row| DependencyInfo {
            job_id: row.0,
            job_worker: row.1,
            job_state: row.2,
            depends_on_job_id: row.3,
            depends_on_worker: row.4,
            depends_on_state: row.5,
            workflow_id: row.6,
            workflow_name: row.7,
        })
        .collect();

    (dependencies, total)
}

/// Sagas table partial for htmx updates.
pub async fn sagas_table(
    State(state): State<AppState>,
    Query(query): Query<SagasQuery>,
) -> SagasTablePartial {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(25).min(100);
    let offset = (page - 1) * per_page;

    let (sagas, total) = get_sagas(
        &state.pool,
        state.schema(),
        query.state.as_deref(),
        per_page,
        offset,
    )
    .await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    SagasTablePartial {
        sagas,
        page,
        total_pages,
    }
}

/// Get sagas (copied from sagas.rs for htmx partial).
async fn get_sagas(
    pool: &PgPool,
    schema: Option<&str>,
    state_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> (Vec<SagaInfo>, i64) {
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };
    let saga_steps_table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let mut conditions =
        vec!["EXISTS (SELECT 1 FROM {} ss WHERE ss.workflow_id = w.id)".to_string()];
    let mut params: Vec<String> = Vec::new();

    if let Some(s) = state_filter {
        params.push(s.to_string());
        conditions.push(format!("w.state = ${}", params.len()));
    }

    conditions[0] = conditions[0].replace("{}", &saga_steps_table);

    let where_clause = format!("WHERE {}", conditions.join(" AND "));

    let count_query = format!(
        "SELECT COUNT(*) FROM {} w {}",
        workflows_table, where_clause
    );

    let list_query = format!(
        r#"
        SELECT
            w.id, w.name, w.state, w.inserted_at, w.completed_at,
            COUNT(s.step_order) as step_count,
            COUNT(s.step_order) FILTER (WHERE s.state = 'completed') as completed_steps,
            COUNT(s.step_order) FILTER (WHERE s.state = 'failed') as failed_steps,
            COUNT(s.step_order) FILTER (WHERE s.state IN ('compensating', 'compensated')) as compensating_steps
        FROM {} w
        JOIN {} s ON s.workflow_id = w.id
        {}
        GROUP BY w.id
        ORDER BY w.id DESC
        LIMIT {} OFFSET {}
        "#,
        workflows_table, saga_steps_table, where_clause, limit, offset
    );

    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    for p in &params {
        count_q = count_q.bind(p);
    }
    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);

    let mut list_q = sqlx::query_as::<
        _,
        (
            i64,
            String,
            String,
            chrono::DateTime<chrono::Utc>,
            Option<chrono::DateTime<chrono::Utc>>,
            i64,
            i64,
            i64,
            i64,
        ),
    >(&list_query);
    for p in &params {
        list_q = list_q.bind(p);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let sagas = rows
        .into_iter()
        .map(|row| SagaInfo {
            id: row.0,
            name: row.1,
            state: row.2,
            inserted_at: row.3,
            completed_at: row.4,
            step_count: row.5,
            completed_steps: row.6,
            failed_steps: row.7,
            compensating_steps: row.8,
        })
        .collect();

    (sagas, total)
}

/// Enhanced jobs table partial for htmx updates.
pub async fn enhanced_jobs_table(
    State(state): State<AppState>,
    Query(query): Query<EnhancedJobsQuery>,
) -> EnhancedJobsTablePartial {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(50).min(100);
    let offset = (page - 1) * per_page;

    let (jobs, total) =
        get_enhanced_jobs(&state.pool, state.schema(), &query, per_page, offset).await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    EnhancedJobsTablePartial {
        jobs,
        page,
        total_pages,
    }
}

/// Get enhanced jobs (copied from jobs.rs for htmx partial).
async fn get_enhanced_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    query: &EnhancedJobsQuery,
    limit: i64,
    offset: i64,
) -> (Vec<EnhancedJobInfo>, i64) {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };
    let workflows_table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    let mut conditions = Vec::new();
    let mut params: Vec<String> = Vec::new();

    if let Some(ref s) = query.state {
        params.push(s.to_string());
        conditions.push(format!("j.state::text = ${}", params.len()));
    }
    if let Some(ref q) = query.queue {
        params.push(q.to_string());
        conditions.push(format!("j.queue = ${}", params.len()));
    }
    if let Some(ref w) = query.worker {
        params.push(format!("%{}%", w));
        conditions.push(format!("j.worker ILIKE ${}", params.len()));
    }
    if query.has_deps == Some(true) {
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM {} d WHERE d.job_id = j.id)",
            deps_table
        ));
    }
    if query.is_blocking == Some(true) {
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM {} d WHERE d.depends_on_job_id = j.id)",
            deps_table
        ));
    }
    if query.in_workflow == Some(true) {
        conditions.push("j.workflow_id IS NOT NULL".to_string());
    }
    if let Some(wf_id) = query.workflow_id {
        params.push(wf_id.to_string());
        conditions.push(format!("j.workflow_id = ${}", params.len()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let count_query = format!("SELECT COUNT(*) FROM {} j {}", jobs_table, where_clause);

    let list_query = format!(
        r#"
        SELECT
            j.id, j.worker, j.queue, j.state::text, j.attempt, j.max_attempts,
            j.workflow_id, w.name as workflow_name, j.inserted_at,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.job_id = j.id), 0) as dependency_count,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.depends_on_job_id = j.id), 0) as blocking_count,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.job_id = j.id AND d.state = 'satisfied'), 0) as satisfied_deps,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.job_id = j.id AND d.state = 'pending'), 0) as pending_deps,
            COALESCE((SELECT COUNT(*) FROM {deps} d WHERE d.job_id = j.id AND d.state = 'failed'), 0) as failed_deps
        FROM {jobs} j
        LEFT JOIN {workflows} w ON w.id = j.workflow_id
        {where_clause}
        ORDER BY j.id DESC
        LIMIT {limit} OFFSET {offset}
        "#,
        deps = deps_table,
        jobs = jobs_table,
        workflows = workflows_table,
        where_clause = where_clause,
        limit = limit,
        offset = offset
    );

    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    for p in &params {
        count_q = count_q.bind(p);
    }
    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);

    let mut list_q = sqlx::query_as::<
        _,
        (
            i64,
            String,
            String,
            String,
            i32,
            i32,
            Option<i64>,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
            i64,
            i64,
            i64,
            i64,
            i64,
        ),
    >(&list_query);
    for p in &params {
        list_q = list_q.bind(p);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let jobs = rows
        .into_iter()
        .map(|row| EnhancedJobInfo {
            id: row.0,
            worker: row.1,
            queue: row.2,
            state: row.3,
            attempt: row.4,
            max_attempts: row.5,
            workflow_id: row.6,
            workflow_name: row.7,
            inserted_at: row.8,
            dependency_count: row.9,
            blocking_count: row.10,
            satisfied_deps: row.11,
            pending_deps: row.12,
            failed_deps: row.13,
        })
        .collect();

    (jobs, total)
}

/// Definitions table partial for htmx updates.
pub async fn definitions_table(
    State(state): State<AppState>,
    Query(query): Query<DefinitionsQuery>,
) -> DefinitionsTablePartial {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(25).min(100);
    let offset = (page - 1) * per_page;

    let (definitions, total) = get_definitions(
        &state.pool,
        state.schema(),
        query.name.as_deref(),
        per_page,
        offset,
    )
    .await;

    let total_pages = (total as f64 / per_page as f64).ceil() as i64;

    DefinitionsTablePartial {
        definitions,
        page,
        total_pages,
    }
}

/// Get workflow definitions (copied from definitions.rs for htmx partial).
async fn get_definitions(
    pool: &PgPool,
    schema: Option<&str>,
    name_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> (Vec<DefinitionInfo>, i64) {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflow_definitions", s),
        None => "ishikari_workflow_definitions".to_string(),
    };

    // Check if table exists first
    let table_exists = check_definitions_table_exists(pool, schema).await;
    if !table_exists {
        return (vec![], 0);
    }

    let mut conditions = Vec::new();
    let mut params: Vec<String> = Vec::new();

    if let Some(n) = name_filter {
        params.push(format!("%{}%", n));
        conditions.push(format!("name ILIKE ${}", params.len()));
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    let count_query = format!("SELECT COUNT(*) FROM {} {}", table, where_clause);
    let list_query = format!(
        r#"
        SELECT id, name, version, description, nodes, created_at, updated_at
        FROM {}
        {}
        ORDER BY name, version DESC
        LIMIT {} OFFSET {}
        "#,
        table, where_clause, limit, offset
    );

    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    for p in &params {
        count_q = count_q.bind(p);
    }
    let total: i64 = count_q.fetch_one(pool).await.unwrap_or(0);

    let mut list_q = sqlx::query_as::<
        _,
        (
            i64,
            String,
            i32,
            Option<String>,
            serde_json::Value,
            chrono::DateTime<chrono::Utc>,
            chrono::DateTime<chrono::Utc>,
        ),
    >(&list_query);
    for p in &params {
        list_q = list_q.bind(p);
    }

    let rows = list_q.fetch_all(pool).await.unwrap_or_default();

    let definitions = rows
        .into_iter()
        .map(|row| {
            let nodes: std::collections::HashMap<String, serde_json::Value> =
                serde_json::from_value(row.4.clone()).unwrap_or_default();
            DefinitionInfo {
                id: row.0,
                name: row.1,
                version: row.2,
                description: row.3,
                node_count: nodes.len(),
                created_at: row.5,
                updated_at: row.6,
            }
        })
        .collect();

    (definitions, total)
}

/// Check if the workflow definitions table exists.
async fn check_definitions_table_exists(pool: &PgPool, schema: Option<&str>) -> bool {
    let (schema_name, table_name) = match schema {
        Some(s) => (s.to_string(), "ishikari_workflow_definitions".to_string()),
        None => (
            "public".to_string(),
            "ishikari_workflow_definitions".to_string(),
        ),
    };

    let result: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        "#,
    )
    .bind(&schema_name)
    .bind(&table_name)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    result.unwrap_or(false)
}

// ============================================================================
// JSON API Endpoints for Workflow Builder
// ============================================================================

/// Node type information for the workflow builder palette.
#[derive(Debug, Clone, Serialize)]
pub struct NodeTypeInfo {
    pub name: String,
    pub description: Option<String>,
    pub category: String,
    pub input_schema: HashMap<String, SchemaField>,
    pub output_schema: HashMap<String, SchemaField>,
}

/// Response containing all available node types.
#[derive(Debug, Serialize)]
pub struct NodeTypesResponse {
    pub node_types: Vec<NodeTypeInfo>,
}

/// Get available node types for the workflow builder palette.
pub async fn node_types() -> Json<NodeTypesResponse> {
    // Built-in node types that match what the NodeJob worker can execute
    let node_types = vec![
        NodeTypeInfo {
            name: "echo".to_string(),
            description: Some("Echo a message back".to_string()),
            category: "core".to_string(),
            input_schema: HashMap::from([(
                "message".to_string(),
                SchemaField {
                    field_type: "string".to_string(),
                    description: Some("Message to echo".to_string()),
                    default: None,
                    required: true,
                },
            )]),
            output_schema: HashMap::from([(
                "message".to_string(),
                SchemaField {
                    field_type: "string".to_string(),
                    description: Some("Echoed message".to_string()),
                    default: None,
                    required: false,
                },
            )]),
        },
        NodeTypeInfo {
            name: "uppercase".to_string(),
            description: Some("Convert text to uppercase".to_string()),
            category: "transform".to_string(),
            input_schema: HashMap::from([(
                "text".to_string(),
                SchemaField {
                    field_type: "string".to_string(),
                    description: Some("Text to transform".to_string()),
                    default: None,
                    required: true,
                },
            )]),
            output_schema: HashMap::from([(
                "result".to_string(),
                SchemaField {
                    field_type: "string".to_string(),
                    description: Some("Transformed text".to_string()),
                    default: None,
                    required: false,
                },
            )]),
        },
        NodeTypeInfo {
            name: "lowercase".to_string(),
            description: Some("Convert text to lowercase".to_string()),
            category: "transform".to_string(),
            input_schema: HashMap::from([(
                "text".to_string(),
                SchemaField {
                    field_type: "string".to_string(),
                    description: Some("Text to transform".to_string()),
                    default: None,
                    required: true,
                },
            )]),
            output_schema: HashMap::from([(
                "result".to_string(),
                SchemaField {
                    field_type: "string".to_string(),
                    description: Some("Transformed text".to_string()),
                    default: None,
                    required: false,
                },
            )]),
        },
        NodeTypeInfo {
            name: "http/get".to_string(),
            description: Some("Make an HTTP GET request".to_string()),
            category: "http".to_string(),
            input_schema: HashMap::from([
                (
                    "url".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("URL to fetch".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "headers".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("Request headers".to_string()),
                        default: None,
                        required: false,
                    },
                ),
            ]),
            output_schema: HashMap::from([
                (
                    "status".to_string(),
                    SchemaField {
                        field_type: "integer".to_string(),
                        description: Some("HTTP status code".to_string()),
                        default: None,
                        required: false,
                    },
                ),
                (
                    "body".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("Response body".to_string()),
                        default: None,
                        required: false,
                    },
                ),
            ]),
        },
        NodeTypeInfo {
            name: "http/post".to_string(),
            description: Some("Make an HTTP POST request".to_string()),
            category: "http".to_string(),
            input_schema: HashMap::from([
                (
                    "url".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("URL to post to".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "body".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("Request body".to_string()),
                        default: None,
                        required: false,
                    },
                ),
                (
                    "headers".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("Request headers".to_string()),
                        default: None,
                        required: false,
                    },
                ),
            ]),
            output_schema: HashMap::from([
                (
                    "status".to_string(),
                    SchemaField {
                        field_type: "integer".to_string(),
                        description: Some("HTTP status code".to_string()),
                        default: None,
                        required: false,
                    },
                ),
                (
                    "body".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("Response body".to_string()),
                        default: None,
                        required: false,
                    },
                ),
            ]),
        },
        NodeTypeInfo {
            name: "json/validate".to_string(),
            description: Some("Validate JSON against a schema".to_string()),
            category: "transform".to_string(),
            input_schema: HashMap::from([
                (
                    "data".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("JSON data to validate".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "schema".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("JSON Schema".to_string()),
                        default: None,
                        required: true,
                    },
                ),
            ]),
            output_schema: HashMap::from([
                (
                    "valid".to_string(),
                    SchemaField {
                        field_type: "boolean".to_string(),
                        description: Some("Whether data is valid".to_string()),
                        default: None,
                        required: false,
                    },
                ),
                (
                    "data".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("The validated data".to_string()),
                        default: None,
                        required: false,
                    },
                ),
            ]),
        },
        NodeTypeInfo {
            name: "json/transform".to_string(),
            description: Some("Transform JSON data using a mapping".to_string()),
            category: "transform".to_string(),
            input_schema: HashMap::from([
                (
                    "data".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("Input JSON data".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "mapping".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("Field mapping configuration".to_string()),
                        default: None,
                        required: true,
                    },
                ),
            ]),
            output_schema: HashMap::from([(
                "records".to_string(),
                SchemaField {
                    field_type: "array".to_string(),
                    description: Some("Transformed records".to_string()),
                    default: None,
                    required: false,
                },
            )]),
        },
        NodeTypeInfo {
            name: "db/query".to_string(),
            description: Some("Execute a database query".to_string()),
            category: "database".to_string(),
            input_schema: HashMap::from([
                (
                    "query".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("SQL query".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "params".to_string(),
                    SchemaField {
                        field_type: "array".to_string(),
                        description: Some("Query parameters".to_string()),
                        default: None,
                        required: false,
                    },
                ),
            ]),
            output_schema: HashMap::from([(
                "rows".to_string(),
                SchemaField {
                    field_type: "array".to_string(),
                    description: Some("Query results".to_string()),
                    default: None,
                    required: false,
                },
            )]),
        },
        NodeTypeInfo {
            name: "db/insert".to_string(),
            description: Some("Insert records into a table".to_string()),
            category: "database".to_string(),
            input_schema: HashMap::from([
                (
                    "table".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("Table name".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "records".to_string(),
                    SchemaField {
                        field_type: "array".to_string(),
                        description: Some("Records to insert".to_string()),
                        default: None,
                        required: true,
                    },
                ),
            ]),
            output_schema: HashMap::from([(
                "rows_affected".to_string(),
                SchemaField {
                    field_type: "integer".to_string(),
                    description: Some("Number of rows inserted".to_string()),
                    default: None,
                    required: false,
                },
            )]),
        },
        NodeTypeInfo {
            name: "email/send".to_string(),
            description: Some("Send an email".to_string()),
            category: "notification".to_string(),
            input_schema: HashMap::from([
                (
                    "to".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("Recipient email".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "subject".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("Email subject".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "body".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("Email body".to_string()),
                        default: None,
                        required: true,
                    },
                ),
            ]),
            output_schema: HashMap::from([(
                "sent".to_string(),
                SchemaField {
                    field_type: "boolean".to_string(),
                    description: Some("Whether email was sent".to_string()),
                    default: None,
                    required: false,
                },
            )]),
        },
        NodeTypeInfo {
            name: "sms/send".to_string(),
            description: Some("Send an SMS message".to_string()),
            category: "notification".to_string(),
            input_schema: HashMap::from([
                (
                    "phone".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("Phone number".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "message".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("Message text".to_string()),
                        default: None,
                        required: true,
                    },
                ),
            ]),
            output_schema: HashMap::from([(
                "sent".to_string(),
                SchemaField {
                    field_type: "boolean".to_string(),
                    description: Some("Whether SMS was sent".to_string()),
                    default: None,
                    required: false,
                },
            )]),
        },
        NodeTypeInfo {
            name: "delay".to_string(),
            description: Some("Wait for a duration".to_string()),
            category: "control".to_string(),
            input_schema: HashMap::from([(
                "seconds".to_string(),
                SchemaField {
                    field_type: "integer".to_string(),
                    description: Some("Seconds to wait".to_string()),
                    default: None,
                    required: true,
                },
            )]),
            output_schema: HashMap::new(),
        },
        NodeTypeInfo {
            name: "condition".to_string(),
            description: Some("Conditional branching".to_string()),
            category: "control".to_string(),
            input_schema: HashMap::from([
                (
                    "expression".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("Condition expression".to_string()),
                        default: None,
                        required: true,
                    },
                ),
                (
                    "value".to_string(),
                    SchemaField {
                        field_type: "object".to_string(),
                        description: Some("Value to compare".to_string()),
                        default: None,
                        required: false,
                    },
                ),
            ]),
            output_schema: HashMap::from([(
                "result".to_string(),
                SchemaField {
                    field_type: "boolean".to_string(),
                    description: Some("Condition result".to_string()),
                    default: None,
                    required: false,
                },
            )]),
        },
    ];

    Json(NodeTypesResponse { node_types })
}

/// Definition detail response for JSON API.
#[derive(Debug, Serialize)]
pub struct DefinitionDetailResponse {
    pub id: i64,
    pub name: String,
    pub version: i32,
    pub description: Option<String>,
    pub definition: WorkflowDefinition,
    pub yaml: String,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

impl From<DefinitionDetail> for DefinitionDetailResponse {
    fn from(detail: DefinitionDetail) -> Self {
        Self {
            id: detail.id,
            name: detail.name,
            version: detail.version,
            description: detail.description,
            definition: detail.definition,
            yaml: detail.yaml,
            created_at: detail.created_at.map(|d| d.to_rfc3339()),
            updated_at: detail.updated_at.map(|d| d.to_rfc3339()),
        }
    }
}

/// Get a single workflow definition as JSON.
pub async fn get_definition(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<DefinitionDetailResponse>, (StatusCode, String)> {
    let definition = fetch_definition(&state.pool, state.schema(), id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
        .ok_or((StatusCode::NOT_FOUND, "Definition not found".to_string()))?;

    Ok(Json(DefinitionDetailResponse::from(definition)))
}

/// Update an existing workflow definition.
pub async fn update_definition(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(def): Json<WorkflowDefinition>,
) -> Result<Json<DefinitionDetailResponse>, (StatusCode, String)> {
    // Validate the definition
    def.validate()
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    // Update in database
    let updated = update_definition_in_db(&state.pool, state.schema(), id, &def)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(DefinitionDetailResponse::from(updated)))
}

/// Create a new workflow definition.
pub async fn create_definition(
    State(state): State<AppState>,
    Json(def): Json<WorkflowDefinition>,
) -> Result<Json<DefinitionDetailResponse>, (StatusCode, String)> {
    // Validate the definition
    def.validate()
        .map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    // Save to database
    let created = save_new_definition(&state.pool, state.schema(), &def)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(DefinitionDetailResponse::from(created)))
}

/// Validation error details.
#[derive(Debug, Serialize)]
pub struct ValidationError {
    pub node_id: Option<String>,
    pub field: Option<String>,
    pub message: String,
}

/// Validation response.
#[derive(Debug, Serialize)]
pub struct ValidationResponse {
    pub valid: bool,
    pub errors: Vec<ValidationError>,
}

/// Validate a workflow definition without saving.
pub async fn validate_definition(
    Json(def): Json<WorkflowDefinition>,
) -> Json<ValidationResponse> {
    match def.validate() {
        Ok(()) => Json(ValidationResponse {
            valid: true,
            errors: vec![],
        }),
        Err(e) => Json(ValidationResponse {
            valid: false,
            errors: vec![ValidationError {
                node_id: None,
                field: None,
                message: e,
            }],
        }),
    }
}

// Helper functions for database operations

async fn fetch_definition(
    pool: &PgPool,
    schema: Option<&str>,
    id: i64,
) -> Result<Option<DefinitionDetail>, String> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflow_definitions", s),
        None => "ishikari_workflow_definitions".to_string(),
    };

    let query = format!(
        r#"
        SELECT id, name, version, description, input_schema, nodes, output_schema, metadata, created_at, updated_at
        FROM {}
        WHERE id = $1
        "#,
        table
    );

    let row: Option<(
        i64,
        String,
        i32,
        Option<String>,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| format!("Database error: {}", e))?;

    match row {
        Some((
            id,
            name,
            version,
            description,
            input_schema,
            nodes,
            output_schema,
            metadata,
            created_at,
            updated_at,
        )) => {
            let inputs: HashMap<String, SchemaField> = serde_json::from_value(input_schema)
                .map_err(|e| format!("Failed to parse inputs: {}", e))?;
            let nodes_map: HashMap<String, NodeDefinition> = serde_json::from_value(nodes)
                .map_err(|e| format!("Failed to parse nodes: {}", e))?;
            let outputs: HashMap<String, String> = serde_json::from_value(output_schema)
                .map_err(|e| format!("Failed to parse outputs: {}", e))?;
            let metadata_map: HashMap<String, Value> = serde_json::from_value(metadata)
                .map_err(|e| format!("Failed to parse metadata: {}", e))?;

            let definition = WorkflowDefinition {
                name: name.clone(),
                version,
                description: description.clone(),
                inputs,
                nodes: nodes_map,
                outputs,
                metadata: metadata_map,
            };

            let yaml = definition
                .to_yaml()
                .map_err(|e| format!("Failed to generate YAML: {}", e))?;

            Ok(Some(DefinitionDetail {
                id,
                name,
                version,
                description,
                definition,
                yaml,
                created_at: Some(created_at),
                updated_at: Some(updated_at),
            }))
        }
        None => Ok(None),
    }
}

async fn update_definition_in_db(
    pool: &PgPool,
    schema: Option<&str>,
    id: i64,
    def: &WorkflowDefinition,
) -> Result<DefinitionDetail, String> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflow_definitions", s),
        None => "ishikari_workflow_definitions".to_string(),
    };

    let nodes_json = serde_json::to_value(&def.nodes).map_err(|e| format!("JSON error: {}", e))?;
    let inputs_json =
        serde_json::to_value(&def.inputs).map_err(|e| format!("JSON error: {}", e))?;
    let outputs_json =
        serde_json::to_value(&def.outputs).map_err(|e| format!("JSON error: {}", e))?;
    let metadata_json =
        serde_json::to_value(&def.metadata).map_err(|e| format!("JSON error: {}", e))?;

    let query = format!(
        r#"
        UPDATE {}
        SET name = $1, description = $2, input_schema = $3, nodes = $4, output_schema = $5, metadata = $6, updated_at = NOW()
        WHERE id = $7
        RETURNING id, name, version, description, input_schema, nodes, output_schema, metadata, created_at, updated_at
        "#,
        table
    );

    let row: (
        i64,
        String,
        i32,
        Option<String>,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
    ) = sqlx::query_as(&query)
        .bind(&def.name)
        .bind(&def.description)
        .bind(&inputs_json)
        .bind(&nodes_json)
        .bind(&outputs_json)
        .bind(&metadata_json)
        .bind(id)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Database error: {}", e))?;

    let inputs: HashMap<String, SchemaField> =
        serde_json::from_value(row.4.clone()).unwrap_or_default();
    let nodes_map: HashMap<String, NodeDefinition> =
        serde_json::from_value(row.5.clone()).unwrap_or_default();
    let outputs: HashMap<String, String> =
        serde_json::from_value(row.6.clone()).unwrap_or_default();
    let metadata_map: HashMap<String, Value> =
        serde_json::from_value(row.7.clone()).unwrap_or_default();

    let definition = WorkflowDefinition {
        name: row.1.clone(),
        version: row.2,
        description: row.3.clone(),
        inputs,
        nodes: nodes_map,
        outputs,
        metadata: metadata_map,
    };

    let yaml = definition
        .to_yaml()
        .unwrap_or_else(|_| "# Error generating YAML".to_string());

    Ok(DefinitionDetail {
        id: row.0,
        name: row.1,
        version: row.2,
        description: row.3,
        definition,
        yaml,
        created_at: Some(row.8),
        updated_at: Some(row.9),
    })
}

async fn save_new_definition(
    pool: &PgPool,
    schema: Option<&str>,
    def: &WorkflowDefinition,
) -> Result<DefinitionDetail, String> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflow_definitions", s),
        None => "ishikari_workflow_definitions".to_string(),
    };

    // Get the next version number
    let next_version: i32 = sqlx::query_scalar(&format!(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM {} WHERE name = $1",
        table
    ))
    .bind(&def.name)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Database error: {}", e))?;

    let nodes_json = serde_json::to_value(&def.nodes).map_err(|e| format!("JSON error: {}", e))?;
    let inputs_json =
        serde_json::to_value(&def.inputs).map_err(|e| format!("JSON error: {}", e))?;
    let outputs_json =
        serde_json::to_value(&def.outputs).map_err(|e| format!("JSON error: {}", e))?;
    let metadata_json =
        serde_json::to_value(&def.metadata).map_err(|e| format!("JSON error: {}", e))?;

    let query = format!(
        r#"
        INSERT INTO {} (name, version, description, input_schema, nodes, output_schema, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, name, version, description, input_schema, nodes, output_schema, metadata, created_at, updated_at
        "#,
        table
    );

    let row: (
        i64,
        String,
        i32,
        Option<String>,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        serde_json::Value,
        chrono::DateTime<chrono::Utc>,
        chrono::DateTime<chrono::Utc>,
    ) = sqlx::query_as(&query)
        .bind(&def.name)
        .bind(next_version)
        .bind(&def.description)
        .bind(&inputs_json)
        .bind(&nodes_json)
        .bind(&outputs_json)
        .bind(&metadata_json)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Database error: {}", e))?;

    let inputs: HashMap<String, SchemaField> =
        serde_json::from_value(row.4.clone()).unwrap_or_default();
    let nodes_map: HashMap<String, NodeDefinition> =
        serde_json::from_value(row.5.clone()).unwrap_or_default();
    let outputs: HashMap<String, String> =
        serde_json::from_value(row.6.clone()).unwrap_or_default();
    let metadata_map: HashMap<String, Value> =
        serde_json::from_value(row.7.clone()).unwrap_or_default();

    let definition = WorkflowDefinition {
        name: row.1.clone(),
        version: row.2,
        description: row.3.clone(),
        inputs,
        nodes: nodes_map,
        outputs,
        metadata: metadata_map,
    };

    let yaml = definition
        .to_yaml()
        .unwrap_or_else(|_| "# Error generating YAML".to_string());

    Ok(DefinitionDetail {
        id: row.0,
        name: row.1,
        version: row.2,
        description: row.3,
        definition,
        yaml,
        created_at: Some(row.8),
        updated_at: Some(row.9),
    })
}
