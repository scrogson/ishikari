//! Workflow definition route handlers.
//!
//! These routes handle workflow definition management:
//! - List all definitions
//! - View definition details
//! - Import definitions from YAML
//! - Export definitions as YAML files
//! - Delete definitions
//! - Run definitions (create workflow instances)

#![allow(clippy::type_complexity)]

use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Redirect, Response},
};
use ishikari::{Dependencies, Workflow};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};

use crate::templates::{
    DefinitionDetailTemplate, DefinitionEditTemplate, DefinitionImportTemplate,
    DefinitionRunTemplate, DefinitionsListTemplate,
};
use crate::AppState;

/// Query parameters for definition listing.
#[derive(Debug, Deserialize, Default)]
pub struct DefinitionsQuery {
    pub name: Option<String>,
    pub page: Option<i64>,
    pub per_page: Option<i64>,
}

/// Form data for importing a definition.
#[derive(Debug, Deserialize)]
pub struct ImportForm {
    pub yaml: String,
}

/// Form data for running a definition.
#[derive(Debug, Deserialize)]
pub struct RunForm {
    pub inputs_json: String,
}

/// Workflow run information for display.
#[derive(Debug)]
pub struct WorkflowRunInfo {
    pub id: i64,
    pub workflow_id: i64,
    pub status: String,
    pub inputs: Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl WorkflowRunInfo {
    /// Get a CSS class for the status badge.
    pub fn status_class(&self) -> &'static str {
        match self.status.as_str() {
            "running" => "badge-info",
            "completed" => "badge-success",
            "failed" => "badge-error",
            "cancelled" => "badge-warning",
            _ => "badge-ghost",
        }
    }

    /// Get inputs as formatted JSON.
    pub fn inputs_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.inputs).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Workflow definition information for display.
#[derive(Debug)]
pub struct DefinitionInfo {
    pub id: i64,
    pub name: String,
    pub version: i32,
    pub description: Option<String>,
    pub node_count: usize,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Schema definition for workflow inputs/outputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    /// The type of the field (string, integer, object, array, boolean).
    #[serde(rename = "type")]
    pub field_type: String,

    /// Optional description.
    pub description: Option<String>,

    /// Default value if not provided.
    pub default: Option<Value>,

    /// Whether this field is required (for workflow inputs).
    #[serde(default)]
    pub required: bool,
}

/// A node definition within a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDefinition {
    /// The node type (e.g., "http/get", "transform/json").
    #[serde(rename = "type")]
    pub node_type: String,

    /// Node IDs that this node depends on.
    #[serde(default)]
    pub depends_on: Vec<String>,

    /// Input values/expressions for the node.
    #[serde(default)]
    pub inputs: HashMap<String, Value>,

    /// Optional condition for execution (expression that must evaluate to true).
    #[serde(rename = "when")]
    pub condition: Option<String>,

    /// Timeout in seconds for this node.
    pub timeout_seconds: Option<u32>,

    /// Maximum retry attempts for this node.
    pub max_retries: Option<u32>,
}

/// A workflow definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    /// Unique workflow name.
    pub name: String,

    /// Optional description.
    #[serde(default)]
    pub description: Option<String>,

    /// Version number (auto-incremented on save).
    #[serde(default = "default_version")]
    pub version: i32,

    /// Input schema for the workflow.
    #[serde(default)]
    pub inputs: HashMap<String, SchemaField>,

    /// Node definitions.
    pub nodes: HashMap<String, NodeDefinition>,

    /// Output expressions.
    #[serde(default)]
    pub outputs: HashMap<String, String>,

    /// Custom metadata.
    #[serde(default)]
    pub metadata: HashMap<String, Value>,
}

fn default_version() -> i32 {
    1
}

impl WorkflowDefinition {
    /// Parse a workflow definition from YAML.
    pub fn from_yaml(yaml: &str) -> Result<Self, String> {
        let def: WorkflowDefinition =
            serde_yaml::from_str(yaml).map_err(|e| format!("YAML parse error: {}", e))?;
        def.validate()?;
        Ok(def)
    }

    /// Serialize the workflow definition to YAML.
    pub fn to_yaml(&self) -> Result<String, String> {
        serde_yaml::to_string(self).map_err(|e| format!("YAML serialize error: {}", e))
    }

    /// Validate the workflow definition.
    pub fn validate(&self) -> Result<(), String> {
        // Check for empty workflow
        if self.nodes.is_empty() {
            return Err("Workflow must have at least one node".to_string());
        }

        // Collect all node IDs
        let node_ids: HashSet<&str> = self.nodes.keys().map(|s| s.as_str()).collect();

        // Check for missing dependencies and cycles
        for (node_id, node_def) in &self.nodes {
            for dep in &node_def.depends_on {
                if !node_ids.contains(dep.as_str()) {
                    return Err(format!(
                        "Node '{}' depends on non-existent node '{}'",
                        node_id, dep
                    ));
                }
            }
        }

        // Check for cycles using DFS
        self.detect_cycles()?;

        Ok(())
    }

    /// Detect cycles in the workflow graph.
    fn detect_cycles(&self) -> Result<(), String> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        for node_id in self.nodes.keys() {
            if !visited.contains(node_id) {
                if let Some(cycle_path) =
                    self.dfs_cycle(node_id, &mut visited, &mut rec_stack, &mut path)
                {
                    return Err(format!("Cycle detected: {}", cycle_path));
                }
            }
        }

        Ok(())
    }

    fn dfs_cycle(
        &self,
        node_id: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<String> {
        visited.insert(node_id.to_string());
        rec_stack.insert(node_id.to_string());
        path.push(node_id.to_string());

        if let Some(node_def) = self.nodes.get(node_id) {
            for dep in &node_def.depends_on {
                if !visited.contains(dep) {
                    if let Some(cycle) = self.dfs_cycle(dep, visited, rec_stack, path) {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(dep) {
                    // Found a cycle
                    let cycle_start = path.iter().position(|n| n == dep).unwrap();
                    let cycle_path = path[cycle_start..].join(" -> ") + " -> " + dep;
                    return Some(cycle_path);
                }
            }
        }

        path.pop();
        rec_stack.remove(node_id);
        None
    }

    /// Get the topological order of nodes for execution.
    pub fn topological_order(&self) -> Result<Vec<String>, String> {
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();

        // Initialize
        for node_id in self.nodes.keys() {
            in_degree.insert(node_id, 0);
            dependents.insert(node_id, Vec::new());
        }

        // Build graph
        for (node_id, node_def) in &self.nodes {
            for dep in &node_def.depends_on {
                *in_degree.get_mut(node_id.as_str()).unwrap() += 1;
                dependents.get_mut(dep.as_str()).unwrap().push(node_id);
            }
        }

        // Kahn's algorithm
        let mut queue: Vec<&str> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&id, _)| id)
            .collect();
        let mut result = Vec::new();

        while let Some(node_id) = queue.pop() {
            result.push(node_id.to_string());
            for &dependent in &dependents[node_id] {
                let deg = in_degree.get_mut(dependent).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push(dependent);
                }
            }
        }

        Ok(result)
    }

    /// Get nodes grouped by execution level (parallel groups).
    pub fn execution_levels(&self) -> Result<Vec<Vec<String>>, String> {
        let mut levels: Vec<Vec<String>> = Vec::new();
        let mut node_level: HashMap<String, usize> = HashMap::new();

        // Calculate level for each node (max level of dependencies + 1)
        for node_id in self.topological_order()? {
            let node_def = &self.nodes[&node_id];
            let level = if node_def.depends_on.is_empty() {
                0
            } else {
                node_def
                    .depends_on
                    .iter()
                    .map(|dep| node_level.get(dep).copied().unwrap_or(0) + 1)
                    .max()
                    .unwrap_or(0)
            };

            node_level.insert(node_id.clone(), level);

            while levels.len() <= level {
                levels.push(Vec::new());
            }
            levels[level].push(node_id);
        }

        Ok(levels)
    }
}

/// Detailed workflow definition information.
#[derive(Debug)]
pub struct DefinitionDetail {
    pub id: i64,
    pub name: String,
    pub version: i32,
    pub description: Option<String>,
    pub definition: WorkflowDefinition,
    pub yaml: String,
    pub created_at: Option<chrono::DateTime<chrono::Utc>>,
    pub updated_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl DefinitionDetail {
    /// Get node count.
    pub fn node_count(&self) -> usize {
        self.definition.nodes.len()
    }

    /// Get input fields as a list.
    pub fn input_fields(&self) -> Vec<(&String, &SchemaField)> {
        self.definition.inputs.iter().collect()
    }

    /// Get nodes as a list with their IDs.
    pub fn nodes_list(&self) -> Vec<(&String, &NodeDefinition)> {
        self.definition.nodes.iter().collect()
    }

    /// Get output expressions as a list.
    pub fn output_expressions(&self) -> Vec<(&String, &String)> {
        self.definition.outputs.iter().collect()
    }

    /// Get metadata as formatted JSON.
    pub fn metadata_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.definition.metadata).unwrap_or_else(|_| "{}".to_string())
    }
}

/// List workflow definitions page.
pub async fn list(
    State(state): State<AppState>,
    Query(query): Query<DefinitionsQuery>,
) -> Result<DefinitionsListTemplate, Response> {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(25).min(100);
    let offset = (page - 1) * per_page;

    let result = get_definitions(
        &state.pool,
        state.schema(),
        query.name.as_deref(),
        per_page,
        offset,
    )
    .await;

    match result {
        Ok((definitions, total)) => {
            let total_pages = (total as f64 / per_page as f64).ceil() as i64;

            Ok(DefinitionsListTemplate {
                definitions,
                current_name: query.name,
                page,
                total,
                total_pages,
                error: None,
            })
        }
        Err(e) => Ok(DefinitionsListTemplate {
            definitions: vec![],
            current_name: query.name,
            page: 1,
            total: 0,
            total_pages: 0,
            error: Some(e),
        }),
    }
}

/// Show single workflow definition.
pub async fn show(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<DefinitionDetailTemplate, Response> {
    let definition = get_definition(&state.pool, state.schema(), id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e).into_response())?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Definition not found").into_response())?;

    Ok(DefinitionDetailTemplate { definition })
}

/// Show visual editor for an existing definition.
pub async fn edit(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<DefinitionEditTemplate, Response> {
    // Verify the definition exists
    let definition = get_definition(&state.pool, state.schema(), id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e).into_response())?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Definition not found").into_response())?;

    Ok(DefinitionEditTemplate {
        definition_id: Some(id),
        name: definition.name,
        is_new: false,
    })
}

/// Show visual editor for creating a new definition.
pub async fn new_definition() -> DefinitionEditTemplate {
    DefinitionEditTemplate {
        definition_id: None,
        name: "New Workflow".to_string(),
        is_new: true,
    }
}

/// Show import form.
pub async fn import_form() -> DefinitionImportTemplate {
    DefinitionImportTemplate {
        error: None,
        yaml: String::new(),
    }
}

/// Import a workflow definition from YAML.
pub async fn import_yaml(
    State(state): State<AppState>,
    axum::Form(form): axum::Form<ImportForm>,
) -> Result<Redirect, DefinitionImportTemplate> {
    // Parse the YAML
    let def = WorkflowDefinition::from_yaml(&form.yaml).map_err(|e| DefinitionImportTemplate {
        error: Some(e),
        yaml: form.yaml.clone(),
    })?;

    // Save to database
    let id = save_definition(&state.pool, state.schema(), &def)
        .await
        .map_err(|e| DefinitionImportTemplate {
            error: Some(format!("Failed to save: {}", e)),
            yaml: form.yaml.clone(),
        })?;

    Ok(Redirect::to(&format!("/definitions/{}", id)))
}

/// Export a workflow definition as YAML.
pub async fn export_yaml(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Response, Response> {
    let definition = get_definition(&state.pool, state.schema(), id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e).into_response())?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Definition not found").into_response())?;

    let filename = format!("{}-v{}.yaml", definition.name, definition.version);

    Ok((
        [
            (header::CONTENT_TYPE, "application/x-yaml"),
            (
                header::CONTENT_DISPOSITION,
                &format!("attachment; filename=\"{}\"", filename),
            ),
        ],
        definition.yaml,
    )
        .into_response())
}

/// Delete a workflow definition.
pub async fn delete(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    delete_definition(&state.pool, state.schema(), id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e).into_response())?;

    Ok(Redirect::to("/definitions"))
}

/// Get workflow definitions with filtering and pagination.
async fn get_definitions(
    pool: &PgPool,
    schema: Option<&str>,
    name_filter: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<(Vec<DefinitionInfo>, i64), String> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflow_definitions", s),
        None => "ishikari_workflow_definitions".to_string(),
    };

    // Check if table exists
    let table_exists = check_table_exists(pool, schema).await;
    if !table_exists {
        return Err(
            "Workflow definitions table not found. Please run ishikari-workflows migrations."
                .to_string(),
        );
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

    // Build count query
    let mut count_q = sqlx::query_scalar::<_, i64>(&count_query);
    for p in &params {
        count_q = count_q.bind(p);
    }
    let total: i64 = count_q
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Database error: {}", e))?;

    // Build list query
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

    let rows = list_q
        .fetch_all(pool)
        .await
        .map_err(|e| format!("Database error: {}", e))?;

    let definitions = rows
        .into_iter()
        .map(|row| {
            let nodes: HashMap<String, serde_json::Value> =
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

    Ok((definitions, total))
}

/// Check if the workflow definitions table exists.
async fn check_table_exists(pool: &PgPool, schema: Option<&str>) -> bool {
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

/// Get a single workflow definition by ID.
async fn get_definition(
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
            // Reconstruct the WorkflowDefinition
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

            // Generate YAML
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

/// Save a workflow definition to the database.
async fn save_definition(
    pool: &PgPool,
    schema: Option<&str>,
    def: &WorkflowDefinition,
) -> Result<i64, String> {
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

    let id: i64 = sqlx::query_scalar(&format!(
        r#"
        INSERT INTO {} (name, version, description, input_schema, nodes, output_schema, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        "#,
        table
    ))
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

    Ok(id)
}

/// Delete a workflow definition by ID.
async fn delete_definition(pool: &PgPool, schema: Option<&str>, id: i64) -> Result<(), String> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflow_definitions", s),
        None => "ishikari_workflow_definitions".to_string(),
    };

    let query = format!("DELETE FROM {} WHERE id = $1", table);

    sqlx::query(&query)
        .bind(id)
        .execute(pool)
        .await
        .map_err(|e| format!("Database error: {}", e))?;

    Ok(())
}

/// Show run form for a workflow definition.
pub async fn run_form(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<DefinitionRunTemplate, Response> {
    let definition = get_definition(&state.pool, state.schema(), id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e).into_response())?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Definition not found").into_response())?;

    // Get recent runs for this definition
    let runs = get_definition_runs(&state.pool, state.schema(), id, 10)
        .await
        .unwrap_or_default();

    Ok(DefinitionRunTemplate {
        definition,
        runs,
        error: None,
    })
}

/// Run a workflow definition.
pub async fn run(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    axum::Form(form): axum::Form<RunForm>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    // Get the definition
    let definition = get_definition(&state.pool, schema, id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e).into_response())?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Definition not found").into_response())?;

    // Parse the inputs JSON
    let inputs: Value = serde_json::from_str(&form.inputs_json).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("Invalid JSON inputs: {}", e),
        )
            .into_response()
    })?;

    // Create the workflow run
    let workflow_id = create_workflow_run(
        &state.pool,
        schema,
        &definition.definition,
        id,
        definition.version,
        &inputs,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e).into_response())?;

    // Redirect to the workflow detail page
    Ok(Redirect::to(&format!("/workflows/{}", workflow_id)))
}

/// Create a workflow run with all node jobs.
async fn create_workflow_run(
    pool: &PgPool,
    schema: Option<&str>,
    def: &WorkflowDefinition,
    definition_id: i64,
    definition_version: i32,
    inputs: &Value,
) -> Result<i64, String> {
    // 1. Create the ishikari-pro workflow
    let workflow = Workflow::create(
        pool,
        &def.name,
        serde_json::json!({
            "definition_id": definition_id,
            "definition_version": definition_version,
        }),
        schema,
    )
    .await
    .map_err(|e| format!("Failed to create workflow: {}", e))?;

    // 2. Create the workflow run record
    let runs_table = match schema {
        Some(s) => format!("{}.ishikari_workflow_runs", s),
        None => "ishikari_workflow_runs".to_string(),
    };

    let _run_id: i64 = sqlx::query_scalar(&format!(
        r#"
        INSERT INTO {} (definition_id, definition_name, definition_version, workflow_id, inputs, status)
        VALUES ($1, $2, $3, $4, $5, 'running')
        RETURNING id
        "#,
        runs_table
    ))
    .bind(definition_id)
    .bind(&def.name)
    .bind(definition_version)
    .bind(workflow.id)
    .bind(inputs)
    .fetch_one(pool)
    .await
    .map_err(|e| format!("Failed to create workflow run: {}", e))?;

    // 3. Get execution levels (nodes grouped by dependency depth)
    let levels = def
        .execution_levels()
        .map_err(|e| format!("Failed to compute execution levels: {}", e))?;

    // 4. Insert jobs for each node
    let mut job_ids: HashMap<String, i64> = HashMap::new();
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let enum_type = match schema {
        Some(s) => format!("{}.ishikari_job_state", s),
        None => "ishikari_job_state".to_string(),
    };

    for level in &levels {
        for node_id in level {
            let node_def = &def.nodes[node_id];
            let has_dependencies = !node_def.depends_on.is_empty();

            // Create the node job args - use "ishikari_worker" tag for typetag compatibility
            let node_job_args = serde_json::json!({
                "ishikari_worker": "ishikari_workflows::NodeJob",
                "workflow_run_id": _run_id,
                "node_id": node_id,
                "node_type": node_def.node_type,
                "raw_inputs": node_def.inputs,
                "condition": node_def.condition,
            });

            let state = if has_dependencies {
                "scheduled"
            } else {
                "available"
            };

            let max_attempts = node_def.max_retries.unwrap_or(3) as i32;

            let job_id: i64 = sqlx::query_scalar(&format!(
                r#"
                INSERT INTO {} (queue, worker, args, max_attempts, workflow_id, state)
                VALUES ($1, $2, $3, $4, $5, $6::{})
                RETURNING id
                "#,
                jobs_table, enum_type
            ))
            .bind("workflows")
            .bind("ishikari_workflows::NodeJob")
            .bind(&node_job_args)
            .bind(max_attempts)
            .bind(workflow.id)
            .bind(state)
            .fetch_one(pool)
            .await
            .map_err(|e| format!("Failed to create node job: {}", e))?;

            job_ids.insert(node_id.clone(), job_id);
        }
    }

    // 5. Create dependencies
    for (node_id, node_def) in &def.nodes {
        let job_id = job_ids[node_id];

        for dep_node_id in &node_def.depends_on {
            let dep_job_id = job_ids[dep_node_id];
            Dependencies::add(pool, job_id, dep_job_id, schema)
                .await
                .map_err(|e| format!("Failed to add dependency: {}", e))?;
        }
    }

    Ok(workflow.id)
}

/// Get workflow runs for a definition.
async fn get_definition_runs(
    pool: &PgPool,
    schema: Option<&str>,
    definition_id: i64,
    limit: i64,
) -> Result<Vec<WorkflowRunInfo>, String> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflow_runs", s),
        None => "ishikari_workflow_runs".to_string(),
    };

    // Check if table exists
    let table_exists = check_runs_table_exists(pool, schema).await;
    if !table_exists {
        return Ok(vec![]);
    }

    let query = format!(
        r#"
        SELECT id, workflow_id, status, inputs, created_at, completed_at
        FROM {}
        WHERE definition_id = $1
        ORDER BY created_at DESC
        LIMIT $2
        "#,
        table
    );

    let rows: Vec<(
        i64,
        i64,
        String,
        Value,
        chrono::DateTime<chrono::Utc>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(&query)
        .bind(definition_id)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(|e| format!("Database error: {}", e))?;

    Ok(rows
        .into_iter()
        .map(|row| WorkflowRunInfo {
            id: row.0,
            workflow_id: row.1,
            status: row.2,
            inputs: row.3,
            created_at: row.4,
            completed_at: row.5,
        })
        .collect())
}

/// Check if the workflow runs table exists.
async fn check_runs_table_exists(pool: &PgPool, schema: Option<&str>) -> bool {
    let (schema_name, table_name) = match schema {
        Some(s) => (s.to_string(), "ishikari_workflow_runs".to_string()),
        None => ("public".to_string(), "ishikari_workflow_runs".to_string()),
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
