//! Worker definitions for the demo
//!
//! Contains all job and worker types used in demo scenarios.

use async_trait::async_trait;
use handlebars::Handlebars;
use ishikari::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// ============ Workflow Node Job State ============

/// State required for NodeJob execution.
/// Must be registered with the ishikari Engine.
#[derive(Clone)]
pub struct NodeJobState {
    /// Database pool for storing outputs.
    pub pool: PgPool,
    /// Optional schema for multi-tenancy.
    pub schema: Option<String>,
}

impl NodeJobState {
    /// Create new NodeJob state.
    pub fn new(pool: PgPool) -> Self {
        Self { pool, schema: None }
    }

    /// Set the schema for multi-tenancy.
    #[allow(dead_code)]
    pub fn with_schema(mut self, schema: Option<String>) -> Self {
        self.schema = schema;
        self
    }
}

// ============ Expression Evaluation ============

/// Context for expression evaluation.
#[derive(Debug, Serialize)]
struct EvaluationContext {
    /// Workflow inputs.
    inputs: Value,
    /// Node outputs (node_id -> { outputs: Value }).
    #[serde(flatten)]
    nodes: HashMap<String, NodeOutputContext>,
}

#[derive(Debug, Serialize)]
struct NodeOutputContext {
    outputs: Value,
}

/// Evaluate expressions in a value recursively.
fn evaluate_value(
    handlebars: &Handlebars,
    value: &Value,
    context: &EvaluationContext,
) -> Result<Value, String> {
    match value {
        Value::String(s) if s.contains("{{") => {
            let result = handlebars
                .render_template(s, context)
                .map_err(|e| format!("Expression error: {}", e))?;
            // Try to parse as JSON, fallback to string
            Ok(serde_json::from_str(&result).unwrap_or(Value::String(result)))
        }
        Value::String(_) => Ok(value.clone()),
        Value::Array(arr) => {
            let evaluated: Result<Vec<Value>, String> = arr
                .iter()
                .map(|v| evaluate_value(handlebars, v, context))
                .collect();
            Ok(Value::Array(evaluated?))
        }
        Value::Object(obj) => {
            let evaluated: Result<serde_json::Map<String, Value>, String> = obj
                .iter()
                .map(|(k, v)| {
                    let evaluated_v = evaluate_value(handlebars, v, context)?;
                    Ok((k.clone(), evaluated_v))
                })
                .collect();
            Ok(Value::Object(evaluated?))
        }
        _ => Ok(value.clone()),
    }
}

// ============ Workflow Node Job ============
// This mimics ishikari::execution::NodeJob for the demo

/// A job that executes a workflow node.
/// Uses the same serialization name as ishikari::execution::NodeJob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeJob {
    /// The workflow run ID.
    pub workflow_run_id: i64,
    /// The node ID within the workflow.
    pub node_id: String,
    /// The node type (e.g., "echo", "http/get").
    pub node_type: String,
    /// Raw inputs with unresolved expressions.
    pub raw_inputs: HashMap<String, serde_json::Value>,
    /// Optional condition for execution.
    pub condition: Option<String>,
}

impl NodeJob {
    /// Load workflow inputs from the database.
    async fn load_workflow_inputs(
        &self,
        pool: &PgPool,
        schema: Option<&str>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_workflow_runs", s),
            None => "ishikari_workflow_runs".to_string(),
        };

        let row: Option<(Value,)> =
            sqlx::query_as(&format!("SELECT inputs FROM {} WHERE id = $1", table))
                .bind(self.workflow_run_id)
                .fetch_optional(pool)
                .await?;

        Ok(row
            .map(|(inputs,)| inputs)
            .unwrap_or(Value::Object(serde_json::Map::new())))
    }

    /// Load all node outputs for this workflow run from node_executions table.
    async fn load_node_outputs(
        &self,
        pool: &PgPool,
        schema: Option<&str>,
    ) -> Result<HashMap<String, Value>, Box<dyn std::error::Error + Send + Sync>> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_node_executions", s),
            None => "ishikari_node_executions".to_string(),
        };

        let rows: Vec<(String, Value)> = sqlx::query_as(&format!(
            "SELECT node_id, output FROM {} WHERE workflow_run_id = $1 AND status = 'completed' AND output IS NOT NULL",
            table
        ))
        .bind(self.workflow_run_id)
        .fetch_all(pool)
        .await?;

        Ok(rows.into_iter().collect())
    }

    /// Create or update node execution record with resolved inputs and output.
    async fn save_node_execution(
        &self,
        pool: &PgPool,
        schema: Option<&str>,
        resolved_inputs: &serde_json::Value,
        output: &serde_json::Value,
        duration_ms: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_node_executions", s),
            None => "ishikari_node_executions".to_string(),
        };

        sqlx::query(&format!(
            r#"
            INSERT INTO {} (workflow_run_id, node_id, node_type, status, inputs, output, duration_ms, started_at, completed_at)
            VALUES ($1, $2, $3, 'completed', $4, $5, $6, now() - ($6 || ' milliseconds')::interval, now())
            ON CONFLICT (workflow_run_id, node_id)
            DO UPDATE SET
                status = 'completed',
                inputs = $4,
                output = $5,
                duration_ms = $6,
                completed_at = now()
            "#,
            table
        ))
        .bind(self.workflow_run_id)
        .bind(&self.node_id)
        .bind(&self.node_type)
        .bind(resolved_inputs)
        .bind(output)
        .bind(duration_ms)
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Get workflow definition info (nodes and output_schema) from the workflow run.
    async fn get_definition_info(
        &self,
        pool: &PgPool,
        schema: Option<&str>,
    ) -> Result<(Value, Value, Value), Box<dyn std::error::Error + Send + Sync>> {
        let runs_table = match schema {
            Some(s) => format!("{}.ishikari_workflow_runs", s),
            None => "ishikari_workflow_runs".to_string(),
        };
        let defs_table = match schema {
            Some(s) => format!("{}.ishikari_workflow_definitions", s),
            None => "ishikari_workflow_definitions".to_string(),
        };

        let row: Option<(Value, Value, Value)> = sqlx::query_as(&format!(
            r#"
            SELECT r.inputs, d.nodes, d.output_schema
            FROM {} r
            JOIN {} d ON d.id = r.definition_id
            WHERE r.id = $1
            "#,
            runs_table, defs_table
        ))
        .bind(self.workflow_run_id)
        .fetch_optional(pool)
        .await?;

        row.ok_or_else(|| "Workflow run or definition not found".into())
    }

    /// Check if all nodes are complete and update workflow run if so.
    async fn check_workflow_completion(
        &self,
        pool: &PgPool,
        schema: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Get definition info
        let (workflow_inputs, nodes_def, output_schema) =
            self.get_definition_info(pool, schema).await?;

        // Get expected node IDs from definition
        let expected_nodes: Vec<String> = match &nodes_def {
            Value::Object(map) => map.keys().cloned().collect(),
            _ => return Ok(()), // Invalid definition, skip
        };

        // Get completed node outputs
        let node_outputs = self.load_node_outputs(pool, schema).await?;
        let completed_nodes: Vec<&String> = node_outputs.keys().collect();

        // Check if all nodes are complete
        let all_complete = expected_nodes
            .iter()
            .all(|node_id| node_outputs.contains_key(node_id));

        if !all_complete {
            tracing::debug!(
                expected = ?expected_nodes,
                completed = ?completed_nodes,
                "Workflow not yet complete"
            );
            return Ok(());
        }

        tracing::info!(
            workflow_run_id = self.workflow_run_id,
            "All nodes complete, computing workflow outputs"
        );

        // Build evaluation context for final outputs
        let nodes: HashMap<String, NodeOutputContext> = node_outputs
            .into_iter()
            .map(|(id, output)| (id, NodeOutputContext { outputs: output }))
            .collect();

        let context = EvaluationContext {
            inputs: workflow_inputs,
            nodes,
        };

        // Setup handlebars
        let mut handlebars = Handlebars::new();
        handlebars.register_escape_fn(handlebars::no_escape);
        handlebars.set_strict_mode(false);

        // Evaluate output schema expressions
        let final_outputs = evaluate_value(&handlebars, &output_schema, &context)
            .map_err(|e| format!("Failed to evaluate outputs: {}", e))?;

        tracing::info!(outputs = ?final_outputs, "Computed workflow outputs");

        // Update workflow run with status and outputs
        let runs_table = match schema {
            Some(s) => format!("{}.ishikari_workflow_runs", s),
            None => "ishikari_workflow_runs".to_string(),
        };

        sqlx::query(&format!(
            r#"
            UPDATE {}
            SET status = 'completed', outputs = $1, completed_at = now()
            WHERE id = $2
            "#,
            runs_table
        ))
        .bind(&final_outputs)
        .bind(self.workflow_run_id)
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Resolve expressions in raw_inputs using the evaluation context.
    fn resolve_inputs(
        &self,
        workflow_inputs: Value,
        node_outputs: HashMap<String, Value>,
    ) -> Result<HashMap<String, Value>, String> {
        // Build evaluation context
        let nodes: HashMap<String, NodeOutputContext> = node_outputs
            .into_iter()
            .map(|(id, output)| (id, NodeOutputContext { outputs: output }))
            .collect();

        let context = EvaluationContext {
            inputs: workflow_inputs,
            nodes,
        };

        // Setup handlebars
        let mut handlebars = Handlebars::new();
        handlebars.register_escape_fn(handlebars::no_escape);
        handlebars.set_strict_mode(false); // Lenient mode for missing values

        // Evaluate each input
        let raw_value = serde_json::to_value(&self.raw_inputs)
            .map_err(|e| format!("Failed to serialize inputs: {}", e))?;

        let resolved = evaluate_value(&handlebars, &raw_value, &context)?;

        // Convert back to HashMap
        match resolved {
            Value::Object(map) => Ok(map.into_iter().collect()),
            _ => Ok(HashMap::new()),
        }
    }
}

#[typetag::serde(name = "ishikari::execution::NodeJob")]
#[async_trait]
impl Worker for NodeJob {
    fn queue(&self) -> &'static str {
        "workflows"
    }

    fn max_attempts(&self) -> i32 {
        3
    }

    async fn perform(&self, ctx: Context) -> PerformResult {
        let start_time = Instant::now();

        tracing::info!(
            workflow_run_id = self.workflow_run_id,
            node_id = %self.node_id,
            node_type = %self.node_type,
            "Executing workflow node"
        );

        // Get the state to access the database pool
        let state = ctx.state::<NodeJobState>()?;
        let schema = state.schema.as_deref();

        // Load workflow inputs and upstream node outputs
        let workflow_inputs = self
            .load_workflow_inputs(&state.pool, schema)
            .await
            .map_err(|e| format!("Failed to load workflow inputs: {}", e))?;

        let node_outputs = self
            .load_node_outputs(&state.pool, schema)
            .await
            .map_err(|e| format!("Failed to load node outputs: {}", e))?;

        tracing::debug!(
            workflow_inputs = ?workflow_inputs,
            node_outputs = ?node_outputs,
            "Loaded context for expression evaluation"
        );

        // Resolve expressions in inputs
        let inputs = self
            .resolve_inputs(workflow_inputs, node_outputs)
            .map_err(|e| format!("Failed to resolve inputs: {}", e))?;

        // Convert resolved inputs to JSON for storage
        let resolved_inputs_json = serde_json::to_value(&inputs)
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        tracing::info!(inputs = ?inputs, "Resolved inputs");

        // Simulate node execution based on type and get output
        let output: serde_json::Value = match self.node_type.as_str() {
            "echo" => {
                let message = inputs
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("(no message)");
                sleep(Duration::from_millis(100)).await;
                serde_json::json!({ "message": message })
            }
            "uppercase" => {
                let text = inputs.get("text").and_then(|v| v.as_str()).unwrap_or("");
                sleep(Duration::from_millis(100)).await;
                serde_json::json!({ "result": text.to_uppercase() })
            }
            "http/get" => {
                // Simulate HTTP fetch
                sleep(Duration::from_secs(1)).await;
                serde_json::json!({ "status": 200, "body": { "data": "sample" } })
            }
            "json/validate" | "json/transform" => {
                sleep(Duration::from_millis(200)).await;
                serde_json::json!({ "valid": true, "records": [] })
            }
            "db/query" | "db/insert" => {
                sleep(Duration::from_millis(300)).await;
                serde_json::json!({ "rows_affected": 1, "email_enabled": true, "sms_enabled": false })
            }
            "email/send" => {
                sleep(Duration::from_secs(1)).await;
                tracing::info!("Simulated email sent");
                serde_json::json!({ "sent": true })
            }
            "sms/send" => {
                sleep(Duration::from_millis(500)).await;
                tracing::info!("Simulated SMS sent");
                serde_json::json!({ "sent": true })
            }
            _ => {
                // Unknown node type - just complete successfully
                sleep(Duration::from_millis(500)).await;
                serde_json::json!({ "node_type": self.node_type })
            }
        };

        // Calculate duration and save the execution record
        let duration_ms = start_time.elapsed().as_millis() as i64;

        self.save_node_execution(
            &state.pool,
            schema,
            &resolved_inputs_json,
            &output,
            duration_ms,
        )
        .await
        .map_err(|e| format!("Failed to save node execution: {}", e))?;

        tracing::info!(node_id = %self.node_id, output = ?output, duration_ms = duration_ms, "Node execution saved");

        // Check if workflow is complete and compute final outputs
        if let Err(e) = self.check_workflow_completion(&state.pool, schema).await {
            tracing::warn!(error = %e, "Failed to check workflow completion");
        }

        Complete::default().into()
    }
}

// ============ Email Processing ============

#[ishikari::job]
#[derive(Clone)]
pub struct ValidateEmail {
    pub email: String,
}

#[ishikari::worker(queue = "email")]
impl Worker for ValidateEmail {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_millis(500)).await;
        // Simple validation
        if self.email.contains('@') {
            Complete::default().message("valid").into()
        } else {
            Err("invalid email format".into())
        }
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct SendEmail {
    pub to: String,
    pub subject: String,
    pub body: String,
}

#[ishikari::worker(queue = "email")]
impl Worker for SendEmail {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        tracing::info!(to = %self.to, subject = %self.subject, "Email sent");
        Complete::default().message("sent").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct LogEmailEvent {
    pub event_type: String,
    pub email: String,
}

#[ishikari::worker(queue = "email")]
impl Worker for LogEmailEvent {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_millis(200)).await;
        Complete::default().into()
    }
}

// ============ ETL Processing ============

#[ishikari::job]
#[derive(Clone)]
pub struct ExtractFromSource {
    pub source: String,
    pub query: String,
}

#[ishikari::worker(queue = "etl")]
impl Worker for ExtractFromSource {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(2)).await;
        tracing::info!(source = %self.source, "Data extracted");
        Complete::default().message(r#"{"rows": 1000}"#).into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct TransformData {
    pub format: String,
    pub rules: String,
}

#[ishikari::worker(queue = "etl")]
impl Worker for TransformData {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(3)).await;
        Complete::default().message("transformed").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct LoadToDestination {
    pub destination: String,
    pub table: String,
}

#[ishikari::worker(queue = "etl")]
impl Worker for LoadToDestination {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        Complete::default().message("loaded").into()
    }
}

// ============ Order Saga ============

#[ishikari::job]
#[derive(Clone)]
pub struct ReserveInventory {
    pub order_id: String,
    pub product_id: String,
    pub quantity: i32,
}

#[ishikari::worker(queue = "orders", max_attempts = 3)]
impl Worker for ReserveInventory {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        Complete::default().message("reserved").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct ReleaseInventory {
    pub order_id: String,
    pub product_id: String,
    pub quantity: i32,
}

#[ishikari::worker(queue = "orders")]
impl Worker for ReleaseInventory {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_millis(500)).await;
        Complete::default().message("released").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct ChargePayment {
    pub order_id: String,
    pub amount: f64,
    pub currency: String,
}

#[ishikari::worker(queue = "payments", max_attempts = 3)]
impl Worker for ChargePayment {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(2)).await;
        Complete::default().message("charged").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct RefundPayment {
    pub order_id: String,
    pub amount: f64,
    pub reason: String,
}

#[ishikari::worker(queue = "payments")]
impl Worker for RefundPayment {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        Complete::default().message("refunded").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct ShipOrder {
    pub order_id: String,
    pub address: String,
    pub carrier: String,
}

#[ishikari::worker(queue = "shipping", max_attempts = 3)]
impl Worker for ShipOrder {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        Complete::default().message("shipped").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct CancelShipment {
    pub order_id: String,
    pub reason: String,
}

#[ishikari::worker(queue = "shipping")]
impl Worker for CancelShipment {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_millis(500)).await;
        Complete::default().message("cancelled").into()
    }
}

/// A ship order that always fails - used to test saga compensation.
/// Simulates a shipping request to an undeliverable address.
#[ishikari::job]
#[derive(Clone)]
pub struct FailingShipOrder {
    pub order_id: String,
    pub address: String,
}

#[ishikari::worker(queue = "shipping", max_attempts = 1)]
impl Worker for FailingShipOrder {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        // Simulate attempting to ship - discover the address is undeliverable
        sleep(Duration::from_millis(500)).await;

        tracing::error!(
            order_id = %self.order_id,
            address = %self.address,
            "Shipping failed: no carriers available for delivery area"
        );

        // Return Cancel with the error discovered during execution
        Cancel::default()
            .message("Shipping failed: no carriers available for delivery area")
            .into()
    }
}

// ============ Misc Jobs ============

#[ishikari::job]
#[derive(Clone)]
pub struct SlowJob {
    pub duration_secs: u64,
    pub label: String,
}

#[ishikari::worker(queue = "default")]
impl Worker for SlowJob {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(self.duration_secs)).await;
        Complete::default().message(&self.label).into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct FailingJob {
    pub fail_until_attempt: i32,
    pub message: String,
}

#[ishikari::worker(queue = "default", max_attempts = 5)]
impl Worker for FailingJob {
    async fn perform(&self, ctx: Context) -> PerformResult {
        if ctx.job().attempt < self.fail_until_attempt {
            Err(format!("Failing on attempt {}", ctx.job().attempt).into())
        } else {
            Complete::default().message(&self.message).into()
        }
    }
}
