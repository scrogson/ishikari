//! Workflow executor.
//!
//! Executes workflows with proper DAG traversal, parallel execution,
//! input/output tracking, and event broadcasting.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use sqlx::PgPool;
use tokio::sync::{broadcast, RwLock, Semaphore};
use uuid::Uuid;

use super::context::NodeContext;
use super::dag::ExecutionLevels;
use super::events::{EventSender, ExecutionEvent};
use super::interpolation::{InterpolationError, Interpolator};
use super::storage::ExecutionStorage;
use super::types::*;
use super::validation::{ValidationResult, Validator};

/// Default maximum concurrent node executions.
const DEFAULT_MAX_CONCURRENCY: usize = 10;

/// Node execution handler trait.
///
/// Implement this trait to define how different node types execute.
#[async_trait::async_trait]
pub trait NodeHandler: Send + Sync {
    /// Execute a node and return its output.
    async fn execute(&self, context: &NodeContext, pool: &PgPool) -> Result<NodeData, NodeError>;
}

/// Error during node execution.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error("Node execution failed: {0}")]
    ExecutionFailed(String),

    #[error("Node timed out after {0} seconds")]
    Timeout(u32),

    #[error("Interpolation error: {0}")]
    Interpolation(#[from] InterpolationError),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Condition evaluation failed: {0}")]
    ConditionError(String),
}

/// Workflow execution error.
#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("Validation failed: {0}")]
    ValidationFailed(String),

    #[error("Node '{node_id}' failed: {error}")]
    NodeFailed { node_id: String, error: String },

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Execution cancelled")]
    Cancelled,
}

/// Workflow executor with event broadcasting and parallel execution.
pub struct WorkflowExecutor {
    pool: PgPool,
    event_sender: EventSender,
    max_concurrency: usize,
    node_handlers: HashMap<String, Arc<dyn NodeHandler>>,
    schema: Option<String>,
    /// Enable database persistence of execution state.
    persist: bool,
}

impl WorkflowExecutor {
    /// Create a new workflow executor.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            event_sender: EventSender::new(),
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            node_handlers: HashMap::new(),
            schema: None,
            persist: false,
        }
    }

    /// Set the maximum concurrent node executions.
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.max_concurrency = max;
        self
    }

    /// Set the database schema for multi-tenancy.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Enable database persistence of execution state.
    ///
    /// When enabled, the executor will persist workflow and node execution
    /// state to the database tables:
    /// - `ishikari_workflow_runs` - Workflow execution records
    /// - `ishikari_node_executions` - Node execution records (inputs, outputs, status, timing)
    pub fn persist(mut self, enabled: bool) -> Self {
        self.persist = enabled;
        self
    }

    /// Create execution storage with the configured schema.
    fn storage(&self) -> ExecutionStorage {
        let storage = ExecutionStorage::new(self.pool.clone());
        if let Some(schema) = &self.schema {
            storage.schema(schema.clone())
        } else {
            storage
        }
    }

    /// Register a node handler for a node type.
    pub fn register_handler(
        mut self,
        node_type: impl Into<String>,
        handler: impl NodeHandler + 'static,
    ) -> Self {
        self.node_handlers
            .insert(node_type.into(), Arc::new(handler));
        self
    }

    /// Subscribe to execution events.
    pub fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.event_sender.subscribe()
    }

    /// Get the event sender for external broadcasting.
    pub fn event_sender(&self) -> &EventSender {
        &self.event_sender
    }

    /// Validate a workflow without executing it.
    pub fn validate(
        &self,
        workflow: &ExecutableWorkflow,
        inputs: &HashMap<String, NodeData>,
    ) -> ValidationResult {
        Validator::new(workflow, inputs).validate()
    }

    /// Execute a workflow.
    ///
    /// Returns the execution ID and outputs on success.
    pub async fn execute(
        &self,
        workflow: ExecutableWorkflow,
        inputs: HashMap<String, NodeData>,
    ) -> Result<WorkflowExecution, ExecutionError> {
        self.execute_with_context(workflow, inputs, 0, 0).await
    }

    /// Execute a workflow with database context.
    ///
    /// This method should be used when persistence is enabled, providing
    /// the definition and workflow IDs for proper database tracking.
    pub async fn execute_with_context(
        &self,
        workflow: ExecutableWorkflow,
        inputs: HashMap<String, NodeData>,
        definition_id: i64,
        workflow_id: i64,
    ) -> Result<WorkflowExecution, ExecutionError> {
        let execution_id = Uuid::new_v4();
        let start_time = Instant::now();

        // Validate before execution
        let validation = self.validate(&workflow, &inputs);
        if !validation.is_valid() {
            let errors: Vec<String> = validation.errors.iter().map(|e| e.to_string()).collect();
            return Err(ExecutionError::ValidationFailed(errors.join("; ")));
        }

        // Compute execution levels
        let levels =
            ExecutionLevels::compute(&workflow).map_err(ExecutionError::ValidationFailed)?;

        // Create workflow run in database if persistence is enabled
        let workflow_run_id = if self.persist {
            let storage = self.storage();
            let inputs_json = serde_json::to_value(
                inputs
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect::<HashMap<_, _>>(),
            )
            .unwrap_or_default();

            let run_id = storage
                .create_workflow_run(
                    execution_id,
                    definition_id,
                    &workflow.name,
                    workflow.version,
                    workflow_id,
                    &inputs_json,
                )
                .await?;

            // Create node execution records for all nodes
            for (node_id, node) in &workflow.nodes {
                storage
                    .create_node_execution(run_id, node_id, &node.node_type, &serde_json::json!({}))
                    .await?;
            }

            Some(run_id)
        } else {
            None
        };

        // Broadcast workflow started
        self.event_sender
            .workflow_started(execution_id, definition_id, &workflow.name);

        // Track node outputs - use Arc<RwLock> for shared access across tasks
        let node_outputs: Arc<RwLock<HashMap<String, NodeData>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let mut completed_nodes = 0;
        let total_nodes = workflow.nodes.len();

        // Create semaphore for concurrency control
        let semaphore = Arc::new(Semaphore::new(self.max_concurrency));

        // Execute level by level
        for (level_idx, node_ids) in levels.iter() {
            let level_start = Instant::now();
            self.event_sender.level_started(
                execution_id,
                level_idx,
                node_ids.iter().cloned().collect(),
            );

            // Execute all nodes in this level in parallel
            let mut handles = Vec::new();

            for node_id in node_ids {
                let node = workflow.nodes.get(node_id).unwrap().clone();
                let pool = self.pool.clone();
                let event_sender = self.event_sender.clone();
                let semaphore = semaphore.clone();
                let handler = self.node_handlers.get(&node.node_type).cloned();
                let inputs_clone = inputs.clone();
                let node_outputs_clone = node_outputs.clone();
                let persist = self.persist;
                let storage = self.storage();
                let run_id = workflow_run_id;

                let handle = tokio::spawn(async move {
                    // Acquire semaphore permit
                    let _permit = semaphore.acquire().await.unwrap();

                    // Read current node outputs
                    let outputs_snapshot = node_outputs_clone.read().await.clone();

                    // Mark node as started in DB
                    if persist {
                        if let Some(run_id) = run_id {
                            let _ = storage.start_node_execution(run_id, &node.id).await;
                        }
                    }

                    // Execute the node
                    let result = execute_node(
                        execution_id,
                        &node,
                        &inputs_clone,
                        &outputs_snapshot,
                        handler.as_deref(),
                        &pool,
                        &event_sender,
                    )
                    .await;

                    // Persist result to DB
                    if persist {
                        if let Some(run_id) = run_id {
                            match &result {
                                Ok(output) => {
                                    let duration_ms = 0; // Will be calculated properly
                                    let _ = storage
                                        .complete_node_execution(
                                            run_id,
                                            &node.id,
                                            &output.to_json(),
                                            duration_ms,
                                        )
                                        .await;
                                }
                                Err(NodeError::ConditionError(reason)) => {
                                    let _ =
                                        storage.skip_node_execution(run_id, &node.id, reason).await;
                                }
                                Err(e) => {
                                    let _ = storage
                                        .fail_node_execution(run_id, &node.id, &e.to_string(), 0)
                                        .await;
                                }
                            }
                        }
                    }

                    result
                });

                handles.push((node_id.clone(), handle));
            }

            // Wait for all nodes in this level to complete
            for (node_id, handle) in handles {
                match handle.await {
                    Ok(Ok(output)) => {
                        node_outputs.write().await.insert(node_id, output);
                        completed_nodes += 1;
                        self.event_sender
                            .progress(execution_id, completed_nodes, total_nodes);
                    }
                    Ok(Err(NodeError::ConditionError(reason))) => {
                        // Node was skipped due to condition
                        self.event_sender
                            .node_skipped(execution_id, &node_id, &reason);
                        completed_nodes += 1;
                        self.event_sender
                            .progress(execution_id, completed_nodes, total_nodes);
                    }
                    Ok(Err(e)) => {
                        let duration_ms = start_time.elapsed().as_millis() as i64;
                        self.event_sender.workflow_failed(
                            execution_id,
                            &e.to_string(),
                            duration_ms,
                        );

                        // Mark workflow as failed in DB
                        if self.persist {
                            if let Some(run_id) = workflow_run_id {
                                let _ = self
                                    .storage()
                                    .fail_workflow_run(run_id, &e.to_string())
                                    .await;
                            }
                        }

                        return Err(ExecutionError::NodeFailed {
                            node_id,
                            error: e.to_string(),
                        });
                    }
                    Err(join_error) => {
                        let duration_ms = start_time.elapsed().as_millis() as i64;
                        let error = format!("Task panicked: {}", join_error);
                        self.event_sender
                            .workflow_failed(execution_id, &error, duration_ms);

                        // Mark workflow as failed in DB
                        if self.persist {
                            if let Some(run_id) = workflow_run_id {
                                let _ = self.storage().fail_workflow_run(run_id, &error).await;
                            }
                        }

                        return Err(ExecutionError::NodeFailed { node_id, error });
                    }
                }
            }

            let level_duration = level_start.elapsed().as_millis() as i64;
            self.event_sender
                .level_completed(execution_id, level_idx, level_duration);
        }

        // Compute workflow outputs
        let final_node_outputs = node_outputs.read().await.clone();
        let mut outputs = HashMap::new();
        let interpolator = Interpolator::new(&inputs, &final_node_outputs);

        for (output_name, expr) in &workflow.outputs {
            match interpolator.interpolate(&serde_json::Value::String(expr.clone())) {
                Ok(value) => {
                    outputs.insert(output_name.clone(), NodeData::Json(value));
                }
                Err(e) => {
                    tracing::warn!("Failed to compute output '{}': {}", output_name, e);
                }
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as i64;
        let outputs_json = serde_json::to_value(
            outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.to_json()))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap_or_default();

        // Complete workflow in DB
        if self.persist {
            if let Some(run_id) = workflow_run_id {
                let _ = self
                    .storage()
                    .complete_workflow_run(run_id, &outputs_json)
                    .await;
            }
        }

        self.event_sender
            .workflow_completed(execution_id, duration_ms, outputs_json);

        Ok(WorkflowExecution {
            id: execution_id,
            definition_id,
            name: workflow.name,
            status: ExecutionStatus::Completed,
            inputs,
            outputs,
            error: None,
            started_at: Some(start_time.elapsed().as_secs() as i64),
            completed_at: Some(start_time.elapsed().as_secs() as i64),
        })
    }
}

/// Execute a single node.
async fn execute_node(
    execution_id: Uuid,
    node: &ExecutableNode,
    workflow_inputs: &HashMap<String, NodeData>,
    node_outputs: &HashMap<String, NodeData>,
    handler: Option<&dyn NodeHandler>,
    pool: &PgPool,
    event_sender: &EventSender,
) -> Result<NodeData, NodeError> {
    let start_time = Instant::now();

    // Check condition first
    if let Some(condition) = &node.condition {
        let interpolator = Interpolator::new(workflow_inputs, node_outputs);
        match interpolator.interpolate(&serde_json::Value::String(condition.clone())) {
            Ok(serde_json::Value::Bool(false)) => {
                return Err(NodeError::ConditionError(format!(
                    "Condition '{}' evaluated to false",
                    condition
                )));
            }
            Ok(serde_json::Value::String(s)) if s.is_empty() || s == "false" || s == "0" => {
                return Err(NodeError::ConditionError(format!(
                    "Condition '{}' evaluated to falsy value",
                    condition
                )));
            }
            Err(e) => {
                return Err(NodeError::ConditionError(format!(
                    "Failed to evaluate condition '{}': {}",
                    condition, e
                )));
            }
            _ => {} // Truthy value, continue execution
        }
    }

    event_sender.node_started(execution_id, &node.id, &node.node_type);

    // Interpolate inputs
    let interpolator = Interpolator::new(workflow_inputs, node_outputs);
    let mut interpolated_inputs = HashMap::new();

    for (key, value) in &node.inputs {
        let interpolated = interpolator.interpolate(value)?;
        interpolated_inputs.insert(key.clone(), interpolated);
    }

    // Build context
    let mut context = NodeContext::new(execution_id, &node.id, &node.node_type);
    context.inputs = interpolated_inputs;
    context.workflow_inputs = workflow_inputs.clone();
    context.dependency_outputs = node_outputs.clone();

    // Execute with handler or default
    let output = if let Some(handler) = handler {
        handler.execute(&context, pool).await?
    } else {
        // Default handler: just return inputs as output
        NodeData::Json(serde_json::to_value(&context.inputs).unwrap_or_default())
    };

    let duration_ms = start_time.elapsed().as_millis() as i64;
    event_sender.node_completed(execution_id, &node.id, duration_ms, &output);

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_test_workflow() -> ExecutableWorkflow {
        let mut nodes = HashMap::new();

        nodes.insert(
            "start".to_string(),
            ExecutableNode {
                id: "start".to_string(),
                node_type: "echo".to_string(),
                depends_on: vec![],
                inputs: {
                    let mut m = HashMap::new();
                    m.insert("message".to_string(), json!("{{inputs.greeting}}"));
                    m
                },
                condition: None,
                timeout_seconds: None,
                max_retries: None,
            },
        );

        nodes.insert(
            "transform".to_string(),
            ExecutableNode {
                id: "transform".to_string(),
                node_type: "uppercase".to_string(),
                depends_on: vec!["start".to_string()],
                inputs: {
                    let mut m = HashMap::new();
                    m.insert("text".to_string(), json!("{{nodes.start.message}}"));
                    m
                },
                condition: None,
                timeout_seconds: None,
                max_retries: None,
            },
        );

        ExecutableWorkflow {
            name: "test-workflow".to_string(),
            description: None,
            version: 1,
            input_schema: {
                let mut m = HashMap::new();
                m.insert(
                    "greeting".to_string(),
                    SchemaField {
                        field_type: "string".to_string(),
                        description: Some("Greeting message".to_string()),
                        default: None,
                        required: true,
                    },
                );
                m
            },
            nodes,
            outputs: {
                let mut m = HashMap::new();
                m.insert("result".to_string(), "{{nodes.transform.text}}".to_string());
                m
            },
        }
    }

    #[tokio::test]
    async fn test_validation_catches_missing_input() {
        let workflow = make_test_workflow();
        let inputs = HashMap::new(); // Missing required 'greeting' input

        let pool = sqlx::PgPool::connect_lazy("postgres://invalid").unwrap();
        let executor = WorkflowExecutor::new(pool);

        let result = executor.validate(&workflow, &inputs);
        assert!(!result.is_valid());
        assert!(result.errors.iter().any(|e| e.code == "E010"));
    }

    #[tokio::test]
    async fn test_validation_passes_with_valid_input() {
        let workflow = make_test_workflow();
        let mut inputs = HashMap::new();
        inputs.insert("greeting".to_string(), NodeData::json("Hello, World!"));

        let pool = sqlx::PgPool::connect_lazy("postgres://invalid").unwrap();
        let executor = WorkflowExecutor::new(pool);

        let result = executor.validate(&workflow, &inputs);
        assert!(result.is_valid(), "Errors: {:?}", result.errors);
    }
}
