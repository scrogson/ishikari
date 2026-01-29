//! Core types for workflow execution.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// Node data that flows between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum NodeData {
    /// JSON value (most common)
    Json(Value),
    /// Raw bytes (for binary data)
    Bytes(Vec<u8>),
    /// Reference to external storage (for large data)
    Reference(String),
}

impl NodeData {
    /// Create JSON node data.
    pub fn json(value: impl Into<Value>) -> Self {
        Self::Json(value.into())
    }

    /// Get as JSON value if possible.
    pub fn as_json(&self) -> Option<&Value> {
        match self {
            Self::Json(v) => Some(v),
            _ => None,
        }
    }

    /// Convert to JSON value, returning null for non-JSON types.
    pub fn to_json(&self) -> Value {
        match self {
            Self::Json(v) => v.clone(),
            Self::Bytes(b) => Value::String(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                b,
            )),
            Self::Reference(r) => Value::String(format!("ref:{}", r)),
        }
    }

    /// Get a nested field using dot notation.
    pub fn get_field(&self, path: &str) -> Option<Value> {
        let json = self.as_json()?;
        get_nested_field(json, path)
    }
}

impl Default for NodeData {
    fn default() -> Self {
        Self::Json(Value::Null)
    }
}

impl From<Value> for NodeData {
    fn from(v: Value) -> Self {
        Self::Json(v)
    }
}

impl From<String> for NodeData {
    fn from(s: String) -> Self {
        Self::Json(Value::String(s))
    }
}

impl From<&str> for NodeData {
    fn from(s: &str) -> Self {
        Self::Json(Value::String(s.to_string()))
    }
}

/// Get a nested field from a JSON value using dot notation.
fn get_nested_field(value: &Value, path: &str) -> Option<Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;

    for part in parts {
        // Check for array indexing: field[0]
        if let Some(bracket_pos) = part.find('[') {
            let field_name = &part[..bracket_pos];
            let index_str = &part[bracket_pos + 1..part.len() - 1];

            // Get the field first
            if !field_name.is_empty() {
                current = current.get(field_name)?;
            }

            // Then index into array
            let index: usize = index_str.parse().ok()?;
            current = current.get(index)?;
        } else {
            current = current.get(part)?;
        }
    }

    Some(current.clone())
}

/// Workflow execution status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    /// Execution is pending start
    Pending,
    /// Execution is currently running
    Running,
    /// Execution completed successfully
    Completed,
    /// Execution failed
    Failed,
    /// Execution was cancelled
    Cancelled,
}

impl std::fmt::Display for ExecutionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// Node execution status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Node is waiting for dependencies
    Pending,
    /// Node is currently executing
    Running,
    /// Node completed successfully
    Completed,
    /// Node failed
    Failed,
    /// Node was skipped (e.g., conditional branch not taken)
    Skipped,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Skipped => write!(f, "skipped"),
        }
    }
}

/// A workflow execution instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecution {
    /// Unique execution ID
    pub id: Uuid,
    /// Definition ID being executed
    pub definition_id: i64,
    /// Workflow name
    pub name: String,
    /// Current status
    pub status: ExecutionStatus,
    /// Input data provided at start
    pub inputs: HashMap<String, NodeData>,
    /// Output data (populated on completion)
    pub outputs: HashMap<String, NodeData>,
    /// Error message if failed
    pub error: Option<String>,
    /// When execution started
    pub started_at: Option<i64>,
    /// When execution completed
    pub completed_at: Option<i64>,
}

/// A node execution instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeExecution {
    /// Execution ID this belongs to
    pub execution_id: Uuid,
    /// Node ID
    pub node_id: String,
    /// Node type
    pub node_type: String,
    /// Current status
    pub status: NodeStatus,
    /// Input data received
    pub inputs: HashMap<String, NodeData>,
    /// Output data produced
    pub output: Option<NodeData>,
    /// Error message if failed
    pub error: Option<String>,
    /// Reason if skipped
    pub skip_reason: Option<String>,
    /// When execution started
    pub started_at: Option<i64>,
    /// When execution completed
    pub completed_at: Option<i64>,
    /// Duration in milliseconds
    pub duration_ms: Option<i64>,
}

/// Node definition for execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutableNode {
    /// Node ID (unique within workflow)
    pub id: String,
    /// Node type (determines execution behavior)
    pub node_type: String,
    /// Dependencies (node IDs that must complete first)
    pub depends_on: Vec<String>,
    /// Input values (may contain expressions)
    pub inputs: HashMap<String, Value>,
    /// Condition expression (node skipped if evaluates to false)
    pub condition: Option<String>,
    /// Timeout in seconds
    pub timeout_seconds: Option<u32>,
    /// Maximum retry attempts
    pub max_retries: Option<u32>,
}

/// Workflow definition for execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutableWorkflow {
    /// Workflow name
    pub name: String,
    /// Workflow description
    pub description: Option<String>,
    /// Version number
    pub version: i32,
    /// Input schema (defines expected inputs)
    pub input_schema: HashMap<String, SchemaField>,
    /// Nodes to execute
    pub nodes: HashMap<String, ExecutableNode>,
    /// Output mapping (maps output names to node output expressions)
    pub outputs: HashMap<String, String>,
}

/// Schema field definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    /// Field type (string, number, boolean, object, array)
    #[serde(rename = "type")]
    pub field_type: String,
    /// Field description
    pub description: Option<String>,
    /// Default value
    pub default: Option<Value>,
    /// Whether field is required
    #[serde(default)]
    pub required: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_get_nested_field() {
        let data = json!({
            "user": {
                "name": "Alice",
                "addresses": [
                    {"city": "NYC"},
                    {"city": "LA"}
                ]
            }
        });

        assert_eq!(
            get_nested_field(&data, "user.name"),
            Some(json!("Alice"))
        );
        assert_eq!(
            get_nested_field(&data, "user.addresses[0].city"),
            Some(json!("NYC"))
        );
        assert_eq!(
            get_nested_field(&data, "user.addresses[1].city"),
            Some(json!("LA"))
        );
        assert_eq!(get_nested_field(&data, "user.missing"), None);
    }

    #[test]
    fn test_node_data_get_field() {
        let data = NodeData::json(json!({
            "result": {"count": 42}
        }));

        assert_eq!(data.get_field("result.count"), Some(json!(42)));
    }
}
