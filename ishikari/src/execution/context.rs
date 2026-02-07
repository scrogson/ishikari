//! Node execution context.
//!
//! Provides input data and utilities to node execution handlers.

use std::collections::HashMap;
use uuid::Uuid;

use super::types::NodeData;

/// Context passed to node execution handlers.
#[derive(Debug, Clone)]
pub struct NodeContext {
    /// Unique execution ID
    pub execution_id: Uuid,
    /// Node ID being executed
    pub node_id: String,
    /// Node type
    pub node_type: String,
    /// Interpolated inputs for this node
    pub inputs: HashMap<String, serde_json::Value>,
    /// Raw workflow inputs (for reference)
    pub workflow_inputs: HashMap<String, NodeData>,
    /// Outputs from dependency nodes (keyed by node ID)
    pub dependency_outputs: HashMap<String, NodeData>,
}

impl NodeContext {
    /// Create a new node context.
    pub fn new(
        execution_id: Uuid,
        node_id: impl Into<String>,
        node_type: impl Into<String>,
    ) -> Self {
        Self {
            execution_id,
            node_id: node_id.into(),
            node_type: node_type.into(),
            inputs: HashMap::new(),
            workflow_inputs: HashMap::new(),
            dependency_outputs: HashMap::new(),
        }
    }

    /// Get an input value by name.
    pub fn get_input(&self, name: &str) -> Option<&serde_json::Value> {
        self.inputs.get(name)
    }

    /// Get an input as a string.
    pub fn get_input_str(&self, name: &str) -> Option<&str> {
        self.inputs.get(name).and_then(|v| v.as_str())
    }

    /// Get an input as an i64.
    pub fn get_input_i64(&self, name: &str) -> Option<i64> {
        self.inputs.get(name).and_then(|v| v.as_i64())
    }

    /// Get an input as a bool.
    pub fn get_input_bool(&self, name: &str) -> Option<bool> {
        self.inputs.get(name).and_then(|v| v.as_bool())
    }

    /// Get output from a dependency node.
    pub fn get_dependency_output(&self, node_id: &str) -> Option<&NodeData> {
        self.dependency_outputs.get(node_id)
    }

    /// Get a workflow input.
    pub fn get_workflow_input(&self, name: &str) -> Option<&NodeData> {
        self.workflow_inputs.get(name)
    }
}
