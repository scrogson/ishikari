//! Pre-execution workflow validation.
//!
//! Validates workflows before execution to catch errors early:
//! - Structural validation (cycles, missing dependencies)
//! - Input validation (required inputs present)
//! - Expression validation (references exist)

use std::collections::{HashMap, HashSet};

use super::interpolation::contains_expression;
use super::types::{ExecutableNode, ExecutableWorkflow, NodeData};

/// Validation error with context.
#[derive(Debug, Clone)]
pub struct ValidationError {
    /// Error code for programmatic handling
    pub code: &'static str,
    /// Node ID if error is node-specific
    pub node_id: Option<String>,
    /// Field name if error is field-specific
    pub field: Option<String>,
    /// Human-readable error message
    pub message: String,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(node_id) = &self.node_id {
            if let Some(field) = &self.field {
                write!(
                    f,
                    "[{}] Node '{}', field '{}': {}",
                    self.code, node_id, field, self.message
                )
            } else {
                write!(f, "[{}] Node '{}': {}", self.code, node_id, self.message)
            }
        } else {
            write!(f, "[{}] {}", self.code, self.message)
        }
    }
}

impl std::error::Error for ValidationError {}

/// Validation result containing errors and warnings.
#[derive(Debug, Default)]
pub struct ValidationResult {
    /// Blocking errors (prevent execution)
    pub errors: Vec<ValidationError>,
    /// Non-blocking warnings
    pub warnings: Vec<ValidationError>,
}

impl ValidationResult {
    /// Check if validation passed (no errors).
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    /// Add an error.
    pub fn error(&mut self, code: &'static str, message: impl Into<String>) {
        self.errors.push(ValidationError {
            code,
            node_id: None,
            field: None,
            message: message.into(),
        });
    }

    /// Add a node-specific error.
    pub fn node_error(
        &mut self,
        code: &'static str,
        node_id: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.errors.push(ValidationError {
            code,
            node_id: Some(node_id.into()),
            field: None,
            message: message.into(),
        });
    }

    /// Add a field-specific error.
    pub fn field_error(
        &mut self,
        code: &'static str,
        node_id: impl Into<String>,
        field: impl Into<String>,
        message: impl Into<String>,
    ) {
        self.errors.push(ValidationError {
            code,
            node_id: Some(node_id.into()),
            field: Some(field.into()),
            message: message.into(),
        });
    }

    /// Add a warning.
    pub fn warning(&mut self, code: &'static str, message: impl Into<String>) {
        self.warnings.push(ValidationError {
            code,
            node_id: None,
            field: None,
            message: message.into(),
        });
    }

    /// Merge another result into this one.
    pub fn merge(&mut self, other: ValidationResult) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
    }
}

/// Workflow validator.
pub struct Validator<'a> {
    workflow: &'a ExecutableWorkflow,
    inputs: &'a HashMap<String, NodeData>,
}

impl<'a> Validator<'a> {
    /// Create a new validator.
    pub fn new(workflow: &'a ExecutableWorkflow, inputs: &'a HashMap<String, NodeData>) -> Self {
        Self { workflow, inputs }
    }

    /// Validate the workflow.
    pub fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::default();

        // Structural validation
        self.validate_structure(&mut result);

        // Input validation
        self.validate_inputs(&mut result);

        // Node validation
        self.validate_nodes(&mut result);

        // Expression validation
        self.validate_expressions(&mut result);

        // Output validation
        self.validate_outputs(&mut result);

        result
    }

    /// Validate workflow structure.
    fn validate_structure(&self, result: &mut ValidationResult) {
        // E001: Check for empty workflow
        if self.workflow.nodes.is_empty() {
            result.error("E001", "Workflow has no nodes");
            return;
        }

        // E002: Check for duplicate node IDs (handled by HashMap, but check anyway)
        // Already prevented by HashMap keys

        // E003: Check for missing dependencies
        let node_ids: HashSet<&str> = self.workflow.nodes.keys().map(|s| s.as_str()).collect();
        for (node_id, node) in &self.workflow.nodes {
            for dep_id in &node.depends_on {
                if !node_ids.contains(dep_id.as_str()) {
                    result.node_error(
                        "E003",
                        node_id,
                        format!("Dependency '{}' does not exist", dep_id),
                    );
                }
            }
        }

        // E004: Check for cycles using DFS
        if let Err(cycle_nodes) = self.detect_cycles() {
            result.error(
                "E004",
                format!("Circular dependency detected: {}", cycle_nodes.join(" -> ")),
            );
        }

        // E005: Check for unreachable nodes (no path from entry nodes)
        self.check_unreachable_nodes(result);
    }

    /// Detect cycles in the dependency graph.
    fn detect_cycles(&self) -> Result<(), Vec<String>> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut path = Vec::new();

        for node_id in self.workflow.nodes.keys() {
            if !visited.contains(node_id) {
                self.dfs_cycle_check(node_id, &mut visited, &mut rec_stack, &mut path)?
            }
        }

        Ok(())
    }

    fn dfs_cycle_check(
        &self,
        node_id: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Result<(), Vec<String>> {
        visited.insert(node_id.to_string());
        rec_stack.insert(node_id.to_string());
        path.push(node_id.to_string());

        if let Some(node) = self.workflow.nodes.get(node_id) {
            for dep_id in &node.depends_on {
                if !visited.contains(dep_id) {
                    self.dfs_cycle_check(dep_id, visited, rec_stack, path)?;
                } else if rec_stack.contains(dep_id) {
                    // Found cycle - return the path
                    let cycle_start = path.iter().position(|x| x == dep_id).unwrap();
                    let mut cycle = path[cycle_start..].to_vec();
                    cycle.push(dep_id.clone());
                    return Err(cycle);
                }
            }
        }

        path.pop();
        rec_stack.remove(node_id);
        Ok(())
    }

    /// Check for unreachable nodes.
    fn check_unreachable_nodes(&self, result: &mut ValidationResult) {
        // Find entry nodes (nodes with no dependencies)
        let entry_nodes: Vec<&str> = self
            .workflow
            .nodes
            .iter()
            .filter(|(_, node)| node.depends_on.is_empty())
            .map(|(id, _)| id.as_str())
            .collect();

        if entry_nodes.is_empty() && !self.workflow.nodes.is_empty() {
            result.error("E005", "No entry nodes found (all nodes have dependencies)");
            return;
        }

        // BFS from entry nodes to find reachable nodes
        let mut reachable = HashSet::new();
        let mut queue: Vec<&str> = entry_nodes;

        // Build reverse dependency map (what nodes depend on this node)
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
        for (node_id, node) in &self.workflow.nodes {
            for dep_id in &node.depends_on {
                dependents.entry(dep_id.as_str()).or_default().push(node_id);
            }
        }

        while let Some(node_id) = queue.pop() {
            if reachable.insert(node_id) {
                if let Some(deps) = dependents.get(node_id) {
                    queue.extend(deps.iter().copied());
                }
            }
        }

        // Check for unreachable nodes
        for node_id in self.workflow.nodes.keys() {
            if !reachable.contains(node_id.as_str()) {
                result.node_error("E006", node_id, "Node is unreachable from entry nodes");
            }
        }
    }

    /// Validate workflow inputs against schema.
    fn validate_inputs(&self, result: &mut ValidationResult) {
        // E010: Check required inputs are provided
        for (input_name, schema) in &self.workflow.input_schema {
            if schema.required && !self.inputs.contains_key(input_name) && schema.default.is_none()
            {
                result.error(
                    "E010",
                    format!("Required input '{}' not provided", input_name),
                );
            }
        }

        // E011: Warn about unexpected inputs
        for input_name in self.inputs.keys() {
            if !self.workflow.input_schema.contains_key(input_name) {
                result.warning(
                    "W011",
                    format!("Unexpected input '{}' provided (not in schema)", input_name),
                );
            }
        }
    }

    /// Validate individual nodes.
    fn validate_nodes(&self, result: &mut ValidationResult) {
        for (node_id, node) in &self.workflow.nodes {
            // E020: Node type required
            if node.node_type.is_empty() {
                result.node_error("E020", node_id, "Node type is required");
            }

            // E021: Self-dependency
            if node.depends_on.contains(&node.id) {
                result.node_error("E021", node_id, "Node cannot depend on itself");
            }

            // E022: Duplicate dependencies
            let mut seen_deps = HashSet::new();
            for dep_id in &node.depends_on {
                if !seen_deps.insert(dep_id) {
                    result.node_error("E022", node_id, format!("Duplicate dependency: {}", dep_id));
                }
            }
        }
    }

    /// Validate expressions in node inputs.
    fn validate_expressions(&self, result: &mut ValidationResult) {
        let node_ids: HashSet<&str> = self.workflow.nodes.keys().map(|s| s.as_str()).collect();
        let input_names: HashSet<&str> = self
            .workflow
            .input_schema
            .keys()
            .map(|s| s.as_str())
            .collect();

        for (node_id, node) in &self.workflow.nodes {
            // Check expressions in inputs
            for (field_name, value) in &node.inputs {
                self.validate_expression_in_value(
                    result,
                    node_id,
                    field_name,
                    value,
                    &node_ids,
                    &input_names,
                    node,
                );
            }

            // Check condition expression
            if let Some(condition) = &node.condition {
                if contains_expression(condition) {
                    self.validate_expression_references(
                        result,
                        node_id,
                        "condition",
                        condition,
                        &node_ids,
                        &input_names,
                        node,
                    );
                }
            }
        }
    }

    fn validate_expression_in_value(
        &self,
        result: &mut ValidationResult,
        node_id: &str,
        field_name: &str,
        value: &serde_json::Value,
        node_ids: &HashSet<&str>,
        input_names: &HashSet<&str>,
        node: &ExecutableNode,
    ) {
        match value {
            serde_json::Value::String(s) => {
                if contains_expression(s) {
                    self.validate_expression_references(
                        result,
                        node_id,
                        field_name,
                        s,
                        node_ids,
                        input_names,
                        node,
                    );
                }
            }
            serde_json::Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    let indexed_field = format!("{}[{}]", field_name, i);
                    self.validate_expression_in_value(
                        result,
                        node_id,
                        &indexed_field,
                        v,
                        node_ids,
                        input_names,
                        node,
                    );
                }
            }
            serde_json::Value::Object(obj) => {
                for (k, v) in obj {
                    let nested_field = format!("{}.{}", field_name, k);
                    self.validate_expression_in_value(
                        result,
                        node_id,
                        &nested_field,
                        v,
                        node_ids,
                        input_names,
                        node,
                    );
                }
            }
            _ => {}
        }
    }

    fn validate_expression_references(
        &self,
        result: &mut ValidationResult,
        node_id: &str,
        field_name: &str,
        expr: &str,
        node_ids: &HashSet<&str>,
        input_names: &HashSet<&str>,
        node: &ExecutableNode,
    ) {
        // Extract all {{...}} expressions
        let re = regex::Regex::new(r"\{\{([^}]+)\}\}").unwrap();

        for cap in re.captures_iter(expr) {
            let inner = cap.get(1).unwrap().as_str().trim();

            if let Some(input_ref) = inner.strip_prefix("inputs.") {
                // Validate input reference
                let input_name = input_ref.split('.').next().unwrap_or(input_ref);
                if !input_names.contains(input_name) {
                    result.field_error(
                        "E030",
                        node_id,
                        field_name,
                        format!("References unknown input: {}", input_name),
                    );
                }
            } else if let Some(node_ref) = inner.strip_prefix("nodes.") {
                // Validate node reference
                let ref_node_id = node_ref.split('.').next().unwrap_or(node_ref);

                if !node_ids.contains(ref_node_id) {
                    result.field_error(
                        "E031",
                        node_id,
                        field_name,
                        format!("References unknown node: {}", ref_node_id),
                    );
                } else if ref_node_id == node_id {
                    result.field_error(
                        "E032",
                        node_id,
                        field_name,
                        "Node cannot reference its own output",
                    );
                } else if !node.depends_on.iter().any(|d| d == ref_node_id) {
                    // Check that referenced node is a dependency
                    result.field_error(
                        "E033",
                        node_id,
                        field_name,
                        format!(
                            "References node '{}' which is not a dependency. Add it to depends_on.",
                            ref_node_id
                        ),
                    );
                }
            } else {
                result.field_error(
                    "E034",
                    node_id,
                    field_name,
                    format!(
                        "Invalid expression '{}'. Must start with 'inputs.' or 'nodes.'",
                        inner
                    ),
                );
            }
        }
    }

    /// Validate workflow outputs.
    fn validate_outputs(&self, result: &mut ValidationResult) {
        let node_ids: HashSet<&str> = self.workflow.nodes.keys().map(|s| s.as_str()).collect();

        for (output_name, expr) in &self.workflow.outputs {
            if !contains_expression(expr) {
                result.error(
                    "E040",
                    format!(
                        "Output '{}' must be an expression (e.g., {{{{nodes.x.result}}}})",
                        output_name
                    ),
                );
                continue;
            }

            // Validate the expression references
            let re = regex::Regex::new(r"\{\{nodes\.([^.}]+)").unwrap();
            for cap in re.captures_iter(expr) {
                let ref_node_id = cap.get(1).unwrap().as_str();
                if !node_ids.contains(ref_node_id) {
                    result.error(
                        "E041",
                        format!(
                            "Output '{}' references unknown node: {}",
                            output_name, ref_node_id
                        ),
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::types::SchemaField;
    use serde_json::json;

    fn make_workflow(nodes: Vec<ExecutableNode>) -> ExecutableWorkflow {
        let mut node_map = HashMap::new();
        for node in nodes {
            node_map.insert(node.id.clone(), node);
        }

        ExecutableWorkflow {
            name: "test".to_string(),
            description: None,
            version: 1,
            input_schema: HashMap::new(),
            nodes: node_map,
            outputs: HashMap::new(),
        }
    }

    fn make_node(id: &str, deps: &[&str]) -> ExecutableNode {
        ExecutableNode {
            id: id.to_string(),
            node_type: "test".to_string(),
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            inputs: HashMap::new(),
            condition: None,
            timeout_seconds: None,
            max_retries: None,
        }
    }

    #[test]
    fn test_valid_linear_workflow() {
        let workflow = make_workflow(vec![
            make_node("a", &[]),
            make_node("b", &["a"]),
            make_node("c", &["b"]),
        ]);

        let inputs = HashMap::new();
        let validator = Validator::new(&workflow, &inputs);
        let result = validator.validate();

        assert!(result.is_valid(), "Errors: {:?}", result.errors);
    }

    #[test]
    fn test_valid_dag_workflow() {
        let workflow = make_workflow(vec![
            make_node("a", &[]),
            make_node("b", &[]),
            make_node("c", &["a", "b"]),
            make_node("d", &["c"]),
        ]);

        let inputs = HashMap::new();
        let validator = Validator::new(&workflow, &inputs);
        let result = validator.validate();

        assert!(result.is_valid(), "Errors: {:?}", result.errors);
    }

    #[test]
    fn test_cycle_detection() {
        let workflow = make_workflow(vec![
            make_node("a", &["c"]),
            make_node("b", &["a"]),
            make_node("c", &["b"]),
        ]);

        let inputs = HashMap::new();
        let validator = Validator::new(&workflow, &inputs);
        let result = validator.validate();

        assert!(!result.is_valid());
        assert!(result.errors.iter().any(|e| e.code == "E004"));
    }

    #[test]
    fn test_missing_dependency() {
        let workflow = make_workflow(vec![make_node("a", &[]), make_node("b", &["missing"])]);

        let inputs = HashMap::new();
        let validator = Validator::new(&workflow, &inputs);
        let result = validator.validate();

        assert!(!result.is_valid());
        assert!(result.errors.iter().any(|e| e.code == "E003"));
    }

    #[test]
    fn test_required_input_missing() {
        let mut workflow = make_workflow(vec![make_node("a", &[])]);
        workflow.input_schema.insert(
            "required_input".to_string(),
            SchemaField {
                field_type: "string".to_string(),
                description: None,
                default: None,
                required: true,
            },
        );

        let inputs = HashMap::new();
        let validator = Validator::new(&workflow, &inputs);
        let result = validator.validate();

        assert!(!result.is_valid());
        assert!(result.errors.iter().any(|e| e.code == "E010"));
    }

    #[test]
    fn test_expression_references_non_dependency() {
        let mut node = make_node("b", &[]);
        node.inputs
            .insert("value".to_string(), json!("{{nodes.a.result}}"));

        let workflow = make_workflow(vec![make_node("a", &[]), node]);

        let inputs = HashMap::new();
        let validator = Validator::new(&workflow, &inputs);
        let result = validator.validate();

        assert!(!result.is_valid());
        assert!(result.errors.iter().any(|e| e.code == "E033"));
    }
}
