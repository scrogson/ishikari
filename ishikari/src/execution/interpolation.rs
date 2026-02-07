//! Expression interpolation for node inputs.
//!
//! Supports template syntax:
//! - `{{inputs.field}}` - Reference workflow input
//! - `{{nodes.node_id.field}}` - Reference node output
//! - `{{nodes.node_id.field.nested.path}}` - Nested field access
//! - `{{nodes.node_id.field[0]}}` - Array indexing

use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::LazyLock;

use super::types::NodeData;

/// Regex for matching expression placeholders: {{...}}
static EXPR_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\{\{([^}]+)\}\}").expect("Invalid regex"));

/// Interpolator for resolving expressions in node inputs.
#[derive(Debug)]
pub struct Interpolator<'a> {
    /// Workflow inputs
    inputs: &'a HashMap<String, NodeData>,
    /// Node outputs (keyed by node ID)
    node_outputs: &'a HashMap<String, NodeData>,
}

impl<'a> Interpolator<'a> {
    /// Create a new interpolator.
    pub fn new(
        inputs: &'a HashMap<String, NodeData>,
        node_outputs: &'a HashMap<String, NodeData>,
    ) -> Self {
        Self {
            inputs,
            node_outputs,
        }
    }

    /// Interpolate all expressions in a value.
    pub fn interpolate(&self, value: &Value) -> Result<Value, InterpolationError> {
        match value {
            Value::String(s) => self.interpolate_string(s),
            Value::Array(arr) => {
                let interpolated: Result<Vec<Value>, _> =
                    arr.iter().map(|v| self.interpolate(v)).collect();
                Ok(Value::Array(interpolated?))
            }
            Value::Object(obj) => {
                let mut result = serde_json::Map::new();
                for (k, v) in obj {
                    result.insert(k.clone(), self.interpolate(v)?);
                }
                Ok(Value::Object(result))
            }
            // Primitives pass through unchanged
            other => Ok(other.clone()),
        }
    }

    /// Interpolate expressions in a string.
    fn interpolate_string(&self, s: &str) -> Result<Value, InterpolationError> {
        // Check if the entire string is a single expression
        if s.starts_with("{{") && s.ends_with("}}") && s.matches("{{").count() == 1 {
            // Return the resolved value directly (preserves type)
            let expr = &s[2..s.len() - 2];
            return self.resolve_expression(expr);
        }

        // Otherwise, do string interpolation
        let mut result = s.to_string();
        let mut replacements = Vec::new();

        for cap in EXPR_REGEX.captures_iter(s) {
            let full_match = cap.get(0).unwrap();
            let expr = cap.get(1).unwrap().as_str();

            let value = self.resolve_expression(expr)?;
            let replacement = value_to_string(&value);

            replacements.push((full_match.start(), full_match.end(), replacement));
        }

        // Apply replacements in reverse order to preserve positions
        for (start, end, replacement) in replacements.into_iter().rev() {
            result.replace_range(start..end, &replacement);
        }

        Ok(Value::String(result))
    }

    /// Resolve a single expression (without {{ }} wrapper).
    fn resolve_expression(&self, expr: &str) -> Result<Value, InterpolationError> {
        let expr = expr.trim();

        // Parse the expression
        if let Some(rest) = expr.strip_prefix("inputs.") {
            // Workflow input reference
            self.resolve_input(rest)
        } else if let Some(rest) = expr.strip_prefix("nodes.") {
            // Node output reference
            self.resolve_node_output(rest)
        } else {
            Err(InterpolationError::InvalidExpression(format!(
                "Unknown expression prefix: {}. Expected 'inputs.' or 'nodes.'",
                expr
            )))
        }
    }

    /// Resolve a workflow input reference.
    fn resolve_input(&self, path: &str) -> Result<Value, InterpolationError> {
        // Split into input name and optional nested path
        let (input_name, nested_path) = split_first_segment(path);

        let input_data = self
            .inputs
            .get(input_name)
            .ok_or_else(|| InterpolationError::MissingInput(input_name.to_string()))?;

        if let Some(nested) = nested_path {
            input_data
                .get_field(nested)
                .ok_or_else(|| InterpolationError::MissingField {
                    location: format!("inputs.{}", input_name),
                    field: nested.to_string(),
                })
        } else {
            Ok(input_data.to_json())
        }
    }

    /// Resolve a node output reference.
    fn resolve_node_output(&self, path: &str) -> Result<Value, InterpolationError> {
        // Split into node ID and field path
        let (node_id, field_path) = split_first_segment(path);

        let node_data = self
            .node_outputs
            .get(node_id)
            .ok_or_else(|| InterpolationError::MissingNodeOutput(node_id.to_string()))?;

        if let Some(field) = field_path {
            node_data
                .get_field(field)
                .ok_or_else(|| InterpolationError::MissingField {
                    location: format!("nodes.{}", node_id),
                    field: field.to_string(),
                })
        } else {
            Ok(node_data.to_json())
        }
    }
}

/// Split a path into the first segment and the rest.
/// e.g., "foo.bar.baz" -> ("foo", Some("bar.baz"))
fn split_first_segment(path: &str) -> (&str, Option<&str>) {
    if let Some(dot_pos) = path.find('.') {
        (&path[..dot_pos], Some(&path[dot_pos + 1..]))
    } else {
        (path, None)
    }
}

/// Convert a JSON value to a string for interpolation.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => "".to_string(),
        other => other.to_string(),
    }
}

/// Check if a string contains any expressions.
pub fn contains_expression(s: &str) -> bool {
    EXPR_REGEX.is_match(s)
}

/// Interpolation errors.
#[derive(Debug, Clone, thiserror::Error)]
pub enum InterpolationError {
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    #[error("Missing workflow input: {0}")]
    MissingInput(String),

    #[error("Missing node output: {0}")]
    MissingNodeOutput(String),

    #[error("Missing field '{field}' in {location}")]
    MissingField { location: String, field: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_inputs() -> HashMap<String, NodeData> {
        let mut inputs = HashMap::new();
        inputs.insert("user_id".to_string(), NodeData::json(json!(42)));
        inputs.insert(
            "config".to_string(),
            NodeData::json(json!({"timeout": 30, "retries": 3})),
        );
        inputs
    }

    fn make_node_outputs() -> HashMap<String, NodeData> {
        let mut outputs = HashMap::new();
        outputs.insert(
            "fetch_user".to_string(),
            NodeData::json(json!({
                "name": "Alice",
                "email": "alice@example.com",
                "roles": ["admin", "user"]
            })),
        );
        outputs.insert(
            "calculate".to_string(),
            NodeData::json(json!({"result": 100})),
        );
        outputs
    }

    #[test]
    fn test_simple_input_reference() {
        let inputs = make_inputs();
        let outputs = HashMap::new();
        let interp = Interpolator::new(&inputs, &outputs);

        let result = interp.interpolate(&json!("{{inputs.user_id}}")).unwrap();
        assert_eq!(result, json!(42));
    }

    #[test]
    fn test_nested_input_reference() {
        let inputs = make_inputs();
        let outputs = HashMap::new();
        let interp = Interpolator::new(&inputs, &outputs);

        let result = interp
            .interpolate(&json!("{{inputs.config.timeout}}"))
            .unwrap();
        assert_eq!(result, json!(30));
    }

    #[test]
    fn test_node_output_reference() {
        let inputs = HashMap::new();
        let outputs = make_node_outputs();
        let interp = Interpolator::new(&inputs, &outputs);

        let result = interp
            .interpolate(&json!("{{nodes.fetch_user.name}}"))
            .unwrap();
        assert_eq!(result, json!("Alice"));
    }

    #[test]
    fn test_array_index_reference() {
        let inputs = HashMap::new();
        let outputs = make_node_outputs();
        let interp = Interpolator::new(&inputs, &outputs);

        let result = interp
            .interpolate(&json!("{{nodes.fetch_user.roles[0]}}"))
            .unwrap();
        assert_eq!(result, json!("admin"));
    }

    #[test]
    fn test_string_interpolation() {
        let inputs = make_inputs();
        let outputs = make_node_outputs();
        let interp = Interpolator::new(&inputs, &outputs);

        let result = interp
            .interpolate(&json!(
                "Hello {{nodes.fetch_user.name}}, your ID is {{inputs.user_id}}"
            ))
            .unwrap();
        assert_eq!(result, json!("Hello Alice, your ID is 42"));
    }

    #[test]
    fn test_object_interpolation() {
        let inputs = make_inputs();
        let outputs = make_node_outputs();
        let interp = Interpolator::new(&inputs, &outputs);

        let result = interp
            .interpolate(&json!({
                "user": "{{nodes.fetch_user.name}}",
                "count": "{{nodes.calculate.result}}"
            }))
            .unwrap();

        assert_eq!(
            result,
            json!({
                "user": "Alice",
                "count": 100
            })
        );
    }

    #[test]
    fn test_missing_input_error() {
        let inputs = HashMap::new();
        let outputs = HashMap::new();
        let interp = Interpolator::new(&inputs, &outputs);

        let result = interp.interpolate(&json!("{{inputs.missing}}"));
        assert!(matches!(result, Err(InterpolationError::MissingInput(_))));
    }

    #[test]
    fn test_missing_node_error() {
        let inputs = HashMap::new();
        let outputs = HashMap::new();
        let interp = Interpolator::new(&inputs, &outputs);

        let result = interp.interpolate(&json!("{{nodes.missing.field}}"));
        assert!(matches!(
            result,
            Err(InterpolationError::MissingNodeOutput(_))
        ));
    }

    #[test]
    fn test_contains_expression() {
        assert!(contains_expression("{{inputs.x}}"));
        assert!(contains_expression("prefix {{nodes.a.b}} suffix"));
        assert!(!contains_expression("no expressions here"));
        assert!(!contains_expression("$not_an_expression"));
        assert!(!contains_expression("{single_brace}"));
    }
}
