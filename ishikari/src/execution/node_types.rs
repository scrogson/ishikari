//! A host's node types: how the workflow builder shows each ([`NodeSpec`]).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::types::SchemaField;

/// A node type as the workflow builder offers it: its name (a node's `type`), the category its
/// palette files it under, what it does, and the inputs it takes and outputs it gives.
///
/// ```
/// use ishikari::execution::{NodeSpec, SchemaField};
///
/// let spec = NodeSpec::new("slack/post")
///     .category("chat")
///     .description("Post a message to a channel")
///     .input("channel", SchemaField::string().required())
///     .input("text", SchemaField::string().required())
///     .output("ts", SchemaField::string().describe("The message's timestamp"));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeSpec {
    /// The type's name, e.g. `slack/post`.
    pub name: String,
    /// What it does.
    pub description: Option<String>,
    /// The palette's group for it, e.g. `http`.
    pub category: String,
    /// The inputs it takes, by name.
    #[serde(default)]
    pub input_schema: BTreeMap<String, SchemaField>,
    /// The outputs it gives, by name.
    #[serde(default)]
    pub output_schema: BTreeMap<String, SchemaField>,
}

impl NodeSpec {
    /// A node type named `name`, in the `other` category, taking and giving nothing.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            category: "other".to_string(),
            input_schema: BTreeMap::new(),
            output_schema: BTreeMap::new(),
        }
    }

    /// File it under `category` in the palette.
    pub fn category(mut self, category: impl Into<String>) -> Self {
        self.category = category.into();
        self
    }

    /// Say what it does.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// It takes an input `name`.
    pub fn input(mut self, name: impl Into<String>, field: SchemaField) -> Self {
        self.input_schema.insert(name.into(), field);
        self
    }

    /// It gives an output `name`.
    pub fn output(mut self, name: impl Into<String>, field: SchemaField) -> Self {
        self.output_schema.insert(name.into(), field);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_spec_is_what_the_builder_reads() {
        let spec = NodeSpec::new("http/get")
            .category("http")
            .description("Make an HTTP GET request")
            .input(
                "url",
                SchemaField::string().required().describe("URL to fetch"),
            )
            .input("headers", SchemaField::object())
            .output("status", SchemaField::integer().default(200));
        assert_eq!(
            serde_json::to_value(&spec).unwrap(),
            json!({
                "name": "http/get",
                "description": "Make an HTTP GET request",
                "category": "http",
                "input_schema": {
                    "headers": { "type": "object", "description": null, "default": null, "required": false },
                    "url": { "type": "string", "description": "URL to fetch", "default": null, "required": true }
                },
                "output_schema": {
                    "status": { "type": "integer", "description": null, "default": 200, "required": false }
                }
            })
        );
    }
}
