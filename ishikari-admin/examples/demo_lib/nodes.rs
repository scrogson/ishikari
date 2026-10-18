//! The demo's node types, for the admin's definition runs: what the builder's palette offers,
//! and pretend implementations of each (sleeps and canned output, no real HTTP, mail or SQL).

use async_trait::async_trait;
use ishikari::execution::{
    is_truthy, NodeContext, NodeData, NodeError, NodeHandler, NodeSpec, NodeTypes, SchemaField,
};
use serde_json::{json, Map, Value};
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::sleep;

/// The demo's node types, each run by [`Demo`].
pub fn node_types(pool: PgPool) -> NodeTypes {
    specs()
        .into_iter()
        .fold(NodeTypes::new(pool), |types, spec| {
            types.register(spec, Demo)
        })
}

fn specs() -> Vec<NodeSpec> {
    let text = || {
        SchemaField::string()
            .required()
            .describe("Text to transform")
    };
    let result = || SchemaField::string().describe("Transformed text");
    let status = || SchemaField::integer().describe("HTTP status code");
    let response = || SchemaField::object().describe("Response body");
    let headers = || SchemaField::object().describe("Request headers");
    vec![
        NodeSpec::new("echo")
            .category("core")
            .description("Echo a message back")
            .input(
                "message",
                SchemaField::string().required().describe("Message to echo"),
            )
            .output("message", SchemaField::string().describe("Echoed message")),
        NodeSpec::new("uppercase")
            .category("transform")
            .description("Convert text to uppercase")
            .input("text", text())
            .output("result", result()),
        NodeSpec::new("lowercase")
            .category("transform")
            .description("Convert text to lowercase")
            .input("text", text())
            .output("result", result()),
        NodeSpec::new("http/get")
            .category("http")
            .description("Make an HTTP GET request")
            .input(
                "url",
                SchemaField::string().required().describe("URL to fetch"),
            )
            .input("headers", headers())
            .output("status", status())
            .output("body", response()),
        NodeSpec::new("http/post")
            .category("http")
            .description("Make an HTTP POST request")
            .input(
                "url",
                SchemaField::string().required().describe("URL to post to"),
            )
            .input("body", SchemaField::object().describe("Request body"))
            .input("headers", headers())
            .output("status", status())
            .output("body", response()),
        NodeSpec::new("json/validate")
            .category("transform")
            .description("Validate JSON against a schema")
            .input(
                "data",
                SchemaField::object()
                    .required()
                    .describe("JSON data to validate"),
            )
            .input(
                "schema",
                SchemaField::object().required().describe("JSON Schema"),
            )
            .output(
                "valid",
                SchemaField::boolean().describe("Whether data is valid"),
            )
            .output("data", SchemaField::object().describe("The validated data")),
        NodeSpec::new("json/transform")
            .category("transform")
            .description("Transform JSON data using a mapping")
            .input(
                "data",
                SchemaField::object().required().describe("Input JSON data"),
            )
            .input(
                "mapping",
                SchemaField::object()
                    .required()
                    .describe("Field mapping configuration"),
            )
            .output(
                "records",
                SchemaField::array().describe("Transformed records"),
            ),
        NodeSpec::new("db/query")
            .category("database")
            .description("Execute a database query")
            .input(
                "query",
                SchemaField::string().required().describe("SQL query"),
            )
            .input("params", SchemaField::array().describe("Query parameters"))
            .output("rows", SchemaField::array().describe("Query results")),
        NodeSpec::new("db/insert")
            .category("database")
            .description("Insert records into a table")
            .input(
                "table",
                SchemaField::string().required().describe("Table name"),
            )
            .input(
                "records",
                SchemaField::array()
                    .required()
                    .describe("Records to insert"),
            )
            .output(
                "rows_affected",
                SchemaField::integer().describe("Number of rows inserted"),
            ),
        NodeSpec::new("email/send")
            .category("notification")
            .description("Send an email")
            .input(
                "to",
                SchemaField::string().required().describe("Recipient email"),
            )
            .input(
                "subject",
                SchemaField::string().required().describe("Email subject"),
            )
            .input(
                "body",
                SchemaField::string().required().describe("Email body"),
            )
            .output(
                "sent",
                SchemaField::boolean().describe("Whether email was sent"),
            ),
        NodeSpec::new("sms/send")
            .category("notification")
            .description("Send an SMS message")
            .input(
                "phone",
                SchemaField::string().required().describe("Phone number"),
            )
            .input(
                "message",
                SchemaField::string().required().describe("Message text"),
            )
            .output(
                "sent",
                SchemaField::boolean().describe("Whether SMS was sent"),
            ),
        NodeSpec::new("delay")
            .category("control")
            .description("Wait for a duration")
            .input(
                "seconds",
                SchemaField::integer()
                    .required()
                    .describe("Seconds to wait"),
            ),
        NodeSpec::new("condition")
            .category("control")
            .description("Conditional branching")
            .input(
                "expression",
                SchemaField::string()
                    .required()
                    .describe("Condition expression"),
            )
            .input("value", SchemaField::object().describe("Value to compare"))
            .output(
                "result",
                SchemaField::boolean().describe("Condition result"),
            ),
    ]
}

/// Runs every demo node type, by the node's type.
struct Demo;

#[async_trait]
impl NodeHandler for Demo {
    async fn execute(&self, ctx: &NodeContext, _pool: &PgPool) -> Result<NodeData, NodeError> {
        let input = |name: &str| ctx.get_input(name).cloned().unwrap_or(Value::Null);
        let text = || ctx.get_input_str("text").unwrap_or_default();
        let output = match ctx.node_type.as_str() {
            "echo" => {
                sleep(Duration::from_millis(100)).await;
                json!({ "message": ctx.get_input_str("message").unwrap_or("(no message)") })
            }
            "uppercase" => {
                sleep(Duration::from_millis(100)).await;
                json!({ "result": text().to_uppercase() })
            }
            "lowercase" => {
                sleep(Duration::from_millis(100)).await;
                json!({ "result": text().to_lowercase() })
            }
            "http/get" => {
                sleep(Duration::from_secs(1)).await;
                json!({ "status": 200, "body": { "url": input("url"), "data": "sample" } })
            }
            "http/post" => {
                sleep(Duration::from_secs(1)).await;
                json!({ "status": 201, "body": { "url": input("url"), "received": input("body") } })
            }
            "json/validate" => {
                sleep(Duration::from_millis(200)).await;
                let data = input("data");
                json!({ "valid": data.is_object() || data.is_array(), "data": data })
            }
            "json/transform" => {
                sleep(Duration::from_millis(200)).await;
                json!({ "records": transform(input("data"), &input("mapping")) })
            }
            "db/query" => {
                // Pretend the query found the user's notification preferences.
                sleep(Duration::from_millis(300)).await;
                json!({ "rows": [{ "email_enabled": true, "sms_enabled": false }] })
            }
            "db/insert" => {
                sleep(Duration::from_millis(300)).await;
                let rows = input("records").as_array().map_or(1, Vec::len);
                json!({ "rows_affected": rows })
            }
            "email/send" => {
                sleep(Duration::from_secs(1)).await;
                tracing::info!(to = %input("to"), "Simulated email sent");
                json!({ "sent": true })
            }
            "sms/send" => {
                sleep(Duration::from_millis(500)).await;
                tracing::info!(phone = %input("phone"), "Simulated SMS sent");
                json!({ "sent": true })
            }
            "delay" => {
                let seconds = ctx.get_input_i64("seconds").unwrap_or(0).clamp(0, 60);
                sleep(Duration::from_secs(seconds as u64)).await;
                json!({})
            }
            "condition" => {
                // The expression's value (its references resolved), compared to `value` when
                // there is one, else whether it's truthy.
                let expression = input("expression");
                let result = match ctx.get_input("value") {
                    Some(value) if !value.is_null() => &expression == value,
                    _ => is_truthy(&expression),
                };
                json!({ "result": result })
            }
            other => {
                return Err(NodeError::ExecutionFailed(format!(
                    "the demo has no node type {other}"
                )))
            }
        };
        Ok(NodeData::json(output))
    }
}

/// `data`'s records (an array's items, or the object itself), each renamed by `mapping`
/// (`{"to": "from"}`) when it maps anything.
fn transform(data: Value, mapping: &Value) -> Vec<Value> {
    let records = match data {
        Value::Array(items) => items,
        Value::Null => Vec::new(),
        other => vec![other],
    };
    let Some(mapping) = mapping.as_object().filter(|m| !m.is_empty()) else {
        return records;
    };
    records
        .into_iter()
        .map(|record| {
            let renamed: Map<String, Value> = mapping
                .iter()
                .map(|(to, from)| {
                    let value = from
                        .as_str()
                        .and_then(|from| record.get(from))
                        .cloned()
                        .unwrap_or(Value::Null);
                    (to.clone(), value)
                })
                .collect();
            Value::Object(renamed)
        })
        .collect()
}
