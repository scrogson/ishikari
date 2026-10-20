//! A host's node types: how the workflow builder shows each ([`NodeSpec`]) and what runs it
//! ([`NodeHandler`]), registered together in [`NodeTypes`].

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use super::context::NodeContext;
use super::executor::{skip_reason, NodeHandler};
use super::interpolation::{InterpolationError, Interpolator};
use super::module_job::ModuleJob;
use super::node_job::{NodeJob, NodeRunner};
use super::node_modules::{ModuleError, NodeModules};
use super::storage::{ExecutionStorage, NodeRecord, RunDefinition};
use super::types::{NodeData, NodeStatus, SchemaField};
use crate::{Cancel, Complete, Context, PerformResult};

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

/// The host's node types, each a [`NodeSpec`] and the [`NodeHandler`] that runs it.
///
/// Its [`catalog`](Self::catalog) is what the admin's builder offers
/// (`ishikari_admin::AppState::with_node_types`). As the [`NodeRunner`] of a
/// [`NodeJobs`](super::NodeJobs), it runs the nodes of the admin's runs: for each node it
/// - resolves the node's inputs against the run's inputs (a declared input left out takes its
///   default, else null) and the outputs of the nodes completed before it;
/// - skips the node when its `when` is falsy ([`is_truthy`](super::is_truthy)) or doesn't
///   resolve, and lets the nodes after it run;
/// - runs the handler of its type, and records the execution (`ishikari_node_executions`);
/// - completes the run with the definition's outputs once every node has completed or been
///   skipped, and fails it when a node fails its last attempt.
///
/// A node of a type not registered is looked up in the host's [`modules`](Self::modules), at
/// the version uploaded last before its run was created. One of neither fails, and fails its
/// run, without retrying. A [`ModuleJob`] runs by the modules too.
///
/// ```rust,no_run
/// use ishikari::execution::{
///     NodeContext, NodeData, NodeError, NodeHandler, NodeJob, NodeJobs, NodeSpec, NodeTypes,
///     SchemaField,
/// };
/// use ishikari::{Engine, Postgres, Queue};
/// use std::sync::Arc;
///
/// struct Shout;
///
/// #[async_trait::async_trait]
/// impl NodeHandler for Shout {
///     async fn execute(
///         &self,
///         ctx: &NodeContext,
///         _pool: &sqlx::PgPool,
///     ) -> Result<NodeData, NodeError> {
///         let text = ctx.get_input_str("text").unwrap_or_default();
///         Ok(NodeData::json(serde_json::json!({ "result": text.to_uppercase() })))
///     }
/// }
///
/// # fn f(pool: sqlx::PgPool) {
/// let types = NodeTypes::new(pool.clone()).register(
///     NodeSpec::new("shout")
///         .category("transform")
///         .input("text", SchemaField::string().required())
///         .output("result", SchemaField::string()),
///     Shout,
/// );
/// let catalog = types.catalog(); // for the admin's `AppState::with_node_types`
/// let engine = Engine::builder("definitions")
///     .with_state(Arc::new(NodeJobs::new(types)))
///     .with_queue(Queue::builder(NodeJob::QUEUE))
///     .start(Postgres::new(pool));
/// # }
/// ```
#[derive(Clone)]
pub struct NodeTypes {
    pool: PgPool,
    schema: Option<String>,
    types: Vec<(NodeSpec, Arc<dyn NodeHandler>)>,
    modules: Option<Arc<dyn NodeModules>>,
}

impl NodeTypes {
    /// No node types yet, keeping runs in `pool`'s search path.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: None,
            types: Vec::new(),
            modules: None,
        }
    }

    /// Keep runs in `schema`, as the engine's [`Postgres`](crate::Postgres) keeps jobs.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Register a node type, `handler` running it. One of the same name is replaced.
    pub fn register(mut self, spec: NodeSpec, handler: impl NodeHandler + 'static) -> Self {
        let handler: Arc<dyn NodeHandler> = Arc::new(handler);
        match self.types.iter_mut().find(|(s, _)| s.name == spec.name) {
            Some(entry) => *entry = (spec, handler),
            None => self.types.push((spec, handler)),
        }
        self
    }

    /// Look up node types not registered here in `modules`: those the host's operators upload
    /// through the admin (`ishikari-wasm`'s `WasmModules`). A type registered here wins over a
    /// module of the same name.
    pub fn modules(mut self, modules: Arc<dyn NodeModules>) -> Self {
        self.modules = Some(modules);
        self
    }

    /// The node types registered here, in the order they were registered.
    pub fn catalog(&self) -> Vec<NodeSpec> {
        self.types.iter().map(|(spec, _)| spec.clone()).collect()
    }

    /// The handler of the type `name`: the one registered here, else its module's version for a
    /// run created at `as_of`.
    async fn handler(
        &self,
        name: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<Arc<dyn NodeHandler>>, ModuleError> {
        if let Some((_, handler)) = self.types.iter().find(|(spec, _)| spec.name == name) {
            return Ok(Some(handler.clone()));
        }
        match &self.modules {
            Some(modules) => modules.handler(name, as_of).await,
            None => Ok(None),
        }
    }

    fn storage(&self) -> ExecutionStorage {
        let storage = ExecutionStorage::new(self.pool.clone());
        match &self.schema {
            Some(schema) => storage.schema(schema.clone()),
            None => storage,
        }
    }
}

#[async_trait::async_trait]
impl NodeRunner for NodeTypes {
    async fn run(&self, job: &NodeJob, ctx: Context) -> PerformResult {
        let attempt = ctx.job();
        let node = Node {
            job,
            storage: self.storage(),
            retry_count: (attempt.attempt - 1).max(0),
            last_attempt: attempt.attempt >= attempt.max_attempts,
        };

        let run = node
            .storage
            .run_definition(job.workflow_run_id)
            .await?
            .ok_or_else(|| format!("no workflow run {}", job.workflow_run_id))?;
        let handler = match self.handler(&job.node_type, run.created_at).await {
            Ok(Some(handler)) => handler,
            Ok(None) => {
                let error = format!("Unknown node type '{}'", job.node_type);
                node.failed(&error, None, None, true).await?;
                return Cancel::default().message(error).into();
            }
            Err(e) => {
                let error = e.to_string();
                node.failed(&error, None, None, node.last_attempt).await?;
                return Err(error.into());
            }
        };
        let inputs = run_inputs(&run);
        let outputs = node
            .storage
            .get_all_node_outputs(job.workflow_run_id)
            .await?;
        let interpolator = Interpolator::new(&inputs, &outputs);

        if let Some(reason) = job
            .condition
            .as_deref()
            .and_then(|when| skip_reason(&interpolator, when))
        {
            node.skipped(&reason).await?;
            node.complete_run_if_finished(&run, &inputs).await;
            return Complete::default().message(reason).into();
        }

        let resolved = match resolve(&interpolator, &job.raw_inputs) {
            Ok(resolved) => resolved,
            Err(e) => {
                let error = format!("Failed to resolve inputs: {e}");
                node.failed(&error, None, None, node.last_attempt).await?;
                return Err(error.into());
            }
        };
        let mut context = NodeContext::new(
            run.execution_id.unwrap_or_else(Uuid::nil),
            &job.node_id,
            &job.node_type,
        );
        context.inputs = resolved;
        context.workflow_inputs = inputs.clone();
        context.dependency_outputs = outputs.clone();

        let started = Instant::now();
        let result = handler.execute(&context, &self.pool).await;
        let duration_ms = started.elapsed().as_millis() as i64;
        let resolved = serde_json::to_value(&context.inputs)?;

        match result {
            Ok(output) => {
                node.completed(&resolved, &output.to_json(), duration_ms)
                    .await?;
                node.complete_run_if_finished(&run, &inputs).await;
                Complete::default().into()
            }
            Err(e) => {
                let error = e.to_string();
                node.failed(
                    &error,
                    Some(&resolved),
                    Some(duration_ms),
                    node.last_attempt,
                )
                .await?;
                Err(Box::new(e))
            }
        }
    }

    async fn run_module(&self, job: &ModuleJob, ctx: Context) -> PerformResult {
        let Some(modules) = &self.modules else {
            let why = format!("this engine runs no modules, so not {}", job.module);
            return Cancel::default().message(why).into();
        };
        let attempt = ctx.job();
        let Some(handler) = modules.handler(&job.module, attempt.inserted_at).await? else {
            let why = format!(
                "no module {} was uploaded before the job was inserted",
                job.module
            );
            return Cancel::default().message(why).into();
        };
        let Value::Object(args) = &job.args else {
            return Cancel::default()
                .message("its args aren't a JSON object")
                .into();
        };
        let mut context = NodeContext::new(Uuid::nil(), format!("job:{}", attempt.id), &job.module);
        context.inputs = args.clone().into_iter().collect();
        let output = handler.execute(&context, &self.pool).await?;
        ModuleJob::outcome(output.to_json())
    }
}

/// A node of a run, being run.
struct Node<'a> {
    job: &'a NodeJob,
    storage: ExecutionStorage,
    retry_count: i32,
    last_attempt: bool,
}

impl Node<'_> {
    fn record(&self, status: NodeStatus) -> NodeRecord<'_> {
        NodeRecord {
            workflow_run_id: self.job.workflow_run_id,
            node_id: &self.job.node_id,
            node_type: &self.job.node_type,
            status,
            inputs: None,
            output: None,
            error: None,
            skip_reason: None,
            retry_count: self.retry_count,
            duration_ms: None,
        }
    }

    async fn completed(
        &self,
        inputs: &Value,
        output: &Value,
        duration_ms: i64,
    ) -> Result<(), sqlx::Error> {
        self.storage
            .record_node_execution(&NodeRecord {
                inputs: Some(inputs),
                output: Some(output),
                duration_ms: Some(duration_ms),
                ..self.record(NodeStatus::Completed)
            })
            .await
    }

    async fn skipped(&self, reason: &str) -> Result<(), sqlx::Error> {
        self.storage
            .record_node_execution(&NodeRecord {
                skip_reason: Some(reason),
                ..self.record(NodeStatus::Skipped)
            })
            .await
    }

    /// Record the node failed, and when it won't run again, fail its run.
    async fn failed(
        &self,
        error: &str,
        inputs: Option<&Value>,
        duration_ms: Option<i64>,
        fail_run: bool,
    ) -> Result<(), sqlx::Error> {
        self.storage
            .record_node_execution(&NodeRecord {
                inputs,
                error: Some(error),
                duration_ms,
                ..self.record(NodeStatus::Failed)
            })
            .await?;
        if fail_run {
            let error = format!("Node '{}' failed: {}", self.job.node_id, error);
            self.storage
                .fail_workflow_run(self.job.workflow_run_id, &error)
                .await?;
        }
        Ok(())
    }

    /// Complete the run with the definition's outputs when every node has completed or been
    /// skipped. The node has finished either way, so a failure here is only logged.
    async fn complete_run_if_finished(
        &self,
        run: &RunDefinition,
        inputs: &HashMap<String, NodeData>,
    ) {
        if let Err(e) = self.try_complete_run(run, inputs).await {
            tracing::warn!(
                workflow_run_id = self.job.workflow_run_id,
                error = %e,
                "Failed to complete the workflow run"
            );
        }
    }

    async fn try_complete_run(
        &self,
        run: &RunDefinition,
        inputs: &HashMap<String, NodeData>,
    ) -> Result<(), sqlx::Error> {
        let run_id = self.job.workflow_run_id;
        let node_ids: Vec<String> = run
            .nodes
            .as_object()
            .map(|nodes| nodes.keys().cloned().collect())
            .unwrap_or_default();
        if self.storage.count_finished_nodes(run_id, &node_ids).await? < node_ids.len() as i64 {
            return Ok(());
        }

        let outputs = self.storage.get_all_node_outputs(run_id).await?;
        let interpolator = Interpolator::new(inputs, &outputs);
        let mut result = serde_json::Map::new();
        for (name, expr) in run.output_schema.as_object().into_iter().flatten() {
            match interpolator.interpolate(expr) {
                Ok(value) => {
                    result.insert(name.clone(), value);
                }
                Err(e) => tracing::warn!("Failed to compute output '{}': {}", name, e),
            }
        }
        self.storage
            .complete_workflow_run(run_id, &Value::Object(result))
            .await
    }
}

/// The run's inputs, with each input the definition declares but the run left out taking its
/// default, else null.
fn run_inputs(run: &RunDefinition) -> HashMap<String, NodeData> {
    let mut inputs: HashMap<String, NodeData> = run
        .inputs
        .as_object()
        .into_iter()
        .flatten()
        .map(|(name, value)| (name.clone(), NodeData::Json(value.clone())))
        .collect();
    for (name, field) in run.input_schema.as_object().into_iter().flatten() {
        inputs.entry(name.clone()).or_insert_with(|| {
            NodeData::Json(field.get("default").cloned().unwrap_or(Value::Null))
        });
    }
    inputs
}

fn resolve(
    interpolator: &Interpolator<'_>,
    raw: &HashMap<String, Value>,
) -> Result<HashMap<String, Value>, InterpolationError> {
    raw.iter()
        .map(|(name, value)| Ok((name.clone(), interpolator.interpolate(value)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Nothing;

    #[async_trait::async_trait]
    impl NodeHandler for Nothing {
        async fn execute(
            &self,
            _: &NodeContext,
            _: &PgPool,
        ) -> Result<NodeData, super::super::NodeError> {
            Ok(NodeData::default())
        }
    }

    #[tokio::test]
    async fn the_catalog_is_the_registered_types_in_order() {
        let pool = PgPool::connect_lazy("postgres://invalid").unwrap();
        let types = NodeTypes::new(pool)
            .register(NodeSpec::new("b"), Nothing)
            .register(NodeSpec::new("a"), Nothing)
            .register(NodeSpec::new("b").description("again"), Nothing);
        let catalog = types.catalog();
        let names: Vec<_> = catalog.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["b", "a"]);
        assert_eq!(catalog[0].description.as_deref(), Some("again"));
    }

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

    #[test]
    fn a_declared_input_left_out_takes_its_default() {
        let run = RunDefinition {
            execution_id: None,
            created_at: Utc::now(),
            inputs: json!({ "name": "ada" }),
            input_schema: json!({
                "name": { "type": "string" },
                "times": { "type": "integer", "default": 3 },
                "loud": { "type": "boolean" }
            }),
            nodes: json!({}),
            output_schema: json!({}),
        };
        let inputs = run_inputs(&run);
        let json = |name: &str| inputs[name].to_json();
        assert_eq!(
            (json("name"), json("times"), json("loud")),
            (json!("ada"), json!(3), json!(null))
        );
    }
}
