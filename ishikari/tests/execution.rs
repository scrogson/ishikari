//! The workflow-definition execution records match the schema they are stored in, and the
//! host's node types run definitions' nodes.

mod common;

use chrono::{DateTime, Utc};
use common::TestDb;
use ishikari::execution::{
    ExecutionStorage, ModuleError, ModuleUpload, NodeContext, NodeData, NodeError, NodeHandler,
    NodeJob, NodeJobs, NodeModule, NodeModules, NodeRunner, NodeSpec, NodeTypes, SchemaField,
    TestRun,
};
use ishikari::prelude::*;
use ishikari::workflows::{Dag, Workflow};
use ishikari::{Engine, Postgres, Queue};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test]
async fn node_executions_go_through_every_status() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let definition: i64 = sqlx::query_scalar(
        "INSERT INTO ishikari_workflow_definitions (name, nodes) VALUES ('etl', '[]') RETURNING id",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    let workflow = Workflow::create(&db.pool, "etl", json!({}), None)
        .await
        .unwrap();
    let storage = ExecutionStorage::new(db.pool.clone());
    let execution = uuid::Uuid::new_v4();
    let run = storage
        .create_workflow_run(
            execution,
            definition,
            "etl",
            1,
            workflow.id,
            &json!({"day": 1}),
        )
        .await
        .unwrap();

    for node in ["extract", "transform", "load"] {
        storage
            .create_node_execution(run, node, "job", &json!({}))
            .await
            .unwrap();
    }
    storage.start_node_execution(run, "extract").await.unwrap();
    storage
        .complete_node_execution(run, "extract", &json!({"rows": 3}), 12)
        .await
        .unwrap();
    storage
        .start_node_execution(run, "transform")
        .await
        .unwrap();
    storage
        .fail_node_execution(run, "transform", "bad row", 5)
        .await
        .unwrap();
    assert_eq!(
        storage
            .increment_node_retry(run, "transform")
            .await
            .unwrap(),
        1
    );
    storage
        .skip_node_execution(run, "load", "upstream failed")
        .await
        .unwrap();

    let nodes = storage.get_node_executions(run).await.unwrap();
    let by: std::collections::HashMap<_, _> = nodes
        .iter()
        .map(|n| {
            (
                n.node_id.as_str(),
                (n.status.as_str(), n.retry_count, n.skip_reason.clone()),
            )
        })
        .collect();
    assert_eq!(by["extract"], ("completed", 0, None));
    assert_eq!(by["transform"], ("pending", 1, None));
    assert_eq!(by["load"], ("skipped", 0, Some("upstream failed".into())));
    assert_eq!(
        storage.get_node_output(run, "extract").await.unwrap(),
        Some(json!({"rows": 3}))
    );

    storage
        .complete_workflow_run(run, &json!({"ok": true}))
        .await
        .unwrap();
    let found = storage
        .get_workflow_run_by_execution_id(execution)
        .await
        .unwrap()
        .expect("the run");
    assert_eq!(found.id, run);

    db.finish().await;
}

/// Records the nodes it runs, completing them.
struct Recorder(tokio::sync::mpsc::UnboundedSender<(i64, String, String)>);

#[async_trait::async_trait]
impl NodeRunner for Recorder {
    async fn run(&self, job: &NodeJob, _ctx: Context) -> PerformResult {
        self.0
            .send((
                job.workflow_run_id,
                job.node_id.clone(),
                job.node_type.clone(),
            ))
            .unwrap();
        Complete::default().into()
    }
}

/// A definition's nodes, as the steps of a Dag, run through the host's runner in the
/// `workflows` queue, each after its upstreams.
#[tokio::test]
async fn node_jobs_run_through_the_engines_runner() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let node = |id: &str, node_type: &str| NodeJob {
        workflow_run_id: 7,
        node_id: id.into(),
        node_type: node_type.into(),
        raw_inputs: Default::default(),
        condition: None,
        max_attempts: 1,
        timeout_seconds: None,
    };
    let workflow = Dag::new("greet")
        .add("hello", node("hello", "echo"))
        .add_after("shout", node("shout", "uppercase"), ["hello"])
        .run(&db.pool)
        .await
        .unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let engine = Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .with_state(Arc::new(NodeJobs::new(Recorder(tx))))
        .with_queue(Queue::builder(NodeJob::QUEUE).interval(Duration::from_millis(20)))
        .start(Postgres::new(db.pool.clone()));

    let ran = tokio::time::timeout(Duration::from_secs(10), async {
        vec![rx.recv().await.unwrap(), rx.recv().await.unwrap()]
    })
    .await
    .expect("both nodes run");
    assert_eq!(
        ran,
        [
            (7, "hello".into(), "echo".into()),
            (7, "shout".into(), "uppercase".into())
        ]
    );
    common::eventually(
        "the workflow completes",
        Duration::from_secs(10),
        || async {
            Workflow::get(&db.pool, workflow.id, None)
                .await
                .unwrap()
                .is_some_and(|w| w.state == ishikari::WorkflowState::Completed)
        },
    )
    .await;

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// Upper-cases its `text` into `result`, reporting the inputs it was given.
struct Shout(tokio::sync::mpsc::UnboundedSender<(String, HashMap<String, Value>)>);

#[async_trait::async_trait]
impl NodeHandler for Shout {
    async fn execute(&self, ctx: &NodeContext, _pool: &PgPool) -> Result<NodeData, NodeError> {
        self.0
            .send((ctx.node_id.clone(), ctx.inputs.clone()))
            .unwrap();
        let text = ctx.get_input_str("text").unwrap_or_default();
        Ok(NodeData::json(json!({ "result": text.to_uppercase() })))
    }
}

/// Outputs its value.
struct Fixed(Value);

#[async_trait::async_trait]
impl NodeHandler for Fixed {
    async fn execute(&self, _ctx: &NodeContext, _pool: &PgPool) -> Result<NodeData, NodeError> {
        Ok(NodeData::json(self.0.clone()))
    }
}

/// Always fails.
struct Fails;

#[async_trait::async_trait]
impl NodeHandler for Fails {
    async fn execute(&self, _ctx: &NodeContext, _pool: &PgPool) -> Result<NodeData, NodeError> {
        Err(NodeError::ExecutionFailed("boom".into()))
    }
}

fn shout() -> NodeSpec {
    NodeSpec::new("shout")
        .category("transform")
        .input("text", SchemaField::string().required())
        .output("result", SchemaField::string())
}

/// `table`, in `schema` when there is one.
fn table(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema) => format!("{schema}.{table}"),
        None => table.to_string(),
    }
}

/// Start a run of `definition` (its `inputs`, `nodes` and `outputs`) as the admin's Run does:
/// the run's row, and a Dag with a `NodeJob` step per node of one attempt each, inserted in
/// `order` (each node after its upstreams). Returns the run's id.
async fn start_run(
    pool: &PgPool,
    schema: Option<&str>,
    definition: Value,
    order: &[&str],
    inputs: Value,
) -> i64 {
    let definition_id: i64 = sqlx::query_scalar(&format!(
        "INSERT INTO {} (name, input_schema, nodes, output_schema) VALUES ('test', $1, $2, $3) \
         RETURNING id",
        table(schema, "ishikari_workflow_definitions")
    ))
    .bind(definition.get("inputs").cloned().unwrap_or(json!({})))
    .bind(&definition["nodes"])
    .bind(definition.get("outputs").cloned().unwrap_or(json!({})))
    .fetch_one(pool)
    .await
    .unwrap();

    let runs = table(schema, "ishikari_workflow_runs");
    let mut tx = pool.begin().await.unwrap();
    let run: i64 = sqlx::query_scalar(&format!(
        "SELECT nextval(pg_get_serial_sequence('{runs}', 'id'))"
    ))
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let mut dag = Dag::new("test");
    if let Some(schema) = schema {
        dag = dag.schema(schema);
    }
    for id in order {
        let node = &definition["nodes"][id];
        let job = NodeJob {
            workflow_run_id: run,
            node_id: id.to_string(),
            node_type: node["type"].as_str().unwrap().into(),
            raw_inputs: serde_json::from_value(node.get("inputs").cloned().unwrap_or(json!({})))
                .unwrap(),
            condition: node.get("when").and_then(Value::as_str).map(Into::into),
            max_attempts: 1,
            timeout_seconds: None,
        };
        let upstreams: Vec<String> =
            serde_json::from_value(node.get("depends_on").cloned().unwrap_or(json!([]))).unwrap();
        dag = dag.add_after(*id, job, upstreams);
    }
    let workflow = dag.run(&mut *tx).await.unwrap();
    sqlx::query(&format!(
        "INSERT INTO {runs} (id, definition_id, definition_name, definition_version, workflow_id, \
         inputs) VALUES ($1, $2, 'test', 1, $3, $4)"
    ))
    .bind(run)
    .bind(definition_id)
    .bind(workflow.id)
    .bind(inputs)
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    run
}

fn start_engine(storage: Postgres, types: NodeTypes) -> Engine {
    Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .with_state(Arc::new(NodeJobs::new(types)))
        .with_queue(Queue::builder(NodeJob::QUEUE).interval(Duration::from_millis(20)))
        .start(storage)
}

/// The run's status, outputs and error, once it has finished.
async fn finished(
    pool: &PgPool,
    schema: Option<&str>,
    run: i64,
) -> (String, Value, Option<String>) {
    let query = format!(
        "SELECT status, outputs, error FROM {} WHERE id = $1",
        table(schema, "ishikari_workflow_runs")
    );
    let start = std::time::Instant::now();
    loop {
        let (status, outputs, error): (String, Option<Value>, Option<String>) =
            sqlx::query_as(&query)
                .bind(run)
                .fetch_one(pool)
                .await
                .unwrap();
        if status != "running" {
            return (status, outputs.unwrap_or(Value::Null), error);
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "timed out waiting for the run to finish"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A node's recorded execution: its status, inputs, output, error, skip reason and duration.
async fn recorded(
    pool: &PgPool,
    schema: Option<&str>,
    run: i64,
    node: &str,
) -> (
    String,
    Option<Value>,
    Option<Value>,
    Option<String>,
    Option<String>,
    Option<i64>,
) {
    sqlx::query_as(&format!(
        "SELECT status, inputs, output, error, skip_reason, duration_ms FROM {} \
         WHERE workflow_run_id = $1 AND node_id = $2",
        table(schema, "ishikari_node_executions")
    ))
    .bind(run)
    .bind(node)
    .fetch_one(pool)
    .await
    .unwrap()
}

/// Each node runs through the handler of its type, given its inputs resolved against the run's
/// inputs (a declared input left out takes its default) and its upstreams' outputs. Each
/// execution is recorded, and the last completes the run with the definition's outputs. The
/// run and its records are in the schema given, not the connection's.
#[tokio::test]
async fn a_run_goes_through_the_hosts_node_types() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let pool = db.plain_pool().await;
    let schema = Some(db.schema.as_str());
    let run = start_run(
        &pool,
        schema,
        json!({
            "inputs": {
                "name": { "type": "string", "required": true },
                "times": { "type": "integer", "default": 3 }
            },
            "nodes": {
                "greet": { "type": "shout", "inputs": { "text": "{{inputs.name}}" } },
                "wrap": {
                    "type": "shout",
                    "depends_on": ["greet"],
                    "inputs": { "text": "hello {{nodes.greet.result}}", "times": "{{inputs.times}}" }
                }
            },
            "outputs": { "greeting": "{{nodes.wrap.result}}" }
        }),
        &["greet", "wrap"],
        json!({ "name": "ada" }),
    )
    .await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let types = NodeTypes::new(pool.clone())
        .schema(&db.schema)
        .register(shout(), Shout(tx));
    let engine = start_engine(Postgres::new(pool.clone()).with_schema(&db.schema), types);

    let (status, outputs, error) = finished(&pool, schema, run).await;
    assert_eq!((status.as_str(), error), ("completed", None));
    assert_eq!(outputs, json!({ "greeting": "HELLO ADA" }));

    let given = |inputs: Value| serde_json::from_value::<HashMap<String, Value>>(inputs).unwrap();
    assert_eq!(
        rx.recv().await.unwrap(),
        ("greet".into(), given(json!({ "text": "ada" })))
    );
    assert_eq!(
        rx.recv().await.unwrap(),
        (
            "wrap".into(),
            given(json!({ "text": "hello ADA", "times": 3 }))
        )
    );

    let (status, inputs, output, error, _, duration) = recorded(&pool, schema, run, "wrap").await;
    assert_eq!(status, "completed");
    assert_eq!(inputs, Some(json!({ "text": "hello ADA", "times": 3 })));
    assert_eq!(output, Some(json!({ "result": "HELLO ADA" })));
    assert_eq!(error, None);
    assert!(duration.is_some());

    engine.shutdown(Duration::from_secs(1)).await;
    pool.close().await;
    db.finish().await;
}

/// A node whose `when` is falsy is skipped, its handler never called, and the nodes after it
/// still run.
#[tokio::test]
async fn a_falsy_when_skips_the_node() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let run = start_run(
        &db.pool,
        None,
        json!({
            "nodes": {
                "check": { "type": "flags" },
                "maybe": {
                    "type": "shout",
                    "depends_on": ["check"],
                    "when": "{{nodes.check.off}}",
                    "inputs": { "text": "maybe" }
                },
                "sure": {
                    "type": "shout",
                    "depends_on": ["check"],
                    "when": "{{nodes.check.on}}",
                    "inputs": { "text": "sure" }
                },
                "last": {
                    "type": "shout",
                    "depends_on": ["maybe", "sure"],
                    "inputs": { "text": "after {{nodes.sure.result}}" }
                }
            },
            "outputs": { "last": "{{nodes.last.result}}" }
        }),
        &["check", "maybe", "sure", "last"],
        json!({}),
    )
    .await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let types = NodeTypes::new(db.pool.clone())
        .register(
            NodeSpec::new("flags"),
            Fixed(json!({ "on": true, "off": false })),
        )
        .register(shout(), Shout(tx));
    let engine = start_engine(Postgres::new(db.pool.clone()), types);

    let (status, outputs, error) = finished(&db.pool, None, run).await;
    assert_eq!((status.as_str(), error), ("completed", None));
    assert_eq!(outputs, json!({ "last": "AFTER SURE" }));

    let mut ran = vec![rx.recv().await.unwrap().0, rx.recv().await.unwrap().0];
    ran.sort();
    assert_eq!(ran, ["last", "sure"]);
    assert!(rx.try_recv().is_err(), "the skipped node ran");

    let (status, _, output, _, reason, _) = recorded(&db.pool, None, run, "maybe").await;
    assert_eq!((status.as_str(), output), ("skipped", None));
    assert!(reason.unwrap().contains("{{nodes.check.off}}"));

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// A node of a type the host didn't register fails, and fails its run, without retrying.
#[tokio::test]
async fn an_unregistered_node_type_fails_the_run() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let run = start_run(
        &db.pool,
        None,
        json!({ "nodes": { "lost": { "type": "nope" } } }),
        &["lost"],
        json!({}),
    )
    .await;

    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let types = NodeTypes::new(db.pool.clone()).register(shout(), Shout(tx));
    let engine = start_engine(Postgres::new(db.pool.clone()), types);

    let (status, _, error) = finished(&db.pool, None, run).await;
    assert_eq!(status, "failed");
    assert!(error.unwrap().contains("Unknown node type 'nope'"));
    let (status, _, _, error, _, _) = recorded(&db.pool, None, run, "lost").await;
    assert_eq!(
        (status.as_str(), error.as_deref()),
        ("failed", Some("Unknown node type 'nope'"))
    );
    common::eventually("the job is cancelled", Duration::from_secs(10), || async {
        sqlx::query_scalar::<_, String>("SELECT state::text FROM ishikari_jobs")
            .fetch_one(&db.pool)
            .await
            .unwrap()
            == "cancelled"
    })
    .await;

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// A node that fails its last attempt is discarded, and fails its run.
#[tokio::test]
async fn a_node_failing_its_last_attempt_fails_the_run() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let run = start_run(
        &db.pool,
        None,
        json!({ "nodes": { "bad": { "type": "fails" } } }),
        &["bad"],
        json!({}),
    )
    .await;

    let types = NodeTypes::new(db.pool.clone()).register(NodeSpec::new("fails"), Fails);
    let engine = start_engine(Postgres::new(db.pool.clone()), types);

    let (status, _, error) = finished(&db.pool, None, run).await;
    assert_eq!(status, "failed");
    assert!(error.unwrap().contains("boom"));
    let (status, _, _, error, _, _) = recorded(&db.pool, None, run, "bad").await;
    assert_eq!(status, "failed");
    assert!(error.unwrap().contains("boom"));
    common::eventually("the job is discarded", Duration::from_secs(10), || async {
        sqlx::query_scalar::<_, String>("SELECT state::text FROM ishikari_jobs")
            .fetch_one(&db.pool)
            .await
            .unwrap()
            == "discarded"
    })
    .await;

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// Node types found as runs run (modules uploaded through the admin, say), each version with
/// when it was uploaded and the output it gives.
#[derive(Default)]
struct Uploaded {
    versions: Vec<(DateTime<Utc>, &'static str, Value)>,
    broken: bool,
    asked: Mutex<Vec<(String, DateTime<Utc>)>>,
}

#[async_trait::async_trait]
impl NodeModules for Uploaded {
    async fn handler(
        &self,
        node_type: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<Arc<dyn NodeHandler>>, ModuleError> {
        self.asked
            .lock()
            .unwrap()
            .push((node_type.to_string(), as_of));
        if self.broken {
            return Err(ModuleError::Runtime("the module doesn't compile".into()));
        }
        Ok(self
            .versions
            .iter()
            .rfind(|(at, name, _)| *name == node_type && *at <= as_of)
            .map(|(_, _, output)| Arc::new(Fixed(output.clone())) as Arc<dyn NodeHandler>))
    }

    async fn modules(&self) -> Result<Vec<NodeModule>, ModuleError> {
        Ok(Vec::new())
    }

    async fn describe(&self, _: &[u8]) -> Result<NodeSpec, ModuleError> {
        Err(ModuleError::Invalid("read-only".into()))
    }

    async fn upload(&self, _: ModuleUpload) -> Result<NodeModule, ModuleError> {
        Err(ModuleError::Invalid("read-only".into()))
    }

    async fn test_run(
        &self,
        _: ModuleUpload,
        _: HashMap<String, Value>,
    ) -> Result<TestRun, ModuleError> {
        Err(ModuleError::Invalid("read-only".into()))
    }
}

async fn created_at(pool: &PgPool, run: i64) -> DateTime<Utc> {
    sqlx::query_scalar("SELECT created_at FROM ishikari_workflow_runs WHERE id = $1")
        .bind(run)
        .fetch_one(pool)
        .await
        .unwrap()
}

/// A node of a type the host didn't register in Rust runs through its modules, at the version
/// uploaded last before the run was created; a type registered in Rust never asks them.
#[tokio::test]
async fn a_type_not_registered_runs_from_the_hosts_modules() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let run = start_run(
        &db.pool,
        None,
        json!({
            "nodes": {
                "greet": { "type": "shout", "inputs": { "text": "hi" } },
                "post": { "type": "slack/post", "depends_on": ["greet"] }
            },
            "outputs": { "posted": "{{nodes.post.version}}" }
        }),
        &["greet", "post"],
        json!({}),
    )
    .await;

    let hour = chrono::Duration::hours(1);
    let modules = Arc::new(Uploaded {
        versions: vec![
            (
                Utc::now() - hour,
                "shout",
                json!({ "result": "from a module" }),
            ),
            (Utc::now() - hour, "slack/post", json!({ "version": 1 })),
            (Utc::now() + hour, "slack/post", json!({ "version": 2 })),
        ],
        ..Uploaded::default()
    });
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let types = NodeTypes::new(db.pool.clone())
        .register(shout(), Shout(tx))
        .modules(modules.clone());
    let engine = start_engine(Postgres::new(db.pool.clone()), types);

    let (status, outputs, error) = finished(&db.pool, None, run).await;
    assert_eq!((status.as_str(), error), ("completed", None));
    assert_eq!(outputs, json!({ "posted": 1 }));
    let (_, _, output, _, _, _) = recorded(&db.pool, None, run, "greet").await;
    assert_eq!(output, Some(json!({ "result": "HI" })));
    let created_at = created_at(&db.pool, run).await;
    assert_eq!(
        *modules.asked.lock().unwrap(),
        [("slack/post".to_string(), created_at)]
    );

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// A type neither registered nor among the modules is unknown, and fails its run.
#[tokio::test]
async fn a_type_the_modules_dont_have_fails_the_run() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let run = start_run(
        &db.pool,
        None,
        json!({ "nodes": { "lost": { "type": "nope" } } }),
        &["lost"],
        json!({}),
    )
    .await;

    let types = NodeTypes::new(db.pool.clone()).modules(Arc::new(Uploaded::default()));
    let engine = start_engine(Postgres::new(db.pool.clone()), types);

    let (status, _, error) = finished(&db.pool, None, run).await;
    assert_eq!(status, "failed");
    assert!(error.unwrap().contains("Unknown node type 'nope'"));

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// A module that can't be loaded fails its node like a failing handler, retried until its last
/// attempt.
#[tokio::test]
async fn a_module_that_cannot_load_fails_the_node() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let run = start_run(
        &db.pool,
        None,
        json!({ "nodes": { "post": { "type": "slack/post" } } }),
        &["post"],
        json!({}),
    )
    .await;

    let modules = Uploaded {
        broken: true,
        ..Uploaded::default()
    };
    let types = NodeTypes::new(db.pool.clone()).modules(Arc::new(modules));
    let engine = start_engine(Postgres::new(db.pool.clone()), types);

    let (status, _, error) = finished(&db.pool, None, run).await;
    assert_eq!(status, "failed");
    assert!(error.unwrap().contains("the module doesn't compile"));
    let (status, _, _, error, _, _) = recorded(&db.pool, None, run, "post").await;
    assert_eq!(status, "failed");
    assert!(error.unwrap().contains("the module doesn't compile"));
    common::eventually("the job is discarded", Duration::from_secs(10), || async {
        sqlx::query_scalar::<_, String>("SELECT state::text FROM ishikari_jobs")
            .fetch_one(&db.pool)
            .await
            .unwrap()
            == "discarded"
    })
    .await;

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}
