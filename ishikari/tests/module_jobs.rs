//! A `ModuleJob` is a job whose code is one of the host's modules (node types its operators
//! upload): the module's node runs with the job's args as its inputs, at the version uploaded
//! last before the job was inserted, and what it returns is the job's outcome.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use common::{eventually, TestDb};
use ishikari::execution::{
    ModuleError, ModuleJob, ModuleUpload, NodeContext, NodeData, NodeError, NodeHandler, NodeJobs,
    NodeModule, NodeModules, NodeSpec, NodeTypes, TestRun,
};
use ishikari::{insert_with, Engine, InsertOpts, Postgres, Queue};
use serde_json::{json, Value};
use sqlx::PgPool;

/// What a module's node was asked to run: the module, as of when, and its node's context.
type Asked = Arc<Mutex<Vec<(String, DateTime<Utc>, String, HashMap<String, Value>)>>>;

/// The modules `send` (uploaded an hour ago) and `later` (in an hour). A node of either returns
/// its input `out`, or fails when it has an input `fail`.
#[derive(Default)]
struct Modules {
    asked: Asked,
}

#[async_trait::async_trait]
impl NodeModules for Modules {
    async fn handler(
        &self,
        node_type: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<Arc<dyn NodeHandler>>, ModuleError> {
        let hour = chrono::Duration::hours(1);
        let uploaded_at = match node_type {
            "send" => Utc::now() - hour,
            "later" => Utc::now() + hour,
            "broken" => return Err(ModuleError::Runtime("the module doesn't compile".into())),
            _ => return Ok(None),
        };
        Ok((uploaded_at <= as_of).then(|| {
            Arc::new(Returns {
                module: node_type.to_string(),
                as_of,
                asked: self.asked.clone(),
            }) as Arc<dyn NodeHandler>
        }))
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

struct Returns {
    module: String,
    as_of: DateTime<Utc>,
    asked: Asked,
}

#[async_trait::async_trait]
impl NodeHandler for Returns {
    async fn execute(&self, ctx: &NodeContext, _pool: &PgPool) -> Result<NodeData, NodeError> {
        self.asked.lock().unwrap().push((
            self.module.clone(),
            self.as_of,
            ctx.node_id.clone(),
            ctx.inputs.clone(),
        ));
        if ctx.inputs.contains_key("fail") {
            return Err(NodeError::ExecutionFailed("boom".into()));
        }
        Ok(NodeData::json(ctx.inputs["out"].clone()))
    }
}

fn start_engine(pool: &PgPool, types: NodeTypes) -> Engine {
    Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .with_state(Arc::new(NodeJobs::new(types)))
        .with_queue(Queue::builder(ModuleJob::QUEUE).interval(Duration::from_millis(20)))
        .start(Postgres::new(pool.clone()))
}

async fn insert(pool: &PgPool, module: &str, args: Value) -> ishikari::Job {
    insert_with(ModuleJob::new(module, args), InsertOpts::new(), pool)
        .await
        .unwrap()
        .into_job()
}

/// A job's state and the errors it recorded, once it's left `available` and `executing`.
async fn settled(db: &TestDb, id: i64) -> (String, Vec<Value>) {
    eventually("the job settles", Duration::from_secs(10), || async {
        !matches!(db.state(id).await.as_str(), "available" | "executing")
    })
    .await;
    sqlx::query_as("SELECT state::text, errors FROM ishikari_jobs WHERE id = $1")
        .bind(id)
        .fetch_one(&db.pool)
        .await
        .unwrap()
}

/// A module job runs its module's node, as of when it was inserted, with its args as the node's
/// inputs and `job:<id>` as its node's id; what the node returns completes it.
#[tokio::test]
async fn a_module_job_runs_its_module_with_its_args() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = Modules::default();
    let asked = modules.asked.clone();
    let types = NodeTypes::new(db.pool.clone()).modules(Arc::new(modules));
    let engine = start_engine(&db.pool, types);

    let job = insert(
        &db.pool,
        "send",
        json!({ "to": "ada", "out": { "sent": 1 } }),
    )
    .await;
    assert_eq!(job.queue, ModuleJob::QUEUE);
    let (state, errors) = settled(&db, job.id).await;
    assert_eq!((state.as_str(), errors), ("completed", vec![]));
    let inputs: HashMap<String, Value> =
        serde_json::from_value(json!({ "to": "ada", "out": { "sent": 1 } })).unwrap();
    assert_eq!(
        *asked.lock().unwrap(),
        [(
            "send".to_string(),
            job.inserted_at,
            format!("job:{}", job.id),
            inputs
        )]
    );

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// What the node returns is the job's outcome: `{"snooze": seconds}` snoozes it, `{"cancel":
/// why}` cancels it, anything else completes it, and a node that fails fails the attempt. A
/// module uploaded after the job was inserted, or none, cancels it, as do args that aren't an
/// object; a module that can't be loaded fails the attempt.
#[tokio::test]
async fn a_module_job_ends_as_its_module_says() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let types = NodeTypes::new(db.pool.clone()).modules(Arc::new(Modules::default()));
    let engine = start_engine(&db.pool, types);

    let snoozed = insert(&db.pool, "send", json!({ "out": { "snooze": 60 } })).await;
    let (state, errors) = settled(&db, snoozed.id).await;
    assert_eq!((state.as_str(), errors.len()), ("scheduled", 0));
    let later: bool = sqlx::query_scalar(
        "SELECT scheduled_at > now() + interval '50 seconds' FROM ishikari_jobs WHERE id = $1",
    )
    .bind(snoozed.id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(later, "snoozed for its seconds");

    let cases = [
        (
            "send",
            json!({ "out": { "cancel": "not today" } }),
            "cancelled",
            "not today",
        ),
        (
            "send",
            json!({ "out": { "cancel": "x", "y": 1 } }),
            "completed",
            "",
        ),
        ("send", json!({ "fail": true }), "retryable", "boom"),
        (
            "broken",
            json!({}),
            "retryable",
            "the module doesn't compile",
        ),
        (
            "later",
            json!({}),
            "cancelled",
            "no module later was uploaded before the job",
        ),
        (
            "nope",
            json!({}),
            "cancelled",
            "no module nope was uploaded before the job",
        ),
        (
            "send",
            json!([1]),
            "cancelled",
            "its args aren't a JSON object",
        ),
    ];
    for (module, args, state, why) in cases {
        let job = insert(&db.pool, module, args.clone()).await;
        let (ended, errors) = settled(&db, job.id).await;
        assert_eq!(ended, state, "{module} {args}");
        let error = errors
            .first()
            .map(|e| e["error"].as_str().unwrap().to_string());
        match why {
            "" => assert_eq!(error, None, "{module} {args}"),
            why => {
                let error = error.unwrap_or_default();
                assert!(error.contains(why), "{module} {args}: {error}");
            }
        }
    }

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// An engine whose node types have no modules cancels a module job.
#[tokio::test]
async fn a_module_job_needs_an_engine_with_modules() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let engine = start_engine(&db.pool, NodeTypes::new(db.pool.clone()));

    let job = insert(&db.pool, "send", json!({})).await;
    let (state, errors) = settled(&db, job.id).await;
    assert_eq!(state, "cancelled");
    assert!(
        errors[0]["error"]
            .as_str()
            .unwrap()
            .contains("runs no modules"),
        "{errors:?}"
    );

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}
