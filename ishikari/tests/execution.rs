//! The workflow-definition execution records match the schema they are stored in.

mod common;

use common::TestDb;
use ishikari::execution::{ExecutionStorage, NodeJob, NodeJobs, NodeRunner};
use ishikari::prelude::*;
use ishikari::workflows::{Dag, Workflow};
use ishikari::{Engine, Postgres, Queue};
use serde_json::json;
use std::sync::Arc;
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
