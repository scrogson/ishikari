//! The workflow-definition execution records match the schema they are stored in.

mod common;

use common::TestDb;
use ishikari::execution::ExecutionStorage;
use ishikari::workflows::Workflow;
use serde_json::json;

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
