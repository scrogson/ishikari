//! Running a definition: its nodes become the steps of a workflow.

mod common;

use axum::http::{header, StatusCode};
use common::{body, eventually, get, post_form, TestDb};
use ishikari::execution::{NodeJob, NodeJobs, NodeRunner};
use ishikari::prelude::*;
use ishikari::{Engine, Postgres, Queue};
use ishikari_admin::AppState;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

/// Completes every node.
struct Nodes;

#[async_trait::async_trait]
impl NodeRunner for Nodes {
    async fn run(&self, _job: &NodeJob, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

const DEFINITION: &str = r#"
name: greet
nodes:
  hello:
    type: echo
    inputs:
      message: "{{inputs.name}}"
    max_retries: 0
    timeout_seconds: 5
  shout:
    type: uppercase
    depends_on: [hello]
    inputs:
      text: "{{hello.outputs.message}}"
"#;

fn location(res: &axum::response::Response) -> String {
    res.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .to_string()
}

/// Run the definition with these inputs, returning its workflow's id.
async fn run(app: &axum::Router, definition: i64) -> i64 {
    let res = post_form(
        app,
        &format!("/definitions/{definition}/run"),
        &[("inputs_json", r#"{"name": "ada"}"#)],
    )
    .await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    location(&res)
        .strip_prefix("/workflows/")
        .expect("redirects to the workflow")
        .parse()
        .unwrap()
}

/// The status and completion time of `workflow`'s run, as the definition's recent runs show
/// them.
async fn shown_run(app: &axum::Router, definition: i64, workflow: i64) -> (String, String) {
    let res = get(app, &format!("/definitions/{definition}/run"), &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let page = body(res).await;
    let row = page
        .split("<tr>")
        .find_map(|row| {
            let row = &row[..row.find("</tr>")?];
            row.contains(&format!("/workflows/{workflow}\""))
                .then_some(row)
        })
        .expect("the run is listed");
    // Run, workflow, status, started, completed: each cell's text.
    let cells: Vec<String> = row
        .split("<td")
        .skip(1)
        .map(|cell| {
            let mut text = String::new();
            let mut in_tag = true;
            for c in cell.chars() {
                match c {
                    '<' => in_tag = true,
                    '>' => in_tag = false,
                    c if !in_tag => text.push(c),
                    _ => {}
                }
            }
            text.trim().to_string()
        })
        .collect();
    (cells[2].clone(), cells[4].clone())
}

/// Import the definition, returning its id.
async fn import(app: &axum::Router) -> i64 {
    let res = post_form(app, "/definitions/import", &[("yaml", DEFINITION)]).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    location(&res).rsplit('/').next().unwrap().parse().unwrap()
}

/// Running a definition inserts its nodes as one workflow's steps, which a host's engine runs:
/// `ishikari::execution::NodeJob`s, named by node, the root available and the rest pending on
/// their upstreams, with the node's attempts and timeout.
#[tokio::test]
async fn running_a_definition_inserts_its_nodes_as_a_workflows_steps() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let s = &db.schema;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), s));
    let definition = import(&app).await;

    let workflow = run(&app, definition).await;

    let (run, inputs): (i64, Value) = sqlx::query_as(&format!(
        "SELECT id, inputs FROM {s}.ishikari_workflow_runs WHERE workflow_id = $1"
    ))
    .bind(workflow)
    .fetch_one(&db.pool)
    .await
    .expect("the run, on the workflow");
    assert_eq!(inputs, json!({"name": "ada"}));

    let steps: Vec<(i64, String, String, String, i32, Value)> = sqlx::query_as(&format!(
        "SELECT id, meta->>'ishikari_step', worker, state::text, max_attempts, args \
         FROM {s}.ishikari_jobs WHERE workflow_id = $1 ORDER BY meta->>'ishikari_step'"
    ))
    .bind(workflow)
    .fetch_all(&db.pool)
    .await
    .unwrap();
    let names: Vec<&str> = steps.iter().map(|s| s.1.as_str()).collect();
    assert_eq!(names, ["hello", "shout"], "one step per node, named by it");

    let (hello, _, worker, state, max_attempts, args) = &steps[0];
    assert_eq!(worker, "ishikari::execution::NodeJob");
    assert_eq!(state, "available", "a root starts at once");
    assert_eq!(*max_attempts, 1, "no retries is one attempt");
    let job: Box<dyn ishikari::Worker> =
        serde_json::from_value(args.clone()).expect("a worker the library registers");
    assert_eq!(job.queue(), "workflows");
    assert_eq!(job.timeout(), Some(std::time::Duration::from_secs(5)));
    let node: NodeJob = serde_json::from_value(args.clone()).unwrap();
    assert_eq!(node.workflow_run_id, run);
    assert_eq!(node.node_id, "hello");
    assert_eq!(node.node_type, "echo");
    assert_eq!(node.raw_inputs["message"], json!("{{inputs.name}}"));

    let (shout, _, _, state, max_attempts, _) = &steps[1];
    assert_eq!(state, "pending", "waits on its upstream");
    assert_eq!(*max_attempts, 3, "the default attempts");
    let upstream: i64 = sqlx::query_scalar(&format!(
        "SELECT depends_on_job_id FROM {s}.ishikari_job_dependencies WHERE job_id = $1"
    ))
    .bind(shout)
    .fetch_one(&db.pool)
    .await
    .expect("shout depends on hello");
    assert_eq!(upstream, *hello);
    assert_eq!(
        shown_run(&app, definition, workflow).await,
        ("running".into(), "—".into())
    );

    let engine = Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .with_state(Arc::new(NodeJobs::new(Nodes)))
        .with_queue(Queue::builder(NodeJob::QUEUE).interval(Duration::from_millis(20)))
        .start(Postgres::new(db.pool.clone()).with_schema(s));
    eventually(
        "the workflow completes",
        Duration::from_secs(10),
        || async {
            let state: String = sqlx::query_scalar(&format!(
                "SELECT state FROM {s}.ishikari_workflows WHERE id = $1"
            ))
            .bind(workflow)
            .fetch_one(&db.pool)
            .await
            .unwrap();
            state == "completed"
        },
    )
    .await;
    engine.shutdown(Duration::from_secs(1)).await;
    let (status, completed) = shown_run(&app, definition, workflow).await;
    assert_eq!(status, "completed", "the run is as done as its workflow");
    assert_ne!(completed, "—", "and says when");

    db.drop().await;
}

/// Fails the `hello` node, completes the rest.
struct FailHello;

#[async_trait::async_trait]
impl NodeRunner for FailHello {
    async fn run(&self, job: &NodeJob, _ctx: Context) -> PerformResult {
        match job.node_id.as_str() {
            "hello" => Err("no greeting".into()),
            _ => Complete::default().into(),
        }
    }
}

/// A node that fails for good fails its workflow, and the run with it; retrying the node
/// runs both again.
#[tokio::test]
async fn a_run_fails_with_its_workflow_and_runs_again_on_a_retry() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let s = &db.schema;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), s));
    let definition = import(&app).await;
    let workflow = run(&app, definition).await;

    let engine = Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .with_state(Arc::new(NodeJobs::new(FailHello)))
        .with_queue(Queue::builder(NodeJob::QUEUE).interval(Duration::from_millis(20)))
        .start(Postgres::new(db.pool.clone()).with_schema(s));
    eventually("the workflow fails", Duration::from_secs(10), || async {
        let state: String = sqlx::query_scalar(&format!(
            "SELECT state FROM {s}.ishikari_workflows WHERE id = $1"
        ))
        .bind(workflow)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        state == "failed"
    })
    .await;
    engine.shutdown(Duration::from_secs(1)).await;
    let (status, completed) = shown_run(&app, definition, workflow).await;
    assert_eq!(status, "failed", "the run fails with its workflow");
    assert_ne!(completed, "—", "and says when");

    let hello: i64 = sqlx::query_scalar(&format!(
        "SELECT id FROM {s}.ishikari_jobs WHERE workflow_id = $1 AND meta->>'ishikari_step' = 'hello'"
    ))
    .bind(workflow)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!(Postgres::new(db.pool.clone())
        .with_schema(s)
        .retry_job(hello)
        .await
        .unwrap());
    assert_eq!(
        shown_run(&app, definition, workflow).await,
        ("running".into(), "—".into()),
        "a retried node runs its workflow, and the run, again"
    );

    db.drop().await;
}
