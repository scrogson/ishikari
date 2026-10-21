//! A module runs as a job, not only as a node: a `ModuleJob` of it, in an engine whose node types
//! have the modules, inserted from Rust or by an operator in the admin.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use common::TestDb;
use ishikari::execution::{
    ModuleGrants, ModuleJob, ModuleUpload, NodeJobs, NodeModules, NodeTypes,
};
use ishikari::{insert_with, Engine, InsertOpts, Postgres, Queue};
use ishikari_admin::AppState;
use ishikari_wasm::WasmModules;
use serde_json::{json, Value};
use sqlx::PgPool;
use tower::ServiceExt;

const JOB: &str = r#"
export const spec = { name: "test/job", category: "test" };

export async function run({ node_id, inputs }: { node_id: string; inputs: any }) {
  console.log(`running ${node_id}`);
  if (inputs.later) return { snooze: inputs.later };
  if (inputs.fail) throw new Error(inputs.fail);
  return { doubled: inputs.n * 2 };
}
"#;

async fn insert(pool: &PgPool, args: Value) -> i64 {
    insert_with(ModuleJob::new("test/job", args), InsertOpts::new(), pool)
        .await
        .unwrap()
        .job()
        .id
}

/// The job's state and errors once it's left `available` and `executing`.
async fn settled(pool: &PgPool, id: i64) -> (String, Vec<Value>) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (state, errors): (String, Vec<Value>) =
            sqlx::query_as("SELECT state::text, errors FROM ishikari_jobs WHERE id = $1")
                .bind(id)
                .fetch_one(pool)
                .await
                .unwrap();
        if !matches!(state.as_str(), "available" | "executing") {
            return (state, errors);
        }
        assert!(Instant::now() < deadline, "job {id} is still {state}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The modules, with [`JOB`] uploaded.
async fn modules(pool: &PgPool) -> Arc<WasmModules> {
    let modules = Arc::new(WasmModules::new(pool.clone()));
    modules
        .upload(ModuleUpload {
            bytes: JOB.as_bytes().to_vec(),
            grants: ModuleGrants::default(),
        })
        .await
        .unwrap();
    modules
}

/// An engine that runs module jobs by `modules`.
fn engine(pool: &PgPool, modules: Arc<WasmModules>) -> Engine {
    let types = NodeTypes::new(pool.clone()).modules(modules);
    Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .with_state(Arc::new(NodeJobs::new(types)))
        .with_queue(Queue::builder(ModuleJob::QUEUE).interval(Duration::from_millis(20)))
        .start(Postgres::new(pool.clone()))
}

/// A script's job completes with what it returns, snoozes when it returns `{ snooze }`, and fails
/// its attempt when it throws.
#[tokio::test]
async fn a_script_runs_as_a_job() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let engine = engine(&db.pool, modules(&db.pool).await);

    let done = insert(&db.pool, json!({ "n": 21 })).await;
    let later = insert(&db.pool, json!({ "later": 120 })).await;
    let failed = insert(&db.pool, json!({ "fail": "no invoice" })).await;

    assert_eq!(settled(&db.pool, done).await, ("completed".into(), vec![]));
    assert_eq!(settled(&db.pool, later).await, ("scheduled".into(), vec![]));
    let (state, errors) = settled(&db.pool, failed).await;
    assert_eq!(state, "retryable");
    let error = errors[0]["error"].as_str().unwrap();
    assert!(error.contains("Error: no invoice (script:"), "{error}");
    assert!(
        error.contains(&format!("info: running job:{failed}")),
        "{error}"
    );

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}

/// An operator inserts a script's job from the admin's Node types page, with its args, and an
/// engine runs it.
#[tokio::test]
async fn an_operator_inserts_a_scripts_job_in_the_admin() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = modules(&db.pool).await;
    let state = AppState::new(db.pool.clone())
        .with_base_path("/admin")
        .with_node_modules(modules.clone());
    let app = Router::new().nest("/admin", ishikari_admin::app(state));

    let form = serde_urlencoded::to_string([("module", "test/job"), ("args", r#"{"n": 4}"#)]);
    let res = app
        .oneshot(
            Request::post("/admin/node-types/job")
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .body(Body::from(form.unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    let location = res.headers()[header::LOCATION].to_str().unwrap();
    let id: i64 = location
        .strip_prefix("/admin/jobs/")
        .unwrap()
        .parse()
        .unwrap();

    let engine = engine(&db.pool, modules);
    assert_eq!(settled(&db.pool, id).await, ("completed".into(), vec![]));

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}
