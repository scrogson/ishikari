//! Modules are uploaded, versioned, and run as node types in a sandbox, within their limits.
//!
//! `fixtures/testkit-v*.wasm` are built from `fixtures/testkit` by `mise run wasm:fixtures`.

mod common;

use chrono::Utc;
use common::{failure, pong, TestDb};
use ishikari::execution::{
    ModuleError, ModuleGrants, ModuleUpload, NodeError, NodeJob, NodeJobs, NodeModule, NodeModules,
    NodeTypes,
};
use ishikari::workflows::Dag;
use ishikari::{Engine, Postgres, Queue};
use ishikari_wasm::WasmModules;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};

const V1: &[u8] = include_bytes!("fixtures/testkit-v1.wasm");
const V2: &[u8] = include_bytes!("fixtures/testkit-v2.wasm");
const KIT: &str = "test/kit";

fn upload(bytes: &[u8], grants: ModuleGrants) -> ModuleUpload {
    ModuleUpload {
        bytes: bytes.to_vec(),
        grants,
    }
}

/// The output of a node of the module's latest version run with `inputs`.
async fn run_node(modules: &WasmModules, pool: &PgPool, inputs: Value) -> Result<Value, NodeError> {
    common::run(modules, pool, KIT, inputs).await
}

/// An upload is described by its spec. The same bytes and grants again change nothing; new
/// bytes or grants are the name's next version, which the list shows.
#[tokio::test]
async fn uploads_are_versioned_by_name() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());

    let described = modules.describe(V1).await.unwrap();
    assert_eq!(described.name, KIT);
    assert!(modules.modules().await.unwrap().is_empty());

    let first = modules
        .upload(upload(V1, ModuleGrants::default()))
        .await
        .unwrap();
    assert_eq!(first.spec, described);
    assert_eq!(first.spec.name, KIT);
    assert_eq!(first.spec.category, "test");
    assert_eq!(
        first.spec.description.as_deref(),
        Some("ishikari-wasm's test module, v1")
    );
    assert!(first.spec.input_schema["op"].required);
    assert_eq!(first.version, 1);
    assert_eq!(first.size, V1.len() as i64);
    assert_eq!(first.sha256.len(), 64);

    let again = modules
        .upload(upload(V1, ModuleGrants::default()))
        .await
        .unwrap();
    assert_eq!(again, first);

    let granted = ModuleGrants {
        allowed_hosts: vec![" api.example.com ".into(), "".into()],
        timeout: Some(Duration::from_secs(2)),
        memory_limit_mb: Some(32),
        secrets: vec!["api/key".into()],
    };
    let regranted = modules.upload(upload(V1, granted)).await.unwrap();
    assert_eq!(regranted.version, 2);
    assert_eq!(regranted.sha256, first.sha256);
    assert_eq!(regranted.grants.allowed_hosts, ["api.example.com"]);
    assert_eq!(regranted.grants.timeout, Some(Duration::from_secs(2)));
    assert_eq!(regranted.grants.memory_limit_mb, Some(32));
    assert_eq!(regranted.grants.secrets, ["api/key"]);

    let second = modules
        .upload(upload(V2, ModuleGrants::default()))
        .await
        .unwrap();
    assert_eq!(second.version, 3);
    assert_ne!(second.sha256, first.sha256);

    let listed = modules.modules().await.unwrap();
    assert_eq!(listed, [second]);

    db.finish().await;
}

/// What isn't a component of the `ishikari:node` world, or asks for grants that can't be
/// given, is refused, and nothing is kept.
#[tokio::test]
async fn uploads_that_arent_node_modules_are_refused() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());

    let invalid = |result: Result<NodeModule, ModuleError>| match result {
        Err(ModuleError::Invalid(why)) => why,
        other => panic!("expected the upload to be refused, got {other:?}"),
    };
    // Neither a component nor text (which would be a script).
    let junk = &[0xff, 0xfe, 0x00, 0x01];
    match modules.describe(junk).await {
        Err(ModuleError::Invalid(why)) => {
            assert!(why.starts_with("neither a WebAssembly"), "{why}")
        }
        other => panic!("expected the bytes to be refused, got {other:?}"),
    }
    let why = invalid(modules.upload(upload(junk, ModuleGrants::default())).await);
    assert!(why.starts_with("neither a WebAssembly component"), "{why}");
    // An empty core module: WebAssembly, but not a component.
    let core = b"\0asm\x01\0\0\0";
    let why = invalid(modules.upload(upload(core, ModuleGrants::default())).await);
    assert!(why.starts_with("not a WebAssembly component"), "{why}");
    let why = invalid(
        modules
            .upload(upload(
                V1,
                ModuleGrants {
                    allowed_hosts: vec!["https://api.example.com/".into()],
                    ..ModuleGrants::default()
                },
            ))
            .await,
    );
    assert!(why.contains("isn't a host"), "{why}");
    let why = invalid(
        modules
            .upload(upload(
                V1,
                ModuleGrants {
                    timeout: Some(Duration::ZERO),
                    ..ModuleGrants::default()
                },
            ))
            .await,
    );
    assert!(why.contains("timeout"), "{why}");

    assert!(modules.modules().await.unwrap().is_empty());
    assert!(modules.handler(KIT, Utc::now()).await.unwrap().is_none());

    db.finish().await;
}

/// A node gets its id, type and inputs; what it logs and prints goes with its error when it
/// fails; and its output must be JSON.
#[tokio::test]
async fn a_node_runs_its_modules_code() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());
    modules
        .upload(upload(V1, ModuleGrants::default()))
        .await
        .unwrap();

    let output = run_node(&modules, &db.pool, json!({ "op": "shout", "text": "hi" }))
        .await
        .unwrap();
    assert_eq!(output["text"], "HI");
    assert_eq!(output["version"], 1);
    assert_eq!(output["input"]["node_id"], "node");
    assert_eq!(output["input"]["node_type"], KIT);
    assert_eq!(output["input"]["inputs"]["text"], "hi");

    let error = failure(run_node(&modules, &db.pool, json!({ "op": "fail" })).await);
    assert_eq!(
        error,
        "asked to fail\nwarn: about to fail\nstderr: failing in v1"
    );

    let error = failure(run_node(&modules, &db.pool, json!({ "op": "json" })).await);
    assert!(error.starts_with("its output isn't JSON"), "{error}");

    db.finish().await;
}

/// A node that runs past its module's timeout is stopped.
#[tokio::test]
async fn a_node_is_stopped_at_its_timeout() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());
    let grants = ModuleGrants {
        timeout: Some(Duration::from_millis(200)),
        ..ModuleGrants::default()
    };
    modules.upload(upload(V1, grants)).await.unwrap();

    let start = Instant::now();
    let error = failure(run_node(&modules, &db.pool, json!({ "op": "spin" })).await);
    assert_eq!(error, "ran longer than its 200ms timeout");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "{:?}",
        start.elapsed()
    );

    db.finish().await;
}

/// A node waiting in WASI (a guest's sleep) is stopped at its timeout too, though the engine
/// can't interrupt it there.
#[tokio::test]
async fn a_node_sleeping_is_stopped_at_its_timeout() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());
    let grants = ModuleGrants {
        timeout: Some(Duration::from_millis(200)),
        ..ModuleGrants::default()
    };
    modules.upload(upload(V1, grants)).await.unwrap();

    let start = Instant::now();
    let error = failure(run_node(&modules, &db.pool, json!({ "op": "sleep" })).await);
    assert_eq!(error, "ran longer than its 200ms timeout");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "{:?}",
        start.elapsed()
    );

    db.finish().await;
}

/// A node that grows its memory past its module's limit fails.
#[tokio::test]
async fn a_node_is_held_to_its_memory_limit() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone()).memory_limit_mb(16);
    modules
        .upload(upload(V1, ModuleGrants::default()))
        .await
        .unwrap();

    let error = failure(run_node(&modules, &db.pool, json!({ "op": "hog" })).await);
    assert!(
        error.starts_with("used more than its 16 MiB of memory\nstderr: memory allocation"),
        "{error}"
    );
    // Within its limit, the module still runs.
    let output = run_node(&modules, &db.pool, json!({ "op": "shout", "text": "ok" }))
        .await
        .unwrap();
    assert_eq!(output["text"], "OK");

    db.finish().await;
}

/// A guest has no files, environment, arguments or sockets.
#[tokio::test]
async fn a_node_reaches_nothing_by_wasi() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());
    modules
        .upload(upload(V1, ModuleGrants::default()))
        .await
        .unwrap();

    let output = run_node(&modules, &db.pool, json!({ "op": "sandbox" }))
        .await
        .unwrap();
    assert_eq!(
        output,
        json!({ "files": false, "env": 0, "args": 0, "socket": false })
    );

    db.finish().await;
}

/// `fetch` reaches the hosts the module was granted, and no others.
#[tokio::test]
async fn a_node_fetches_from_its_granted_hosts_only() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let port = pong().await;
    let url = format!("http://127.0.0.1:{port}/ping");
    let modules = WasmModules::new(db.pool.clone());

    modules
        .upload(upload(V1, ModuleGrants::default()))
        .await
        .unwrap();
    let error = failure(run_node(&modules, &db.pool, json!({ "op": "fetch", "url": url })).await);
    assert_eq!(error, "127.0.0.1 isn't a host this module may reach");

    let grants = ModuleGrants {
        allowed_hosts: vec![format!("127.0.0.1:{port}")],
        ..ModuleGrants::default()
    };
    modules.upload(upload(V1, grants)).await.unwrap();
    let output = run_node(&modules, &db.pool, json!({ "op": "fetch", "url": url }))
        .await
        .unwrap();
    assert_eq!(output, json!({ "status": 200, "body": "pong" }));
    let other_port = format!("http://127.0.0.1:{}/", port.wrapping_add(1));
    let error = failure(
        run_node(
            &modules,
            &db.pool,
            json!({ "op": "fetch", "url": other_port }),
        )
        .await,
    );
    assert!(
        error.contains("isn't a host this module may reach"),
        "{error}"
    );

    db.finish().await;
}

/// Start a run of one node of the module's type, as the admin's Run does. Returns its id.
async fn start_run(pool: &PgPool, text: &str) -> i64 {
    let nodes =
        json!({ "kit": { "type": KIT, "inputs": { "op": "shout", "text": "{{inputs.text}}" } } });
    let definition: i64 = sqlx::query_scalar(
        "INSERT INTO ishikari_workflow_definitions (name, nodes, output_schema) \
         VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(format!("kit {text}"))
    .bind(&nodes)
    .bind(json!({ "text": "{{nodes.kit.text}}", "version": "{{nodes.kit.version}}" }))
    .fetch_one(pool)
    .await
    .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let run: i64 = sqlx::query_scalar(
        "SELECT nextval(pg_get_serial_sequence('ishikari_workflow_runs', 'id'))",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    let job = NodeJob {
        workflow_run_id: run,
        node_id: "kit".into(),
        node_type: KIT.into(),
        raw_inputs: serde_json::from_value(nodes["kit"]["inputs"].clone()).unwrap(),
        condition: None,
        max_attempts: 1,
        timeout_seconds: None,
    };
    let workflow = Dag::new("kit").add("kit", job).run(&mut *tx).await.unwrap();
    sqlx::query(
        "INSERT INTO ishikari_workflow_runs (id, definition_id, definition_name, \
         definition_version, workflow_id, inputs) VALUES ($1, $2, 'kit', 1, $3, $4)",
    )
    .bind(run)
    .bind(definition)
    .bind(workflow.id)
    .bind(json!({ "text": text }))
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    run
}

/// The run's status, outputs and error, once it has finished.
async fn finished(pool: &PgPool, run: i64) -> (String, Value, Option<String>) {
    let start = Instant::now();
    loop {
        let (status, outputs, error): (String, Option<Value>, Option<String>) = sqlx::query_as(
            "SELECT status, outputs, error FROM ishikari_workflow_runs WHERE id = $1",
        )
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

/// The engine runs the admin's runs' nodes of an uploaded type, each at the version uploaded
/// last before its run was created, so a run isn't changed by a later upload.
#[tokio::test]
async fn runs_use_the_version_uploaded_before_them() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = Arc::new(WasmModules::new(db.pool.clone()));
    modules
        .upload(upload(V1, ModuleGrants::default()))
        .await
        .unwrap();
    let before = start_run(&db.pool, "before").await;
    modules
        .upload(upload(V2, ModuleGrants::default()))
        .await
        .unwrap();
    let after = start_run(&db.pool, "after").await;

    let types = NodeTypes::new(db.pool.clone()).modules(modules);
    let engine = Engine::builder("test")
        .stager_interval(Duration::from_millis(20))
        .with_state(Arc::new(NodeJobs::new(types)))
        .with_queue(Queue::builder(NodeJob::QUEUE).interval(Duration::from_millis(20)))
        .start(Postgres::new(db.pool.clone()));

    let (status, outputs, error) = finished(&db.pool, before).await;
    assert_eq!((status.as_str(), error), ("completed", None));
    assert_eq!(outputs, json!({ "text": "BEFORE", "version": 1 }));
    let (status, outputs, error) = finished(&db.pool, after).await;
    assert_eq!((status.as_str(), error), ("completed", None));
    assert_eq!(outputs, json!({ "text": "AFTER", "version": 2 }));

    engine.shutdown(Duration::from_secs(1)).await;
    db.finish().await;
}
