//! Scripts, node types written in JavaScript, are uploaded and versioned as components are, and
//! run by the JavaScript runtime ishikari-wasm ships, in the same sandbox and within the same
//! limits.

mod common;

use common::{failure, pong, TestDb};
use ishikari::execution::{ModuleError, ModuleGrants, ModuleUpload, NodeModules};
use ishikari_wasm::WasmModules;
use serde_json::json;
use std::time::{Duration, Instant};

const GREET: &str = r#"
export const spec = {
  name: "test/greet",
  category: "test",
  description: "Greets someone",
  input_schema: { name: { type: "string", required: true } },
  output_schema: { greeting: { type: "string" } },
};

export async function run({ node_type, inputs, workflow_inputs }) {
  console.log(`greeting ${inputs.name}`);
  return { greeting: `hello ${inputs.name}`, node_type, workflow_inputs };
}
"#;

const SCRIPT: &str = "test/script";

fn upload(source: &str, grants: ModuleGrants) -> ModuleUpload {
    ModuleUpload {
        bytes: source.as_bytes().to_vec(),
        grants,
    }
}

/// A script named `test/script` whose `run` (async, given `inputs`) is `body`.
fn running(body: &str) -> String {
    format!(
        "export const spec = {{ name: {SCRIPT:?}, category: \"test\" }};\n\
         export async function run({{ inputs }}) {{\n{body}\n}}\n"
    )
}

/// A script describes itself by its `spec` export, is versioned as components are, and runs its
/// `run` export with the node's input.
#[tokio::test]
async fn a_script_is_a_node_type() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());

    let spec = modules.describe(GREET.as_bytes()).await.unwrap();
    assert_eq!(spec.name, "test/greet");
    assert_eq!(spec.category, "test");
    assert_eq!(spec.description.as_deref(), Some("Greets someone"));
    assert!(spec.input_schema["name"].required);

    let module = modules
        .upload(upload(GREET, ModuleGrants::default()))
        .await
        .unwrap();
    assert_eq!(module.spec, spec);
    assert_eq!(module.version, 1);
    assert_eq!(module.size, GREET.len() as i64);
    assert_eq!(module.source.as_deref(), Some(GREET));
    let again = modules
        .upload(upload(GREET, ModuleGrants::default()))
        .await
        .unwrap();
    assert_eq!(again, module);
    assert_eq!(modules.modules().await.unwrap(), [module]);

    let output = common::run(&modules, &db.pool, "test/greet", json!({ "name": "Ada" }))
        .await
        .unwrap();
    assert_eq!(
        output,
        json!({
            "greeting": "hello Ada",
            "node_type": "test/greet",
            "workflow_inputs": {}
        })
    );

    db.finish().await;
}

/// A script that doesn't evaluate, or doesn't describe a node type, is refused, as is what's
/// neither a component nor text; nothing is kept.
#[tokio::test]
async fn scripts_that_arent_node_types_are_refused() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());
    fn refused<T: std::fmt::Debug>(result: Result<T, ModuleError>) -> String {
        match result {
            Err(ModuleError::Invalid(why)) => why,
            other => panic!("expected the script to be refused, got {other:?}"),
        }
    }

    let why = refused(modules.describe(b"export const spec = {").await);
    assert!(why.starts_with("its spec failed: SyntaxError"), "{why}");
    let why = refused(modules.describe(b"export function run() {}").await);
    assert_eq!(why, "its spec failed: the script exports no `spec`");
    let why = refused(
        modules
            .describe(b"throw new RangeError('broken'); export const spec = {};")
            .await,
    );
    assert_eq!(why, "its spec failed: RangeError: broken");
    let why = refused(
        modules
            .describe(b"export const spec = { name: ' ', category: 'test' };")
            .await,
    );
    assert_eq!(why, "its spec has no name");
    let why = refused(modules.describe(&[0xff, 0xfe, 0x00, 0x01]).await);
    assert_eq!(
        why,
        "neither a WebAssembly component nor a script (UTF-8 text)"
    );
    let why = refused(
        modules
            .upload(upload("export const spec = {", ModuleGrants::default()))
            .await,
    );
    assert!(why.starts_with("its spec failed: SyntaxError"), "{why}");

    assert!(modules.modules().await.unwrap().is_empty());

    db.finish().await;
}

/// What a script throws, or the promise it returns rejects with, fails its node, with what it
/// logged.
#[tokio::test]
async fn a_failing_script_fails_its_node() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());
    let source = running(
        r#"
        switch (inputs.op) {
          case "throw":
            console.log("about to fail");
            throw new TypeError("asked to fail");
          case "reject":
            return Promise.reject(new Error("rejected"));
          case "string":
            throw "plain";
          case "undefined":
            return undefined;
          case "bigint":
            return { n: 1n };
        }
        "#,
    );
    modules
        .upload(upload(&source, ModuleGrants::default()))
        .await
        .unwrap();
    let run = |op: &str| common::run(&modules, &db.pool, SCRIPT, json!({ "op": op }));

    // With where it was thrown: the script's line and column.
    let error = failure(run("throw").await);
    assert!(
        error.starts_with("TypeError: asked to fail (script:")
            && error.ends_with(")\ninfo: about to fail"),
        "{error}"
    );
    let error = failure(run("reject").await);
    assert!(error.starts_with("Error: rejected (script:"), "{error}");
    assert_eq!(failure(run("string").await), "plain");
    assert_eq!(run("undefined").await.unwrap(), json!(null));
    let error = failure(run("bigint").await);
    assert!(error.starts_with("TypeError"), "{error}");

    let spec_only = "export const spec = { name: 'test/script', category: 'test' };";
    modules
        .upload(upload(spec_only, ModuleGrants::default()))
        .await
        .unwrap();
    assert_eq!(
        failure(run("throw").await),
        "the script exports no `run` function"
    );

    db.finish().await;
}

/// A script spinning, or waiting on a timer, is stopped at its timeout; a timer due before it
/// fires.
#[tokio::test]
async fn a_script_is_stopped_at_its_timeout() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());
    let source = running(
        r#"
        const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
        switch (inputs.op) {
          case "spin":
            while (true) {}
          case "wait":
            await wait(inputs.ms);
            return "waited";
        }
        "#,
    );
    let grants = ModuleGrants {
        timeout: Some(Duration::from_millis(300)),
        ..ModuleGrants::default()
    };
    modules.upload(upload(&source, grants)).await.unwrap();
    let run = |inputs| common::run(&modules, &db.pool, SCRIPT, inputs);

    assert_eq!(
        run(json!({ "op": "wait", "ms": 20 })).await.unwrap(),
        json!("waited")
    );
    for inputs in [json!({ "op": "spin" }), json!({ "op": "wait", "ms": 5000 })] {
        let start = Instant::now();
        assert_eq!(
            failure(run(inputs).await),
            "ran longer than its 300ms timeout"
        );
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "{:?}",
            start.elapsed()
        );
    }

    db.finish().await;
}

/// A script growing its memory past its module's limit fails.
#[tokio::test]
async fn a_script_is_held_to_its_memory_limit() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone()).memory_limit_mb(16);
    let source = running(
        r#"
        const chunks = [];
        for (let i = 0; i < inputs.chunks; i++) chunks.push(new Array(1024 * 1024).fill(i));
        return chunks.length;
        "#,
    );
    modules
        .upload(upload(&source, ModuleGrants::default()))
        .await
        .unwrap();

    let error = failure(common::run(&modules, &db.pool, SCRIPT, json!({ "chunks": 64 })).await);
    assert!(
        error.starts_with("used more than its 16 MiB of memory"),
        "{error}"
    );
    let output = common::run(&modules, &db.pool, SCRIPT, json!({ "chunks": 0 }))
        .await
        .unwrap();
    assert_eq!(output, json!(0));

    db.finish().await;
}

/// A script's `fetch` reaches the hosts its module was granted, and no others.
#[tokio::test]
async fn a_script_fetches_from_its_granted_hosts_only() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let port = pong().await;
    let url = format!("http://127.0.0.1:{port}/ping");
    let modules = WasmModules::new(db.pool.clone());
    let source = running(
        r#"
        const response = await fetch(inputs.url, { headers: { "x-script": "1" } });
        return { status: response.status, ok: response.ok, body: await response.text() };
        "#,
    );

    modules
        .upload(upload(&source, ModuleGrants::default()))
        .await
        .unwrap();
    let error = failure(common::run(&modules, &db.pool, SCRIPT, json!({ "url": url })).await);
    assert!(
        error.starts_with("TypeError: fetch") && error.contains("127.0.0.1 isn't a host"),
        "{error}"
    );

    let grants = ModuleGrants {
        allowed_hosts: vec![format!("127.0.0.1:{port}")],
        ..ModuleGrants::default()
    };
    modules.upload(upload(&source, grants)).await.unwrap();
    let output = common::run(&modules, &db.pool, SCRIPT, json!({ "url": url }))
        .await
        .unwrap();
    assert_eq!(output, json!({ "status": 200, "ok": true, "body": "pong" }));

    db.finish().await;
}
