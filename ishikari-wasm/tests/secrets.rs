//! A module reads the host's secrets it was granted, by name, from the backend its
//! `WasmModules` was given; their values are redacted from its output, errors and logs.

mod common;

use common::{failure, TestDb};
use ishikari::execution::{ModuleError, ModuleGrants, ModuleUpload, NodeModules};
use ishikari::secrets::{MemorySecrets, Secret, SecretError, Secrets};
use ishikari_wasm::WasmModules;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

const SCRIPT: &str = "test/script";

const KEY: &str = "sk_live_0123456789";

/// A script named `test/script` reading secrets as `inputs.op` says.
const READS: &str = r#"
export const spec = { name: "test/script", category: "test" };
export function run({ inputs }) {
  switch (inputs.op) {
    case "read":
      return { length: secret(inputs.name).length };
    case "leak":
      console.log(`the key is ${secret("api/key")}`);
      return { key: secret("api/key"), nested: [`Bearer ${secret("api/key")}`] };
    case "throw":
      throw new Error(`refused ${secret("api/key")}`);
  }
}
"#;

fn upload(secrets: &[&str]) -> ModuleUpload {
    ModuleUpload {
        bytes: READS.as_bytes().to_vec(),
        grants: ModuleGrants {
            secrets: secrets.iter().map(|name| name.to_string()).collect(),
            ..ModuleGrants::default()
        },
    }
}

fn host_secrets() -> Arc<MemorySecrets> {
    Arc::new(
        MemorySecrets::new()
            .with("api/key", KEY)
            .with("api/other", "not granted"),
    )
}

/// A script's `secret(name)` is the host's secret of that name when its module was granted it:
/// one it wasn't, or the host has none of, throws.
#[tokio::test]
async fn a_script_reads_the_secrets_it_was_granted() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone()).secrets(host_secrets());
    modules
        .upload(upload(&["api/key", "api/missing"]))
        .await
        .unwrap();
    let read = |name: &str| {
        common::run(
            &modules,
            &db.pool,
            SCRIPT,
            json!({ "op": "read", "name": name }),
        )
    };

    assert_eq!(
        read("api/key").await.unwrap(),
        json!({ "length": KEY.len() })
    );
    let error = failure(read("api/other").await);
    assert!(
        error.contains("api/other isn't a secret this module was granted"),
        "{error}"
    );
    let error = failure(read("api/missing").await);
    assert!(error.contains("no secret api/missing"), "{error}");

    db.finish().await;
}

/// The values of a module's secrets are redacted from what its node returns, throws and logs.
#[tokio::test]
async fn secrets_are_redacted() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone()).secrets(host_secrets());
    modules.upload(upload(&["api/key"])).await.unwrap();
    let run = |op: &str| common::run(&modules, &db.pool, SCRIPT, json!({ "op": op }));

    assert_eq!(
        run("leak").await.unwrap(),
        json!({ "key": "[redacted]", "nested": ["Bearer [redacted]"] })
    );
    let error = failure(run("throw").await);
    assert!(error.contains("refused [redacted]"), "{error}");
    assert!(!error.contains(KEY), "{error}");

    let inputs: HashMap<_, _> = [("op".to_string(), json!("leak"))].into();
    let run = modules
        .test_run(upload(&["api/key"]), inputs)
        .await
        .unwrap();
    assert_eq!(
        run.output,
        Ok(json!({ "key": "[redacted]", "nested": ["Bearer [redacted]"] }))
    );
    assert_eq!(run.logs, ["info: the key is [redacted]"]);
    assert_eq!(run.secrets, ["api/key"]);

    db.finish().await;
}

/// A test run says which secrets its node read, of those granted.
#[tokio::test]
async fn a_test_run_says_which_secrets_it_read() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone()).secrets(host_secrets());
    let inputs = |name: &str| -> HashMap<String, serde_json::Value> {
        [
            ("op".to_string(), json!("read")),
            ("name".to_string(), json!(name)),
        ]
        .into()
    };

    let run = modules
        .test_run(upload(&["api/key", "api/other"]), inputs("api/other"))
        .await
        .unwrap();
    assert_eq!(run.output, Ok(json!({ "length": 11 })));
    assert_eq!(run.secrets, ["api/other"]);
    let run = modules
        .test_run(upload(&["api/key"]), inputs("api/other"))
        .await
        .unwrap();
    assert!(run.output.is_err());
    assert!(run.secrets.is_empty());

    db.finish().await;
}

/// A module granted secrets on a host that keeps none can't read them.
#[tokio::test]
async fn a_host_without_secrets_gives_none() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());
    modules.upload(upload(&["api/key"])).await.unwrap();

    let error = failure(
        common::run(
            &modules,
            &db.pool,
            SCRIPT,
            json!({ "op": "read", "name": "api/key" }),
        )
        .await,
    );
    assert!(error.contains("this host keeps no secrets"), "{error}");

    db.finish().await;
}

/// A backend that fails fails the node, before it runs.
#[tokio::test]
async fn a_failing_backend_fails_the_node() {
    struct Down;

    #[async_trait::async_trait]
    impl Secrets for Down {
        async fn get(&self, _name: &str) -> Result<Option<Secret>, SecretError> {
            Err(SecretError::Backend("the vault is sealed".into()))
        }
    }

    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone()).secrets(Arc::new(Down));
    modules.upload(upload(&["api/key"])).await.unwrap();

    let error = failure(
        common::run(
            &modules,
            &db.pool,
            SCRIPT,
            json!({ "op": "read", "name": "api/key" }),
        )
        .await,
    );
    assert_eq!(error, "its secrets couldn't be read: the vault is sealed");

    db.finish().await;
}

/// A module's secrets are names of secrets, trimmed, without blanks or repeats, and kept with its
/// other grants: granting others uploads a new version.
#[tokio::test]
async fn granted_secrets_are_checked_and_kept() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone());

    let refused = modules.upload(upload(&["api/key", "not a name"])).await;
    assert!(
        matches!(&refused, Err(ModuleError::Invalid(why)) if why.contains("\"not a name\" isn't a secret's name")),
        "{refused:?}"
    );

    let module = modules
        .upload(upload(&[" b/key ", "", "a/key", "b/key"]))
        .await
        .unwrap();
    assert_eq!(module.grants.secrets, ["a/key", "b/key"]);
    assert_eq!(module.version, 1);
    let again = modules.upload(upload(&["b/key", "a/key"])).await.unwrap();
    assert_eq!(again, module);
    assert_eq!(modules.modules().await.unwrap(), [module]);

    let next = modules.upload(upload(&["a/key"])).await.unwrap();
    assert_eq!(next.version, 2);
    assert_eq!(next.grants.secrets, ["a/key"]);

    db.finish().await;
}
