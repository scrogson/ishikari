//! A module is given a placeholder for each of the host's secrets it was granted, never its
//! value: the host puts the value in a request's headers, to the hosts the secret is bound to.
//! What would carry a value out is redacted all the same.

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

/// `KEY` as base64.
const ENCODED_KEY: &str = "c2tfbGl2ZV8wMTIzNDU2Nzg5";

/// The test component, which prints its `text` for `op: "print"`.
const KIT: &[u8] = include_bytes!("fixtures/testkit-v1.wasm");

/// A script named `test/script` using secrets as `inputs.op` says.
const READS: &str = r#"
export const spec = { name: "test/script", category: "test" };
export async function run({ inputs }) {
  switch (inputs.op) {
    case "read":
      return { value: secret(inputs.name) };
    case "leak":
      console.log(`the key is ${secret("api/key")}`);
      return {
        key: secret("api/key"),
        nested: [`Bearer ${secret("api/key")}`],
        encoded: btoa(secret("api/key")),
        reversed: [...secret("api/key")].reverse().join(""),
      };
    case "throw":
      throw new Error(`refused ${secret("api/key")}`);
    case "send": {
      const response = await fetch(inputs.url, {
        method: inputs.body ? "POST" : "GET",
        headers: { authorization: `Bearer ${secret("api/key")}` },
        body: inputs.body ?? undefined,
      });
      return await response.text();
    }
  }
}
"#;

fn upload(secrets: &[&str]) -> ModuleUpload {
    upload_reaching(secrets, vec![])
}

fn upload_reaching(secrets: &[&str], allowed_hosts: Vec<String>) -> ModuleUpload {
    ModuleUpload {
        bytes: READS.as_bytes().to_vec(),
        grants: ModuleGrants {
            secrets: secrets.iter().map(|name| name.to_string()).collect(),
            allowed_hosts,
            ..ModuleGrants::default()
        },
    }
}

fn host_secrets() -> Arc<MemorySecrets> {
    Arc::new(
        MemorySecrets::new()
            .with_hosts("api/key", KEY, &["127.0.0.1"])
            .with("api/other", "not granted"),
    )
}

/// A script's `secret(name)` is a placeholder for the host's secret of that name, when its
/// module was granted it: one it wasn't, or the host has none of, throws.
#[tokio::test]
async fn a_script_is_given_placeholders_for_its_secrets() {
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
        json!({ "value": "{{secret:api/key}}" })
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

/// What a node returns, throws and logs, however it's encoded, holds no secret's value: its
/// script never had one.
#[tokio::test]
async fn a_script_never_has_a_secrets_value() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = WasmModules::new(db.pool.clone()).secrets(host_secrets());
    modules.upload(upload(&["api/key"])).await.unwrap();
    let run = |op: &str| common::run(&modules, &db.pool, SCRIPT, json!({ "op": op }));

    let output = run("leak").await.unwrap();
    assert_eq!(output["key"], "{{secret:api/key}}");
    assert_eq!(output["nested"], json!(["Bearer {{secret:api/key}}"]));
    let text = output.to_string();
    for value in [KEY, ENCODED_KEY, "9876543210_evil_ks"] {
        assert!(!text.contains(value), "{text}");
    }
    let error = failure(run("throw").await);
    assert!(error.contains("refused {{secret:api/key}}"), "{error}");

    let inputs: HashMap<_, _> = [("op".to_string(), json!("leak"))].into();
    let run = modules
        .test_run(upload(&["api/key"]), inputs)
        .await
        .unwrap();
    let output = run.output.unwrap().to_string();
    assert!(
        !output.contains(KEY) && !output.contains(ENCODED_KEY),
        "{output}"
    );
    assert_eq!(run.logs, ["info: the key is {{secret:api/key}}"]);
    assert_eq!(run.secrets, ["api/key"]);

    db.finish().await;
}

/// A script's request carries a secret's value in its headers to a host the secret is bound
/// to; to a host the module may reach that the secret isn't bound to, or in its body, it fails.
#[tokio::test]
async fn a_script_sends_a_secret_to_its_hosts_only() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let port = common::keyed(KEY).await;
    let modules = WasmModules::new(db.pool.clone()).secrets(host_secrets());
    let reaching = vec![format!("127.0.0.1:{port}"), format!("localhost:{port}")];
    modules
        .upload(upload_reaching(&["api/key"], reaching))
        .await
        .unwrap();
    let send = |url: String, body: Option<&str>| {
        common::run(
            &modules,
            &db.pool,
            SCRIPT,
            json!({ "op": "send", "url": url, "body": body }),
        )
    };

    let sent = send(format!("http://127.0.0.1:{port}/"), None)
        .await
        .unwrap();
    assert_eq!(sent, json!("sent"));

    let error = failure(send(format!("http://localhost:{port}/"), None).await);
    assert!(
        error.contains("the secret api/key may not be sent to localhost"),
        "{error}"
    );
    let error = failure(
        send(
            format!("http://127.0.0.1:{port}/"),
            Some("key={{secret:api/key}}"),
        )
        .await,
    );
    assert!(
        error.contains("a secret is sent only in a request's headers"),
        "{error}"
    );

    db.finish().await;
}

/// The values of a component's secrets are redacted from what it prints, a value of several
/// lines whole, and a line cut by the end of what's kept dropped.
#[tokio::test]
async fn secrets_are_redacted_from_what_a_component_prints() {
    const PEM: &str =
        "-----BEGIN PRIVATE KEY-----\nMIIBVgIBADANBgkqhkiG9w0BAQEFAASC\n-----END PRIVATE KEY-----";
    let Some(db) = TestDb::new().await else {
        return;
    };
    let secrets = MemorySecrets::new()
        .with("tls/key", PEM)
        .with("api/key", KEY);
    let modules = WasmModules::new(db.pool.clone()).secrets(Arc::new(secrets));
    let print = |text: String| {
        let upload = ModuleUpload {
            bytes: KIT.to_vec(),
            grants: ModuleGrants {
                secrets: vec!["tls/key".into(), "api/key".into()],
                ..ModuleGrants::default()
            },
        };
        let inputs = [
            ("op".to_string(), json!("print")),
            ("text".to_string(), json!(text)),
        ];
        modules.test_run(upload, inputs.into())
    };

    let run = print(format!("the key is {PEM}")).await.unwrap();
    assert_eq!(run.logs, ["stdout: the key is [redacted]"]);

    // What a stream keeps ends (at 64 KiB) inside the key.
    let run = print(format!("{}\n{}{KEY}", "a".repeat(100), "b".repeat(65_530)))
        .await
        .unwrap();
    assert_eq!(run.logs, [format!("stdout: {}", "a".repeat(100))]);

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
    assert_eq!(run.output, Ok(json!({ "value": "{{secret:api/other}}" })));
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
