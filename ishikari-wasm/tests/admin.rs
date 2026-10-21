//! Through the admin (`AppState::with_node_modules`), an operator uploads a component, or writes
//! a script in the editor, which the builder then offers; and sets the secrets modules read
//! (`AppState::with_secrets`).

mod common;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use common::TestDb;
use ishikari::secrets::{PostgresSecrets, SecretKey};
use ishikari_admin::AppState;
use ishikari_wasm::WasmModules;
use serde_json::Value;
use std::sync::Arc;
use tower::ServiceExt;

const V1: &[u8] = include_bytes!("fixtures/testkit-v1.wasm");

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, String, Option<String>) {
    let res = app.clone().oneshot(request).await.unwrap();
    let status = res.status();
    let location = res
        .headers()
        .get(header::LOCATION)
        .map(|l| l.to_str().unwrap().to_string());
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (
        status,
        String::from_utf8_lossy(&bytes).into_owned(),
        location,
    )
}

fn get(path: &str) -> Request<Body> {
    Request::get(path).body(Body::empty()).unwrap()
}

/// `module` with `allowed_hosts`, as the upload form posts them.
fn upload(module: &[u8], allowed_hosts: &str) -> Request<Body> {
    let boundary = "ik-test-boundary";
    let mut form = Vec::new();
    form.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"module\"; \
             filename=\"testkit.wasm\"\r\nContent-Type: application/wasm\r\n\r\n"
        )
        .as_bytes(),
    );
    form.extend_from_slice(module);
    form.extend_from_slice(
        format!(
            "\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"allowed_hosts\"\r\n\r\n\
             {allowed_hosts}\r\n--{boundary}--\r\n"
        )
        .as_bytes(),
    );
    Request::post("/admin/node-types")
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(form))
        .unwrap()
}

#[tokio::test]
async fn an_operator_uploads_a_component_the_builder_offers() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = Arc::new(WasmModules::new(db.pool.clone()));
    let state = AppState::new(db.pool.clone())
        .with_base_path("/admin")
        .with_node_modules(modules);
    let app = Router::new().nest("/admin", ishikari_admin::app(state));

    // A core module: WebAssembly, but not a component.
    let (status, page, _) = send(&app, upload(b"\0asm\x01\0\0\0", "")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(page.contains("not a WebAssembly component"), "{page}");

    let (status, _, location) = send(&app, upload(V1, "api.example.com")).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let location = location.unwrap();
    let (status, page, _) = send(&app, get(&location)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(page.contains("Uploaded test/kit v1"), "{page}");
    assert!(page.contains("test module, v1"), "{page}");
    assert!(page.contains("api.example.com"));

    let (status, types, _) = send(&app, get("/admin/api/node-types")).await;
    assert_eq!(status, StatusCode::OK);
    let types: Value = serde_json::from_str(&types).unwrap();
    assert_eq!(types["node_types"][0]["name"], "test/kit");
    assert_eq!(
        types["node_types"][0]["input_schema"]["op"]["required"],
        true
    );

    db.finish().await;
}

/// POST `fields` as the script editor's form.
fn script(fields: &[(&str, &str)]) -> Request<Body> {
    Request::post("/admin/node-types/script")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from(serde_urlencoded::to_string(fields).unwrap()))
        .unwrap()
}

/// What the page's textarea `name` holds, unescaped.
fn textarea(page: &str, name: &str) -> String {
    let start = page.find(&format!(r#"name="{name}""#)).unwrap();
    let start = start + page[start..].find('>').unwrap() + 1;
    let end = start + page[start..].find("</textarea>").unwrap();
    page[start..end]
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&#34;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

const SHOUT: &str = r#"export const spec = {
  name: "test/shout",
  category: "test",
  input_schema: { text: { type: "string", required: true } },
};

interface Node {
  inputs: { text: string };
}

export function run({ inputs }: Node): { text: string } {
  console.log("shouting");
  return { text: inputs.text.toUpperCase() };
}
"#;

#[tokio::test]
async fn an_operator_writes_a_script_in_the_editor() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let modules = Arc::new(WasmModules::new(db.pool.clone()));
    let state = AppState::new(db.pool.clone())
        .with_base_path("/admin")
        .with_node_modules(modules);
    let app = Router::new().nest("/admin", ishikari_admin::app(state));

    // The editor's example is a node type, which runs with the example's test inputs.
    let (status, page, _) = send(&app, get("/admin/node-types/script")).await;
    assert_eq!(status, StatusCode::OK);
    let example = textarea(&page, "source");
    let inputs = textarea(&page, "inputs");
    let (status, page, _) = send(
        &app,
        script(&[
            ("source", &example),
            ("inputs", &inputs),
            ("action", "test"),
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert!(page.contains("Output"), "{page}");
    assert!(!page.contains("Failed"), "{page}");

    let (status, page, _) = send(
        &app,
        script(&[
            ("source", SHOUT),
            ("inputs", r#"{"text": "hey"}"#),
            ("action", "test"),
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(page.contains("Test run of test/shout"), "{page}");
    assert!(page.contains("HEY"), "{page}");
    assert!(page.contains("info: shouting"), "{page}");

    let (status, _, location) = send(
        &app,
        script(&[
            ("source", SHOUT),
            ("allowed_hosts", "api.example.com"),
            ("action", "save"),
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let (_, page, _) = send(&app, get(&location.unwrap())).await;
    assert!(page.contains("Uploaded test/shout v1"), "{page}");
    let edit = "/admin/node-types/script?module=test%2Fshout";
    assert!(page.contains(edit), "{page}");

    let (status, page, _) = send(&app, get(edit)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(textarea(&page, "source"), SHOUT);
    assert!(page.contains("api.example.com"), "{page}");

    db.finish().await;
}

const PAYS: &str = r#"export const spec = { name: "test/pays", category: "test" };

export function run() {
  const key = secret("stripe/api-key");
  console.log(`paying with ${key}`);
  return { key, length: key.length };
}
"#;

/// An operator sets a secret on the Secrets page, which a script granted it reads, its value
/// redacted from what the test run shows; the page lists the module as granted it.
#[tokio::test]
async fn an_operator_sets_a_secret_a_script_reads() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let secrets = Arc::new(PostgresSecrets::new(
        db.pool.clone(),
        SecretKey::new("test", [7; 32]),
    ));
    let modules = Arc::new(WasmModules::new(db.pool.clone()).secrets(secrets.clone()));
    let state = AppState::new(db.pool.clone())
        .with_base_path("/admin")
        .with_node_modules(modules)
        .with_secrets(secrets);
    let app = Router::new().nest("/admin", ishikari_admin::app(state));

    let set = Request::post("/admin/secrets")
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Body::from("name=stripe%2Fapi-key&value=sk_live_0123456789"))
        .unwrap();
    let (status, _, location) = send(&app, set).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(
        location.as_deref(),
        Some("/admin/secrets?set=stripe%2Fapi-key")
    );

    let (status, page, _) = send(
        &app,
        script(&[
            ("source", PAYS),
            ("secrets", "stripe/api-key"),
            ("action", "test"),
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{page}");
    for text in [
        "[redacted]",
        "info: paying with [redacted]",
        "Secrets read",
        "stripe/api-key",
    ] {
        assert!(page.contains(text), "the test run shows {text:?}: {page}");
    }
    assert!(!page.contains("0123456789"), "{page}");

    let (status, _, _) = send(
        &app,
        script(&[
            ("source", PAYS),
            ("secrets", "stripe/api-key"),
            ("action", "save"),
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let (status, page, _) = send(&app, get("/admin/secrets")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(page.contains("test/pays"), "{page}");
    assert!(!page.contains("0123456789"), "{page}");

    db.finish().await;
}
