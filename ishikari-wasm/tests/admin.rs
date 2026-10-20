//! Through the admin (`AppState::with_node_modules`), an operator uploads a component, which the
//! builder then offers.

mod common;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use common::TestDb;
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

    let (status, page, _) = send(&app, upload(b"not a component", "")).await;
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
