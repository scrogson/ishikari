//! The workflow builder ships in the crate: a script the definition editor loads, and the JSON
//! API it saves through.

mod common;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use axum::Router;
use common::{body, get, TestDb};
use ishikari_admin::AppState;
use serde_json::{json, Value};
use tower::ServiceExt;

fn mounted_at_admin() -> Router {
    let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
    Router::new().nest(
        "/admin",
        ishikari_admin::app(AppState::new(pool).with_base_path("/admin")),
    )
}

async fn send(app: &Router, method: &str, path: &str, json: &Value) -> (StatusCode, String) {
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json.to_string()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    (res.status(), body(res).await)
}

/// The builder's script is served under the admin's base path.
#[tokio::test]
async fn the_builders_script_is_served_under_the_base_path() {
    let res = get(&mounted_at_admin(), "/admin/assets/builder.js", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let ct = res.headers()[header::CONTENT_TYPE].to_str().unwrap();
    assert!(ct.contains("javascript"), "{ct}");
    assert!(body(res).await.contains("[data-ik-builder]"));
}

/// The editor for a new definition is the builder's shell, which tells the script where the
/// admin is mounted, and loads it from there.
#[tokio::test]
async fn the_editor_loads_the_builder_under_the_base_path() {
    let res = get(&mounted_at_admin(), "/admin/definitions/new", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let html = body(res).await;
    assert!(
        html.contains(r#"data-ik-builder data-base-path="/admin""#),
        "{html}"
    );
    assert!(!html.contains("data-definition-id"), "{html}");
    assert!(
        html.contains(r#"src="/admin/assets/builder.js?v="#),
        "{html}"
    );
}

/// The builder's old pages (`/builder/...`) go to the editor, under the base path.
#[tokio::test]
async fn the_builders_old_paths_go_to_the_editor() {
    for path in [
        "/admin/builder",
        "/admin/builder/definitions/new",
        "/admin/builder/assets/index.js",
    ] {
        let res = get(&mounted_at_admin(), path, &[]).await;
        assert_eq!(res.status(), StatusCode::SEE_OTHER, "{path}");
        assert_eq!(res.headers()[header::LOCATION], "/admin/definitions/new");
    }
}

/// A definition as the builder saves it (optional fields left out) is created, read back as
/// it was, edited in the builder, and updated.
#[tokio::test]
async fn a_definition_saved_by_the_builder_round_trips() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));
    let mut def = json!({
        "name": "greet",
        "version": 1,
        "inputs": { "name": { "type": "string", "required": true } },
        "nodes": {
            "hello": { "type": "echo", "depends_on": [], "inputs": { "message": "{{inputs.name}}" } },
            "shout": {
                "type": "uppercase",
                "depends_on": ["hello"],
                "inputs": { "text": "{{nodes.hello.message}}" },
                "max_retries": 0
            }
        },
        "outputs": { "result": "{{nodes.shout.text}}" },
        "metadata": {}
    });

    let (status, created) = send(&app, "POST", "/api/definitions", &def).await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let id = serde_json::from_str::<Value>(&created).unwrap()["id"]
        .as_i64()
        .unwrap();

    let html = body(get(&app, &format!("/definitions/{id}/edit"), &[]).await).await;
    assert!(
        html.contains(&format!(r#"data-definition-id="{id}""#)),
        "{html}"
    );

    let read = body(get(&app, &format!("/api/definitions/{id}"), &[]).await).await;
    let read: Value = serde_json::from_str(&read).unwrap();
    assert_eq!(
        read["definition"]["nodes"]["shout"]["depends_on"],
        json!(["hello"])
    );
    assert_eq!(
        read["definition"]["nodes"]["shout"]["max_retries"],
        json!(0)
    );
    assert_eq!(read["definition"]["outputs"], def["outputs"]);

    def["nodes"]["shout"]["when"] = json!("{{inputs.loud}}");
    def["description"] = json!("Says hello");
    let (status, updated) = send(&app, "PUT", &format!("/api/definitions/{id}"), &def).await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    let updated: Value = serde_json::from_str(&updated).unwrap();
    assert_eq!(updated["description"], json!("Says hello"));
    assert_eq!(
        updated["definition"]["nodes"]["shout"]["when"],
        json!("{{inputs.loud}}")
    );

    // A cycle is refused, with a message the builder shows.
    def["nodes"]["hello"]["depends_on"] = json!(["shout"]);
    let (status, error) = send(&app, "PUT", &format!("/api/definitions/{id}"), &def).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(!error.is_empty());

    db.drop().await;
}
