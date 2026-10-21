//! With the host's secrets (`AppState::with_secrets`), operators set and delete them on the
//! Secrets page, which lists them by name, never with their values, and which modules were
//! granted each.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{header, StatusCode};
use axum::Router;
use chrono::{DateTime, Utc};
use common::{body, get, post_form};
use ishikari::execution::{
    ModuleError, ModuleGrants, ModuleUpload, NodeHandler, NodeModule, NodeModules, NodeSpec,
    TestRun,
};
use ishikari::secrets::{MemorySecrets, Secret, SecretStore, Secrets};
use ishikari_admin::AppState;

/// Modules that are only listed: `test/kit`, granted `stripe/api-key` and `slack/token`.
struct Listed;

#[async_trait::async_trait]
impl NodeModules for Listed {
    async fn handler(
        &self,
        _node_type: &str,
        _as_of: DateTime<Utc>,
    ) -> Result<Option<Arc<dyn NodeHandler>>, ModuleError> {
        Ok(None)
    }

    async fn modules(&self) -> Result<Vec<NodeModule>, ModuleError> {
        Ok(vec![NodeModule {
            spec: NodeSpec::new("test/kit"),
            version: 1,
            sha256: "00".into(),
            size: 1,
            source: None,
            grants: ModuleGrants {
                secrets: vec!["slack/token".into(), "stripe/api-key".into()],
                ..ModuleGrants::default()
            },
            uploaded_at: Utc::now(),
        }])
    }

    async fn describe(&self, _bytes: &[u8]) -> Result<NodeSpec, ModuleError> {
        Err(ModuleError::Invalid("only listed".into()))
    }

    async fn upload(&self, _upload: ModuleUpload) -> Result<NodeModule, ModuleError> {
        Err(ModuleError::Invalid("only listed".into()))
    }

    async fn test_run(
        &self,
        _upload: ModuleUpload,
        _inputs: HashMap<String, serde_json::Value>,
    ) -> Result<TestRun, ModuleError> {
        Err(ModuleError::Invalid("only listed".into()))
    }
}

/// The admin at `/admin` with `secrets`, if any, and [`Listed`] modules. Nothing here reaches
/// the database.
fn admin(secrets: Option<Arc<MemorySecrets>>) -> Router {
    let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
    let mut state = AppState::new(pool)
        .with_base_path("/admin")
        .with_node_modules(Arc::new(Listed));
    if let Some(secrets) = secrets {
        state = state.with_secrets(secrets);
    }
    Router::new().nest("/admin", ishikari_admin::app(state))
}

fn location(res: &axum::response::Response) -> String {
    res.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .to_string()
}

/// The page lists each secret by name, with the modules granted it, and the names modules were
/// granted that aren't set; never a value. The navigation links to it.
#[tokio::test]
async fn the_secrets_page_lists_names_never_values() {
    let secrets = Arc::new(
        MemorySecrets::new()
            .with("stripe/api-key", "sk_live_0123456789")
            .with("unused", "u_0123456789"),
    );
    let app = admin(Some(secrets));

    let res = get(&app, "/admin/secrets", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let page = body(res).await;
    assert!(page.contains(r#"href="/admin/secrets""#), "nav link");
    for text in [
        "stripe/api-key",
        "unused",
        "test/kit",
        "slack/token",
        "Not set",
        r#"action="/admin/secrets""#,
        r#"action="/admin/secrets/delete""#,
    ] {
        assert!(page.contains(text), "the page shows {text:?}: {page}");
    }
    assert!(!page.contains("0123456789"), "{page}");
}

/// A secret is set from the page, its surrounding whitespace dropped, set again to replace it,
/// and deleted.
#[tokio::test]
async fn secrets_are_set_and_deleted() {
    let secrets = Arc::new(MemorySecrets::new());
    let app = admin(Some(secrets.clone()));

    let res = post_form(
        &app,
        "/admin/secrets",
        &[("name", " api/key "), ("value", "  v_first_0123\r\n")],
    )
    .await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(&res), "/admin/secrets?set=api%2Fkey");
    assert_eq!(
        secrets.get("api/key").await.unwrap(),
        Some(Secret::from("v_first_0123"))
    );
    let page = body(get(&app, &location(&res), &[]).await).await;
    assert!(page.contains("Set api/key"), "{page}");
    assert!(!page.contains("v_first_0123"), "{page}");

    let res = post_form(
        &app,
        "/admin/secrets",
        &[("name", "api/key"), ("value", "second")],
    )
    .await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        secrets.get("api/key").await.unwrap(),
        Some(Secret::from("second"))
    );

    let res = post_form(&app, "/admin/secrets/delete", &[("name", "api/key")]).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(&res), "/admin/secrets?deleted=api%2Fkey");
    assert_eq!(secrets.get("api/key").await.unwrap(), None);
    let page = body(get(&app, &location(&res), &[]).await).await;
    assert!(page.contains("Deleted api/key"), "{page}");
}

/// What can't be set or deleted is refused with why, the form keeping the name but never the
/// value.
#[tokio::test]
async fn a_secret_refused_says_why() {
    let secrets = Arc::new(MemorySecrets::new());
    let app = admin(Some(secrets.clone()));

    let cases: [(&[(&str, &str)], &str); 3] = [
        (
            &[("name", ""), ("value", "v_0123456789")],
            "Name the secret",
        ),
        (
            &[("name", "not a name"), ("value", "v_0123456789")],
            "isn&#39;t a secret&#39;s name",
        ),
        (
            &[("name", "api/key"), ("value", " \r\n")],
            "Enter its value",
        ),
    ];
    for (form, why) in cases {
        let res = post_form(&app, "/admin/secrets", form).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{form:?}");
        let page = body(res).await;
        assert!(page.contains(why), "{why:?}: {page}");
        assert!(!page.contains("0123456789"), "{page}");
        if form[0].1 == "not a name" {
            assert!(page.contains(r#"value="not a name""#), "{page}");
        }
    }
    assert!(secrets.list().await.unwrap().is_empty());

    let res = post_form(&app, "/admin/secrets/delete", &[("name", "api/key")]).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(body(res).await.contains("No secret api/key"));
}

/// Without secrets there's no page, nothing to set, and nothing in the navigation.
#[tokio::test]
async fn without_secrets_there_is_no_page() {
    let app = admin(None);

    assert_eq!(
        get(&app, "/admin/secrets", &[]).await.status(),
        StatusCode::NOT_FOUND
    );
    let res = post_form(
        &app,
        "/admin/secrets",
        &[("name", "api/key"), ("value", "v")],
    )
    .await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let res = post_form(&app, "/admin/secrets/delete", &[("name", "api/key")]).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let page = body(get(&app, "/admin/", &[]).await).await;
    assert!(!page.contains(r#"href="/admin/secrets""#), "{page}");
}
