//! The Secrets page: the host's secrets (`AppState::with_secrets`), set and deleted by name,
//! their values never shown, with the hosts modules may send each to and the modules granted
//! each.

use std::collections::BTreeMap;

use axum::extract::rejection::FormRejection;
use axum::extract::{Form, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use chrono::{DateTime, Utc};
use ishikari::secrets::{self, Secret, SecretError, SecretStore};
use serde::Deserialize;

use crate::templates::SecretsTemplate;
use crate::AppState;

/// A secret, as the page lists it: one the host keeps, or one a module was granted that isn't
/// set.
#[derive(Debug, Clone)]
pub struct SecretRow {
    pub name: String,
    /// The hosts modules may send it to.
    pub hosts: Vec<String>,
    /// When its value was last set; `None` when it isn't.
    pub updated_at: Option<DateTime<Utc>>,
    /// The modules whose latest version was granted it.
    pub granted_to: Vec<String>,
}

/// What the page says after a change.
#[derive(Debug, Deserialize)]
pub struct ChangedQuery {
    pub set: Option<String>,
    pub bound: Option<String>,
    pub deleted: Option<String>,
}

/// The form setting a secret: with no value, only its hosts change.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct SetForm {
    pub name: String,
    pub value: String,
    /// The hosts modules may send it to, split by commas or whitespace.
    pub hosts: String,
}

/// The form deleting a secret.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct DeleteForm {
    pub name: String,
}

type Refused = (StatusCode, String);

/// The Secrets page.
pub async fn list(State(state): State<AppState>, Query(query): Query<ChangedQuery>) -> Response {
    let Some(store) = state.secrets.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let notice = match (query.set, query.bound, query.deleted) {
        (Some(name), _, _) => Some(format!("Set {name}")),
        (_, Some(name), _) => Some(format!("Set the hosts of {name}")),
        (_, _, Some(name)) => Some(format!("Deleted {name}")),
        _ => None,
    };
    page(
        &state,
        store.as_ref(),
        StatusCode::OK,
        notice,
        None,
        Kept::default(),
    )
    .await
}

/// Set a secret, an `application/x-www-form-urlencoded` [`SetForm`]: its value without the
/// whitespace around it, or with none, only its hosts.
pub async fn set(
    State(state): State<AppState>,
    form: Result<Form<SetForm>, FormRejection>,
) -> Response {
    let Some(store) = state.secrets.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let form = match form {
        Ok(Form(form)) => form,
        Err(rejection) => return (rejection.status(), rejection.body_text()).into_response(),
    };
    let name = form.name.trim().to_string();
    let hosts: Vec<String> = form
        .hosts
        .split(|c: char| c == ',' || c.is_whitespace())
        .map(str::to_string)
        .collect();
    match setting(store.as_ref(), &name, &form.value, &hosts).await {
        Ok(done) => changed(&state, done, &name),
        Err((status, error)) => {
            let kept = Kept {
                name,
                hosts: form.hosts,
            };
            page(&state, store.as_ref(), status, None, Some(error), kept).await
        }
    }
}

/// Set the secret `name`, or with no `value`, its hosts: which was done.
async fn setting(
    store: &dyn SecretStore,
    name: &str,
    value: &str,
    hosts: &[String],
) -> Result<&'static str, Refused> {
    let bad = |why: String| (StatusCode::BAD_REQUEST, why);
    if name.is_empty() {
        return Err(bad("Name the secret".into()));
    }
    secrets::check_name(name).map_err(|e| bad(e.to_string()))?;
    let hosts = secrets::check_hosts(hosts).map_err(|e| bad(e.to_string()))?;
    let value = value.trim();
    if value.is_empty() {
        return match store.bind(name, &hosts).await.map_err(refused)? {
            true => Ok("bound"),
            false => Err(bad("Enter its value".into())),
        };
    }
    store
        .set(name, Secret::from(value), &hosts)
        .await
        .map_err(refused)?;
    Ok("set")
}

/// Delete a secret, an `application/x-www-form-urlencoded` [`DeleteForm`].
pub async fn delete(
    State(state): State<AppState>,
    form: Result<Form<DeleteForm>, FormRejection>,
) -> Response {
    let Some(store) = state.secrets.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let name = match form {
        Ok(Form(form)) => form.name.trim().to_string(),
        Err(rejection) => return (rejection.status(), rejection.body_text()).into_response(),
    };
    let result = match store.delete(&name).await {
        Ok(true) => Ok(()),
        Ok(false) => Err((StatusCode::NOT_FOUND, format!("No secret {name}"))),
        Err(e) => Err(refused(e)),
    };
    match result {
        Ok(()) => changed(&state, "deleted", &name),
        Err((status, error)) => {
            page(
                &state,
                store.as_ref(),
                status,
                None,
                Some(error),
                Kept::default(),
            )
            .await
        }
    }
}

fn refused(error: SecretError) -> Refused {
    match error {
        SecretError::InvalidName(_) | SecretError::InvalidHost(_) => {
            (StatusCode::BAD_REQUEST, error.to_string())
        }
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

/// Back to the page, which says what was `done` to `name`.
fn changed(state: &AppState, done: &str, name: &str) -> Response {
    match serde_urlencoded::to_string([(done, name)]) {
        Ok(query) => Redirect::to(&format!("{}/secrets?{query}", state.base_path)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// What the form keeps when it's refused: never the value.
#[derive(Default)]
struct Kept {
    name: String,
    hosts: String,
}

async fn page(
    state: &AppState,
    store: &dyn SecretStore,
    status: StatusCode,
    notice: Option<String>,
    mut error: Option<String>,
    kept: Kept,
) -> Response {
    let mut rows = BTreeMap::new();
    match store.list().await {
        Ok(kept) => {
            for info in kept {
                let row = SecretRow {
                    name: info.name.clone(),
                    hosts: info.hosts,
                    updated_at: Some(info.updated_at),
                    granted_to: Vec::new(),
                };
                rows.insert(info.name, row);
            }
        }
        Err(e) => {
            error.get_or_insert(format!("Couldn't list the secrets: {e}"));
        }
    }
    if let Some(modules) = &state.node_modules {
        match modules.modules().await {
            Ok(modules) => {
                for module in modules {
                    for granted in module.grants.secrets {
                        let row = rows.entry(granted.clone()).or_insert_with(|| SecretRow {
                            name: granted,
                            hosts: Vec::new(),
                            updated_at: None,
                            granted_to: Vec::new(),
                        });
                        row.granted_to.push(module.spec.name.clone());
                    }
                }
            }
            Err(e) => {
                error.get_or_insert(format!("Couldn't list the modules: {e}"));
            }
        }
    }
    let template = SecretsTemplate {
        base_path: state.base_path.to_string(),
        nav_items: state.nav_items.to_vec(),
        secrets: rows.into_values().collect(),
        modules: state.node_modules.is_some(),
        notice,
        error,
        name: kept.name,
        hosts: kept.hosts,
    };
    (status, template).into_response()
}
