//! The Node types page: the types the host registered in Rust, and the modules its operators
//! upload (`AppState::with_node_modules`).

use std::time::Duration;

use axum::extract::multipart::{Multipart, MultipartRejection};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use chrono::{DateTime, Utc};
use ishikari::execution::{ModuleError, ModuleGrants, ModuleUpload, NodeModule, NodeModules};
use serde::Deserialize;

use crate::templates::NodeTypesTemplate;
use crate::AppState;

/// The largest upload the page takes, module and form together.
pub const MAX_UPLOAD_BYTES: usize = 32 * 1024 * 1024;

/// An uploaded module, as the page lists it.
#[derive(Debug, Clone)]
pub struct ModuleInfo {
    pub name: String,
    pub version: i32,
    pub category: String,
    pub description: Option<String>,
    pub allowed_hosts: Vec<String>,
    /// Its timeout, or `None` for the runtime's.
    pub timeout: Option<String>,
    /// Its memory limit, or `None` for the runtime's.
    pub memory: Option<String>,
    pub size: String,
    /// The start of its sha256.
    pub sha256: String,
    pub uploaded_at: DateTime<Utc>,
    /// Whether a type registered in Rust has its name, and so runs instead.
    pub shadowed: bool,
}

/// What the upload form was given, to show again when it's refused.
#[derive(Debug, Clone, Default)]
pub struct UploadForm {
    pub allowed_hosts: String,
    pub timeout_seconds: String,
    pub memory_limit_mb: String,
}

/// What the page says after an upload.
#[derive(Debug, Deserialize)]
pub struct UploadedQuery {
    pub uploaded: Option<String>,
    pub version: Option<i32>,
    pub unchanged: Option<bool>,
}

/// The Node types page.
pub async fn list(State(state): State<AppState>, Query(query): Query<UploadedQuery>) -> Response {
    let notice = match (query.uploaded, query.version) {
        (Some(name), Some(version)) if query.unchanged == Some(true) => Some(format!(
            "{name} v{version} already has these bytes and grants"
        )),
        (Some(name), Some(version)) => Some(format!("Uploaded {name} v{version}")),
        _ => None,
    };
    page(&state, StatusCode::OK, notice, None, UploadForm::default()).await
}

/// Upload a module, a `multipart/form-data` form of `module` (its bytes), `allowed_hosts` (one
/// per line or comma-separated), `timeout_seconds` and `memory_limit_mb` (blank for the
/// runtime's defaults).
pub async fn upload(
    State(state): State<AppState>,
    multipart: Result<Multipart, MultipartRejection>,
) -> Response {
    let Some(modules) = state.node_modules.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let mut form = UploadForm::default();
    let result = match read(multipart, &mut form).await {
        Ok(bytes) => uploaded(&state, modules.as_ref(), bytes, &form).await,
        Err(refused) => Err(refused),
    };
    match result {
        Ok(location) => Redirect::to(&location).into_response(),
        Err((status, error)) => page(&state, status, None, Some(error), form).await,
    }
}

type Refused = (StatusCode, String);

/// The module's bytes, if any, with the form's other fields read into `form`.
async fn read(
    multipart: Result<Multipart, MultipartRejection>,
    form: &mut UploadForm,
) -> Result<Option<Vec<u8>>, Refused> {
    let mut multipart = multipart.map_err(|e| (e.status(), e.body_text()))?;
    let mut bytes = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| (e.status(), e.body_text()))?
    {
        let name = field.name().unwrap_or_default().to_string();
        if name == "module" {
            let read = field
                .bytes()
                .await
                .map_err(|e| (e.status(), e.body_text()))?;
            bytes = Some(read.to_vec()).filter(|b| !b.is_empty());
            continue;
        }
        let text = field
            .text()
            .await
            .map_err(|e| (e.status(), e.body_text()))?;
        match name.as_str() {
            "allowed_hosts" => form.allowed_hosts = text,
            "timeout_seconds" => form.timeout_seconds = text,
            "memory_limit_mb" => form.memory_limit_mb = text,
            _ => {}
        }
    }
    Ok(bytes)
}

/// Upload `bytes` with the form's grants: where the page goes next, or why it was refused.
async fn uploaded(
    state: &AppState,
    modules: &dyn NodeModules,
    bytes: Option<Vec<u8>>,
    form: &UploadForm,
) -> Result<String, Refused> {
    let bad = |why: &str| (StatusCode::BAD_REQUEST, why.to_string());
    let bytes = bytes.ok_or_else(|| bad("Choose a module to upload"))?;
    let grants = grants(form).map_err(bad)?;

    let spec = modules.describe(&bytes).await.map_err(refused)?;
    if state.node_types.iter().any(|t| t.name == spec.name) {
        return Err(bad(&format!(
            "{} is registered in Rust, which runs instead of any module of that name",
            spec.name
        )));
    }
    let before = modules
        .modules()
        .await
        .map_err(refused)?
        .into_iter()
        .find(|m| m.spec.name == spec.name)
        .map(|m| m.version);
    let module = modules
        .upload(ModuleUpload { bytes, grants })
        .await
        .map_err(refused)?;

    let query = serde_urlencoded::to_string([
        ("uploaded", module.spec.name.clone()),
        ("version", module.version.to_string()),
        ("unchanged", (before == Some(module.version)).to_string()),
    ])
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(format!("{}/node-types?{query}", state.base_path))
}

fn refused(error: ModuleError) -> Refused {
    match error {
        ModuleError::Invalid(why) => (StatusCode::BAD_REQUEST, why),
        other => (StatusCode::INTERNAL_SERVER_ERROR, other.to_string()),
    }
}

/// The grants the form gives, or why they can't be.
fn grants(form: &UploadForm) -> Result<ModuleGrants, &'static str> {
    let allowed_hosts = form
        .allowed_hosts
        .split(['\n', ','])
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(str::to_string)
        .collect();
    let timeout = match form.timeout_seconds.trim() {
        "" => None,
        seconds => Some(
            seconds
                .parse::<f64>()
                .ok()
                .filter(|s| *s > 0.0)
                .and_then(|s| Duration::try_from_secs_f64(s).ok())
                .ok_or("The timeout must be a number of seconds")?,
        ),
    };
    let memory_limit_mb = match form.memory_limit_mb.trim() {
        "" => None,
        mb => Some(
            mb.parse::<u32>()
                .ok()
                .filter(|mb| *mb > 0)
                .ok_or("The memory limit must be a whole number of MiB")?,
        ),
    };
    Ok(ModuleGrants {
        allowed_hosts,
        timeout,
        memory_limit_mb,
    })
}

async fn page(
    state: &AppState,
    status: StatusCode,
    notice: Option<String>,
    mut error: Option<String>,
    form: UploadForm,
) -> Response {
    let modules = match &state.node_modules {
        None => None,
        Some(modules) => match modules.modules().await {
            Ok(modules) => Some(modules.into_iter().map(|m| info(state, m)).collect()),
            Err(e) => {
                error.get_or_insert(format!("Couldn't list the modules: {e}"));
                Some(Vec::new())
            }
        },
    };
    let mut registered = state.node_types.to_vec();
    registered.sort_by(|a, b| a.name.cmp(&b.name));
    let template = NodeTypesTemplate {
        base_path: state.base_path.to_string(),
        nav_items: state.nav_items.to_vec(),
        registered,
        modules,
        notice,
        error,
        form,
    };
    (status, template).into_response()
}

fn info(state: &AppState, module: NodeModule) -> ModuleInfo {
    let NodeModule {
        spec,
        version,
        sha256,
        size,
        grants,
        uploaded_at,
    } = module;
    ModuleInfo {
        shadowed: state.node_types.iter().any(|t| t.name == spec.name),
        name: spec.name,
        version,
        category: spec.category,
        description: spec.description,
        allowed_hosts: grants.allowed_hosts,
        timeout: grants.timeout.map(|t| format!("{}s", t.as_secs_f64())),
        memory: grants.memory_limit_mb.map(|mb| format!("{mb} MiB")),
        size: bytes(size),
        sha256: sha256.chars().take(12).collect(),
        uploaded_at,
    }
}

/// `size` bytes, for people.
fn bytes(size: i64) -> String {
    const KIB: f64 = 1024.0;
    let size = size as f64;
    if size < KIB {
        format!("{size} B")
    } else if size < KIB * KIB {
        format!("{:.1} KiB", size / KIB)
    } else {
        format!("{:.1} MiB", size / (KIB * KIB))
    }
}
