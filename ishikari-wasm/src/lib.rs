//! Node types for ishikari's workflow definitions uploaded by the host's operators through the
//! admin, as WebAssembly components or as scripts, and run in a sandbox.
//!
//! [`WasmModules`] keeps the modules in Postgres (`ishikari_node_modules`, schemas v08 and v09)
//! and is the host's [`NodeModules`]: give it to [`NodeTypes::modules`] to run the admin's runs'
//! nodes of uploaded types, and to `ishikari_admin::AppState::with_node_modules` to upload them
//! and offer them in the builder.
//!
//! ```rust,no_run
//! use ishikari::execution::{NodeJob, NodeJobs, NodeTypes};
//! use ishikari::{Engine, Postgres, Queue};
//! use ishikari_wasm::WasmModules;
//! use std::sync::Arc;
//!
//! # fn f(pool: sqlx::PgPool) {
//! let modules = Arc::new(WasmModules::new(pool.clone()));
//! let types = NodeTypes::new(pool.clone()).modules(modules.clone());
//! let engine = Engine::builder("definitions")
//!     .with_state(Arc::new(NodeJobs::new(types)))
//!     .with_queue(Queue::builder(NodeJob::QUEUE))
//!     .start(Postgres::new(pool));
//! # }
//! ```
//!
//! A module is a component of the `ishikari:node` world (`wit/node.wit`, [`WIT`]): it exports
//! `spec`, the node type as JSON, and `run`, which takes a node's inputs as JSON and gives its
//! output. It reaches nothing but what its upload granted ([`ModuleGrants`]): the hosts it may
//! send HTTP requests to through `host.fetch`, and the host's secrets it may read through
//! `host.secret` (from the backend given to [`WasmModules::secrets`]; their values are redacted
//! from what its nodes return, fail with and log), within its time and memory limits. WASI is
//! linked for guests' standard libraries, with no files, environment, arguments or sockets;
//! stdout and stderr go to the node's logs with `host.log`.
//!
//! Or a module is a script, uploaded as text: an ES module in TypeScript or JavaScript exporting
//! `spec` (the node type, as an object) and `run` (from the node's input to its output, or a
//! promise of it), run by the JavaScript runtime this crate ships (a component of the
//! `script-node` world, with Boa) in the same sandbox. Its types are stripped where they stand,
//! so what it throws is placed in its TypeScript. `console` goes to the node's logs, `fetch`
//! through `host.fetch`, and `secret(name)` (which throws when there's none it may read) is
//! `host.secret`.
//!
//! ```js
//! export const spec = {
//!   name: "greet",
//!   category: "text",
//!   input_schema: { name: { type: "string", required: true } },
//!   output_schema: { greeting: { type: "string" } },
//! };
//!
//! export async function run({ inputs }) {
//!   return { greeting: `hello ${inputs.name}` };
//! }
//! ```
//!
//! [`NodeModules`]: ishikari::execution::NodeModules
//! [`NodeTypes::modules`]: ishikari::execution::NodeTypes::modules

mod output;
mod redact;
mod runtime;
mod sandbox;
mod typescript;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ishikari::execution::{
    ModuleError, ModuleGrants, ModuleUpload, NodeContext, NodeData, NodeError, NodeHandler,
    NodeModule, NodeModules, NodeSpec, TestRun,
};
use ishikari::secrets::{self, Secrets};
use sha2::{Digest, Sha256};
use sqlx::types::Json;
use sqlx::PgPool;

use redact::Redact;
use runtime::{Call, Guest, Kind, Limits, Runtime};
use sandbox::Sandbox;

/// The `ishikari:node` world a module implements.
pub const WIT: &str = include_str!("../wit/node.wit");

/// How long a call to a module's `spec` may take when it's uploaded.
const SPEC_TIMEOUT: Duration = Duration::from_secs(5);

/// The lines of a failed node's logs its error keeps.
const LOG_LINES_IN_ERRORS: usize = 20;

/// Modules (WebAssembly components and scripts) uploaded as node types, kept in
/// `ishikari_node_modules`.
#[derive(Clone)]
pub struct WasmModules {
    pool: PgPool,
    schema: Option<String>,
    runtime: Arc<Runtime>,
    timeout: Duration,
    memory_limit_mb: u32,
    secrets: Option<Arc<dyn Secrets>>,
}

impl WasmModules {
    /// Modules kept in `pool`'s search path; a node may run for 30s and use 64 MiB unless its
    /// module was granted otherwise. Every `WasmModules` of the process shares one engine, and
    /// what it has compiled.
    ///
    /// # Panics
    ///
    /// When the WebAssembly engine can't be set up (the platform has no compiler for it).
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: None,
            runtime: Runtime::shared(),
            timeout: Duration::from_secs(30),
            memory_limit_mb: 64,
            secrets: None,
        }
    }

    /// Keep modules in `schema`, as the engine's `Postgres` keeps jobs.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// How long a node may run when its module wasn't granted a timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// How much memory a node may use, in MiB, when its module wasn't granted a limit.
    pub fn memory_limit_mb(mut self, memory_limit_mb: u32) -> Self {
        self.memory_limit_mb = memory_limit_mb;
        self
    }

    /// The host's secrets, which a module reads those of it was granted from. Without them, a
    /// module reads none.
    pub fn secrets(mut self, secrets: Arc<dyn Secrets>) -> Self {
        self.secrets = Some(secrets);
        self
    }

    fn table(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.ishikari_node_modules"),
            None => "ishikari_node_modules".to_string(),
        }
    }

    fn limits(&self, grants: &ModuleGrants) -> Limits {
        Limits {
            timeout: grants.timeout.unwrap_or(self.timeout),
            memory_bytes: grants.memory_limit_mb.unwrap_or(self.memory_limit_mb) as usize
                * 1024
                * 1024,
        }
    }

    /// The module of `bytes` loaded, on a blocking thread.
    async fn load(&self, sha256: String, bytes: Vec<u8>) -> Result<Guest, String> {
        let runtime = self.runtime.clone();
        tokio::task::spawn_blocking(move || runtime.load(&sha256, &bytes))
            .await
            .map_err(|e| format!("loading the module failed: {e}"))?
    }

    /// The module of `bytes` loaded, with its spec, called within `grants`' memory limit.
    async fn spec(
        &self,
        sha256: String,
        bytes: &[u8],
        grants: &ModuleGrants,
    ) -> Result<(Guest, NodeSpec), ModuleError> {
        let guest = self
            .load(sha256, bytes.to_vec())
            .await
            .map_err(ModuleError::Invalid)?;
        let runtime = self.runtime.clone();
        let loaded = guest.clone();
        let limits = Limits {
            timeout: SPEC_TIMEOUT,
            ..self.limits(grants)
        };
        let spec = tokio::task::spawn_blocking(move || runtime.spec(&loaded, limits))
            .await
            .map_err(|e| ModuleError::Runtime(format!("calling the module's spec failed: {e}")))?
            .result
            .map_err(|e| ModuleError::Invalid(format!("its spec failed: {e}")))?;
        if spec.name.trim().is_empty() {
            return Err(ModuleError::Invalid("its spec has no name".into()));
        }
        Ok((guest, spec))
    }

    /// Run `guest` as a node, on a blocking thread, with the secrets `grants` grants read
    /// first: `input` is what its `run` takes. Its output is JSON; the secrets' values are
    /// redacted from it, its error and its logs.
    async fn run(
        &self,
        guest: Guest,
        grants: &ModuleGrants,
        input: String,
    ) -> Result<Call<serde_json::Value>, String> {
        let secrets = match &self.secrets {
            Some(secrets) if !grants.secrets.is_empty() => Some(
                secrets
                    .get_all(&grants.secrets)
                    .await
                    .map_err(|e| format!("its secrets couldn't be read: {e}"))?,
            ),
            Some(_) => Some(HashMap::new()),
            None => None,
        };
        let redact = Redact::new(secrets.iter().flat_map(|secrets| secrets.values()));
        let sandbox = Sandbox::new(
            grants.allowed_hosts.clone(),
            grants.secrets.clone(),
            secrets,
        );
        let runtime = self.runtime.clone();
        let limits = self.limits(grants);
        let call =
            tokio::task::spawn_blocking(move || runtime.run(&guest, sandbox, limits, &input))
                .await
                .map_err(|e| format!("the module's call failed: {e}"))?;
        let result = call.result.and_then(|output| {
            serde_json::from_str(&output).map_err(|e| format!("its output isn't JSON: {e}"))
        });
        Ok(Call {
            result: result
                .map(|output| redact.json(output))
                .map_err(|e| redact.text(&e)),
            logs: call.logs.iter().map(|line| redact.text(line)).collect(),
            secrets: call.secrets,
        })
    }

    /// A row's columns, with its bytes as `source` when they're a script (not a component's
    /// `\0asm`).
    const COLUMNS: &'static str =
        "id, name, version, sha256, spec, octet_length(bytes)::bigint AS size, \
        CASE WHEN substring(bytes FROM 1 FOR 4) = decode('0061736d', 'hex') THEN NULL \
        ELSE convert_from(bytes, 'UTF8') END AS source, \
        allowed_hosts, timeout_ms, memory_limit_mb, secrets, created_at";
}

#[async_trait::async_trait]
impl NodeModules for WasmModules {
    async fn handler(
        &self,
        node_type: &str,
        as_of: DateTime<Utc>,
    ) -> Result<Option<Arc<dyn NodeHandler>>, ModuleError> {
        let query = format!(
            "SELECT {} FROM {} WHERE name = $1 AND created_at <= $2 \
             ORDER BY version DESC LIMIT 1",
            Self::COLUMNS,
            self.table()
        );
        let Some(row) = sqlx::query_as::<_, ModuleRow>(&query)
            .bind(node_type)
            .bind(as_of)
            .fetch_optional(&self.pool)
            .await?
        else {
            return Ok(None);
        };
        let id = row.id;
        let module = NodeModule::from(row);

        let guest = match self.runtime.cached(&module.sha256) {
            Some(guest) => guest,
            None => {
                let bytes: Vec<u8> = sqlx::query_scalar(&format!(
                    "SELECT bytes FROM {} WHERE id = $1",
                    self.table()
                ))
                .bind(id)
                .fetch_one(&self.pool)
                .await?;
                self.load(module.sha256.clone(), bytes).await.map_err(|e| {
                    ModuleError::Runtime(format!(
                        "module {} v{}: {e}",
                        module.spec.name, module.version
                    ))
                })?
            }
        };
        Ok(Some(Arc::new(WasmNode {
            modules: self.clone(),
            guest,
            module,
        })))
    }

    async fn modules(&self) -> Result<Vec<NodeModule>, ModuleError> {
        let query = format!(
            "SELECT DISTINCT ON (name) {} FROM {} ORDER BY name, version DESC",
            Self::COLUMNS,
            self.table()
        );
        let rows: Vec<ModuleRow> = sqlx::query_as(&query).fetch_all(&self.pool).await?;
        Ok(rows.into_iter().map(NodeModule::from).collect())
    }

    async fn describe(&self, bytes: &[u8]) -> Result<NodeSpec, ModuleError> {
        let sha256 = format!("{:x}", Sha256::digest(bytes));
        let (_, spec) = self.spec(sha256, bytes, &ModuleGrants::default()).await?;
        Ok(spec)
    }

    async fn upload(&self, upload: ModuleUpload) -> Result<NodeModule, ModuleError> {
        let ModuleUpload { bytes, grants } = upload;
        let grants = checked(grants)?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let size = bytes.len() as i64;
        let source = match Kind::of(&bytes) {
            Ok(Kind::Script(source)) => Some(source.to_string()),
            _ => None,
        };

        let (_, spec) = self.spec(sha256.clone(), &bytes, &grants).await?;

        let mut tx = self.pool.begin().await?;
        // One upload of a name at a time, so each gets the next version.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!("{}:{}", self.table(), spec.name))
            .execute(&mut *tx)
            .await?;
        let latest: Option<ModuleRow> = sqlx::query_as(&format!(
            "SELECT {} FROM {} WHERE name = $1 ORDER BY version DESC LIMIT 1",
            Self::COLUMNS,
            self.table()
        ))
        .bind(&spec.name)
        .fetch_optional(&mut *tx)
        .await?;
        let latest = latest.map(NodeModule::from);
        if let Some(latest) = latest
            .as_ref()
            .filter(|latest| latest.sha256 == sha256 && latest.grants == grants)
        {
            return Ok(latest.clone());
        }

        let version = latest.map_or(1, |latest| latest.version + 1);
        let uploaded_at: DateTime<Utc> = sqlx::query_scalar(&format!(
            "INSERT INTO {} (name, version, sha256, spec, bytes, allowed_hosts, timeout_ms, \
             memory_limit_mb, secrets) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
             RETURNING created_at",
            self.table()
        ))
        .bind(&spec.name)
        .bind(version)
        .bind(&sha256)
        .bind(Json(&spec))
        .bind(&bytes)
        .bind(&grants.allowed_hosts)
        .bind(grants.timeout.map(|t| t.as_millis() as i32))
        .bind(grants.memory_limit_mb.map(|mb| mb as i32))
        .bind(&grants.secrets)
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(NodeModule {
            spec,
            version,
            sha256,
            size,
            source,
            grants,
            uploaded_at,
        })
    }

    async fn test_run(
        &self,
        upload: ModuleUpload,
        inputs: HashMap<String, serde_json::Value>,
    ) -> Result<TestRun, ModuleError> {
        let ModuleUpload { bytes, grants } = upload;
        let grants = checked(grants)?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let (guest, spec) = self.spec(sha256, &bytes, &grants).await?;

        let input = node_input("test", &spec.name, &inputs, serde_json::Map::new());
        let call = self
            .run(guest, &grants, input)
            .await
            .map_err(ModuleError::Runtime)?;
        Ok(TestRun {
            spec,
            output: call.result,
            logs: call.logs,
            secrets: call.secrets,
        })
    }
}

/// What a module's `run` takes: the node's id, type and inputs, and its workflow's inputs.
fn node_input(
    node_id: &str,
    node_type: &str,
    inputs: &HashMap<String, serde_json::Value>,
    workflow_inputs: serde_json::Map<String, serde_json::Value>,
) -> String {
    serde_json::json!({
        "node_id": node_id,
        "node_type": node_type,
        "inputs": inputs,
        "workflow_inputs": workflow_inputs,
    })
    .to_string()
}

/// `grants` with its hosts and secrets trimmed and blanks dropped, its secrets in order and
/// once each, or why they can't be granted.
fn checked(mut grants: ModuleGrants) -> Result<ModuleGrants, ModuleError> {
    grants.allowed_hosts = grants
        .allowed_hosts
        .iter()
        .map(|host| host.trim().to_string())
        .filter(|host| !host.is_empty())
        .collect();
    if let Some(host) = grants
        .allowed_hosts
        .iter()
        .find(|host| host.contains('/') || host.contains(char::is_whitespace))
    {
        return Err(ModuleError::Invalid(format!(
            "{host} isn't a host (a name, name:port or *.domain)"
        )));
    }
    if grants
        .timeout
        .is_some_and(|t| t.is_zero() || t.as_millis() > i32::MAX as u128)
    {
        return Err(ModuleError::Invalid("its timeout is out of range".into()));
    }
    if grants
        .memory_limit_mb
        .is_some_and(|mb| mb == 0 || mb > i32::MAX as u32)
    {
        return Err(ModuleError::Invalid(
            "its memory limit is out of range".into(),
        ));
    }
    grants.secrets = grants
        .secrets
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .map(|name| secrets::check_name(name).map(str::to_string))
        .collect::<Result<_, _>>()
        .map_err(|e| ModuleError::Invalid(e.to_string()))?;
    grants.secrets.sort();
    grants.secrets.dedup();
    Ok(grants)
}

#[derive(sqlx::FromRow)]
struct ModuleRow {
    id: i64,
    name: String,
    version: i32,
    sha256: String,
    spec: Json<NodeSpec>,
    size: i64,
    source: Option<String>,
    allowed_hosts: Vec<String>,
    timeout_ms: Option<i32>,
    memory_limit_mb: Option<i32>,
    secrets: Vec<String>,
    created_at: DateTime<Utc>,
}

impl From<ModuleRow> for NodeModule {
    fn from(row: ModuleRow) -> Self {
        let mut spec = row.spec.0;
        spec.name = row.name;
        NodeModule {
            spec,
            version: row.version,
            sha256: row.sha256,
            size: row.size,
            source: row.source,
            grants: ModuleGrants {
                allowed_hosts: row.allowed_hosts,
                timeout: row
                    .timeout_ms
                    .map(|ms| Duration::from_millis(ms.max(0) as u64)),
                memory_limit_mb: row.memory_limit_mb.map(|mb| mb.max(0) as u32),
                secrets: row.secrets,
            },
            uploaded_at: row.created_at,
        }
    }
}

/// A version of a module, running nodes of its type.
struct WasmNode {
    modules: WasmModules,
    guest: Guest,
    module: NodeModule,
}

#[async_trait::async_trait]
impl NodeHandler for WasmNode {
    async fn execute(&self, ctx: &NodeContext, _pool: &PgPool) -> Result<NodeData, NodeError> {
        let workflow_inputs: serde_json::Map<String, serde_json::Value> = ctx
            .workflow_inputs
            .iter()
            .map(|(name, value)| (name.clone(), value.to_json()))
            .collect();
        let input = node_input(&ctx.node_id, &ctx.node_type, &ctx.inputs, workflow_inputs);
        let call = self
            .modules
            .run(self.guest.clone(), &self.module.grants, input)
            .await
            .map_err(NodeError::ExecutionFailed)?;

        for line in &call.logs {
            tracing::info!(
                module = %self.module.spec.name,
                version = self.module.version,
                node = %ctx.node_id,
                "{line}"
            );
        }
        call.result
            .map(NodeData::json)
            .map_err(|e| NodeError::ExecutionFailed(with_logs(e, &call.logs)))
    }
}

/// `error`, followed by the last lines of the node's logs.
fn with_logs(error: String, logs: &[String]) -> String {
    if logs.is_empty() {
        return error;
    }
    let tail = &logs[logs.len().saturating_sub(LOG_LINES_IN_ERRORS)..];
    format!("{error}\n{}", tail.join("\n"))
}
