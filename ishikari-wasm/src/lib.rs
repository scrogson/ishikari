//! WebAssembly components as node types for ishikari's workflow definitions, uploaded by the
//! host's operators through the admin and run in a sandbox.
//!
//! [`WasmModules`] keeps the modules in Postgres (`ishikari_node_modules`, schema v08) and is
//! the host's [`NodeModules`]: give it to [`NodeTypes::modules`] to run the admin's runs' nodes
//! of uploaded types, and to `ishikari_admin::AppState::with_node_modules` to upload them and
//! offer them in the builder.
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
//! send HTTP requests to through `host.fetch`, within its time and memory limits. WASI is
//! linked for guests' standard libraries, with no files, environment, arguments or sockets;
//! stdout and stderr go to the node's logs with `host.log`.
//!
//! [`NodeModules`]: ishikari::execution::NodeModules
//! [`NodeTypes::modules`]: ishikari::execution::NodeTypes::modules

mod output;
mod runtime;
mod sandbox;

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use ishikari::execution::{
    ModuleError, ModuleGrants, ModuleUpload, NodeContext, NodeData, NodeError, NodeHandler,
    NodeModule, NodeModules, NodeSpec,
};
use sha2::{Digest, Sha256};
use sqlx::types::Json;
use sqlx::PgPool;

use runtime::{Limits, NodePre, Runtime, State};
use sandbox::Sandbox;

/// The `ishikari:node` world a module implements.
pub const WIT: &str = include_str!("../wit/node.wit");

/// How long a call to a module's `spec` may take when it's uploaded.
const SPEC_TIMEOUT: Duration = Duration::from_secs(5);

/// The lines of a failed node's logs its error keeps.
const LOG_LINES_IN_ERRORS: usize = 20;

/// WebAssembly components uploaded as node types, kept in `ishikari_node_modules`.
#[derive(Clone)]
pub struct WasmModules {
    pool: PgPool,
    schema: Option<String>,
    runtime: Arc<Runtime>,
    timeout: Duration,
    memory_limit_mb: u32,
}

impl WasmModules {
    /// Modules kept in `pool`'s search path; a node may run for 30s and use 64 MiB unless its
    /// module was granted otherwise.
    ///
    /// # Panics
    ///
    /// When the WebAssembly engine can't be set up (the platform has no compiler for it).
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: None,
            runtime: Arc::new(Runtime::new().expect("set up the WebAssembly engine")),
            timeout: Duration::from_secs(30),
            memory_limit_mb: 64,
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

    /// `bytes` compiled, on a blocking thread.
    async fn compile(&self, sha256: String, bytes: Vec<u8>) -> Result<NodePre<State>, String> {
        let runtime = self.runtime.clone();
        tokio::task::spawn_blocking(move || runtime.compile(&sha256, &bytes))
            .await
            .map_err(|e| format!("compiling the module failed: {e}"))?
    }

    /// The spec of the module of `bytes`, called within `grants`' memory limit.
    async fn spec(
        &self,
        sha256: String,
        bytes: &[u8],
        grants: &ModuleGrants,
    ) -> Result<NodeSpec, ModuleError> {
        let pre = self
            .compile(sha256, bytes.to_vec())
            .await
            .map_err(ModuleError::Invalid)?;
        let runtime = self.runtime.clone();
        let limits = Limits {
            timeout: SPEC_TIMEOUT,
            ..self.limits(grants)
        };
        let spec = tokio::task::spawn_blocking(move || runtime.spec(&pre, limits))
            .await
            .map_err(|e| ModuleError::Runtime(format!("calling the module's spec failed: {e}")))?
            .result
            .map_err(|e| ModuleError::Invalid(format!("its spec failed: {e}")))?;
        if spec.name.trim().is_empty() {
            return Err(ModuleError::Invalid("its spec has no name".into()));
        }
        Ok(spec)
    }

    const COLUMNS: &'static str =
        "id, name, version, sha256, spec, octet_length(bytes)::bigint AS size, \
        allowed_hosts, timeout_ms, memory_limit_mb, created_at";
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

        let pre = match self.runtime.cached(&module.sha256) {
            Some(pre) => pre,
            None => {
                let bytes: Vec<u8> = sqlx::query_scalar(&format!(
                    "SELECT bytes FROM {} WHERE id = $1",
                    self.table()
                ))
                .bind(id)
                .fetch_one(&self.pool)
                .await?;
                self.compile(module.sha256.clone(), bytes)
                    .await
                    .map_err(|e| {
                        ModuleError::Runtime(format!(
                            "module {} v{}: {e}",
                            module.spec.name, module.version
                        ))
                    })?
            }
        };
        Ok(Some(Arc::new(WasmNode {
            runtime: self.runtime.clone(),
            pre,
            limits: self.limits(&module.grants),
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
        self.spec(sha256, bytes, &ModuleGrants::default()).await
    }

    async fn upload(&self, upload: ModuleUpload) -> Result<NodeModule, ModuleError> {
        let ModuleUpload { bytes, grants } = upload;
        let grants = checked(grants)?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let size = bytes.len() as i64;

        let spec = self.spec(sha256.clone(), &bytes, &grants).await?;

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
             memory_limit_mb) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING created_at",
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
        .fetch_one(&mut *tx)
        .await?;
        tx.commit().await?;

        Ok(NodeModule {
            spec,
            version,
            sha256,
            size,
            grants,
            uploaded_at,
        })
    }
}

/// `grants` with its hosts trimmed and blanks dropped, or why they can't be granted.
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
    allowed_hosts: Vec<String>,
    timeout_ms: Option<i32>,
    memory_limit_mb: Option<i32>,
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
            grants: ModuleGrants {
                allowed_hosts: row.allowed_hosts,
                timeout: row
                    .timeout_ms
                    .map(|ms| Duration::from_millis(ms.max(0) as u64)),
                memory_limit_mb: row.memory_limit_mb.map(|mb| mb.max(0) as u32),
            },
            uploaded_at: row.created_at,
        }
    }
}

/// A version of a module, running nodes of its type.
struct WasmNode {
    runtime: Arc<Runtime>,
    pre: NodePre<State>,
    module: NodeModule,
    limits: Limits,
}

#[async_trait::async_trait]
impl NodeHandler for WasmNode {
    async fn execute(&self, ctx: &NodeContext, _pool: &PgPool) -> Result<NodeData, NodeError> {
        let workflow_inputs: serde_json::Map<String, serde_json::Value> = ctx
            .workflow_inputs
            .iter()
            .map(|(name, value)| (name.clone(), value.to_json()))
            .collect();
        let input = serde_json::json!({
            "node_id": ctx.node_id,
            "node_type": ctx.node_type,
            "inputs": ctx.inputs,
            "workflow_inputs": workflow_inputs,
        })
        .to_string();

        let runtime = self.runtime.clone();
        let pre = self.pre.clone();
        let sandbox = Sandbox::new(self.module.grants.allowed_hosts.clone());
        let limits = self.limits;
        let call = tokio::task::spawn_blocking(move || runtime.run(&pre, sandbox, limits, &input))
            .await
            .map_err(|e| NodeError::ExecutionFailed(format!("the module's call failed: {e}")))?;

        for line in &call.logs {
            tracing::info!(
                module = %self.module.spec.name,
                version = self.module.version,
                node = %ctx.node_id,
                "{line}"
            );
        }
        let output = call
            .result
            .map_err(|e| NodeError::ExecutionFailed(with_logs(e, &call.logs)))?;
        serde_json::from_str::<serde_json::Value>(&output)
            .map(NodeData::json)
            .map_err(|e| NodeError::ExecutionFailed(format!("its output isn't JSON: {e}")))
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
