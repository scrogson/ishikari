//! Compiling and calling components: a wasmtime engine whose epoch a ticker advances, a linker
//! with WASI (nothing to reach) and the `host` interface, and compiled components cached by
//! their sha256.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use ishikari::execution::NodeSpec;
use wasmtime::component::{HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, ResourceLimiter, Store, Trap};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::output::Output;
use crate::sandbox::Sandbox;

pub(crate) mod bindings {
    wasmtime::component::bindgen!({ path: "wit", world: "node" });
}

pub(crate) use bindings::{Node, NodePre};

/// How often the engine's epoch advances: the granularity of a call's timeout.
const TICK: Duration = Duration::from_millis(10);

/// The most a table may grow to, in elements.
const MAX_TABLE_ELEMENTS: usize = 1 << 20;

/// What a call may use.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub timeout: Duration,
    pub memory_bytes: usize,
}

/// How a call ended, with what the guest logged and printed.
pub(crate) struct Call<R> {
    pub result: Result<R, String>,
    pub logs: Vec<String>,
}

pub(crate) struct Runtime {
    engine: Engine,
    linker: Linker<State>,
    compiled: Mutex<HashMap<String, NodePre<State>>>,
}

impl Runtime {
    pub fn new() -> wasmtime::Result<Self> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config)?;

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
        Node::add_to_linker::<State, HasSelf<Sandbox>>(&mut linker, |state| &mut state.sandbox)?;

        let weak = engine.weak();
        std::thread::Builder::new()
            .name("ishikari-wasm-epoch".into())
            .spawn(move || loop {
                std::thread::sleep(TICK);
                match weak.upgrade() {
                    Some(engine) => engine.increment_epoch(),
                    None => break,
                }
            })?;

        Ok(Self {
            engine,
            linker,
            compiled: Mutex::new(HashMap::new()),
        })
    }

    /// The component of `sha256`, if it's been compiled here.
    pub fn cached(&self, sha256: &str) -> Option<NodePre<State>> {
        self.compiled.lock().unwrap().get(sha256).cloned()
    }

    /// `bytes` (whose sha256 is `sha256`) compiled, and checked to be a `node` this runtime can
    /// run: one importing nothing beyond WASI and `host`, and exporting `spec` and `run`.
    pub fn compile(&self, sha256: &str, bytes: &[u8]) -> Result<NodePre<State>, String> {
        if let Some(pre) = self.cached(sha256) {
            return Ok(pre);
        }
        let component = wasmtime::component::Component::new(&self.engine, bytes)
            .map_err(|e| format!("not a WebAssembly component: {e:#}"))?;
        let pre = self
            .linker
            .instantiate_pre(&component)
            .and_then(NodePre::new)
            .map_err(|e| format!("not an ishikari:node component: {e:#}"))?;
        self.compiled
            .lock()
            .unwrap()
            .insert(sha256.to_string(), pre.clone());
        Ok(pre)
    }

    /// The node type the component describes itself as.
    pub fn spec(&self, pre: &NodePre<State>, limits: Limits) -> Call<NodeSpec> {
        let call = self.call(pre, Sandbox::new(Vec::new()), limits, |node, store| {
            node.call_spec(store).map(Ok)
        });
        Call {
            result: call.result.and_then(|json| {
                serde_json::from_str(&json).map_err(|e| format!("its spec isn't a node spec: {e}"))
            }),
            logs: call.logs,
        }
    }

    /// Run a node: `input` is the JSON the component's `run` takes.
    pub fn run(
        &self,
        pre: &NodePre<State>,
        sandbox: Sandbox,
        limits: Limits,
        input: &str,
    ) -> Call<String> {
        self.call(pre, sandbox, limits, |node, store| {
            node.call_run(store, input)
        })
    }

    /// Call `f` on a fresh instance of `pre`, which may reach what `sandbox` grants within
    /// `limits`.
    fn call<R>(
        &self,
        pre: &NodePre<State>,
        mut sandbox: Sandbox,
        limits: Limits,
        f: impl FnOnce(&Node, &mut Store<State>) -> wasmtime::Result<Result<R, String>>,
    ) -> Call<R> {
        let stdout = Output::default();
        let stderr = Output::default();
        sandbox.deadline = Some(Instant::now() + limits.timeout);
        let state = State {
            wasi: WasiCtx::builder()
                .stdout(stdout.clone())
                .stderr(stderr.clone())
                .build(),
            table: ResourceTable::new(),
            sandbox,
            memory_bytes: limits.memory_bytes,
            refused_memory: false,
        };
        let mut store = Store::new(&self.engine, state);
        store.limiter(|state| state);
        store.set_epoch_deadline(limits.timeout.div_duration_f64(TICK).ceil().max(1.0) as u64);
        store.epoch_deadline_trap();

        let result = pre
            .instantiate(&mut store)
            .and_then(|node| f(&node, &mut store));

        let state = store.into_data();
        let mut logs = state.sandbox.logs;
        logs.extend(stdout.lines().map(|line| format!("stdout: {line}")));
        logs.extend(stderr.lines().map(|line| format!("stderr: {line}")));
        let result = match result {
            Ok(result) => result,
            Err(_) if state.refused_memory => Err(format!(
                "used more than its {} MiB of memory",
                limits.memory_bytes / (1024 * 1024)
            )),
            Err(e) if e.downcast_ref::<Trap>() == Some(&Trap::Interrupt) => {
                Err(format!("ran longer than its {:?} timeout", limits.timeout))
            }
            Err(e) => Err(format!("{e:#}")),
        };
        Call { result, logs }
    }
}

/// A store's state: the guest's WASI context, its sandbox, and its memory limit.
pub(crate) struct State {
    wasi: WasiCtx,
    table: ResourceTable,
    sandbox: Sandbox,
    memory_bytes: usize,
    /// Whether the limiter refused the guest memory, which it fails for.
    refused_memory: bool,
}

impl WasiView for State {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl ResourceLimiter for State {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let allowed = desired <= self.memory_bytes;
        self.refused_memory |= !allowed;
        Ok(allowed)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= MAX_TABLE_ELEMENTS)
    }
}
