//! Loading and calling modules: a wasmtime engine whose epoch a ticker advances, a linker with
//! WASI (nothing to reach, and sleeps that end at the call's deadline), `host` and `script`, and
//! loaded modules cached by their sha256. A component is compiled; a script is stripped of its
//! TypeScript and run by the JavaScript runtime (`js-runtime.wasm`, built from `js-runtime/`),
//! compiled once.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use ishikari::execution::NodeSpec;
use wasmtime::component::{HasData, HasSelf, Linker, Resource, ResourceTable};
use wasmtime::{Config, Engine, ResourceLimiter, Store, Trap};
use wasmtime_wasi::clocks::{WasiClocksCtxView, WasiClocksView};
use wasmtime_wasi::p2::bindings::clocks::monotonic_clock;
use wasmtime_wasi::p2::DynPollable;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::output::Output;
use crate::sandbox::Sandbox;
use crate::typescript;

pub(crate) mod bindings {
    // The world of the runtime of scripts, which is a node's and `script`: a component of the
    // `node` world is instantiated through its linker just the same.
    wasmtime::component::bindgen!({ path: "wit", world: "script-node" });
}

use bindings::{ScriptNode as Node, ScriptNodePre as NodePre};

/// The runtime of scripts: a component of the `script-node` world.
const JS_RUNTIME: &[u8] = include_bytes!("../js-runtime.wasm");

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

/// A module ready to call: a component, or a script with the runtime that runs it.
#[derive(Clone)]
pub(crate) struct Guest {
    pre: NodePre<State>,
    script: Option<Arc<str>>,
}

/// What a module's bytes are.
pub(crate) enum Kind<'a> {
    Component,
    Script(&'a str),
}

impl<'a> Kind<'a> {
    pub fn of(bytes: &'a [u8]) -> Result<Self, String> {
        if bytes.starts_with(b"\0asm") {
            return Ok(Kind::Component);
        }
        std::str::from_utf8(bytes)
            .map(Kind::Script)
            .map_err(|_| "neither a WebAssembly component nor a script (UTF-8 text)".into())
    }
}

pub(crate) struct Runtime {
    engine: Engine,
    linker: Linker<State>,
    loaded: Mutex<HashMap<String, Guest>>,
    js_runtime: OnceLock<Result<NodePre<State>, String>>,
}

impl Runtime {
    /// The process's runtime: one engine, and one compile of each module (and of the runtime of
    /// scripts), for every `WasmModules`.
    ///
    /// # Panics
    ///
    /// When the engine can't be set up (the platform has no compiler for it).
    pub fn shared() -> Arc<Runtime> {
        static SHARED: OnceLock<Arc<Runtime>> = OnceLock::new();
        SHARED
            .get_or_init(|| Arc::new(Runtime::new().expect("set up the WebAssembly engine")))
            .clone()
    }

    fn new() -> wasmtime::Result<Self> {
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config)?;

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
        linker.allow_shadowing(true);
        monotonic_clock::add_to_linker::<State, HasClock>(&mut linker, |state| {
            let deadline = state.sandbox.deadline;
            Clock {
                wasi: state.clocks(),
                deadline,
            }
        })?;
        linker.allow_shadowing(false);
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
            loaded: Mutex::new(HashMap::new()),
            js_runtime: OnceLock::new(),
        })
    }

    /// The module of `sha256`, if it's been loaded here.
    pub fn cached(&self, sha256: &str) -> Option<Guest> {
        self.loaded.lock().unwrap().get(sha256).cloned()
    }

    /// The module of `bytes` (whose sha256 is `sha256`), ready to call: a component compiled,
    /// and checked to be a `node` this runtime can run (importing nothing beyond WASI and
    /// `host`, exporting `spec` and `run`), or a script.
    pub fn load(&self, sha256: &str, bytes: &[u8]) -> Result<Guest, String> {
        if let Some(guest) = self.cached(sha256) {
            return Ok(guest);
        }
        let guest = match Kind::of(bytes)? {
            Kind::Component => Guest {
                pre: self.compile(bytes)?,
                script: None,
            },
            Kind::Script(source) => Guest {
                pre: self.js_runtime()?,
                script: Some(typescript::javascript(source)?.into()),
            },
        };
        self.loaded
            .lock()
            .unwrap()
            .insert(sha256.to_string(), guest.clone());
        Ok(guest)
    }

    fn compile(&self, bytes: &[u8]) -> Result<NodePre<State>, String> {
        let component = wasmtime::component::Component::new(&self.engine, bytes)
            .map_err(|e| format!("not a WebAssembly component: {e:#}"))?;
        self.linker
            .instantiate_pre(&component)
            .and_then(NodePre::new)
            .map_err(|e| format!("not an ishikari:node component: {e:#}"))
    }

    fn js_runtime(&self) -> Result<NodePre<State>, String> {
        self.js_runtime
            .get_or_init(|| {
                self.compile(JS_RUNTIME)
                    .map_err(|e| format!("the runtime of scripts: {e}"))
            })
            .clone()
    }

    /// The node type the module describes itself as.
    pub fn spec(&self, guest: &Guest, limits: Limits) -> Call<NodeSpec> {
        let call = self.call(guest, Sandbox::new(Vec::new()), limits, |node, store| {
            node.call_spec(store)
        });
        Call {
            result: call.result.and_then(|json| {
                serde_json::from_str(&json).map_err(|e| format!("its spec isn't a node spec: {e}"))
            }),
            logs: call.logs,
        }
    }

    /// Run a node: `input` is the JSON the module's `run` takes.
    pub fn run(
        &self,
        guest: &Guest,
        sandbox: Sandbox,
        limits: Limits,
        input: &str,
    ) -> Call<String> {
        self.call(guest, sandbox, limits, |node, store| {
            node.call_run(store, input)
        })
    }

    /// Call `f` on a fresh instance of `guest`, which may reach what `sandbox` grants within
    /// `limits`.
    fn call<R>(
        &self,
        guest: &Guest,
        mut sandbox: Sandbox,
        limits: Limits,
        f: impl FnOnce(&Node, &mut Store<State>) -> wasmtime::Result<Result<R, String>>,
    ) -> Call<R> {
        let stdout = Output::default();
        let stderr = Output::default();
        sandbox.deadline = Some(Instant::now() + limits.timeout);
        sandbox.script = guest.script.clone();
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

        let result = guest
            .pre
            .instantiate(&mut store)
            .and_then(|node| f(&node, &mut store));

        let state = store.into_data();
        // A guest that slept until its deadline may have returned before the epoch caught up.
        let result = result.and_then(|result| match state.sandbox.deadline {
            Some(deadline) if Instant::now() >= deadline => Err(Trap::Interrupt.into()),
            _ => Ok(result),
        });
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

/// The guest's `wasi:clocks/monotonic-clock`, whose sleeps end by its call's deadline: a guest
/// waiting in `wasi:io/poll` is in the host, where epoch interruption can't stop it.
struct Clock<'a> {
    wasi: WasiClocksCtxView<'a>,
    deadline: Option<Instant>,
}

struct HasClock;

impl HasData for HasClock {
    type Data<'a> = Clock<'a>;
}

impl monotonic_clock::Host for Clock<'_> {
    fn now(&mut self) -> wasmtime::Result<monotonic_clock::Instant> {
        self.wasi.now()
    }

    fn resolution(&mut self) -> wasmtime::Result<monotonic_clock::Duration> {
        self.wasi.resolution()
    }

    fn subscribe_instant(
        &mut self,
        when: monotonic_clock::Instant,
    ) -> wasmtime::Result<Resource<DynPollable>> {
        let now = self.wasi.now()?;
        self.subscribe_duration(when.saturating_sub(now))
    }

    fn subscribe_duration(
        &mut self,
        duration: monotonic_clock::Duration,
    ) -> wasmtime::Result<Resource<DynPollable>> {
        let left = self.deadline.map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(Instant::now())
        });
        let left = u64::try_from(left.as_nanos()).unwrap_or(u64::MAX);
        self.wasi.subscribe_duration(duration.min(left))
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
