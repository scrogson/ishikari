# WebAssembly node types

Status: phase 1 (prebuilt components uploaded through the admin) is built; phases 2 and 3 are
designs.

A host registers node types in Rust (`NodeTypes::register`). This adds node types its operators
upload while it runs: WebAssembly components, built from any language that targets the
component model, stored in Postgres and run in a sandbox on whichever engine claims the node.

## Who uploads

The host's operators: people who can change its code anyway. A module is trusted not to be
malicious, but not to be correct: it can't take the process down, run forever, eat the
machine's memory, or reach anything it wasn't granted. Uploading sits behind the host's admin
auth like the rest of the dashboard, and only exists when the host opts in.

## Pieces

- `ishikari::execution::NodeModules`: node types found at run time. `NodeTypes::modules` asks it
  for a type that has no Rust handler; the admin (`AppState::with_node_modules`) asks it for the
  palette's catalog, the module list and uploads. ishikari doesn't depend on any runtime.
- `ishikari-wasm`: `WasmModules`, the `NodeModules` of WebAssembly components. A separate crate
  because wasmtime is large and needs a newer Rust (1.96) than the rest (1.83), and a workspace
  of its own with its own lockfile: Cargo 1.83 can't even read the lockfile entries of
  wasmtime's dependencies (edition 2024).
- Schema v08: `ishikari_node_modules`, every uploaded version of every module.
- The admin's Node types page: the registered types, the modules, and an upload form (the
  module, its allowed hosts, timeout and memory limit; up to 32 MiB). It asks the modules to
  `describe` an upload first, and refuses one named as a type registered in Rust, which would
  run instead. The palette and the definition checks offer the registered types, then the
  modules not named as one.

## The interface

`ishikari-wasm/wit/node.wit`, package `ishikari:node@0.1.0`:

```wit
world node {
    import host;
    export spec: func() -> string;                          // a NodeSpec, as JSON
    export run: func(input: string) -> result<string, string>;
}
```

- `spec` is the node type as the builder offers it (name, category, description, inputs and
  outputs). The module names itself: an upload is a new version of the module of that name.
- `run` takes a JSON object (`node_id`, `node_type`, `inputs` resolved, `workflow_inputs`) and
  returns the node's output as JSON, or an error that fails the node (and is retried like any
  node's).
- `host` has `log(level, message)` and `fetch(request) -> result<response, string>`.

JSON strings rather than WIT records for inputs and outputs: a node's inputs are whatever its
spec says, and every guest language has JSON.

## Capabilities

What a module can reach is what the operator grants it on upload, nothing by default:

- **HTTP**: `fetch` to hosts on the module's allow-list (`api.example.com`, `127.0.0.1:8080`,
  `*.example.com`). Redirects aren't followed, so one can't leave the list; a guest that wants
  to follows them with another `fetch`, checked again.
- **WASI**: components built for `wasm32-wasip2` import WASI. They get no preopened directories,
  no environment, no arguments, no sockets and no name lookup. Clocks and randomness are
  allowed. stdout and stderr are captured, with `log`, into the node's logs.
- No database access. A node that needs one is written in Rust.

## Limits

Per module, with defaults on `WasmModules`:

- **Timeout** (30s): wall clock for the whole call. CPU is cut off by epoch interruption (a
  ticker advances the engine's epoch every 10ms, the store traps past its deadline). Waiting in
  the host can't be interrupted, so it's held to what's left: a `fetch` gets it as its own
  timeout, and a sleep (`wasi:clocks/monotonic-clock` subscriptions) ends by the deadline. A
  call that returns after its deadline fails all the same.
- **Memory** (64 MiB): the store's limiter refuses growth past it, and the guest traps.

A module that breaks a limit fails the node with what it broke, and the node is retried like
any failure.

## Versions

Every upload of a name is its next version, with the grants given with it (a version's grants
aren't carried over to the next); uploading the same bytes and grants again is a no-op. A run
uses, for each wasm node type, the latest version uploaded before the run was created, so a run
and its retries run the same code even while a new version lands, and a new run picks the new
version up. Old versions are kept (a run in flight may need one); pruning them is left for
later.

## Running

Each call gets a fresh store and instance: no state survives between nodes, and a trap can't
leave a module half-broken for the next one. Compiling a component is the expensive part, so
compiled components are cached by their sha256 and a node only loads a module's bytes the first
time an engine sees that version. The call runs on a blocking thread.

## Phase 2: TypeScript in the browser

An editor in the admin, without compiling each script to wasm:

- ishikari ships one component, a JavaScript runtime (QuickJS built for `wasm32-wasip2`, the
  way Javy does), that exports the same `node` world and evaluates a script it's given.
- The editor strips TypeScript's types in the browser (TypeScript's own transpiler or
  `amaro`/swc's wasm build) and saves the JavaScript as the module's source.
- A JS module runs the shared runtime with its source: same sandbox, same limits, saved in
  milliseconds, no toolchain on the server.

Rust written in the browser isn't planned: compiling it needs rustc and the wasm target on the
server, takes seconds to minutes, and runs untrusted build scripts and proc macros outside any
sandbox. Rust nodes are built locally (`cargo build --target wasm32-wasip2`) and uploaded.

## Phase 3: jobs

One Rust worker, `WasmJob { module, version, args }`, whose `perform` runs the module's
`perform(args)` (a second world, `ishikari:job`, returning complete, snooze, cancel or an
error). Queues, retries, unique jobs and cron work unchanged; the job pins its module's version
when inserted. Inserted from Rust (`WasmJob::new("send-invoice", json!({..}))`), the admin, or a
cron entry.

## Open questions

- Pruning old versions no run or job can need.
- Carrying a module's grants over to its next upload, or editing them without re-uploading.
- Bounding the compiled-component cache (one entry per version an engine has run or described).
- Secrets: a capability handing a module named secrets from the host, rather than putting them
  in node inputs.
- Whether `fetch` should become `wasi:http`, so guests' own HTTP libraries work.
- An admin running in a different process than the engines sees uploads (they're in Postgres),
  but each engine compiles a module itself the first time it runs one.
