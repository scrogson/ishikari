# ishikari-wasm

Node types for [ishikari](https://github.com/scrogson/ishikari)'s workflow definitions, uploaded
by the app's operators through the admin while it runs: WebAssembly components, from any
language that targets the component model, run in a sandbox.

```rust
use ishikari::execution::{NodeJob, NodeJobs, NodeTypes};
use ishikari_wasm::WasmModules;

let modules = Arc::new(WasmModules::new(pool.clone()));
let types = NodeTypes::new(pool.clone()).modules(modules.clone()); // and .register(...)
let state = ishikari_admin::AppState::new(pool.clone())
    .with_node_types(types.catalog())
    .with_node_modules(modules);
let engine = Engine::builder("definitions")
    .with_state(Arc::new(NodeJobs::new(types)))
    .with_queue(Queue::builder(NodeJob::QUEUE))
    .start(Postgres::new(pool.clone()));
```

Modules are kept in `ishikari_node_modules` (schema v08). Needs Rust 1.96, wasmtime's.

## A module

A component of the `ishikari:node` world ([`wit/node.wit`](wit/node.wit)):

- `spec() -> string`: the node type, an `ishikari::execution::NodeSpec` as JSON (`name`,
  `category`, `description`, `input_schema`, `output_schema`). Its name is the module's.
- `run(input: string) -> result<string, string>`: a node. `input` is JSON with `node_id`,
  `node_type`, `inputs` (resolved against the run) and `workflow_inputs`; the result is the
  node's output as JSON, or why it failed.
- It may call `host.log(level, message)`, and `host.fetch(request)` for HTTP to the hosts it was
  granted.

In Rust, with [wit-bindgen](https://crates.io/crates/wit-bindgen) and `crate-type = ["cdylib"]`:

```rust
wit_bindgen::generate!({ path: "wit", world: "node" }); // a copy of wit/node.wit

struct Shout;

impl Guest for Shout {
    fn spec() -> String {
        r#"{ "name": "text/shout", "category": "text",
             "input_schema": { "text": { "type": "string", "required": true } },
             "output_schema": { "text": { "type": "string" } } }"#
            .into()
    }

    fn run(input: String) -> Result<String, String> {
        let input: serde_json::Value = serde_json::from_str(&input).map_err(|e| e.to_string())?;
        let text = input["inputs"]["text"].as_str().unwrap_or_default().to_uppercase();
        Ok(serde_json::json!({ "text": text }).to_string())
    }
}

export!(Shout);
```

```bash
cargo build --release --target wasm32-wasip2
```

Then upload `target/wasm32-wasip2/release/<crate>.wasm` on the admin's Node types page.

## The sandbox

Each call gets a fresh instance. A module reaches only what its upload granted:

- HTTP through `host.fetch` to its allowed hosts: `api.example.com` (any port),
  `127.0.0.1:8080` (that port), `*.example.com` (its subdomains). Redirects aren't followed.
- WASI without files, environment, arguments or sockets. stdout and stderr go to the node's
  logs, and the last lines of them to a failed node's error.
- Its timeout and memory limit, by default 30s and 64 MiB (`WasmModules::timeout`,
  `WasmModules::memory_limit_mb`).

Uploading a name again makes its next version, with the grants given then; the same bytes and
grants again change nothing. A run uses the version uploaded last before it was created.

## Development

A workspace of its own, apart from ishikari's (whose MSRV is 1.83). Its tests upload
`tests/fixtures/testkit-v1.wasm` and `-v2.wasm`, built from `tests/fixtures/testkit` by
`mise run wasm:fixtures` (needs the `wasm32-wasip2` target). `mise run wasm:demo` serves the
admin with modules on http://127.0.0.1:3001.
