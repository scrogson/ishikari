# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

Tasks are [mise](https://mise.jdx.dev) file tasks in `.mise/tasks`:

- `mise run dev` - format, lint, test
- `mise run build`, `mise run check`, `mise run clippy`, `mise run fmt`, `mise run clean`
  (each for `ishikari-wasm` too)
- `mise run test [filter]` - all tests (`--watch` to re-run on change)
- `mise run pg:start` / `pg:stop` - the local Postgres in `.postgres` (the `DATABASE_URL` in
  `mise.toml`); `mise run db:setup` creates the dev database
- `mise run demo` - the admin demo (`ishikari-admin/examples/demo.rs`)
- `mise run wasm:demo` - the admin with uploaded node types (`ishikari-wasm/examples/admin.rs`)
- `mise run wasm:fixtures` - build ishikari-wasm's test modules (needs `wasm32-wasip2`)
- `mise run wasm:js-runtime` - build ishikari-wasm's JavaScript runtime (`js-runtime.wasm`)
- `mise run version:patch|minor|major` - bump and sync the crates' versions
- `mise run publish --dry-run` - check the crates package; `mise run publish` publishes them

### Tests

Integration tests (`ishikari/tests`, `ishikari-admin/tests`, `ishikari-wasm/tests`) run against a real Postgres: each
test migrates a schema of its own (`ishikari_t_*`) and drops it at the end. They use
`ISHIKARI_TEST_DATABASE_URL`, else `DATABASE_URL`; without either they fail loudly, and
`ISHIKARI_SKIP_DB_TESTS=1` skips them. Helpers are in `ishikari/tests/common/mod.rs`
(`TestDb`, `eventually`). Fix a bug by writing the failing test first.

`tests/only-ishikari` is a workspace member (`publish = false`) whose only dependency is
`ishikari`: the macros must compile there, so their expansion names `::ishikari::__private`,
never `serde`, `typetag` or `async_trait`.

## Project Architecture

A workspace of four crates, and `ishikari-wasm` in a workspace of its own:

- `ishikari` - the library
- `ishikari-macros` - `#[ishikari::job]` and `#[ishikari::worker]` (syn 2)
- `ishikari-cli` - `ishikari generate migration|version`
- `ishikari-admin` - the dashboard: an axum router a host mounts behind its own auth, as
  standalone pages or inside the host's layout (`Layout`, `app_with_layout`). Its CSS is built
  from `styles/` (`mise run admin:css`) into `assets/admin.css` (with themes) and
  `assets/admin-embed.css` (no themes, for a host's layout); both are committed. Its scripts
  are plain JS with no build step: `assets/admin.js` (graphs, tooltips...) and
  `assets/builder.js`, the definition editor's workflow builder (HTML nodes, SVG edges, a
  `<canvas>` minimap), which lays nodes out with admin.js's `window.ikGraph`. Its Node types
  page uploads the host's node modules (`AppState::with_node_modules`), its script editor
  (`/node-types/script`) writes, test-runs and saves scripts, and its job form
  (`/node-types/job`) inserts a module's `ModuleJob`. Its Secrets page sets and deletes the
  host's secrets (`AppState::with_secrets`), never showing a value.
- `ishikari-wasm` - `WasmModules`, node types uploaded as WebAssembly components (wasmtime,
  the `ishikari:node` world in `wit/node.wit`) or as TypeScript/JavaScript scripts, each call
  in a sandbox with its module's grants and limits. A script's types are stripped by swc
  (`src/typescript.rs`), and it runs on `js-runtime.wasm`, a Boa component (`script-node`
  world) built from `js-runtime/` by `mise run wasm:js-runtime` and committed. Its own
  workspace and lockfile: wasmtime needs Rust 1.96, the workspace's MSRV is 1.83. Its tests
  upload `tests/fixtures/testkit-v*.wasm` (committed; rebuilt by `mise run wasm:fixtures`).

### Core components (`ishikari/src`)

- **`engine.rs`**:
  - `Engine`/`EngineBuilder` start the queues, stager, heartbeat, rescuer, resolver, pruner,
    cron task and the listener (`LISTEN` on `{schema}.ishikari_insert`, waking queues);
  - `Engine::shutdown(grace)` stops them all.
- **`engine/postgres.rs`**: `Postgres`, the storage (it carries the schema). Every job statement
  is here:
  - claiming;
  - outcome writes, fenced on `state = 'executing' AND attempt = $n`;
  - staging;
  - rescue;
  - give-back;
  - releasing workflow dependents on completion, and cascading failures;
  - saga rollback (`postgres/saga.rs`), in the transaction that ends a saga's job;
  - `retry_job` and `cancel_job`;
  - pruning;
  - releasing unique keys.
- **`queue.rs`**: a loop per queue, claiming on each poll, when the engine's listener wakes it, and
  as slots free up while jobs wait. It tracks running jobs in a `JoinSet` (which bounds
  concurrency) and drains on stop.
- **`insert.rs`**: `insert`, `insert_with`, `insert_all`, `InsertOpts`, `Unique`, `Inserted`.
- **`cron.rs`**: `Cron` entries, inserted once per slot through a unique key.
- **`stager.rs`**: moves due scheduled and retryable jobs to available.
- **`model.rs`**: `Job` and `JobState`, which decodes the enum from any schema.
- **`migrations.rs`, `../migrations/v0N.sql`**: the versioned schema SQL; the CLI writes it into
  hosts' migrations.
- **`workflows/`**:
  - `Pipeline`, `Dag`, `Saga`, sharing `step.rs` to insert steps (`pending` when they have
    upstreams; saga compensations parked `pending` until their rollback releases them);
  - `Workflow::progress`.
- **`dependencies/`**: dependency rows and the `DependencyResolver`, a safety-net sweep for
  workflows and sagas a job left behind (one rescued and discarded).
- **`execution/`**: workflow definitions run by the admin (YAML/JSON node graphs): `NodeTypes`
  runs their nodes by the host's registered types, then its `NodeModules` (uploaded types, at
  the version uploaded last before the run was created). `ModuleJob` runs a module as a plain
  job, through `NodeJobs`.
- **`secrets/`**: the host's secrets, read by name from a backend it picks (`Secrets`;
  `SecretStore` for one the admin writes to): `PostgresSecrets` (encrypted with the host's key,
  `ishikari_secrets`), `EnvSecrets`, `MemorySecrets`. Rust nodes read them from their context;
  modules only those granted (`ModuleGrants::secrets`), redacted from what they return and log.

### Job lifecycle

1. A job is inserted, optionally in the caller's transaction:
   - `available`, or `scheduled` for later;
   - `pending` for a workflow step waiting on its upstreams.
2. A queue claims it: `executing`, `attempt + 1`, with the node appended to `attempted_by`.
3. The run's outcome is one of:
   - `completed`, which releases its dependents and completes the workflow when it was the
     last step;
   - `retryable` with an error record;
   - `discarded` on the last attempt, which cancels its dependents and fails the workflow;
   - `cancelled`;
   - snoozed back to `scheduled`.
4. A job whose node stops heartbeating is rescued after `node_timeout`. Shutdown gives
   interrupted jobs back with the attempt refunded.
5. Finished jobs are pruned after `prune_after`.

### Schema

Each version adds to the last; never edit a released version's SQL in a way that changes what
it creates.
- v01: `ishikari_jobs`
- v02: workflows
- v03: dependencies, saga steps
- v04: definitions, runs, node executions
- v05: nodes, unique keys, error records, the `pending` state
- v06: notify when jobs become available (insert or update), the staging index, saga step indexes
- v07: saga rollbacks in progress moved off dependency chains (data only)
- v08: node modules (node types uploaded through the admin)
- v09: secrets (kept encrypted, and those modules are granted)

The installed version is the comment on `ishikari_jobs`. A new version means a new
`migrations/v0N.sql` and a bump of `CURRENT_VERSION`.

### Key design patterns

- **Type-safe job serialization**: jobs serialize as `Box<dyn Worker>` using typetag for dynamic
  dispatch.
- **Shared state**: the engine's `with_state` value is available to workers via
  `Context::state`.
- **Backoff**: Fixed, Linear, Exponential, ExponentialJitter or Custom.
- **Postgres is the coordinator**: `SKIP LOCKED` claims, attempt fencing, unique indexes (cron
  slots, unique jobs) and advisory locks (the resolver). No leader election.
