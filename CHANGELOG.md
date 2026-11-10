# Changelog

## 0.6.0

Migration **v10**. Modules no longer read secrets' values: see Upgrading.

### Upgrading

- **sqlx 0.9**, in every crate (from 0.8): a host's `PgPool` must be sqlx 0.9's. The crates
  other than `ishikari-wasm` now need Rust 1.94, sqlx's (from 1.83). sqlx 0.9 takes a query
  built at run time only as `AssertSqlSafe(..)`, and no longer takes a connection's user from
  the operating system: name it in the URL or `PGUSER`.
- **Run v10** (`ishikari generate migration --database-url …`, or `migrations::migrate`). It
  adds `ishikari_jobs.claims`, `ishikari_secrets.hosts` and
  `ishikari_workflow_runs.module_versions`. Upgrade every node together: a node
  that doesn't count claims writes outcomes the others' fence refuses.
- **A module is never given a secret's value.** `secret(name)` in a script, and `host.secret`
  in a component, give a placeholder, `{{secret:NAME}}`. `fetch` replaces it with the value
  in a request's headers, and only in a request to a host the secret is bound to; a
  placeholder in a request to another host, or in its URL or body, fails the request. What a
  bound host sends back has the value replaced by the placeholder again. A module that used a
  value otherwise (in a URL or body, to sign a request, …) can't any longer: a Rust node,
  which reads values (`NodeContext::secret`), can.
- **Bind each secret to the hosts it's for** (`api.stripe.com`, `127.0.0.1:8080`,
  `*.example.com`), or modules can send it nowhere: on the dashboard's Secrets page (its
  Hosts; set with no value, only the hosts change), with `SecretStore::bind`, or for
  `EnvSecrets` in `{PREFIX}{NAME}_HOSTS`, split by commas. A secret kept before v10 is bound
  to none.
- `NodeModules::handler` takes a `ModuleVersion` (the latest uploaded by a time, or exactly
  one) and gives a `ModuleHandler`, the handler with its version.
- `SecretStore::set` takes the secret's hosts, and an implementation of `SecretStore` needs
  `bind`. `SecretInfo` has `hosts`; `SecretError` has `InvalidHost`. A `Secrets` backend of
  your own binds its secrets to no host unless it overrides `Secrets::get_bound`.
- `Job` has a new public field, `claims`: a struct literal of it needs it too.
- A schema name given to any of the crates (`Postgres::with_schema`, each builder's
  `schema`, the migrations) must be lowercase ASCII letters, digits and
  `_`, not starting with a digit, of at most 63 bytes (`migrations::is_valid_schema`). Any
  other panics, and `migrations::migrate` refuses it with an error.
- `EnvSecrets::new("")` panics: with no prefix, every environment variable would be a secret.
- A definition may have at most 1000 nodes.
- A script nested more than 2048 deep is refused on upload, as is one with a `/` that may
  divide or begin a regular expression after a `}` or `)` (put either in parentheses) or an
  escape in a name.
- A module may be granted at most 10 minutes and 1 GiB, unless the host raises them
  (`WasmModules::max_timeout`, `WasmModules::max_memory_mb`); one kept with more runs within
  them. A module is at most 32 MiB (`ishikari_wasm::MAX_MODULE_BYTES`).

### Added

- `ishikari_admin::local_only`, the admin answering only requests for `localhost`,
  `127.0.0.1` or `[::1]`, for serving it on the loopback interface without auth. The
  `ishikari-admin` binary and the examples use it.
- `WasmModules::max_timeout` and `WasmModules::max_memory_mb`.
- `migrations::is_valid_schema` and `migrations::checked_schema`.
- `Secrets::get_bound` and `BoundSecret` (a secret's value with its hosts),
  `SecretStore::bind`, `secrets::check_hosts`, and `MemorySecrets::with_hosts`.

### Fixed

- A run keeps the version of each module its nodes first ran
  (`ishikari_workflow_runs.module_versions`), and a `ModuleJob` its module's, in its `meta`:
  an upload that started before a run and was kept after it no longer changes what the run's
  later nodes and retries run.
- A job's outcome is fenced on its claim (`claims`, counted on each), not its attempt, which a
  retry, give-back or cancel and retry reuses.
- An outcome that can't be written after retrying is replaced by an error, so its job doesn't
  stay executing.
- A job row with null `tags` is claimed as having none, rather than failing its claim's batch.
- The resolver's advisory lock is a transaction's, released however its sweep ends.
- A definition's cycle check, and `Validator`'s, no longer recurse once per node.
- A malformed field path (`a[`, `a[x]`, …) finds nothing rather than panicking.
- A job whose arguments don't deserialize logs and fails with where and why, not their values.
- The admin keeps `X-Frame-Options` on pages inside the host's layout (`app_with_layout`).
- A module's log lines are cut at 8 KiB and its logs at 1 MiB per call; its output at 10 MiB
  and its error at 64 KiB.
- A script's types are stripped on a thread of its own, once its nesting is bounded.
- The modules kept compiled are the 64 uploaded or run last; a module only described or
  test-run isn't kept.
- A definition's export is named with only `[A-Za-z0-9._-]` of its name.
- A list's page and its size are kept in range (pages up to 1,000,000, of 1 to 100 rows).

## 0.5.0

All five crates move to 0.5.0, `ishikari-wasm` among them for the first time. Migrations **v08**
and **v09**.

### Upgrading

- **Run v08 and v09** (`ishikari generate migration --database-url …`, or
  `migrations::migrate`). Both only add: v08 `ishikari_node_modules`, v09 `ishikari_secrets` and
  modules' granted secrets. Nothing changes for a host that uses neither.
- **Give the builder your node types:** its palette no longer lists 13 node types hardcoded in
  the admin. Pass `AppState::with_node_types(types.catalog())` (see `NodeTypes` below). With a
  catalog, saving, validating or importing a definition with a node of another type answers
  400 "Unknown node type 'x'"; with none, the palette is empty and any type is saved.
- **`when` counts null and 0 as false** (`is_truthy`), in `WorkflowExecutor` as in `NodeTypes`:
  a node whose `when` resolves to either is now skipped.
- **A skipped node's outputs are null** where a node refers to them, rather than failing it.
- Importing a definition that doesn't parse answers 400, not 200.
- The `embed-frontend` feature does nothing and can be dropped (see the builder below).
- `ishikari-wasm` needs Rust 1.96 (wasmtime's); the other crates' MSRV stays 1.83.
- The admin's pages may be framed by their own origin only (see Security). A host that frames
  them from another sets `X-Frame-Options` itself.

### Added

- **`NodeTypes`** (`ishikari::execution`), the host's node types for the admin's definition runs:
  each a `NodeSpec` (name, palette category, description, input and output schemas) and the
  `NodeHandler` that runs it. Its `catalog()` is the builder's palette, and as the `NodeRunner`
  of a `NodeJobs` it runs each node:
  - resolves its inputs against the run's inputs (a declared input left out takes its
    `default`) and the outputs of the nodes before it;
  - skips it when its `when` is falsy or doesn't resolve, and runs the nodes after it;
  - records its inputs, output and duration;
  - completes the run with the definition's outputs once every node has completed or been
    skipped, and fails it when a node fails its last attempt. A node of a type neither
    registered nor uploaded fails its run without retrying.

  `SchemaField` gains builders (`string()`, `integer()`, … `.required()`, `.describe()`,
  `.default()`), and `is_truthy` is public.
- **`NodeModules`**, node types found at run time: `NodeTypes` falls back to them for a type
  with no Rust handler, at the version uploaded last before the run (or job) was created, so a
  run and its retries run the same code.
- **`ishikari-wasm`**, a new crate: `WasmModules` runs node types uploaded as WebAssembly
  components of the `ishikari:node@0.1.0` world (`wit/node.wit`), or as scripts.
  - Each call gets a fresh store, a timeout (epoch interruption, with its sleeps and host waits
    capped at the deadline) and a memory limit on all its memories together.
  - `fetch` reaches only allow-listed hosts, with the URL's `Host` header whatever the guest
    sets; WASI gets no files, env, args or sockets.
  - Compiled components are cached by sha256.
- **Scripts:** TypeScript or JavaScript ES modules exporting `spec` and `run`, on
  `js-runtime.wasm` (Boa). swc strips the types and keeps positions, so errors point at the
  TypeScript as written. The globals are `console`, `fetch`, `URL`, text encoding, base64,
  `structuredClone`, timers, `AbortController` and `secret`.
- **`ModuleJob`**: `ModuleJob::new("name", json!({..}))`, in the `modules` queue, runs a
  module's node as a plain job. Its output completes the job, `{"snooze": s}` snoozes it and
  `{"cancel": why}` cancels it. Retries, uniqueness and cron work as for any job.
- **Secrets** (`ishikari::secrets`), the host's, read by name from a backend it picks:
  - the `Secrets` trait, and `SecretStore` for a backend the admin writes to;
  - `PostgresSecrets`, encrypted (XChaCha20-Poly1305) with a key the host holds, and
    `rotate()` for a new key; `EnvSecrets`; `MemorySecrets`.

  Names are letters, digits and `_ - . /`, without empty, `.` or `..` segments. Rust nodes
  read them with `NodeContext::secret`. A module reads only those it's granted
  (`ModuleGrants::secrets`), and their values are `[redacted]` in its output, errors and logs
  (what it prints whole, so a value of several lines goes too). Redaction guards against a
  mistake, not a module meant to leak one: whoever uploads modules can read what they grant.
- **The admin**, for a host that opts in (`AppState::with_node_modules`, `with_secrets`):
  - a Node types page uploads modules with their grants and limits, and lists their versions;
  - a script editor (`/node-types/script`) writes, test-runs and saves scripts;
  - a job form (`/node-types/job`) inserts a `ModuleJob`;
  - a Secrets page sets and deletes secrets, never showing a value, with the modules granted
    each.

### Changed

- **The workflow builder is one plain script** (`assets/builder.js`, no React and no build step):
  HTML node cards and an SVG edge layer in one pan and zoom, a `<canvas>` minimap, and the run
  graphs' layered layout for positions and Tidy.
  - Add a node from the palette by clicking (after the selected node, connected) or dragging.
  - Connect nodes by dragging ports; cycles, self-links and duplicates are refused.
  - A properties panel edits the workflow's inputs and outputs and each node's inputs, `when`,
    timeout and retries, with a `{{ }}` menu of inputs and upstream outputs.
  - Renaming a node rewrites its dependents and every `{{nodes.id.…}}` reference.
  - Undo and redo, keyboard shortcuts, and a warning before leaving unsaved changes.

  The React app and `rust-embed` are gone; `/builder` paths still go to the definition editor.

### Fixed

- **A definition run no longer stays `running` forever when a node's job ends where its
  runner can't see,** whatever the runner: timed out, panicking, cancelled, or rescued from a
  stopped node and discarded. The node is recorded failed with the job's error and the run
  fails; the resolver's sweep catches runs a rescued job, or an older ishikari, left behind.
  Retrying the job runs the run again.
- **A node referring to a skipped node's outputs** failed to resolve its inputs, and so failed
  the run after retrying in vain. They're null now, so branches that merge can read whichever
  ran.
- `InsertOpts::schedule_in` panicked on a delay past chrono's last time, and ran the job now on
  one past its longest duration. Either schedules it at the last time.

### Security

- The admin's responses say `X-Frame-Options: SAMEORIGIN` (unless the host set it): another
  site framing a page could have its user click the page's buttons, which pass the same-origin
  check as the page's own.
- The lockfiles take rustls-webpki 0.103.16 (RUSTSEC-2026-0381, RUSTSEC-2026-0382), and
  `ishikari-wasm`'s takes wasmtime 49.0.5 (RUSTSEC-2026-0420, -0422, -0423, -0424) and tokio
  1.54.1 (GHSA-5wjr-9356-4wc8).

## 0.4.0

All four crates move to 0.4.0. Migrations **v06** and **v07**.

### Upgrading

- **Run v06 and v07** (`ishikari generate migration --database-url …`, or
  `migrations::migrate`). On a large `ishikari_jobs`, create `ishikari_jobs_staging_index`
  `CONCURRENTLY` first (see `migrations/v06.sql`); the migration then skips it.
- **Upgrade every node together:** v07 changes how sagas roll back, and an older node's
  resolver would roll them back the old way.
- **Keep the `Engine`:** dropping it now stops it, so `let _ = Engine::builder(..).start(..)`
  stops at once. Hold it until `shutdown`.
- **Behind PgBouncer in transaction mode**, set `EngineBuilder::listen(false)`.
- **`tokio` features:** `ishikari` asks only for `macros`, `rt`, `sync` and `time`. Ask for
  `rt-multi-thread`, `signal` and the like yourself.
- `Workflow::set_state`, `complete`, `fail` and `cancel` return `Result<bool>`.
- The admin's job lists take `before`/`after` instead of `page`, and its list templates take a
  `Pager`.

### Added

- **`EngineBuilder::listen`** (on by default): the engine listens for jobs becoming available
  on one of the pool's connections and wakes their queue.
- **`Postgres::cancel_saga`** cancels a running saga and rolls it back. A step that is running is
  left to finish, then undone.
- **`Queue::heartbeat_interval`**, for a queue started on its own (default 15 s).
- **`ishikari::execution::NodeJob`**, the job for one node of a workflow definition's run, is
  now the library's: its arguments, its name (`ishikari::execution::NodeJob`), its queue
  (`workflows`), and the node's attempts and timeout. A host runs its node types through a
  `NodeRunner`, in an engine whose state is a `NodeJobs` (see the README's Running
  definitions).

### Changed

- **Jobs start when there's room, not at the next poll:**
  - a queue refills a finished job's slot at once while jobs are waiting;
  - the engine wakes a queue when a job is inserted available or becomes available (staged,
    released by its upstreams, retried, given back);
  - each queue's `interval` is now a fallback poll;
  - a queue was capped at concurrency ÷ interval (10 jobs/s by default), and a new job waited up
    to a second.

  v06 notifies on jobs becoming available by update, and adds an index that makes staging an
  index scan.
- **Sagas roll back in the transaction that fails their step**, not on the resolver's next tick:
  - its unstarted steps are cancelled;
  - once nothing is running, the completed steps are undone in reverse order, one compensation
    at a time;
  - a failed compensation marks its step `compensation_failed` and the rest still run;
  - the saga ends `compensated`, or `failed` if a compensation failed;
  - retrying a failed compensation (`retry_job`, or the admin's Retry) finishes the rollback.

  Cancelling a saga (the admin's Cancel) rolls it back too, ending `compensated` or `failed`.
  Compensations wait in `pending` rather than `scheduled` in the year 3000. v07 moves rollbacks
  in progress to the new scheme.
- **`#[ishikari::job]` and `#[ishikari::worker]` need only `ishikari`:** they reach `serde`,
  `typetag` and `async-trait` through it, so a host can drop those dependencies.
  `#[ishikari::job]` with arguments is a compile error; it ignored them, and options such as
  `queue` go on `#[ishikari::worker]`.
- **Engines supervise their background tasks.** The queues, heartbeat, stager, rescuer,
  resolver, cron, pruner and listener restart after a panic, with a backoff of 1 s doubling to
  60 s, and the panic is logged at error. A queue keeps the jobs it is running.
- **`Engine` is `#[must_use]`, and dropping it stops it:** its queues give their running jobs
  back with the attempt refunded, and its background tasks are aborted. `shutdown` is still the
  graceful path.
- **The admin reads indexes, not the whole jobs table:**
  - the job lists page by id;
  - counts are exact up to 10,000 and estimated past that (`≈1.2M`);
  - the dashboard's and queues' per-state counts, and the filter dropdowns, come from the state
    index.

  On a million jobs, the dashboard went from 348 ms to 8 ms and a deep `/jobs` page from 17 s to
  2 ms.
- **Inserts log only the job's id, worker and queue, at debug.** They no longer log the args,
  which may hold personal data, at info.
- **A worker's error is stored with its sources**, as `outer: inner: root`.
- **`serde_norway` replaces `serde_yaml`.** The admin reads and writes definitions' YAML with
  the maintained fork of the deprecated `serde_yaml`, whose `unsafe-libyaml` is unmaintained
  (RUSTSEC-2024-0449).
- **`rust-version = "1.83"`** on every crate, checked in CI.
- **Fewer dependencies:**
  - `ishikari` no longer depends on `figment` or `tracing-subscriber`;
  - it asks `tokio` (at least 1.41) for `macros`, `rt`, `sync` and `time` instead of `full`.

### Fixed

- **A queue started on its own** (`Queue::builder(..).build(..).start()`) **heartbeats as its own
  node:** an engine's rescuer used to run its jobs a second time after `node_timeout`. Its node is
  named `{queue}-{host}-{pid}-{random}`; the old `{queue}-{pid}` collided across containers.
- **Jobs whose worker type the node doesn't have**, as during a rolling deploy, are put back for
  later without using an attempt or recording an error: 5 s, doubling to 5 min. Args that don't
  fit a known worker are still an error.
- **Saga compensations can't run or be retried before their rollback reaches them.** A
  compensation dropped after a successful saga can't be retried either.
- **A rolled-back saga can't be retried forward:** `retry_job` refuses a step of a saga that
  isn't running.
- A saga with no compensations whose step is cancelled ends `compensated`, not `completed`.
- The resolver no longer flags every compensation in progress as stuck on every tick.
- **Running a definition from the admin** inserted jobs for `ishikari_workflows::NodeJob`, a
  worker nothing registers, so none could run. A run is now one transaction:
  - its row, and a Dag of `NodeJob`s, each step named by its node;
  - the roots are available and the rest `pending` until their upstreams complete (they were
    `scheduled`, so the stager started them at once);
  - a node's `max_retries: 0` is one attempt (it was zero, which the database refused);
  - its `timeout_seconds` applies.
- **A definition's recent runs in the admin showed every run `running`,** even once its workflow
  had completed or failed: nothing updated the run row's status. A run's status and completion
  time are now its workflow's once the workflow has finished, so the run follows it through a
  failure and a retry. While the workflow runs, the run row's status stands.
- **The published `ishikari-admin` has the workflow builder.** `frontend/dist` was gitignored, so
  the crate embedded nothing and the definition editor never loaded. The build is now committed
  (`mise run frontend:build`). Its assets' URLs are relative, so they work under a base path,
  and the builder's other paths (`/builder/...`) go to the definition editor.
- **A job's Retry or Cancel that changes nothing** (the job had moved on) says so on the job's
  page, rather than looking as if it worked.
- **The admin refuses (403) actions a browser sends from another site (CSRF):** a POST or PUT
  whose `Sec-Fetch-Site` is another site's, or, without it, whose `Origin` isn't the `Host`.
  Requests with neither header (not a browser's) pass.
- **A job whose outcome couldn't be written no longer stays `executing` until its node
  restarts.** The rescuer leaves a live node's jobs alone, so:
  - a failed write is retried with backoff for up to a minute;
  - a worker's `backoff` that panics gets the default instead;
  - a worker's `timeout` that panics fails the attempt as a panic, rather than killing the run.
- **`Backoff` no longer overflows or panics:**
  - `Exponential` at high attempts;
  - `ExponentialJitter` on a base under a second;
  - `Linear` and `Fixed` past chrono's range.

  The built-in strategies work in milliseconds, so a sub-second base no longer counts as zero.
  They never wait longer than `Backoff::MAX_DELAY` (7 days).
- **A zero interval panics where it is set** (`Queue::interval`, the engine's `*_interval`
  setters, `Stager::new`, `DependencyResolver::new`), naming it. It used to silently kill the
  background task it drives: a queue, or the heartbeat, which got the node's jobs rescued.
- **Cron slots are no longer lost:**
  - a slot whose insert fails is tried again on later checks, within `cron_catch_up`;
  - when more than 100 slots are due at once, the rest are inserted on the next check, with a
    warning, instead of being skipped.

### Removed

- **The admin's `POST /jobs/{id}/discard`** (`routes::jobs::discard`). Nothing used it, and it
  marked a job discarded outside the library, setting `completed_at` rather than `discarded_at`.
  Cancel stops a job.

## 0.3.0

All four crates move to 0.3.0 together; only `ishikari-admin` changed: a redesigned dashboard.
No migration. A host that styles its own pages with the classes `admin-embed.css` used to carry
(daisyUI's `btn`, `badge`, `table`...) needs to bring daisyUI itself.

### Changed

- **The admin's new look:** a sidebar layout, cards, tables, badges and forms of its own (`ik-*`
  classes) in place of daisyUI. Job counts link to their jobs; the jobs' distribution by state
  is a bar; the standalone pages follow the system's light or dark mode, with a toggle that
  remembers the choice, and have a jump-to-job box.
- **Theming from the host's variables:** the dashboard's colors and radius come from
  `--ik-<token>`, else shadcn/ui's `--<token>`, else Tailwind's `--color-<token>`, else
  daisyUI 5's, else defaults that follow `color-scheme` (see the README's Theming). A host
  that themed the dashboard with daisyUI keeps its colors. `admin-embed.css` no longer has
  daisyUI's component classes (`btn`, `badge`, `table`...).
- **Graphs:** a workflow's jobs, a definition's nodes and a job's dependencies are drawn as a
  graph, laid out left to right with an edge from each upstream to what waits on it, colored
  by the upstream's state. Drag to pan, ctrl/⌘ + scroll or pinch to zoom, and hover a node to
  trace what it depends on and what depends on it. A workflow's nodes are titled by step name.
- **Syntax highlighting:** arguments, metadata, inputs and outputs (JSON) and a definition's
  source (YAML) are highlighted when rendered, in the theme's colors, with `{{ ... }}`
  expressions marked inside strings.

## 0.2.2

All four crates move to 0.2.2 together; `ishikari` and `ishikari-admin` changed. Fixes only, no
migration.

### Fixed

- A workflow whose last steps finish at the same time completes then, not at the resolver's
  next sweep: each completion locks the workflow's row before checking whether every step has
  completed, so the last one sees the others.
- The admin's workflow Retry and Cancel each run in one transaction, locking the jobs' rows
  before the workflow's: a retried step that completes during the retry completes the workflow
  rather than leaving it running until the resolver's next sweep, and a cancel racing the last
  step's completion leaves the workflow completed instead of overwriting it with cancelled.
  Cancelling a workflow also records why in each step's errors, releases their unique keys and
  cancels what depends on them outside it, as cancelling a job does.
- Retrying a workflow keeps its steps' errors, the history of their earlier runs, instead of
  clearing them, and clears their `discarded_at` and `cancelled_at`, as retrying a job does.

## 0.2.1

All four crates move to 0.2.1 together; only `ishikari-admin` changed.

### Added

- **The dashboard inside the host's own layout:** `app_with_layout(state, layout)` renders each
  page through the host's `Layout` (`fn render(&self, parts: &Parts, page: Page) -> Response`).
  It gets the request's parts, so whatever the host's auth middleware put in the extensions, and
  the `Page`: its title, body, the dashboard's sections and the current one. htmx fragments are
  never wrapped. `app` is unchanged: standalone pages.
- **`admin-embed.css`** (`embed_stylesheet_href(base_path)`): the dashboard's daisyUI components
  and Tailwind utilities without themes, reset or root styles, so the host's daisyUI theme
  colors it. `Page::nav_html` renders the sections as a row of links for a layout.
- **Pending on the dashboard:** the count of workflow steps waiting on their upstreams, next to
  the other states (`JobStats::pending`).
- Asset links carry the crate's version, so an upgrade isn't hidden behind the browser's cache.

### Fixed

- The dashboard's counts no longer turn into raw JSON when they refresh: `/api/stats` answers
  htmx with the counts' HTML, and other clients with JSON as before.
- Recent Failures on the dashboard, and the failure patterns on Analytics, were always empty:
  their queries read the jobs' errors as JSON rather than an array of error records.
- The queue page's state filter replaces the page's content, not the whole body.

## 0.2.0

Production readiness: jobs survive restarts and crashed nodes, plus unique jobs, cron and
workflows that run in order. Migration v05; regenerate with `ishikari generate migration
--database-url $DATABASE_URL`, or call `ishikari::migrations::migrate`.

### Breaking

- The `Storage` trait is gone. `Postgres` is the storage, and it carries the schema. `Engine`,
  `EngineBuilder`, `Queue`, `QueueBuilder` and `Stager` are no longer generic.
  `Engine::builder(name)…start(Postgres::new(pool))` is unchanged.
- `QueueBuilder::schema` is gone; set the schema on the engine (`EngineBuilder::schema`).
- `insert`, `Engine::insert_tx` and workflow `run` take `sqlx::Acquire` (a pool, connection or
  transaction) rather than `PgExecutor`. `&pool` and `&mut *tx` work as before. They (and
  `insert_with`, `insert_all`, `Workflow::progress`) return a boxed `Send` future, so they can
  be awaited inside a worker's `perform`.
- Job errors are records (`{attempt, at, kind, error}`) rather than strings. v05 converts old
  ones.
- `JobState` has a `Pending` variant, for workflow steps waiting on their dependencies.
- `#[worker]` keeps every method written in the impl. Unknown attributes, and options set both
  as an attribute and a method, are compile errors.
- The migration SQL moved from `ishikari-cli` to `ishikari::migrations`.

### Fixed

- Workflow steps no longer start before their upstreams finish. The stager used to release
  them about a second after insert.
- Queues no longer run more jobs at once than their concurrency.
- An engine configured with a schema writes every job change there. Before, only inserts used
  it, and reading a job outside `public` failed.
- A run's result is written only to its own attempt. A job changed while it ran keeps its new
  state.
- A last failed attempt is discarded in the same statement that records its error. A crash can
  no longer leave a job available forever.
- The stager survives database errors.
- Migration foreign-key checks are scoped to their schema, so a second schema gets its foreign
  keys.
- Node executions (workflow definitions) match their schema.

### Added

- **Nodes and rescue:** every engine heartbeats into `ishikari_nodes` and records itself in
  `attempted_by`. Jobs left executing by a node that stopped are given back.
- **`Engine::shutdown(grace)`:** stops claiming, waits for running jobs, and gives back the rest
  with their attempt refunded.
- **`Worker::timeout`** / `#[worker(timeout = "30s")]`.
- **`InsertOpts`:** queue, priority, schedule, max attempts, tags, meta, unique, schema.
  `insert_with` returns `Inserted::New`/`Duplicate`; `insert_all` inserts a batch.
- **`Unique`:** by worker, queue and args (all or chosen), an explicit key, a period, and the
  states the key is held in. `Worker::unique` sets a default.
- **`Cron`:** five- or six-field patterns in any time zone, once per slot across nodes, with
  catch-up after downtime.
- **Workflows:**
  - steps are released and failures cascaded in the transaction that finishes the upstream;
  - `run` is atomic;
  - steps are named (`meta.ishikari_step`);
  - `Workflow::progress` reads each step;
  - the engine starts the dependency resolver itself (one node at a time).
- **`retry_job` / `cancel_job`** for single jobs:
  - retrying brings back what a failure cancelled;
  - cancelling covers executing and pending jobs;
  - cancelling also cancels downstream jobs.
- **Pruning:** finished jobs are deleted after `prune_after` (7 days), batched, keeping running
  workflows.
- **`ishikari::migrations`:** `sql`, `migrated_version`, `migrate`.
- **Admin:**
  - the pending state;
  - error records with kind;
  - Give back for executing jobs and Run now for scheduled ones;
  - assets compiled in, with no current-directory `/static`.
- **Tests:** integration tests against real Postgres, one schema per test.
