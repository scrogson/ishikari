# Changelog

## 0.3.1 (unreleased)

`ishikari` and `ishikari-admin` change. No migration.

### Added

- **`ishikari::execution::NodeJob`**, the job for one node of a workflow definition's run, is
  now the library's: its arguments, its name (`ishikari::execution::NodeJob`), its queue
  (`workflows`), and the node's attempts and timeout. A host runs its node types through a
  `NodeRunner`, in an engine whose state is a `NodeJobs` (see the README's Running
  definitions).

### Changed

- **`rust-version = "1.83"`** on every crate, checked in CI.
- **Fewer dependencies:** `ishikari` no longer depends on `figment` or `tracing-subscriber`, and
  asks `tokio` for `macros`, `rt`, `sync` and `time` (at least 1.41) instead of `full`. A host
  that relied on `ishikari` to turn on other `tokio` features (`rt-multi-thread` for
  `#[tokio::main]`, `signal`, `net`...) needs to ask for them itself.

### Fixed

- Running a definition from the admin inserted jobs for `ishikari_workflows::NodeJob`, a worker
  nothing registers, so none could run. A run is now one transaction: its row and a Dag of
  `NodeJob`s, each step named by its node, the roots available and the rest `pending` until
  their upstreams complete (they were `scheduled`, so the stager started them at once). A
  node's `max_retries: 0` is one attempt (it was zero, which the database refused), and its
  `timeout_seconds` applies.
- The published `ishikari-admin` has the workflow builder: `frontend/dist` was gitignored, so
  the crate embedded nothing and the definition editor never loaded. The build is committed
  now (`mise run frontend:build`). Its assets' URLs are relative, so they work under a base
  path, and the builder's other paths (`/builder/...`) go to the definition editor.
- A job's Retry or Cancel that changes nothing (the job had moved on) says so on the job's
  page, rather than looking as if it worked.
- The admin refuses (403) actions a browser sends from another site, so another site can't
  have an admin's browser retry, cancel or run jobs (CSRF): a POST or PUT whose
  `Sec-Fetch-Site` is another site's, or, without it, whose `Origin` isn't the `Host`. Requests
  with neither header (not a browser's) pass.

### Removed

- The admin's `POST /jobs/{id}/discard` (`routes::jobs::discard`): nothing used it, and it
  marked a job discarded with `completed_at` set rather than `discarded_at`, outside the
  library. Cancel stops a job.

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
