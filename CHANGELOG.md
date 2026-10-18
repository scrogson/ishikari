# Changelog

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
