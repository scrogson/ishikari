# Ishikari 0.2: production-ready for a host app

The first host is Local Roster (axum + sqlx 0.8, one App Platform instance, which restarts on every
deploy). It needs ishikari for three things:
- durable multi-step workflows: preparing a town is a DAG of six steps;
- cron, replacing the `tokio::interval` loops in its `main.rs`;
- uniqueness, so the same work isn't queued twice.

This document is the plan to get there. It lists the problems found while reading 0.1.x and
how 0.2 fixes each one. Every bug below gets a failing integration test first, then the fix.

## Where 0.1.x stands

### Correctness bugs (P0: fix before any production use)

1. **The Stager releases workflow steps early.** Dependent steps are inserted as `scheduled` with
   `scheduled_at = now()`. `stage_jobs` promotes every due `scheduled` row without looking at its
   dependencies, so a DAG step becomes `available` about a second after insert, before its
   upstream finishes. (`stager.rs`, `engine/postgres.rs` `stage_jobs`; `workflows/dag.rs`,
   `pipeline.rs` and `saga.rs` insert paths.)
2. **Queue concurrency isn't enforced.** Each tick fetches up to `concurrency` jobs and spawns them
   detached, without counting the jobs still running. Long jobs pile up past the limit
   (`queue.rs`).
3. **The schema setting is mostly ignored.** `insert`, the outcome writes (complete, error,
   discard, snooze, cancel), `stage_jobs` and `prune_jobs` use an unqualified `ishikari_jobs`.
   Only `Engine::insert`/`insert_tx` qualify the table.
4. **Outcome writes are unguarded.** They are `UPDATE … WHERE id = $1` with no state or owner check.
   Once rescue exists, a slow original run could overwrite the rescued job.
5. **The final attempt is written in two statements** (`error_job`, then `discard_job`). A crash in
   between leaves `retryable` with `attempt == max_attempts`. The Stager then makes the job
   `available`, and the fetch (`attempt < max_attempts`) never takes it, so it stays `available`
   forever.
6. **The Stager stops for good on its first database error** (`?` in its loop, with the handle
   dropped).
7. **`#[worker]` silently drops every method except `perform` and `backoff`.** So `queue()`
   overrides written in the impl and any new trait method are lost.
8. **The execution module (v04) drifts from its schema.** `execution/storage.rs` uses
   `retry_count`, `skip_reason`, `created_at` and a `'pending'` status, none of which v04 creates.
9. **The published 0.1.1 is old.** It has no workflows, dependencies or execution, and
   `ishikari-cli` and `ishikari-admin` aren't published. Hosts end up on a path dependency.

### Missing for production (P1)

- **Rescue of orphaned jobs.** A job left `executing` by a crash or deploy stays there forever.
  Its workflow never completes and saga compensation waits on it. There is no node identity
  (`attempted_by` is commented out, and `EngineName` is unused) and no heartbeat.
- **Graceful shutdown.** There is no stop or drain. On SIGTERM, in-flight jobs are left
  `executing`.
- **Per-job timeout.**
- **Unique jobs.**
- **Cron.**
- **Insert options:** `scheduled_at`/`schedule_in`, priority, tags, meta, unique, and `insert_all`.
- **Inserting a workflow inside the host's transaction.** `run(&PgPool)` writes rows one statement
  at a time.
- **Dependency release.** It is a separate polling task that the host must remember to start. It
  isn't transactional and isn't safe on several nodes.
- **A core API for workflow progress,** so a host can show steps without the admin's HTML.
- **A safe pruner.** The current `prune_jobs` deletes everything terminal (including workflow
  history the resolver joins on) and is never called.
- **Admin:** no way to rescue or cancel an `executing` job; `/static` is served relative to the
  current directory, which breaks when mounted in a host.
- **Integration tests.** None of the 51 tests touch Postgres, and CI starts Postgres but applies no
  migrations.

## Design

### Node identity and heartbeats

- **New table:** `ishikari_nodes (name PK, queues TEXT[], started_at, heartbeat_at)`.
- **Node name:** each `Engine` gets one, by default `{hostname}-{pid}-{random}`. It upserts its row
  every `heartbeat` (default 15 s) and deletes it on shutdown.
- **Claiming** writes `attempted_by = attempted_by || node` (the last element is the owner).
- **Every outcome write** is guarded with `WHERE id = $1 AND state = 'executing' AND
  attempted_by[cardinality(attempted_by)] = $node`. If no row is updated, the job was rescued or
  cancelled meanwhile: that's logged and the result is dropped.
- **The final attempt** becomes one statement: `discarded`, with the error appended.

### Lifeline (rescue)

- **Where it runs:** every node runs the rescuer, every `rescue_interval` (default 60 s), as one
  statement with `FOR UPDATE SKIP LOCKED`.
- **What it takes:** jobs `executing` whose owner node is missing or has a heartbeat older than
  `node_timeout` (default 60 s). Each becomes `retryable` with the error "rescued: node X stopped
  heartbeating", or `discarded` if it has no attempts left.
- **Live nodes:** jobs on a node that is still heartbeating are never rescued, however long they
  run. Per-job timeouts cover runaway jobs.
- **Index:** a partial index on `(attempted_at) WHERE state = 'executing'`.

### Timeouts and graceful shutdown

- **Timeouts:** `Worker::timeout() -> Option<Duration>` (default `None`). `perform` runs under
  `tokio::time::timeout`; expiry is an error, retried with backoff.
- **Shutdown:** `Engine::shutdown(grace)` does the following in order:
  1. stop fetching on every queue;
  2. wait up to `grace` for in-flight jobs;
  3. return the rest to `available` with their attempt refunded (they were interrupted, not
     failed), using guarded writes;
  4. stop the Stager, rescuer, cron and resolver tasks (one `CancellationToken`);
  5. delete the node row.
- **Hosts** call it from their SIGTERM handler. `Engine::start` returns a handle that owns every
  task's `JoinHandle`, so nothing is detached.

### Concurrency

- **Per queue,** a `Semaphore(concurrency)`. A tick fetches only `available_permits()` jobs, and
  each spawned job holds a permit until its outcome is written.
- **Tick:** the polling interval stays.

### One schema-aware storage

- **Single path:** every SQL statement goes through `Storage`, which holds the schema. That covers
  insert, outcomes, staging, rescue, pruning, cron, dependencies and workflows.
- **Free functions:** `ishikari::insert(job, executor)` keeps working for the `public` schema; its
  schema-aware form is `insert_with(job, opts, executor)`. Every statement is built from one table
  name helper.

### Insert options and transactional inserts

- **Options:** `InsertOpts { queue, priority, scheduled_at, schedule_in, max_attempts, tags, meta,
  unique }`. The worker's defaults fill whatever isn't set.
- **Calls:**
  - `insert_with(job, opts, executor)`;
  - `insert_all(jobs, executor)`, one multi-row `INSERT`;
  - `Engine::insert*` delegate to them.
- **Executors:** every insert takes `impl PgExecutor` or `&mut PgConnection`, so a host can insert
  inside its own transaction.

### Unique jobs

- **New columns:** `unique_key TEXT` and `unique_states ishikari_job_state[]`. A partial unique
  index `ON ishikari_jobs (unique_key) WHERE unique_key IS NOT NULL`.
- **`Unique { by, period, states }`:**
  - `by` lists the fields that make the key: worker, queue, all args or chosen arg keys.
  - `period` adds a time bucket to the key (`floor(now / period)`).
  - `states` defaults to the incomplete states (available, scheduled, pending, executing,
    retryable).
- **Insert:** `INSERT … ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL DO NOTHING
  RETURNING id`. On a conflict, the existing id is selected. The result is
  `Inserted::New(id) | Inserted::Duplicate(id)`.
- **Releasing:** an outcome write that moves a job to a state outside its `unique_states` sets
  `unique_key = NULL`, freeing the key.
  - With the default states, "don't queue it twice while one is pending or running".
  - With `states = all` and a `period`, "once per period", until pruned.
- **Workers** can declare a default with `fn unique() -> Option<Unique>` or
  `#[worker(unique(by = "args", period = "1h"))]`.

### Cron

- **Registration:**
  `Engine::builder().cron("0 * * * *", || EngineTick {})`, or a `Cron` list with names and time
  zones (crontab syntax through the `cron` crate).
- **Every node** runs the cron task once a minute. For each entry due in the current minute, and
  for a short catch-up window after downtime (configurable, default none), it inserts the job with
  `unique_key = "cron:{name}:{slot}"` and `unique_states = all`. The unique index makes each slot
  run once across all nodes, with no leader election.
- **Pruning** keeps cron rows longer than the catch-up window.

### Workflows: a `pending` state and release on completion

- **New state:** `pending`, meaning waiting on dependencies (`ALTER TYPE … ADD VALUE`, in its own
  non-transactional migration). DAG, pipeline and saga steps that have upstreams are inserted
  `pending`, not `scheduled`. The Stager never touches `pending`, which fixes bug 1 by
  construction.
- **On completion:** the transaction that writes `completed` also marks the job's dependency rows
  satisfied. It then moves to `available` every dependent whose dependencies are all satisfied
  (`FOR UPDATE` on the dependents).
- **On final failure:** the transaction that writes `discarded`/`cancelled` marks the dependencies
  failed, cancels dependents transitively and marks the workflow failed, or starts compensation
  for a saga.
- **No polling needed:** release follows each outcome. The resolver stays as a slow safety sweep
  (every 60 s, `SKIP LOCKED`). The Engine starts it, so hosts can't forget it.
- **Transactional insert:** `Dag::run`, `Pipeline::run` and `Saga::run` take an executor, and
  insert the workflow, jobs and dependencies in the caller's transaction (or their own).
- **Step names:** stored in `meta.ishikari_step`.

### Workflow progress API

- **`Workflow::progress(executor, id) -> WorkflowProgress`:** the workflow row and its steps. Each
  step carries name, job id, worker, state, attempt and max attempts, errors, timings and
  `depends_on` by name.
- **Plain data:** serde-serializable, so a host can render it, or return JSON from its own route.

### Errors as records

Errors are stored as `{attempt, at, error, kind}` objects (`kind`: error, panic, timeout, rescued,
cancelled), not bare strings. The admin and the progress API show them per attempt.

### Pruner

- **Where it runs:** on every node (`SKIP LOCKED`), every `prune_interval`.
- **What it deletes,** in batches of 1,000: `completed`, `cancelled` and `discarded` jobs older
  than `prune_after` (default 7 days).
- **What it keeps:** jobs whose workflow isn't terminal, and cron rows within the catch-up window.
- **Workflow rows** go with their last job.

### Macros

- **Pass-through:** `#[worker]` keeps every method of the impl block.
- **New attributes:** `timeout = "30s"`, `priority = N` and `unique(...)`, beside `queue` and
  `max_attempts`.
- **syn:** moves to syn 2.

### Admin

- **Assets** are embedded with `rust-embed`, so nothing is read from the current directory.
- **Mounting:** `app_at(path, state)` is documented for nesting under a host's own auth layer.
- **New:** shows `pending` and `rescued`; rescues an `executing` job by hand; cancels `executing`
  (guarded).
- **Published** as `ishikari-admin` 0.2.

### Migrations

- **v05** adds `ishikari_nodes`, `unique_key` and `unique_states` with their index, the
  executing index, and moves errors to records (a backfill that keeps old strings as
  `{error: <string>}`).
- **v06** (non-transactional) adds the `pending` enum value. A follow-up statement moves
  dependent `scheduled` rows that still have pending dependencies to `pending`.
- **`ishikari-cli`** is published so hosts can run `ishikari generate migration`. Its foreign-key
  existence checks filter on schema (`pg_constraint` joined with `pg_namespace`), so a second
  tenant schema gets its foreign keys.

### Out of scope for 0.2

- **Data passing between steps.** Steps read and write the host's tables, which Local Roster's
  steps need anyway.
- **LISTEN/NOTIFY fetching.** Polling stays, and the insert trigger stays for later.
- **The v04 definitions, executor and builder SPA.** They are marked experimental behind an
  `experimental-definitions` feature, and their schema drift (bug 8) is fixed or the module is cut.

## Tasks

Each task lands with integration tests against real Postgres, plus docs for its own feature.

### 1. Test harness

- [x] 1.1 Integration tests in `ishikari/tests/`, against real Postgres: a helper creates a schema
  per test, applies the generated migrations into it and drops it afterwards. CI applies migrations
  and runs these tests. Verify: CI green with one trivial insert → complete test.

### 2. P0 bugs, each failing test first

- [x] 2.1 One schema-aware `Storage` path for every statement (bug 3). Verify: the whole suite
  passes in a non-`public` schema.
- [x] 2.2 Guarded outcome writes, and the final attempt in one statement (bugs 4, 5). Verify: a
  test where the job is moved away mid-run and the late completion is ignored; a test that the
  final failure goes straight to `discarded`.
- [x] 2.3 Per-queue semaphore (bug 2). Verify: concurrency 2 with 10 slow jobs never has more than
  2 executing.
- [x] 2.4 The Stager survives errors and logs them (bug 6). Verify: a test with an injected failing
  query.
- [x] 2.5 `#[worker]` passes every method through and moves to syn 2 (bug 7). Verify: a test
  overriding `queue()` in the impl, plus trybuild tests for the attributes.

### 3. Nodes, rescue, timeouts, shutdown

- [x] 3.1 v05 migration (nodes, unique columns, executing index, error records), `attempted_by`
  written and node heartbeats. Verify: a test that a node row appears and refreshes.
- [x] 3.2 Lifeline. Verify: a job left executing by a killed engine (a dropped runtime) is retried
  by a second engine after `node_timeout`; a long job on a live node is not rescued.
- [x] 3.3 `Worker::timeout`. Verify: a job sleeping past its timeout becomes `retryable` with a
  timeout error.
- [x] 3.4 `Engine::shutdown(grace)`, with a handle owning every task. Verify: in-flight jobs finish
  within grace; the rest return to `available` with their attempt refunded; no task outlives it.

### 4. Inserts, unique, cron

- [x] 4.1 `InsertOpts`, `insert_with`, `insert_all`, executor and transaction support. Verify: an
  insert inside a rolled-back host transaction leaves nothing, and a scheduled insert runs after
  its time.
- [x] 4.2 Unique jobs. Verify:
  - a duplicate insert while the first is pending returns `Duplicate(id)`;
  - after the first completes, a new insert is `New`;
  - `period` and `states = all` give once per period.
- [x] 4.3 Cron. Verify: two engines on one database run an every-minute entry exactly once per
  minute over 3 minutes (with a mocked clock); the catch-up window is honoured.

### 5. Workflows

- [x] 5.1 v06 `pending` state, with steps inserted `pending`. Verify: the bug 1 test (a dependent
  step does not run before its upstream completes, with the Stager running) passes.
- [x] 5.2 Release on completion and cascade on failure, in the outcome transaction. The resolver
  becomes an engine-started safety sweep. Verify:
  - a 3-level DAG runs in order with no resolver;
  - a failed middle step cancels its dependents and fails the workflow;
  - two engines never release a step twice.
- [x] 5.3 Transactional `run(executor)` for DAG, pipeline and saga, with step names in meta.
  Verify: a workflow inserted in a rolled-back transaction leaves nothing.
- [x] 5.4 `Workflow::progress`. Verify: a test that the steps, states, `depends_on` and errors match
  a running DAG.

### 6. Pruner, admin, release

- [x] 6.1 Age-based, batched pruner. Verify: old completed jobs are deleted, and jobs of a running
  workflow and recent cron rows are kept.
- [x] 6.2 Admin: embedded assets, `pending`/`rescued` shown, rescue and cancel of executing jobs,
  mount docs. Verify: admin tests, and the admin mounted under a prefix in an example host, behind
  an auth layer.
- [x] 6.3 Decide on the v04 definitions (feature-gated and fixed, or cut); fix the CLI's
  foreign-key checks. Verify: `cargo test --all-features`; a second schema gets its foreign keys.
- [ ] 6.4 Docs: README brought up to date with the real API (the engine builder, queues, cron,
  unique, workflows, shutdown, admin), CLAUDE.md fixed (mise, not Just), CHANGELOG. Release 0.2.0
  of `ishikari`, `ishikari-macros`, `ishikari-cli` and `ishikari-admin`. Verify: `just
  publish-dry-run` (or the mise task), then publish.

## Local Roster adoption (after 0.2.0, as a Local Roster change)

- **Dependencies:** move to the crates.io versions, and drop the `sed` lines in its Dockerfiles and
  CI that strip ishikari and `localroster-cli`.
- **Migration:** generate the upgrade, v2 → v6, into `/migrations`.
- **Engine in the web app:** the web app starts the engine with queues (default, engine, sync,
  autopilot) and shuts it down on SIGTERM. The `main.rs` intervals become cron entries: engine
  tick, event sync, newsletter sync, billing, sponsorships, rollups and housekeeping.
- **Town preparation** (change `town-autopilot`) becomes a DAG, and its progress page reads
  `Workflow::progress`.
- **Admin:** the ishikari admin is mounted at `/jobs`, for network admins.
