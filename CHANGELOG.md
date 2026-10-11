# Changelog

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
