# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

Tasks are [mise](https://mise.jdx.dev) file tasks in `.mise/tasks`:

- `mise run dev` - format, lint, test
- `mise run build`, `mise run check`, `mise run clippy`, `mise run fmt`, `mise run clean`
- `mise run test [filter]` - all tests (`--watch` to re-run on change)
- `mise run pg:start` / `pg:stop` - the local Postgres in `.postgres` (the `DATABASE_URL` in
  `mise.toml`); `mise run db:setup` creates the dev database
- `mise run demo` - the admin demo (`ishikari-admin/examples/demo.rs`)
- `mise run version:patch|minor|major` - bump and sync the crates' versions
- `mise run publish --dry-run` - check the crates package; `mise run publish` publishes them

### Tests

Integration tests (`ishikari/tests`, `ishikari-admin/tests`) run against a real Postgres: each
test migrates a schema of its own (`ishikari_t_*`) and drops it at the end. They use
`ISHIKARI_TEST_DATABASE_URL`, else `DATABASE_URL`; without either they fail loudly, and
`ISHIKARI_SKIP_DB_TESTS=1` skips them. Helpers are in `ishikari/tests/common/mod.rs`
(`TestDb`, `eventually`). Fix a bug by writing the failing test first.

## Project Architecture

A workspace of four crates:

- `ishikari` - the library
- `ishikari-macros` - `#[ishikari::job]` and `#[ishikari::worker]` (syn 2)
- `ishikari-cli` - `ishikari generate migration|version`
- `ishikari-admin` - the dashboard: an axum router a host mounts behind its own auth

### Core components (`ishikari/src`)

- **`engine.rs`**:
  - `Engine`/`EngineBuilder` start the queues, stager, heartbeat, rescuer, resolver, pruner and
    cron task;
  - `Engine::shutdown(grace)` stops them all.
- **`engine/postgres.rs`**: `Postgres`, the storage (it carries the schema). Every job statement
  is here:
  - claiming;
  - outcome writes, fenced on `state = 'executing' AND attempt = $n`;
  - staging;
  - rescue;
  - give-back;
  - releasing workflow dependents on completion, and cascading failures;
  - `retry_job` and `cancel_job`;
  - pruning;
  - releasing unique keys.
- **`queue.rs`**: a polling loop per queue. It tracks running jobs in a `JoinSet` (which bounds
  concurrency) and drains on stop.
- **`insert.rs`**: `insert`, `insert_with`, `insert_all`, `InsertOpts`, `Unique`, `Inserted`.
- **`cron.rs`**: `Cron` entries, inserted once per slot through a unique key.
- **`stager.rs`**: moves due scheduled and retryable jobs to available.
- **`model.rs`**: `Job` and `JobState`, which decodes the enum from any schema.
- **`migrations.rs`, `../migrations/v0N.sql`**: the versioned schema SQL; the CLI writes it into
  hosts' migrations.
- **`workflows/`**:
  - `Pipeline`, `Dag`, `Saga`, sharing `step.rs` to insert steps (`pending` when they have
    upstreams);
  - `Workflow::progress`.
- **`dependencies/`**: dependency rows and the `DependencyResolver`, a sweep that also runs saga
  compensation.
- **`execution/`**: workflow definitions run by the admin (YAML/JSON node graphs).

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
