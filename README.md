# Ishikari

Background jobs for Rust, stored in Postgres. Jobs are rows, so you insert them in the same
transaction as your own writes, and they survive restarts, deploys and crashed nodes.

- **Workers** are plain structs with `#[ishikari::job]` and `#[ishikari::worker]`.
- **Queues** run a set number of jobs at once, with retries, backoff and per-job timeouts.
- **Reliable across restarts:**
  - every node heartbeats;
  - jobs left running by a node that stopped are given back to their queue;
  - a graceful shutdown waits for running jobs, then returns the rest.
- **Inserts** take options: a time to run, priority, tags, meta, a schema and uniqueness.
  `insert_all` inserts a batch in one statement.
- **Unique jobs** are unique by worker, queue and some or all arguments, optionally within a
  period. Duplicates return the existing job.
- **Cron** runs jobs on a crontab schedule in any time zone, each slot exactly once however
  many nodes run, with no leader election.
- **Workflows:**
  - pipelines, DAGs and sagas with compensation;
  - each is inserted atomically;
  - steps start the moment their upstreams complete;
  - a failure cancels everything downstream;
  - progress can be read step by step.
- **Pruning** deletes finished jobs after a retention period.
- **Admin dashboard** (`ishikari-admin`): an axum router you mount inside your app, behind your
  own auth.

PostgreSQL 12 or later, and Rust 1.83 or later.

## Install

```toml
[dependencies]
ishikari = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The macros need nothing else: no `serde`, `typetag` or `async-trait` of your own.

### The schema

Write ishikari's tables into your migrations with the CLI:

```bash
cargo install ishikari-cli
ishikari generate migration                         # all versions, public schema
ishikari generate migration --database-url $DATABASE_URL   # only what your database lacks
ishikari generate migration --schema tenant_a       # a schema of its own
sqlx migrate run
```

Or apply it from code, which suits tests and apps without a migration tool:

```rust
ishikari::migrations::migrate(&pool, "public").await?;
```

The installed version is recorded as the comment on `ishikari_jobs`, so upgrades only add
what's missing.

## Workers

```rust
use ishikari::prelude::*;

#[ishikari::job]
struct SendWelcome {
    user_id: i64,
}

#[ishikari::worker(queue = "email", max_attempts = 5, timeout = "30s")]
impl Worker for SendWelcome {
    async fn perform(&self, ctx: Context) -> PerformResult {
        let mailer = ctx.state::<Mailer>()?;
        mailer.welcome(self.user_id).await?;
        Complete::default().into()
    }
}
```

`perform` can complete the job, cancel it (`Cancel::default().message(..)`), snooze it
(`Snooze(seconds)`), or return an error to retry it with the worker's backoff. A panic counts as
an error. Any of `queue`, `max_attempts`, `timeout`, `backoff` and `unique` can also be written
as methods in the impl.

## Inserting jobs

```rust
use ishikari::{InsertOpts, Unique};

// With the worker's defaults, inside your own transaction.
let mut tx = pool.begin().await?;
// ... your writes ...
ishikari::insert(SendWelcome { user_id }, &mut *tx).await?;
tx.commit().await?;

// With options: in an hour, unless one for this user is already waiting or running.
let inserted = ishikari::insert_with(
    SendWelcome { user_id },
    InsertOpts::new()
        .schedule_in(std::time::Duration::from_secs(3600))
        .unique(Unique::new().args(["user_id"])),
    &pool,
)
.await?;
if inserted.is_duplicate() { /* already queued */ }
```

`Unique::new().period(Duration::from_secs(3600)).all_states()` means "at most once per hour".

## Running jobs

```rust
use ishikari::{Cron, Engine, Postgres, Queue};
use std::{sync::Arc, time::Duration};

let engine = Engine::builder("my-app")
    .with_state(Arc::new(Mailer::new()))
    .with_queue(Queue::builder("default").concurrency(10))
    .with_queue(Queue::builder("email").concurrency(2))
    .cron(Cron::new("daily-digest", "0 7 * * *", || DailyDigest)?
        .timezone(ishikari::chrono_tz::America::Chicago))
    .start(Postgres::new(pool.clone()));

// On SIGTERM: wait up to 25 s for running jobs, give the rest back.
let report = engine.shutdown(Duration::from_secs(25)).await;
```

The builder also sets:
- `schema`;
- `node_name`, `heartbeat_interval`, `node_timeout`, `rescue_interval` (rescue of jobs whose node
  stopped);
- `stager_interval`;
- `cron_interval`, `cron_catch_up`;
- `resolver_interval`;
- `prune_after` (7 days by default) and `no_pruning`;
- `listen`.

A queue starts a job as soon as there's room for it. The engine listens (`LISTEN`, on one of the
pool's connections) for jobs becoming available and wakes their queue, and a slot whose job
finishes is refilled at once while jobs are waiting. Each queue's `interval` (1 s) is a fallback
poll. Behind PgBouncer in transaction mode, where `LISTEN` doesn't work, set `.listen(false)`:
queues then rely on polling.

`engine.retry_job(id)` and `engine.cancel_job(id, reason)` act on single jobs, workflow steps
included.

## Workflows

```rust
use ishikari::workflows::{Dag, Workflow};

let workflow = Dag::new("prepare-town")
    .add("directory", ImportDirectory { town })
    .add_after("sources", FindSources { town }, ["directory"])
    .add_after("listings", CheckListings { town }, ["directory"])
    .add("hero", DrawHero { town })
    .run(&mut *tx)          // the whole workflow, or none of it
    .await?;

let progress = Workflow::progress(&pool, workflow.id, None).await?;
for step in progress.unwrap().steps {
    println!("{}: {:?} ({} attempts)", step.name, step.state, step.attempt);
}
```

`Pipeline` chains steps in order. A `Saga` runs a compensation for each completed step, last
first, when a later one fails or the saga is cancelled; the rollback starts in the transaction
that ends the failed step, and a failed compensation doesn't stop the rest. Steps wait in the
`pending` state until their upstreams complete. If a step fails for good, its dependents are
cancelled and the workflow fails; retrying the step brings them back.

## Admin dashboard

```rust
let admin = ishikari_admin::app(
    ishikari_admin::AppState::new(pool.clone()).with_base_path("/jobs"),
);
let app = Router::new()
    .nest("/jobs", admin)
    .layer(axum::middleware::from_fn(require_admin)); // your auth
```

Actions a browser sends from another site (cross-origin POSTs and PUTs) are refused; who may
use the dashboard is your auth's call.

Each page is a standalone page with its own sidebar and light/dark theme. To show the pages inside
your app's admin instead (your header, navigation and theme), implement `Layout` and
build the router with `app_with_layout`:

```rust
use ishikari_admin::{embed_stylesheet_href, Layout, Page};

struct MyAdmin;

impl Layout for MyAdmin {
    // `parts` is the request: its extensions carry what your auth middleware put there.
    fn render(&self, parts: &Parts, page: Page) -> Response {
        let Some(user) = parts.extensions.get::<User>() else {
            return StatusCode::UNAUTHORIZED.into_response();
        };
        // Your page, with in <head> your theme, htmx 2 and
        // <link rel="stylesheet" href="{embed_stylesheet_href(&page.base_path)}">,
        // and in your main area `page.nav_html()` (the dashboard's sections) and `page.body`.
        my_admin_page(user, &page.title, &page.nav_html(), &page.body)
    }
}

let admin = ishikari_admin::app_with_layout(
    ishikari_admin::AppState::new(pool.clone()).with_base_path("/jobs"),
    MyAdmin,
);
```

The embed stylesheet has the dashboard's components (classes prefixed `ik-`) and the Tailwind
utilities its pages use: no theme, nothing on `:root`, and a reset only inside the dashboard, so
it doesn't touch the rest of your page. htmx fragments (the tables and counts that refresh in
place) never go through the layout.

### Running definitions

The Definitions pages keep workflow definitions: YAML node graphs, imported or drawn in the
builder. **Run** inserts one as a workflow with a step per node, each an
`ishikari::execution::NodeJob` in the `workflows` queue that waits on its upstreams. Your node
types run them: implement `NodeRunner`, and run the queue in an engine whose state is a
`NodeJobs` (an engine of its own when your workers have a state of their own):

```rust
use ishikari::execution::{NodeJob, NodeJobs};

let definitions = Engine::builder("definitions")
    .with_state(Arc::new(NodeJobs::new(MyNodes)))
    .with_queue(Queue::builder(NodeJob::QUEUE))
    .start(Postgres::new(pool.clone()));
```

### Theming

The dashboard takes its colors from your CSS variables, so it matches your app without setup.
Each token takes the first of these that your page sets:

1. `--ik-<token>`: just the dashboard's;
2. `--<token>`: shadcn/ui's names (`--primary`, `--card`, `--border`, `--radius`...);
3. `--color-<token>`: your Tailwind theme's colors (`--color-primary`...);
4. daisyUI 5's (`--color-base-100`, `--color-base-content`, `--color-error`, `--radius-box`...);
5. a neutral default, light or dark by the page's `color-scheme`.

The tokens: `background`, `foreground`, `card`, `card-foreground`, `muted`, `muted-foreground`,
`accent`, `border`, `input`, `primary`, `primary-foreground`, `ring`, `success`, `warning`,
`info`, `destructive`, `violet` and `radius`. Values are whole colors (`oklch(...)`, `#hex`),
not shadcn's older bare HSL numbers; set `--ik-*` where yours are those. They're read where
they're used, so a theme set on any ancestor (`.dark`, `data-theme`) applies. To restyle only the
dashboard:

```css
.ishikari-admin {
  --ik-primary: oklch(0.6 0.17 150);
  --ik-radius: 0.375rem;
}
```

The standalone pages work the same way, following the system's light or dark mode unless the
theme toggle picks one.

## Development

Tests run against a real Postgres. Each test makes and drops a schema of its own.

```bash
export ISHIKARI_TEST_DATABASE_URL=postgres://postgres:postgres@localhost/ishikari_test
cargo test --workspace
```

Without a database URL the database tests fail loudly; set `ISHIKARI_SKIP_DB_TESTS=1` to
skip them.

## Crates

- `ishikari`: the library.
- `ishikari-macros`: `#[ishikari::job]` and `#[ishikari::worker]`.
- `ishikari-cli`: `ishikari generate migration` and `ishikari generate version`.
- `ishikari-admin`: the dashboard.

## License

MIT
