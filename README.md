# Ishikari

Ishikari is a robust job processing system written in Rust, designed for reliable background job execution with features like retries, backoff strategies, and queue management.

## Features

- **Job Processing**: Reliable background job execution with PostgreSQL-backed storage
- **Queue Management**: Support for multiple queues with configurable priorities
- **Retry Mechanism**: Flexible retry strategies with various backoff options:
  - Fixed delay
  - Linear backoff
  - Exponential backoff
  - Exponential backoff with jitter
  - Custom backoff strategies
- **Worker System**: Easy-to-use worker trait for implementing job processors
- **State Management**: Context-based state sharing between jobs and workers
- **PostgreSQL Integration**: Built-in support for PostgreSQL as the job storage backend
- **Workflow Orchestration**:
  - **Pipelines** - Linear chains of jobs executed in sequence
  - **DAGs** - Directed acyclic graphs for complex job dependencies
  - **Sagas** - Workflows with automatic compensation/rollback on failure
- **Admin Dashboard**: Web UI for monitoring jobs, queues, and workflows

## Prerequisites

- Rust (latest stable version)
- PostgreSQL

## Setup

Add to Cargo.toml

```toml
ishikari = "0.1.0"
```

### Database Setup

Ishikari requires PostgreSQL. Set up your database using the `ishikari-cli` tool:

```bash
# 1. Install the CLI tool
cargo install ishikari-cli

# 2. Generate migration files
ishikari generate migration

# 3. Apply migrations
sqlx migrate run --database-url $DATABASE_URL
```

**Custom schemas** (for multi-tenant applications):
```bash
# Generate schema-specific migrations
ishikari gen migration --schema tenant_a
ishikari g migration --schema tenant_b

# Apply all migrations
sqlx migrate run --database-url $DATABASE_URL
```

**Runtime migrations** (for containers/auto-setup):
```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&database_url).await?;

    // Run migrations embedded in your binary
    sqlx::migrate!("./migrations").run(&pool).await?;

    // Now you can use Ishikari...
    Ok(())
}
```

**CLI Options:**
- `--output <DIR>` - Output directory (default: `migrations/`)
- `--schema <NAME>` - Custom schema name (default: public)
- `--name <PREFIX>` - Migration name prefix
- `--force` - Overwrite existing files

This approach provides the best of both worlds: CLI-generated migrations with sqlx's embedded runtime migration system.

## Usage

### Creating a Job and implementing Worker

```rust
use ishikari::prelude::*;

#[derive(Debug)]
#[ishikari::job]
struct MyJob {
    // Your worker state here
}

#[ishikari::worker]
impl Worker for MyJob {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        // Your job processing logic here
        Complete::default().into()
    }
}
```

### Running Jobs

```rust
use ishikari::prelude::*;

async fn schedule_job(worker: MyJob) -> Result<Job, sqlx::Error> {
    let job = ishikari::insert(worker, &pool).await?;
    Ok(job)
}
```

### Setting up the Engine

For single-tenant applications (public schema):
```rust
use ishikari::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&database_url).await?;

    let engine = Engine::builder()
        .add_queue(Queue::builder("default").build())
        .start(PostgresStorage::new(pool.clone()), pool)
        .await?;

    // Engine is now running...
    Ok(())
}
```

For multi-tenant applications (custom schema):
```rust
use ishikari::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&database_url).await?;

    let engine = Engine::builder()
        .schema("tenant_a")  // All jobs will use tenant_a schema
        .add_queue(Queue::builder("default").build())
        .start(PostgresStorage::new(pool.clone()), pool)
        .await?;

    // Engine is now running with tenant_a schema...
    Ok(())
}
```

## Project Structure

- `ishikari/` - Main library crate
  - `engine.rs` - Core job processing engine
  - `queue.rs` - Queue management
  - `model.rs` - Data models
  - `workflow.rs` - Workflow orchestration (pipelines, DAGs, sagas)
- `ishikari-macros/` - Procedural macros (`#[ishikari::job]`, `#[ishikari::worker]`)
- `ishikari-cli/` - CLI tool for generating migrations
- `ishikari-admin/` - Web dashboard for monitoring and management

## License

MIT License
