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

## Usage

Add to Cargo.toml:

```toml
ishikari = "0.1.0"
```

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

## License

MIT License
