mod common;

use crate::common::AppState;
use ishikari::{Engine, Postgres, Queue};
use sqlx::PgPool;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup tracing
    tracing_subscriber::fmt::init();

    // Setup the database
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let pool = PgPool::connect(&database_url).await.unwrap();

    // Run the migrations
    // TODO: migrations should be generated and placed in the user's application
    sqlx::migrate!("../migrations").run(&pool).await.unwrap();

    // Clear the jobs table. This is only for demonstration purposes
    sqlx::query("TRUNCATE jobs RESTART IDENTITY")
        .execute(&pool)
        .await?;

    // Set up some application state which will be available to all workers
    let state = AppState { pool: pool.clone() };

    // Setup and start the engine
    // Give the engine a name and setup some queues.
    //
    // The name can be anything. If you run multiple nodes, this should
    // be the node name to differentiate from each node in your cluster.
    //
    // For instance, you can use a Kubernetes Pod DNS name:
    //
    // 10-0-1-162.backend.pod.cluster.local
    let _engine = Engine::builder("ishikari-example")
        .with_queue(Queue::builder("default").concurrency(10))
        .with_queue(Queue::builder("low_latency").concurrency(20))
        .with_state(Arc::new(state))
        .start(Postgres::new(pool.clone()));

    // Wait for a signal to shutdown
    tokio::signal::ctrl_c().await.unwrap();

    Ok(())
}
