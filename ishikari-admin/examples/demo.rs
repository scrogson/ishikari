//! Live demo for Ishikari Admin
//!
//! Populates the database with sample jobs, pipelines, DAGs, and sagas,
//! then runs workers to process them while serving the admin UI.
//!
//! Run with:
//!   DATABASE_URL=postgres://... cargo run -p ishikari-admin --example demo

mod demo_lib;

use demo_lib::scenarios;
use demo_lib::workers::NodeJobState;
use ishikari::dependencies::DependencyResolver;
use ishikari::{Engine, Postgres, Queue};
use ishikari_admin::{app, AppState};
use sqlx::PgPool;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "demo=info,ishikari=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await?;

    tracing::info!("Creating sample data...");

    // Create sample standalone jobs
    scenarios::create_sample_jobs(&pool).await?;
    tracing::info!("Created sample jobs");

    // Create workflows
    let pipeline_id = scenarios::create_email_pipeline(&pool).await?;
    tracing::info!(id = pipeline_id, "Created email pipeline");

    let dag_id = scenarios::create_etl_dag(&pool).await?;
    tracing::info!(id = dag_id, "Created ETL DAG");

    let saga_id = scenarios::create_order_saga(&pool).await?;
    tracing::info!(id = saga_id, "Created order saga (will succeed)");

    let failing_saga_id = scenarios::create_failing_order_saga(&pool).await?;
    tracing::info!(
        id = failing_saga_id,
        "Created failing order saga (will trigger compensation)"
    );

    // Create sample workflow definitions (for the definitions UI)
    scenarios::create_sample_workflow_definitions(&pool).await?;
    tracing::info!("Created sample workflow definitions");

    // Start dependency resolver
    let resolver = DependencyResolver::new(pool.clone(), Duration::from_millis(500)).start();
    tracing::info!("Started dependency resolver");

    // Start worker engine with NodeJobState for workflow node execution
    let node_job_state = NodeJobState::new(pool.clone());
    let _engine = Engine::builder("demo")
        .stager_interval(Duration::from_secs(1))
        .with_state(Arc::new(node_job_state))
        .with_queue(Queue::builder("default").concurrency(5))
        .with_queue(Queue::builder("email").concurrency(3))
        .with_queue(Queue::builder("etl").concurrency(2))
        .with_queue(Queue::builder("orders").concurrency(2))
        .with_queue(Queue::builder("payments").concurrency(2))
        .with_queue(Queue::builder("shipping").concurrency(2))
        .with_queue(Queue::builder("workflows").concurrency(4))
        .start(Postgres::new(pool.clone()));
    tracing::info!("Started worker engine");

    // Setup admin
    let state = AppState::new(pool);
    let admin = app(state);

    // Start server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("Admin UI available at http://{}", addr);
    tracing::info!("Press Ctrl+C to stop");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, admin).await?;

    resolver.abort();
    Ok(())
}
