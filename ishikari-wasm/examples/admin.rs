//! The admin with node types uploaded as WebAssembly components, and an engine running the
//! definitions' nodes of them.
//!
//!   DATABASE_URL=postgres://... cargo run --example admin   (or `mise run wasm:demo`)
//!
//! Then upload `tests/fixtures/testkit-v1.wasm` on http://127.0.0.1:3001/node-types, make a
//! definition with a `test/kit` node (inputs `op: shout`, `text: hi`), and run it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use ishikari::execution::{NodeJob, NodeJobs, NodeTypes};
use ishikari::{Engine, Postgres, Queue};
use ishikari_admin::AppState;
use ishikari_wasm::WasmModules;
use sqlx::PgPool;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ishikari=info,ishikari_wasm=info".into()),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await?;
    ishikari::migrations::migrate(&pool, "public").await?;

    let modules = Arc::new(WasmModules::new(pool.clone()).timeout(Duration::from_secs(10)));
    let types = NodeTypes::new(pool.clone()).modules(modules.clone());
    let catalog = types.catalog();
    let _engine = Engine::builder("wasm-demo")
        .with_state(Arc::new(NodeJobs::new(types)))
        .with_queue(Queue::builder(NodeJob::QUEUE).concurrency(4))
        .start(Postgres::new(pool.clone()));

    let state = AppState::new(pool)
        .with_node_types(catalog)
        .with_node_modules(modules);
    let addr = SocketAddr::from(([127, 0, 0, 1], 3001));
    tracing::info!("Admin UI available at http://{addr}/node-types");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, ishikari_admin::app(state)).await?;
    Ok(())
}
