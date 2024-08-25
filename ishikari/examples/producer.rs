mod common;

use crate::common::{Fail, Sum};
use sqlx::PgPool;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup tracing
    tracing_subscriber::fmt::init();

    // Setup the database
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let pool = PgPool::connect(&database_url).await.unwrap();

    // Enqueue a job outside of a transaction
    let _ = ishikari::insert(Sum { a: 1, b: 2 }, &pool).await?;

    // Start a transaction
    let mut tx = pool.begin().await.unwrap();
    // Enqueue a 2 jobs together inside a transaction
    let _ = ishikari::insert(Sum { a: 3, b: 8 }, &mut *tx).await?;
    let _ = ishikari::insert(Fail, &mut *tx).await?;
    // Commit the transaction
    tx.commit().await.unwrap();

    Ok(())
}
