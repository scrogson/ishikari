use ishikari::worker::*;
use ishikari::{async_trait, Deserialize, Queue, Serialize, Stager};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::info;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(crate = "ishikari::serde")]
pub struct Sum {
    a: i32,
    b: i32,
}

#[typetag::serde]
#[async_trait]
impl Worker for Sum {
    async fn perform(&self) -> PerformResult {
        let result = self.a + self.b;
        info!("{} + {} = {}", self.a, self.b, &result);
        Complete::default().message(result).into()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(crate = "ishikari::serde")]
pub struct Fail;

#[typetag::serde]
#[async_trait]
impl Worker for Fail {
    async fn perform(&self) -> PerformResult {
        "this will fail".parse::<i32>()?;
        Complete::default().into()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let pool = PgPool::connect(&database_url).await.unwrap();
    let pool = Arc::new(pool);

    let stager = Stager::new(pool.clone());
    tokio::spawn(async move { stager.run().await });

    let queue = Queue::builder()
        .name("default".to_string())
        .pool(pool.clone())
        .build();

    tokio::spawn(async move { queue.run().await });

    let mut tx = pool.begin().await.unwrap();

    //for i in 0..5 {
    let i = 1;
    let _ = ishikari::insert(&mut *tx, "default", Sum { a: i, b: 2 }).await?;
    //}

    let _ = ishikari::insert(&mut *tx, "default", Fail).await?;

    tx.commit().await.unwrap();

    tokio::signal::ctrl_c().await.unwrap();

    Ok(())
}
