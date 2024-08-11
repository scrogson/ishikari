mod workers {
    use ishikari::prelude::*;
    use serde::{Deserialize, Serialize};
    use tracing::{info, instrument};

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[ishikari::job]
    pub struct Sum {
        pub a: i32,
        pub b: i32,
    }

    #[ishikari::worker]
    impl Worker for Sum {
        #[instrument]
        async fn perform(&self, _ctx: Context) -> PerformResult {
            let result = self.a + self.b;
            info!("{} + {} = {}", self.a, self.b, &result);

            Complete::default().message(result).into()
        }
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[ishikari::job]
    pub struct Fail;

    #[ishikari::worker]
    impl Worker for Fail {
        fn max_attempts(&self) -> i32 {
            5
        }

        #[instrument(skip(ctx))]
        async fn perform(&self, ctx: Context) -> PerformResult {
            let context = ctx.extract::<sqlx::PgPool>()?;

            info!("job id: {}", ctx.job().id);
            info!("queue name: {}", ctx.queue().name);
            info!("Context: {:?}", context);

            "this will fail".parse::<i32>()?;

            Complete::default().into()
        }
    }
}

use ishikari::{Queue, Stager};
use sqlx::PgPool;
use std::sync::Arc;
use workers::{Fail, Sum};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let database_url = std::env::var("DATABASE_URL").unwrap();
    let pool = PgPool::connect(&database_url).await.unwrap();

    sqlx::migrate!("../migrations").run(&pool).await.unwrap();

    let pool = Arc::new(pool);

    sqlx::query("TRUNCATE jobs RESTART IDENTITY")
        .execute(&*pool)
        .await?;

    let _stager_handle = Stager::new(pool.clone()).start();

    let queue = Arc::new(
        Queue::builder()
            .name("default".to_string())
            .pool(pool.clone())
            .context(pool.clone())
            .build(),
    );

    queue.start();

    let mut tx = pool.begin().await.unwrap();

    let _ = ishikari::insert(Sum { a: 1, b: 2 }, &mut *tx).await?;
    let _ = ishikari::insert(Fail, &mut *tx).await?;

    tx.commit().await.unwrap();

    tokio::signal::ctrl_c().await.unwrap();

    Ok(())
}
