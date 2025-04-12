use ishikari::prelude::*;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

#[derive(Debug)]
pub struct AppState {
    #[allow(dead_code)]
    pub pool: sqlx::PgPool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[ishikari::job]
pub struct Sum {
    pub a: i32,
    pub b: i32,
}

#[ishikari::worker]
impl Worker for Sum {
    #[instrument(skip(_ctx))]
    async fn perform(&self, _ctx: Context) -> PerformResult {
        let result = self.a + self.b;
        info!("{} + {} = {}", self.a, self.b, &result);

        Complete::default().message(result).into()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[ishikari::job]
pub struct Fail;

#[ishikari::worker(queue = "low_latency", max_attempts = 3)]
impl Worker for Fail {
    #[instrument(skip(ctx))]
    async fn perform(&self, ctx: Context) -> PerformResult {
        let state = ctx.state::<AppState>()?;

        info!("Job ID: {}", ctx.job().id);
        info!("State: {:?}", state);

        "this will fail".parse::<i32>()?;

        Complete::default().into()
    }
}
