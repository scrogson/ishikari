//! A job's life: inserted, picked up by an engine, finished.

mod common;

use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::{Engine, Postgres, Queue};
use std::time::Duration;

#[ishikari::job]
struct Hello {
    name: String,
}

#[ishikari::worker]
impl Worker for Hello {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default()
            .message(format!("hello {}", self.name))
            .into()
    }
}

#[tokio::test]
async fn an_inserted_job_runs_to_completion() {
    let Some(db) = TestDb::new().await else {
        return;
    };

    let job = ishikari::insert(
        Hello {
            name: "world".into(),
        },
        &db.pool,
    )
    .await
    .unwrap();
    assert_eq!(db.state(job.id).await, "available");

    let _engine = Engine::builder("test")
        .with_queue(Queue::builder("default").interval(Duration::from_millis(20)))
        .start(Postgres::new(db.pool.clone()));

    eventually("the job completes", Duration::from_secs(10), || async {
        db.state(job.id).await == "completed"
    })
    .await;

    db.finish().await;
}
