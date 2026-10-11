//! A worker can insert jobs and workflows inside its own transaction (the future `perform`
//! returns must be Send, which generic insert functions can break).

mod common;

use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::workflows::Dag;
use ishikari::{Engine, InsertOpts, Postgres, Queue};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;

#[ishikari::job]
#[derive(Clone)]
struct Child;

#[ishikari::worker]
impl Worker for Child {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[ishikari::job]
struct Parent;

#[ishikari::worker]
impl Worker for Parent {
    async fn perform(&self, ctx: Context) -> PerformResult {
        let pool = ctx.state::<PgPool>()?;
        let mut tx = pool.begin().await?;
        ishikari::insert(Child, &mut *tx).await?;
        ishikari::insert_with(Child, InsertOpts::new().priority(1), &mut *tx).await?;
        ishikari::insert_all([Child, Child], InsertOpts::new(), &mut *tx).await?;
        Dag::new("from-a-worker")
            .add("a", Child)
            .add_after("b", Child, ["a"])
            .run(&mut *tx)
            .await?;
        tx.commit().await?;
        // And straight on the pool.
        ishikari::insert(Child, &*pool).await?;
        Complete::default().into()
    }
}

#[tokio::test]
async fn a_worker_inserts_jobs_and_a_workflow_in_its_transaction() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let parent = ishikari::insert(Parent, &db.pool).await.unwrap();
    let _engine = Engine::builder("test")
        .with_state(Arc::new(db.pool.clone()))
        .with_queue(Queue::builder("default").interval(Duration::from_millis(20)))
        .start(Postgres::new(db.pool.clone()));

    eventually("everything completes", Duration::from_secs(10), || async {
        let (done, all): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE state = 'completed'), count(*) FROM ishikari_jobs",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        all == 8 && done == all
    })
    .await;
    assert_eq!(db.state(parent.id).await, "completed");

    db.finish().await;
}
