//! Old finished jobs are pruned; recent ones, unfinished ones and running workflows are kept.

mod common;

use common::{eventually, TestDb};
use ishikari::prelude::*;
use ishikari::workflows::Pipeline;
use ishikari::{Engine, Postgres};
use std::time::Duration;

#[ishikari::job]
#[derive(Clone)]
struct Done;

#[ishikari::worker]
impl Worker for Done {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

async fn exists(db: &TestDb, table: &str, id: i64) -> bool {
    sqlx::query_scalar(&format!(
        "SELECT EXISTS (SELECT 1 FROM {table} WHERE id = $1)"
    ))
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

async fn set(db: &TestDb, sql: &str, id: i64) {
    sqlx::query(sql).bind(id).execute(&db.pool).await.unwrap();
}

#[tokio::test]
async fn old_finished_jobs_and_their_workflows_go() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let old = ishikari::insert(Done, &db.pool).await.unwrap().id;
    let recent = ishikari::insert(Done, &db.pool).await.unwrap().id;
    let running = ishikari::insert(Done, &db.pool).await.unwrap().id;
    let finished_wf = Pipeline::new("done")
        .step(Done)
        .run(&db.pool)
        .await
        .unwrap()
        .id;
    // Its first step finished long ago; its second hasn't run.
    let running_wf = Pipeline::new("going")
        .step(Done)
        .step(Done)
        .run(&db.pool)
        .await
        .unwrap()
        .id;

    let eight_days = "now() - interval '8 days'";
    set(&db, &format!("UPDATE ishikari_jobs SET state = 'completed', completed_at = {eight_days} WHERE id = $1"), old).await;
    set(&db, "UPDATE ishikari_jobs SET state = 'completed', completed_at = now() - interval '1 hour' WHERE id = $1", recent).await;
    set(&db, &format!("UPDATE ishikari_jobs SET state = 'executing', attempted_at = {eight_days} WHERE id = $1"), running).await;
    set(&db, &format!("UPDATE ishikari_jobs SET state = 'completed', completed_at = {eight_days} WHERE workflow_id = $1"), finished_wf).await;
    set(
        &db,
        "UPDATE ishikari_workflows SET state = 'completed', completed_at = now() WHERE id = $1",
        finished_wf,
    )
    .await;
    set(&db, &format!("UPDATE ishikari_jobs SET state = 'completed', completed_at = {eight_days} WHERE workflow_id = $1 AND meta->>'ishikari_step' = '1'"), running_wf).await;

    let _engine = Engine::builder("test")
        .prune_after(Duration::from_secs(7 * 24 * 3600))
        .prune_interval(Duration::from_millis(50))
        .start(Postgres::new(db.pool.clone()));

    eventually("the old job is pruned", Duration::from_secs(5), || async {
        !exists(&db, "ishikari_jobs", old).await
    })
    .await;
    eventually(
        "the finished workflow goes",
        Duration::from_secs(5),
        || async { !exists(&db, "ishikari_workflows", finished_wf).await },
    )
    .await;
    assert!(
        exists(&db, "ishikari_jobs", recent).await,
        "finished an hour ago"
    );
    assert!(exists(&db, "ishikari_jobs", running).await, "not finished");
    assert!(exists(&db, "ishikari_workflows", running_wf).await);
    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM ishikari_jobs WHERE workflow_id = $1")
        .bind(running_wf)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(kept, 2, "a running workflow keeps its old steps");

    db.finish().await;
}
