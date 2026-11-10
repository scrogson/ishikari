//! The dependency resolver, the safety-net sweep for what a job left behind.

mod common;

use common::{eventually, TestDb};
use ishikari::dependencies::DependencyResolver;
use std::time::Duration;

/// The resolver's lock is its sweep's: a resolver stopped mid-sweep doesn't leave it held on a
/// pooled connection, where every node's resolver would skip its sweeps.
#[tokio::test]
async fn a_resolver_stopped_mid_sweep_lets_go_of_its_lock() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let key = format!("ishikari:resolver:{}", db.schema);
    // Whether another node could take the lock, asked on a connection of its own.
    let probe = tokio::sync::Mutex::new(db.another_pool().await.acquire().await.unwrap());
    let free = || async {
        let mut probe = probe.lock().await;
        let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtext($1))")
            .bind(&key)
            .fetch_one(&mut **probe)
            .await
            .unwrap();
        if free {
            sqlx::query("SELECT pg_advisory_unlock(hashtext($1))")
                .bind(&key)
                .execute(&mut **probe)
                .await
                .unwrap();
        }
        free
    };

    // Its sweep waits on the dependencies table, with its lock taken.
    let mut blocking = db.pool.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE {}.ishikari_job_dependencies IN ACCESS EXCLUSIVE MODE",
        db.schema
    )))
    .execute(&mut *blocking)
    .await
    .unwrap();
    let resolver = DependencyResolver::new(db.pool.clone(), Duration::from_millis(20))
        .schema(&db.schema)
        .start();
    for _ in 0..100 {
        if !free().await {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!free().await, "the sweep holds the lock");

    resolver.abort();
    blocking.rollback().await.unwrap();
    eventually("the lock is let go", Duration::from_secs(5), || async {
        free().await
    })
    .await;

    db.finish().await;
}
