//! A schema of the test's own in a real Postgres, as ishikari's tests have (`ishikari/tests/common`),
//! and running a node of an uploaded type.

#![allow(dead_code)]

use chrono::Utc;
use ishikari::execution::{NodeContext, NodeError, NodeModules};
use ishikari_wasm::WasmModules;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};

pub struct TestDb {
    /// A pool whose connections use the test's schema.
    pub pool: PgPool,
    pub schema: String,
    url: String,
}

impl TestDb {
    /// A fresh, migrated schema, or `None` when database tests are switched off
    /// (`ISHIKARI_SKIP_DB_TESTS`).
    pub async fn new() -> Option<TestDb> {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return None;
        }
        let url = std::env::var("ISHIKARI_TEST_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .expect(
                "set ISHIKARI_TEST_DATABASE_URL (or DATABASE_URL), or ISHIKARI_SKIP_DB_TESTS=1",
            );
        let schema = format!(
            "ishikari_t_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        );
        let admin = PgPool::connect(&url).await.expect("connect");
        ishikari::migrations::migrate(&admin, &schema)
            .await
            .expect("migrate the test schema");
        admin.close().await;

        let path = format!("SET search_path TO {schema}");
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .after_connect(move |conn, _| {
                let path = path.clone();
                Box::pin(async move {
                    conn.execute(path.as_str()).await?;
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .expect("connect");
        Some(TestDb { pool, schema, url })
    }

    /// Drop the schema; a failed test leaves its schema behind for inspection.
    pub async fn finish(self) {
        self.pool.close().await;
        let admin = PgPool::connect(&self.url).await.expect("connect");
        for attempt in 1.. {
            match admin
                .execute(format!("DROP SCHEMA {} CASCADE", self.schema).as_str())
                .await
            {
                Ok(_) => break,
                Err(sqlx::Error::Database(e))
                    if e.code().as_deref() == Some("40P01") && attempt < 10 =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                Err(e) => panic!("drop the test schema: {e}"),
            }
        }
    }
}

/// The output of a node of `node_type`'s latest version run with `inputs`.
pub async fn run(
    modules: &WasmModules,
    pool: &PgPool,
    node_type: &str,
    inputs: Value,
) -> Result<Value, NodeError> {
    let handler = modules
        .handler(node_type, Utc::now())
        .await
        .unwrap()
        .expect("the module");
    let mut ctx = NodeContext::new(uuid::Uuid::new_v4(), "node", node_type);
    ctx.inputs = serde_json::from_value(inputs).unwrap();
    handler
        .execute(&ctx, pool)
        .await
        .map(|output| output.to_json())
}

/// Why the node failed.
pub fn failure(result: Result<Value, NodeError>) -> String {
    match result {
        Err(NodeError::ExecutionFailed(error)) => error,
        other => panic!("expected the node to fail, got {other:?}"),
    }
}

/// A server on a local port answering every request with `pong`.
pub async fn pong() -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let mut request = [0u8; 4096];
                let _ = socket.read(&mut request).await;
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-length: 4\r\nconnection: close\r\n\r\npong",
                    )
                    .await;
            });
        }
    });
    port
}
