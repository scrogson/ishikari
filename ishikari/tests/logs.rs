//! What inserting logs: the job, never its args. In a binary of its own: a thread's logs are
//! captured reliably only while no other test logs.

mod common;

use common::TestDb;
use ishikari::prelude::*;
use ishikari::{InsertOpts, Unique};

/// Its args are personal data.
#[ishikari::job]
struct Welcome {
    email: String,
}

#[ishikari::worker]
impl Worker for Welcome {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[ishikari::job]
struct Digest;

#[ishikari::worker]
impl Worker for Digest {
    fn unique(&self) -> Option<Unique> {
        Some(Unique::new())
    }

    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[tokio::test]
async fn inserts_log_the_job_not_its_args() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (_guard, logs) = common::capture_logs();
    let welcome = |n: u32| Welcome {
        email: format!("person{n}@example.com"),
    };
    let job = ishikari::insert(welcome(1), &db.pool).await.unwrap();
    ishikari::insert(Digest, &db.pool).await.unwrap();
    ishikari::insert(Digest, &db.pool).await.unwrap();
    ishikari::insert_all([welcome(2), welcome(3)], InsertOpts::new(), &db.pool)
        .await
        .unwrap();

    let logs = logs.text();
    assert!(!logs.contains("example.com"), "args logged:\n{logs}");
    let inserted = logs
        .lines()
        .find(|l| l.contains(&format!(" id={} ", job.id)))
        .unwrap_or_else(|| panic!("the insert isn't logged:\n{logs}"));
    assert!(inserted.contains(" DEBUG "), "{inserted}");
    assert!(inserted.contains("worker="), "{inserted}");
    assert!(inserted.contains("queue="), "{inserted}");
    assert!(
        !logs
            .lines()
            .any(|l| l.contains(" INFO ") && l.contains("insert")),
        "inserts logged at info:\n{logs}"
    );

    db.finish().await;
}
