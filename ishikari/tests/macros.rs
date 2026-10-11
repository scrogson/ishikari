//! `#[worker]` keeps the methods written in the impl and applies its attributes.

mod common;

use common::TestDb;
use ishikari::prelude::*;

#[ishikari::job]
struct ByMethods;

#[ishikari::worker]
impl Worker for ByMethods {
    fn queue(&self) -> &'static str {
        "emails"
    }

    fn max_attempts(&self) -> i32 {
        3
    }

    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[ishikari::job]
struct ByAttributes;

#[ishikari::worker(queue = "reports", max_attempts = 2, timeout = "90s")]
impl Worker for ByAttributes {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[ishikari::job]
#[derive(Clone, PartialEq)]
struct WithDerives {
    n: u32,
}

#[ishikari::worker]
impl Worker for WithDerives {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[test]
fn options_come_from_methods_or_attributes() {
    assert_eq!(ByMethods.queue(), "emails");
    assert_eq!(ByMethods.max_attempts(), 3);
    assert_eq!(ByAttributes.queue(), "reports");
    assert_eq!(ByAttributes.max_attempts(), 2);
    assert_eq!(
        ByAttributes.timeout(),
        Some(std::time::Duration::from_secs(90))
    );
    assert_eq!(ByMethods.timeout(), None);
    assert_eq!(WithDerives { n: 1 }.queue(), "default");
    assert_eq!(WithDerives { n: 1 }.max_attempts(), 20);
    assert_eq!(WithDerives { n: 1 }.clone(), WithDerives { n: 1 });
}

#[tokio::test]
async fn inserted_jobs_carry_the_methods_options() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let job = ishikari::insert(ByMethods, &db.pool).await.unwrap();
    assert_eq!((job.queue.as_str(), job.max_attempts), ("emails", 3));
    db.finish().await;
}

#[test]
fn misuse_is_a_compile_error() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
