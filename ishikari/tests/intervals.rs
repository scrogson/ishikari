//! A zero interval is refused when it is set, rather than panicking the task it would drive.

use ishikari::dependencies::DependencyResolver;
use ishikari::{Engine, EngineBuilder, Postgres, Queue, Stager};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

#[test]
#[should_panic(expected = "interval must be greater than zero")]
fn a_zero_queue_interval_is_refused() {
    Queue::builder("default").interval(Duration::ZERO);
}

#[test]
fn zero_engine_intervals_are_refused() {
    type Setter = fn(EngineBuilder, Duration) -> EngineBuilder;
    let setters: [(&str, Setter); 6] = [
        ("stager_interval", EngineBuilder::stager_interval),
        ("heartbeat_interval", EngineBuilder::heartbeat_interval),
        ("rescue_interval", EngineBuilder::rescue_interval),
        ("cron_interval", EngineBuilder::cron_interval),
        ("resolver_interval", EngineBuilder::resolver_interval),
        ("prune_interval", EngineBuilder::prune_interval),
    ];
    for (name, set) in setters {
        let refused = catch_unwind(|| set(Engine::builder("test"), Duration::ZERO)).is_err();
        assert!(refused, "{name}(Duration::ZERO) was accepted");
        set(Engine::builder("test"), Duration::from_millis(1));
    }
}

#[tokio::test]
async fn zero_stager_and_resolver_intervals_are_refused() {
    let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
    let storage = Arc::new(Postgres::new(pool.clone()));
    let stager = AssertUnwindSafe(|| Stager::new(storage, Duration::ZERO, 10));
    assert!(catch_unwind(stager).is_err(), "Stager::new");
    let resolver = AssertUnwindSafe(|| DependencyResolver::new(pool, Duration::ZERO));
    assert!(catch_unwind(resolver).is_err(), "DependencyResolver::new");
}
