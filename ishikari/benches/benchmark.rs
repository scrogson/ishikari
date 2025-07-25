use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ishikari::{Backoff, Context, Job, JobState, Worker, Complete};
use chrono::{Duration, Utc};
use std::sync::Arc;

#[ishikari::job]
struct BenchmarkJob {
    data: i32,
}

#[ishikari::worker]
impl Worker for BenchmarkJob {
    async fn perform(&self, _ctx: Context) -> ishikari::PerformResult {
        // Simple computation for benchmarking
        let result = self.data * 2 + 1;
        black_box(result);
        Complete::default().into()
    }
}

fn benchmark_backoff_strategies(c: &mut Criterion) {
    let mut group = c.benchmark_group("backoff_strategies");
    
    group.bench_function("fixed", |b| {
        let backoff = Backoff::Fixed(Duration::seconds(5));
        b.iter(|| {
            black_box(backoff.next_retry(black_box(3)));
        });
    });
    
    group.bench_function("linear", |b| {
        let backoff = Backoff::Linear(Duration::seconds(2));
        b.iter(|| {
            black_box(backoff.next_retry(black_box(3)));
        });
    });
    
    group.bench_function("exponential", |b| {
        let backoff = Backoff::Exponential(Duration::seconds(1));
        b.iter(|| {
            black_box(backoff.next_retry(black_box(3)));
        });
    });
    
    group.bench_function("exponential_jitter", |b| {
        let backoff = Backoff::ExponentialJitter(Duration::seconds(1));
        b.iter(|| {
            black_box(backoff.next_retry(black_box(3)));
        });
    });
    
    group.finish();
}

fn benchmark_context_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("context_operations");
    
    let job = Arc::new(Job {
        id: 123,
        state: JobState::Executing,
        queue: "benchmark".to_string(),
        worker: "BenchmarkWorker".to_string(),
        args: serde_json::json!({}),
        errors: vec![],
        attempt: 1,
        max_attempts: 3,
        attempted_by: None,
        priority: 0,
        tags: vec![],
        meta: None,
        inserted_at: Utc::now(),
        scheduled_at: Utc::now(),
        attempted_at: Some(Utc::now()),
        completed_at: None,
        discarded_at: None,
        cancelled_at: None,
    });
    
    let state = Arc::new(());
    let context = Context::new(job, state);
    
    group.bench_function("job_access", |b| {
        b.iter(|| {
            black_box(context.job());
        });
    });
    
    group.bench_function("state_access", |b| {
        b.iter(|| {
            black_box(context.state::<()>().unwrap());
        });
    });
    
    group.finish();
}

fn benchmark_job_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("job_serialization");
    
    let job = BenchmarkJob { data: 42 };
    
    group.bench_function("serialize", |b| {
        b.iter(|| {
            black_box(serde_json::to_value(&job as &dyn Worker).unwrap());
        });
    });
    
    let serialized = serde_json::to_value(&job as &dyn Worker).unwrap();
    
    group.bench_function("deserialize", |b| {
        b.iter(|| {
            let _: Box<dyn Worker> = black_box(
                serde_json::from_value(serialized.clone()).unwrap()
            );
        });
    });
    
    group.finish();
}

criterion_group!(
    benches, 
    benchmark_backoff_strategies,
    benchmark_context_operations,
    benchmark_job_serialization
);
criterion_main!(benches);