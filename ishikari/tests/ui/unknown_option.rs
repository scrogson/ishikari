#![allow(unused_imports)]
use ishikari::prelude::*;

#[ishikari::job]
struct Unknown;

#[ishikari::worker(queues = "a")]
impl Worker for Unknown {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

fn main() {}
