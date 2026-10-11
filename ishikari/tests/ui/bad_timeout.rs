#![allow(unused_imports)]
use ishikari::prelude::*;

#[ishikari::job]
struct BadTimeout;

#[ishikari::worker(timeout = "soon")]
impl Worker for BadTimeout {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

fn main() {}
