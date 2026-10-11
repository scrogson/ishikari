#![allow(unused_imports)]
use ishikari::prelude::*;

#[ishikari::job]
struct Twice;

#[ishikari::worker(queue = "a")]
impl Worker for Twice {
    fn queue(&self) -> &'static str {
        "b"
    }

    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

fn main() {}
