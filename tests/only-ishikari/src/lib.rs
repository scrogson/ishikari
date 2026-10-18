//! Jobs and workers in a crate that depends on nothing but ishikari.

use ishikari::prelude::*;

#[ishikari::job]
#[derive(Clone, PartialEq)]
pub struct SendWelcome {
    pub user_id: i64,
}

#[ishikari::worker(queue = "email", max_attempts = 5, timeout = "30s")]
impl Worker for SendWelcome {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[ishikari::job]
pub struct Cleanup;

#[ishikari::worker]
impl ishikari::Worker for Cleanup {
    fn queue(&self) -> &'static str {
        "maintenance"
    }

    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn workers_carry_their_options() {
        let job = SendWelcome { user_id: 1 };
        assert_eq!(job.queue(), "email");
        assert_eq!(job.max_attempts(), 5);
        assert_eq!(job.timeout(), Some(Duration::from_secs(30)));
        assert_eq!(job.clone(), job);
        assert_eq!(Cleanup.queue(), "maintenance");
    }
}
