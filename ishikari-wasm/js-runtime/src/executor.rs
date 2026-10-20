//! Boa's `SimpleJobExecutor`, except for timers: waiting for the next `setTimeout` or
//! `setInterval` sleeps (which the host ends by the node's deadline) rather than spinning.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::mem;
use std::rc::Rc;

use boa_engine::context::time::JsInstant;
use boa_engine::job::{IntervalJob, Job, JobExecutor, SimpleJobExecutor, TimeoutJob};
use boa_engine::{Context, JsResult};

#[derive(Default)]
pub(crate) struct Executor {
    jobs: Rc<SimpleJobExecutor>,
    timers: RefCell<BTreeMap<JsInstant, Vec<Timer>>>,
}

enum Timer {
    Timeout(TimeoutJob),
    Interval(IntervalJob),
}

impl Timer {
    fn cancelled(&self) -> bool {
        match self {
            Timer::Timeout(job) => job.cancelled(),
            Timer::Interval(job) => job.cancelled(),
        }
    }
}

impl Executor {
    fn schedule(&self, timer: Timer, context: &Context) {
        let due = context.clock().now()
            + match &timer {
                Timer::Timeout(job) => job.timeout(),
                Timer::Interval(job) => job.interval(),
            };
        self.timers.borrow_mut().entry(due).or_default().push(timer);
    }

    /// When the next timer that's still set is due.
    fn next_due(&self) -> Option<JsInstant> {
        let mut timers = self.timers.borrow_mut();
        timers.retain(|_, timers| {
            timers.retain(|timer| !timer.cancelled());
            !timers.is_empty()
        });
        timers.keys().next().copied()
    }
}

impl JobExecutor for Executor {
    fn enqueue_job(self: Rc<Self>, job: Job, context: &mut Context) {
        match job {
            Job::TimeoutJob(job) => self.schedule(Timer::Timeout(job), context),
            Job::IntervalJob(job) => self.schedule(Timer::Interval(job), context),
            job => self.jobs.clone().enqueue_job(job, context),
        }
    }

    fn run_jobs(self: Rc<Self>, context: &mut Context) -> JsResult<()> {
        loop {
            self.jobs.clone().run_jobs(context)?;
            let Some(due) = self.next_due() else {
                return Ok(());
            };
            let now = context.clock().now();
            if due > now {
                std::thread::sleep((due - now).into());
            }

            let now = context.clock().now();
            let timers = {
                let mut timers = self.timers.borrow_mut();
                let later = timers.split_off(&now);
                mem::replace(&mut *timers, later)
            };
            for timer in timers.into_values().flatten() {
                if timer.cancelled() {
                    continue;
                }
                match timer {
                    Timer::Timeout(job) => {
                        job.call(context)?;
                    }
                    Timer::Interval(job) => {
                        job.call(context)?;
                        self.schedule(Timer::Interval(job), context);
                    }
                }
            }
        }
    }
}
