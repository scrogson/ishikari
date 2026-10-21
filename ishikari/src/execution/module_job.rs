//! The job whose code is one of the host's modules, rather than a worker written in Rust.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::node_job::NodeJobs;
use crate::{Cancel, Complete, Context, PerformResult, Snooze, Worker};

/// A job whose code is a module the host's operators uploaded (one of its
/// [`NodeModules`](super::NodeModules)), not a worker written in Rust: the module's node runs with
/// the job's `args` as its inputs, and `job:<id>` as its id. It runs the version of the module
/// uploaded last before the job was inserted, so its retries run the same code.
///
/// What the node returns completes the job, unless it's `{"snooze": seconds}`, which snoozes it,
/// or `{"cancel": "why"}`, which cancels it. A node that fails fails the attempt, retried as any
/// job's. A module uploaded after the job was inserted, or none, cancels it.
///
/// It runs in the `modules` queue (or the queue it's inserted to), through the
/// [`NodeRunner`](super::NodeRunner) of an engine whose state is a [`NodeJobs`]:
/// [`NodeTypes`](super::NodeTypes) runs it by its modules.
///
/// ```rust,no_run
/// use ishikari::execution::{ModuleJob, NodeJob, NodeJobs, NodeModules, NodeTypes};
/// use ishikari::{insert_with, Engine, InsertOpts, Postgres, Queue};
/// use std::sync::Arc;
///
/// # async fn f(pool: sqlx::PgPool, modules: Arc<dyn NodeModules>) -> Result<(), sqlx::Error> {
/// let types = NodeTypes::new(pool.clone()).modules(modules);
/// let engine = Engine::builder("modules")
///     .with_state(Arc::new(NodeJobs::new(types)))
///     .with_queue(Queue::builder(NodeJob::QUEUE))
///     .with_queue(Queue::builder(ModuleJob::QUEUE))
///     .start(Postgres::new(pool.clone()));
///
/// let invoice = serde_json::json!({ "invoice_id": 42 });
/// insert_with(ModuleJob::new("send-invoice", invoice), InsertOpts::new(), &pool).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleJob {
    /// The module, by name.
    pub module: String,
    /// Its node's inputs: a JSON object (anything else cancels the job).
    #[serde(default = "no_args")]
    pub args: Value,
}

fn no_args() -> Value {
    Value::Object(Default::default())
}

impl ModuleJob {
    /// The name its jobs are stored under: their `worker`, and their arguments' tag.
    pub const WORKER: &'static str = "ishikari::execution::ModuleJob";
    /// The queue its jobs run in, unless inserted to another.
    pub const QUEUE: &'static str = "modules";

    /// A job of the module `module`, its node given `args`: a JSON object of its inputs.
    pub fn new(module: impl Into<String>, args: Value) -> Self {
        Self {
            module: module.into(),
            args,
        }
    }

    /// What the node's `output` makes of the job: `{"snooze": seconds}` snoozes it,
    /// `{"cancel": "why"}` cancels it, and anything else completes it.
    pub fn outcome(output: Value) -> PerformResult {
        if let Value::Object(fields) = &output {
            if fields.len() == 1 {
                if let Some(seconds) = fields.get("snooze").and_then(Value::as_u64) {
                    return Snooze(seconds).into();
                }
                if let Some(why) = fields.get("cancel").and_then(Value::as_str) {
                    return Cancel::default().message(why).into();
                }
            }
        }
        Complete::default().message(output).into()
    }
}

#[typetag::serde(name = "ishikari::execution::ModuleJob")]
#[async_trait::async_trait]
impl Worker for ModuleJob {
    fn worker() -> &'static str {
        Self::WORKER
    }

    fn queue(&self) -> &'static str {
        Self::QUEUE
    }

    async fn perform(&self, ctx: Context) -> PerformResult {
        let jobs = ctx
            .state::<NodeJobs>()
            .map_err(|_| "a ModuleJob runs in an engine whose state is a NodeJobs")?;
        jobs.runner().run_module(self, ctx).await
    }
}
