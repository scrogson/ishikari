//! The job that runs one node of a workflow definition's run.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Context, PerformResult, Worker};

/// One node of a workflow definition's run. The admin's Run inserts one per node as the steps
/// of a [`Dag`](crate::workflows::Dag), each named by its node: the roots available, the rest
/// pending until their upstreams complete. It runs in the `workflows` queue, through the
/// [`NodeRunner`] of an engine whose state is a [`NodeJobs`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeJob {
    /// The run (`ishikari_workflow_runs`) the node belongs to, with the run's inputs.
    pub workflow_run_id: i64,
    /// The node's id in the definition.
    pub node_id: String,
    /// The node's type, e.g. `http/get`.
    pub node_type: String,
    /// The node's inputs as defined, their expressions unresolved.
    #[serde(default)]
    pub raw_inputs: HashMap<String, Value>,
    /// The node's `when` condition, unevaluated.
    #[serde(default)]
    pub condition: Option<String>,
    /// How many attempts the node gets: its `max_retries` plus one.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i32,
    /// How long one attempt may run.
    #[serde(default)]
    pub timeout_seconds: Option<u32>,
}

fn default_max_attempts() -> i32 {
    3
}

impl NodeJob {
    /// The name its jobs are stored under: their `worker`, and their arguments' tag.
    pub const WORKER: &'static str = "ishikari::execution::NodeJob";
    /// The queue its jobs run in.
    pub const QUEUE: &'static str = "workflows";
}

/// Runs [`NodeJob`]s: the host's node types, from resolving a node's inputs to recording its
/// output. [`NodeTypes`](super::NodeTypes) does all that around a handler per type; implement
/// it yourself to do it your own way ([`Interpolator`](super::Interpolator) and
/// [`ExecutionStorage`](super::ExecutionStorage) help). What it returns is the job's outcome, as
/// a worker's `perform` would.
#[async_trait::async_trait]
pub trait NodeRunner: Send + Sync + 'static {
    async fn run(&self, job: &NodeJob, ctx: Context) -> PerformResult;
}

/// The engine state [`NodeJob`]s need: the host's [`NodeRunner`]. An engine has one state, so
/// a host whose own workers need another runs the `workflows` queue in an engine of its own.
///
/// ```rust,no_run
/// use ishikari::execution::{NodeJob, NodeJobs, NodeRunner};
/// use ishikari::prelude::*;
/// use ishikari::{Engine, Postgres, Queue};
/// use std::sync::Arc;
///
/// struct Nodes;
///
/// #[async_trait::async_trait]
/// impl NodeRunner for Nodes {
///     async fn run(&self, job: &NodeJob, _ctx: Context) -> PerformResult {
///         match job.node_type.as_str() {
///             "echo" => Complete::default().into(),
///             other => Err(format!("unknown node type {other}").into()),
///         }
///     }
/// }
///
/// # fn f(pool: sqlx::PgPool) {
/// let engine = Engine::builder("definitions")
///     .with_state(Arc::new(NodeJobs::new(Nodes)))
///     .with_queue(Queue::builder(NodeJob::QUEUE))
///     .start(Postgres::new(pool));
/// # }
/// ```
pub struct NodeJobs {
    runner: Arc<dyn NodeRunner>,
}

impl NodeJobs {
    pub fn new(runner: impl NodeRunner) -> Self {
        Self {
            runner: Arc::new(runner),
        }
    }
}

#[typetag::serde(name = "ishikari::execution::NodeJob")]
#[async_trait::async_trait]
impl Worker for NodeJob {
    fn worker() -> &'static str {
        Self::WORKER
    }

    fn queue(&self) -> &'static str {
        Self::QUEUE
    }

    fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    fn timeout(&self) -> Option<Duration> {
        self.timeout_seconds.map(|s| Duration::from_secs(s.into()))
    }

    async fn perform(&self, ctx: Context) -> PerformResult {
        let jobs = ctx
            .state::<NodeJobs>()
            .map_err(|_| "a NodeJob runs in an engine whose state is a NodeJobs")?;
        jobs.runner.run(self, ctx).await
    }
}
