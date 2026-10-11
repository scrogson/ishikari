mod postgres;

pub(crate) use postgres::qualified;
pub use postgres::{ErrorKind, Postgres};

use crate::queue::{QueueBuilder, QueueStop};
use crate::{Cron, InsertOpts, Inserted, Job, Stager, State, Worker};
use serde::Serialize;
use std::{
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};
use tracing::{error, info, instrument, warn};

/// A unique identifier for an engine instance.
///
/// This is used to distinguish between different engine instances in the same application.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EngineName(Arc<str>);

impl From<&str> for EngineName {
    fn from(name: &str) -> Self {
        Self(name.into())
    }
}

impl EngineName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Builder for configuring and creating an [`Engine`] instance.
///
/// The builder pattern allows for flexible configuration of the engine with sensible defaults.
/// You can configure:
/// - The stager interval (how often to check for scheduled jobs)
/// - The stager limit (maximum number of jobs to process per interval)
/// - Multiple queues with different configurations
/// - Custom state to be shared across queues
/// - The schema the jobs live in
///
/// # Example
/// ```rust,no_run
/// # use ishikari::{Engine, Postgres, Queue};
/// # use std::{sync::Arc, time::Duration};
/// # use sqlx::PgPool;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     // Setup database connection
///     let database_url = std::env::var("DATABASE_URL")?;
///     let pool = PgPool::connect(&database_url).await?;
///
///     // Create a simple shared state
///     let shared_state = Arc::new(());
///
///     // Build and start the engine
///     let engine = Engine::builder("my-engine")
///         .stager_interval(Duration::from_secs(5))
///         .stager_limit(50)
///         .with_state(shared_state)
///         .with_queue(Queue::builder("default").concurrency(10))
///         .start(Postgres::new(pool));
///
///     // Engine is now running and processing jobs
/// #   Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct EngineBuilder {
    name: EngineName,
    queues: Vec<QueueBuilder>,
    stager_interval: Option<Duration>,
    stager_limit: Option<i32>,
    state: Option<State>,
    schema: Option<String>,
    node: Option<String>,
    heartbeat_interval: Duration,
    node_timeout: Duration,
    rescue_interval: Duration,
    rescue_unowned_after: Duration,
    crons: Vec<Cron>,
    cron_interval: Duration,
    cron_catch_up: Duration,
    resolver_interval: Duration,
    prune_after: Option<Duration>,
    prune_interval: Duration,
}

impl EngineBuilder {
    /// Sets the interval at which the stager checks for scheduled jobs.
    ///
    /// Defaults to 1 second if not specified.
    pub fn stager_interval(mut self, interval: Duration) -> Self {
        self.stager_interval = Some(interval);
        self
    }

    /// Sets the maximum number of jobs the stager will process in a single interval.
    ///
    /// Defaults to 100 if not specified.
    pub fn stager_limit(mut self, limit: i32) -> Self {
        self.stager_limit = Some(limit);
        self
    }

    /// Adds a queue to the engine.
    ///
    /// Multiple queues can be added, each with its own configuration.
    pub fn with_queue(mut self, queue_builder: QueueBuilder) -> Self {
        self.queues.push(queue_builder);
        self
    }

    /// Sets the shared state that will be available to all queues.
    ///
    /// This state is cloned for each queue and can be used to share resources
    /// or configuration across all queues in the engine.
    pub fn with_state(mut self, state: State) -> Self {
        self.state = Some(state);
        self
    }

    /// Sets the schema the engine's jobs live in.
    ///
    /// Every job the engine inserts, runs, stages or records is read and written there. If not
    /// specified, the storage's schema is used (the connection's search path by default).
    pub fn schema<T: Into<String>>(mut self, schema: T) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Names this node (this engine instance) in `ishikari_nodes` and in the jobs it runs.
    ///
    /// Defaults to `{engine name}-{host}-{pid}-{random}`, unique per process.
    pub fn node_name(mut self, node: impl Into<String>) -> Self {
        self.node = Some(node.into());
        self
    }

    /// How often the node records that it is alive. Defaults to 15 seconds.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// How long a node may go without a heartbeat before the jobs it was running are given
    /// back to their queues. Defaults to 60 seconds; keep it well above the heartbeat interval.
    pub fn node_timeout(mut self, timeout: Duration) -> Self {
        self.node_timeout = timeout;
        self
    }

    /// How often to look for jobs whose node stopped. Defaults to 60 seconds.
    pub fn rescue_interval(mut self, interval: Duration) -> Self {
        self.rescue_interval = interval;
        self
    }

    /// How long a job claimed by a node that didn't record itself (an ishikari older than 0.2)
    /// may run before it is given back. Defaults to 1 hour.
    pub fn rescue_unowned_after(mut self, after: Duration) -> Self {
        self.rescue_unowned_after = after;
        self
    }

    /// Insert a job on a schedule; see [`Cron`].
    pub fn cron(mut self, cron: Cron) -> Self {
        self.crons.push(cron);
        self
    }

    /// How often to check for cron slots that are due. Defaults to 15 seconds; a slot's job is
    /// inserted within this long of its time.
    pub fn cron_interval(mut self, interval: Duration) -> Self {
        self.cron_interval = interval;
        self
    }

    /// How far back a starting node catches up on cron slots no node ran (during a deploy, for
    /// example). Defaults to 5 minutes. Slots already inserted by any node are never inserted
    /// again.
    pub fn cron_catch_up(mut self, catch_up: Duration) -> Self {
        self.cron_catch_up = catch_up;
        self
    }

    /// How often the dependency resolver sweeps workflows: it runs saga compensation and catches
    /// any step a completion didn't release. Defaults to 30 seconds; steps are normally released
    /// the moment their last upstream completes.
    pub fn resolver_interval(mut self, interval: Duration) -> Self {
        self.resolver_interval = interval;
        self
    }

    /// Delete finished jobs (completed, cancelled, discarded) this long after they finished,
    /// and finished workflows with them. Defaults to 7 days; never less than the cron catch-up,
    /// so a recent cron slot isn't forgotten and run again.
    pub fn prune_after(mut self, after: Duration) -> Self {
        self.prune_after = Some(after);
        self
    }

    /// Keep finished jobs forever.
    pub fn no_pruning(mut self) -> Self {
        self.prune_after = None;
        self
    }

    /// How often to prune (1,000 jobs at a time). Defaults to 60 seconds.
    pub fn prune_interval(mut self, interval: Duration) -> Self {
        self.prune_interval = interval;
        self
    }

    /// Starts the engine with the provided storage backend.
    ///
    /// This will:
    /// 1. Initialize the stager with the configured interval and limit
    /// 2. Build and start all configured queues
    /// 3. Return an [`Engine`] instance that can be used to insert jobs
    pub fn start(self, storage: Postgres) -> Engine {
        let storage = Arc::new(match self.schema {
            Some(schema) => storage.with_schema(schema),
            None => storage,
        });
        let state = self.state.unwrap_or(Arc::new(()));
        let node: Arc<str> = self
            .node
            .unwrap_or_else(|| default_node_name(self.name.as_str()))
            .into();
        let queue_names: Vec<String> = self
            .queues
            .iter()
            .map(|q| q.name.as_str().to_string())
            .collect();

        let mut tasks = Vec::new();
        tasks.push(heartbeat(
            storage.clone(),
            node.clone(),
            queue_names,
            self.heartbeat_interval,
        ));
        tasks.push(rescuer(
            storage.clone(),
            self.rescue_interval,
            self.node_timeout,
            self.rescue_unowned_after,
        ));

        let mut resolver = crate::dependencies::DependencyResolver::new(
            storage.pool.clone(),
            self.resolver_interval,
        );
        if let Some(schema) = storage.schema() {
            resolver = resolver.schema(schema);
        }
        tasks.push(resolver.start());

        if let Some(after) = self.prune_after {
            tasks.push(pruner(
                storage.clone(),
                after.max(self.cron_catch_up),
                self.prune_interval,
            ));
        }

        if !self.crons.is_empty() {
            tasks.push(cron_task(
                storage.clone(),
                self.crons,
                self.cron_interval,
                self.cron_catch_up,
            ));
        }

        let stager_interval = self.stager_interval.unwrap_or(Duration::from_secs(1));
        let stager_limit = self.stager_limit.unwrap_or(100);
        tasks.push(Stager::new(storage.clone(), stager_interval, stager_limit).start());

        let (stop, stop_rx) = watch::channel(None);
        let queues = self
            .queues
            .into_iter()
            .map(|queue| {
                queue
                    .build_on(
                        storage.clone(),
                        state.clone(),
                        node.clone(),
                        stop_rx.clone(),
                    )
                    .spawn()
            })
            .collect();

        Engine {
            name: self.name,
            node,
            storage,
            tasks,
            queues,
            stop,
        }
    }
}

/// The core component of Ishikari that manages job queues and scheduling.
///
/// The engine is responsible for:
/// - Managing multiple job queues
/// - Running the stager to process scheduled jobs
/// - Inserting jobs into its schema
///
/// An engine instance is created using the [`EngineBuilder`] and represents
/// a running job processing system. Once started, it will continue processing
/// jobs until the application terminates.
pub struct Engine {
    name: EngineName,
    node: Arc<str>,
    storage: Arc<Postgres>,
    /// Heartbeat, rescuer and stager.
    tasks: Vec<JoinHandle<()>>,
    queues: Vec<JoinHandle<QueueStop>>,
    stop: watch::Sender<Option<Instant>>,
}

/// What [`Engine::shutdown`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Shutdown {
    /// Jobs that finished during the grace period.
    pub finished: usize,
    /// Jobs interrupted when the grace period ran out, given back to their queues with the
    /// attempt refunded.
    pub returned: usize,
}

impl Engine {
    /// Creates a new builder for configuring an engine instance.
    ///
    /// The name parameter is used to identify this engine instance and should be
    /// unique within your application.
    pub fn builder(name: &str) -> EngineBuilder {
        EngineBuilder {
            name: name.into(),
            queues: Vec::new(),
            stager_interval: None,
            stager_limit: None,
            state: None,
            schema: None,
            node: None,
            heartbeat_interval: Duration::from_secs(15),
            node_timeout: Duration::from_secs(60),
            rescue_interval: Duration::from_secs(60),
            rescue_unowned_after: Duration::from_secs(3600),
            crons: Vec::new(),
            cron_interval: Duration::from_secs(15),
            cron_catch_up: Duration::from_secs(300),
            resolver_interval: Duration::from_secs(30),
            prune_after: Some(Duration::from_secs(7 * 24 * 3600)),
            prune_interval: Duration::from_secs(60),
        }
    }

    /// Create a new engine builder named "default".
    pub fn postgres() -> EngineBuilder {
        Self::builder("default")
    }

    /// The engine's name.
    pub fn name(&self) -> &EngineName {
        &self.name
    }

    /// Stop the engine: stop claiming jobs, wait up to `grace` for running jobs to finish, give
    /// the rest back to their queues (with the attempt refunded), stop the background tasks
    /// and remove this node's row. Call it from the host's shutdown signal (SIGTERM) so a deploy
    /// doesn't leave jobs to the rescuer.
    ///
    /// Jobs given back were interrupted mid-run, so workers should be safe to run twice.
    pub async fn shutdown(self, grace: Duration) -> Shutdown {
        let _ = self.stop.send(Some(Instant::now() + grace));
        let mut report = Shutdown::default();
        for queue in self.queues {
            match queue.await {
                Ok(stop) => {
                    report.finished += stop.finished;
                    report.returned += stop.returned;
                }
                Err(e) => error!(error = %e, "a queue task failed while stopping"),
            }
        }
        for task in self.tasks {
            task.abort();
            let _ = task.await;
        }
        if let Err(e) = self.storage.forget_node(&self.node).await {
            error!(node = %self.node, error = %e, "failed to remove the node's row");
        }
        info!(node = %self.node, finished = report.finished, returned = report.returned, "engine stopped");
        report
    }

    /// This node's name, as recorded in `ishikari_nodes` and in `attempted_by`.
    pub fn node(&self) -> &str {
        &self.node
    }

    /// The engine's storage.
    pub fn storage(&self) -> &Postgres {
        &self.storage
    }

    /// Insert a job into this engine's schema.
    #[instrument(skip(self))]
    pub async fn insert<J>(&self, job: J) -> Result<Job, sqlx::Error>
    where
        J: Debug + Serialize + Worker + Send + Sync + 'static,
    {
        self.insert_tx(job, &*self.storage.pool).await
    }

    /// Insert a job into this engine's schema on the given connection, for example inside the
    /// caller's transaction (`&mut *tx`).
    #[instrument(skip(self, conn))]
    pub async fn insert_tx<'c, J, A>(&self, job: J, conn: A) -> Result<Job, sqlx::Error>
    where
        J: Debug + Serialize + Worker + Send + Sync + 'static,
        A: sqlx::Acquire<'c, Database = sqlx::Postgres>,
    {
        self.insert_with(job, InsertOpts::new(), conn)
            .await
            .map(Inserted::into_job)
    }

    /// Insert a job with options into this engine's schema (unless the options name another).
    #[instrument(skip(self, conn))]
    pub async fn insert_with<'c, J, A>(
        &self,
        job: J,
        opts: InsertOpts,
        conn: A,
    ) -> Result<Inserted, sqlx::Error>
    where
        J: Debug + Serialize + Worker + Send + Sync + 'static,
        A: sqlx::Acquire<'c, Database = sqlx::Postgres>,
    {
        crate::insert_with(job, opts.schema_or(self.storage.schema()), conn).await
    }
}

/// `{engine}-{host}-{pid}-{random}`: unique per process, readable in the admin.
fn default_node_name(engine: &str) -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".into());
    let random: u16 = rand::random();
    format!("{engine}-{host}-{}-{random:04x}", std::process::id())
}

/// Keep this node's row fresh so its running jobs aren't taken for lost.
fn heartbeat(
    storage: Arc<Postgres>,
    node: Arc<str>,
    queues: Vec<String>,
    every: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(every);
        loop {
            interval.tick().await;
            if let Err(e) = storage.heartbeat(&node, &queues).await {
                error!(node = %node, error = %e, "failed to record heartbeat");
            }
        }
    })
}

/// Give back jobs whose node stopped heartbeating.
fn rescuer(
    storage: Arc<Postgres>,
    every: Duration,
    node_timeout: Duration,
    unowned_after: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(every);
        loop {
            interval.tick().await;
            match storage.rescue(node_timeout, unowned_after).await {
                Ok(rescued) => {
                    for (id, node) in rescued {
                        warn!(id, node = %node, "rescued a job whose node stopped");
                    }
                }
                Err(e) => error!(error = %e, "failed to rescue jobs"),
            }
        }
    })
}

/// Insert cron slots as they fall due, starting `catch_up` in the past.
fn cron_task(
    storage: Arc<Postgres>,
    crons: Vec<Cron>,
    every: Duration,
    catch_up: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut after =
            chrono::Utc::now() - chrono::Duration::from_std(catch_up).unwrap_or_default();
        let mut interval = tokio::time::interval(every);
        loop {
            interval.tick().await;
            let now = chrono::Utc::now();
            crate::cron::run_due(&crons, &storage, after, now).await;
            after = now;
        }
    })
}

/// Delete old finished jobs, 1,000 at a time, until none are left, then wait for the next tick.
fn pruner(storage: Arc<Postgres>, after: Duration, every: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(every);
        loop {
            interval.tick().await;
            loop {
                match storage.prune(after, 1000).await {
                    Ok(0) => break,
                    Ok(n) => tracing::debug!(n, "pruned finished jobs"),
                    Err(e) => {
                        error!(error = %e, "failed to prune jobs");
                        break;
                    }
                }
            }
        }
    })
}
