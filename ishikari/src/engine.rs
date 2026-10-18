mod postgres;

pub(crate) use postgres::qualified;
pub use postgres::{ErrorKind, Postgres};

use crate::queue::{extract_panic_message, QueueBuilder, QueueStop, RESTART_AFTER};
use crate::{Cron, InsertOpts, Inserted, Job, Stager, State, Worker};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::Serialize;
use sqlx::postgres::PgListener;
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{watch, Notify},
    task::JoinHandle,
};
use tracing::{error, info, warn};

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
/// The `*_interval` setters panic on a zero interval.
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
    listen: bool,
}

impl EngineBuilder {
    /// Sets the interval at which the stager checks for scheduled jobs.
    ///
    /// Defaults to 1 second if not specified.
    #[track_caller]
    pub fn stager_interval(mut self, interval: Duration) -> Self {
        self.stager_interval = Some(crate::nonzero(interval, "stager_interval"));
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
    #[track_caller]
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = crate::nonzero(interval, "heartbeat_interval");
        self
    }

    /// How long a node may go without a heartbeat before the jobs it was running are given
    /// back to their queues. Defaults to 60 seconds; keep it well above the heartbeat interval.
    pub fn node_timeout(mut self, timeout: Duration) -> Self {
        self.node_timeout = timeout;
        self
    }

    /// How often to look for jobs whose node stopped. Defaults to 60 seconds.
    #[track_caller]
    pub fn rescue_interval(mut self, interval: Duration) -> Self {
        self.rescue_interval = crate::nonzero(interval, "rescue_interval");
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
    #[track_caller]
    pub fn cron_interval(mut self, interval: Duration) -> Self {
        self.cron_interval = crate::nonzero(interval, "cron_interval");
        self
    }

    /// How far back a starting node catches up on cron slots no node ran (during a deploy, for
    /// example). Defaults to 5 minutes. Slots already inserted by any node are never inserted
    /// again. A slot whose insert fails is tried again on later checks, while it is within this
    /// long.
    pub fn cron_catch_up(mut self, catch_up: Duration) -> Self {
        self.cron_catch_up = catch_up;
        self
    }

    /// How often the dependency resolver sweeps workflows: it runs saga compensation and catches
    /// any step a completion didn't release. Defaults to 30 seconds; steps are normally released
    /// the moment their last upstream completes.
    #[track_caller]
    pub fn resolver_interval(mut self, interval: Duration) -> Self {
        self.resolver_interval = crate::nonzero(interval, "resolver_interval");
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
    #[track_caller]
    pub fn prune_interval(mut self, interval: Duration) -> Self {
        self.prune_interval = crate::nonzero(interval, "prune_interval");
        self
    }

    /// Whether to listen for jobs becoming available, so a queue claims them at once rather than
    /// at its next poll. Defaults to true. The engine holds one of the pool's connections for
    /// it.
    ///
    /// Turn it off where `LISTEN` doesn't work, such as through PgBouncer in transaction mode;
    /// queues then find jobs by polling alone (see [`QueueBuilder::interval`]).
    pub fn listen(mut self, listen: bool) -> Self {
        self.listen = listen;
        self
    }

    /// Starts the engine with the provided storage backend.
    ///
    /// This will:
    /// 1. Initialize the stager with the configured interval and limit
    /// 2. Build and start all configured queues
    /// 3. Return an [`Engine`] instance that can be used to insert jobs
    ///
    /// The engine runs until it is shut down or dropped; keep it for as long as it should run.
    /// Each of its background tasks (the queues, heartbeat, stager, rescuer, resolver, cron,
    /// pruner and listener) is restarted if it panics, after a backoff, and the panic is logged
    /// at error.
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
        let (every, s, n) = (self.heartbeat_interval, storage.clone(), node.clone());
        tasks.push(spawn_supervised("heartbeat", move || {
            heartbeat(s.clone(), n.clone(), queue_names.clone(), every)
        }));
        let (s, every) = (storage.clone(), self.rescue_interval);
        let (node_timeout, unowned_after) = (self.node_timeout, self.rescue_unowned_after);
        tasks.push(spawn_supervised("rescuer", move || {
            rescuer(s.clone(), every, node_timeout, unowned_after)
        }));

        let (pool, every) = (storage.pool.clone(), self.resolver_interval);
        let schema = storage.schema().map(str::to_string);
        tasks.push(spawn_supervised("dependency resolver", move || {
            let mut resolver = crate::dependencies::DependencyResolver::new(pool.clone(), every);
            if let Some(schema) = &schema {
                resolver = resolver.schema(schema);
            }
            spawned(resolver.start())
        }));

        if let Some(after) = self.prune_after {
            let (s, after, every) = (
                storage.clone(),
                after.max(self.cron_catch_up),
                self.prune_interval,
            );
            tasks.push(spawn_supervised("pruner", move || {
                pruner(s.clone(), after, every)
            }));
        }

        if !self.crons.is_empty() {
            let (s, crons) = (storage.clone(), Arc::new(self.crons));
            let (every, catch_up) = (self.cron_interval, self.cron_catch_up);
            tasks.push(spawn_supervised("cron", move || {
                cron_task(s.clone(), crons.clone(), every, catch_up)
            }));
        }

        let stager_interval = self.stager_interval.unwrap_or(Duration::from_secs(1));
        let stager_limit = self.stager_limit.unwrap_or(100);
        let s = storage.clone();
        tasks.push(spawn_supervised("stager", move || {
            spawned(Stager::new(s.clone(), stager_interval, stager_limit).start())
        }));

        let wakes: HashMap<String, Arc<Notify>> = self
            .queues
            .iter()
            .map(|q| (q.name.as_str().to_string(), Arc::default()))
            .collect();
        if self.listen && !wakes.is_empty() {
            let (s, wakes) = (storage.clone(), wakes.clone());
            tasks.push(spawn_supervised("listener", move || {
                listener(s.clone(), wakes.clone())
            }));
        }

        let (stop, stop_rx) = watch::channel(None);
        let queues = self
            .queues
            .into_iter()
            .map(|queue| {
                let wake = wakes[queue.name.as_str()].clone();
                queue
                    .build_on(
                        storage.clone(),
                        state.clone(),
                        node.clone(),
                        stop_rx.clone(),
                        wake,
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
/// a running job processing system. It runs until [`shutdown`](Engine::shutdown), the graceful
/// way to stop it, or until it is dropped. Dropping it stops it without waiting: its running
/// jobs are aborted and given back to their queues (with the attempt refunded) while the runtime
/// lasts, its background tasks are aborted and its node's row is left for the rescuer.
#[must_use = "an engine stops when it is dropped"]
pub struct Engine {
    name: EngineName,
    node: Arc<str>,
    storage: Arc<Postgres>,
    /// The background tasks, each supervised.
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
            listen: true,
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
    pub async fn shutdown(mut self, grace: Duration) -> Shutdown {
        let _ = self.stop.send(Some(Instant::now() + grace));
        let mut report = Shutdown::default();
        for queue in std::mem::take(&mut self.queues) {
            match queue.await {
                Ok(stop) => {
                    report.finished += stop.finished;
                    report.returned += stop.returned;
                }
                Err(e) => error!(error = %e, "a queue task failed while stopping"),
            }
        }
        for task in std::mem::take(&mut self.tasks) {
            task.abort();
            let _ = task.await;
        }
        if let Err(e) = self.storage.forget_node(&self.node).await {
            error!(node = %self.node, error = %e, "failed to remove the node's row");
        }
        info!(node = %self.node, finished = report.finished, returned = report.returned, "engine stopped");
        report
    }

    /// Run a job again as soon as it can; see [`Postgres::retry_job`].
    pub async fn retry_job(&self, id: i64) -> Result<bool, sqlx::Error> {
        self.storage.retry_job(id).await
    }

    /// Cancel a job and what depends on it; see [`Postgres::cancel_job`].
    pub async fn cancel_job(&self, id: i64, reason: Option<&str>) -> Result<bool, sqlx::Error> {
        self.storage.cancel_job(id, reason).await
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
    pub fn insert<J>(&self, job: J) -> BoxFuture<'_, Result<Job, sqlx::Error>>
    where
        J: Debug + Serialize + Worker + Send + Sync + 'static,
    {
        self.insert_tx(job, &*self.storage.pool)
    }

    /// Insert a job into this engine's schema on the given connection, for example inside the
    /// caller's transaction (`&mut *tx`).
    pub fn insert_tx<'c, J, A>(&self, job: J, conn: A) -> BoxFuture<'c, Result<Job, sqlx::Error>>
    where
        J: Debug + Serialize + Worker + Send + Sync + 'static,
        A: sqlx::Acquire<'c, Database = sqlx::Postgres> + Send + 'c,
    {
        let inserted = self.insert_with(job, InsertOpts::new(), conn);
        Box::pin(async move { inserted.await.map(Inserted::into_job) })
    }

    /// Insert a job with options into this engine's schema (unless the options name another).
    pub fn insert_with<'c, J, A>(
        &self,
        job: J,
        opts: InsertOpts,
        conn: A,
    ) -> BoxFuture<'c, Result<Inserted, sqlx::Error>>
    where
        J: Debug + Serialize + Worker + Send + Sync + 'static,
        A: sqlx::Acquire<'c, Database = sqlx::Postgres> + Send + 'c,
    {
        crate::insert_with(job, opts.schema_or(self.storage.schema()), conn)
    }
}

impl Drop for Engine {
    /// Stop without waiting; see [`Engine`]. After [`shutdown`](Engine::shutdown) there is
    /// nothing left to stop.
    fn drop(&mut self) {
        if self.queues.is_empty() && self.tasks.is_empty() {
            return;
        }
        info!(node = %self.node, "engine dropped; stopping without waiting for running jobs");
        // Each queue aborts its running jobs and gives them back, then ends.
        let _ = self.stop.send(Some(Instant::now()));
        self.queues.clear();
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

/// Spawn [`supervise`]`(task, make)`.
fn spawn_supervised<F, Fut>(task: &'static str, make: F) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(supervise(task, make))
}

/// Run the future `make` returns, and when it panics, log the panic at error and run a new one
/// after a backoff: 1 second, doubling up to a minute while it keeps panicking soon after it
/// starts. Returns when a run returns.
pub(crate) async fn supervise<F, Fut>(task: &'static str, make: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    supervise_with(task, RESTART_AFTER, make).await
}

async fn supervise_with<F, Fut>(task: &'static str, backoff: (Duration, Duration), mut make: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let mut delay = backoff.0;
    loop {
        let started = Instant::now();
        let Err(panic) = AssertUnwindSafe(make()).catch_unwind().await else {
            return;
        };
        if started.elapsed() >= backoff.1 {
            delay = backoff.0;
        }
        let panic = extract_panic_message(&panic);
        error!(task, panic, restart_in = ?delay, "background task panicked; restarting it");
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(backoff.1);
    }
}

/// Wait for a task that spawned itself, passing on its panic, and abort it when dropped.
async fn spawned(task: JoinHandle<()>) {
    struct AbortOnDrop(JoinHandle<()>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }
    let mut task = AbortOnDrop(task);
    if let Err(e) = (&mut task.0).await {
        if e.is_panic() {
            std::panic::resume_unwind(e.into_panic());
        }
    }
}

/// `{engine}-{host}-{pid}-{random}`: unique per process, readable in the admin.
pub(crate) fn default_node_name(engine: &str) -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "localhost".into());
    let random: u16 = rand::random();
    format!("{engine}-{host}-{}-{random:04x}", std::process::id())
}

/// Keep this node's row fresh so its running jobs aren't taken for lost.
pub(crate) async fn heartbeat(
    storage: Arc<Postgres>,
    node: Arc<str>,
    queues: Vec<String>,
    every: Duration,
) {
    let mut interval = tokio::time::interval(every);
    loop {
        interval.tick().await;
        if let Err(e) = storage.heartbeat(&node, &queues).await {
            error!(node = %node, error = %e, "failed to record heartbeat");
        }
    }
}

/// Wake a queue when jobs become available in it. The jobs table notifies its channel with the
/// job's queue whenever a job is inserted available or becomes available (staged, released by
/// its upstreams, retried, given back). Notifications sent while the connection was down are
/// lost, so every queue is woken on (re)connecting; polling covers the rest.
async fn listener(storage: Arc<Postgres>, wakes: HashMap<String, Arc<Notify>>) {
    let wake_all = move |wakes: &HashMap<String, Arc<Notify>>| {
        wakes.values().for_each(|wake| wake.notify_one());
    };
    let mut delay = Duration::from_millis(100);
    loop {
        let mut listener = match subscribe(&storage).await {
            Ok(listener) => listener,
            Err(e) => {
                warn!(error = %e, retry_in = ?delay, "failed to listen for available jobs; polling meanwhile");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        delay = Duration::from_millis(100);
        wake_all(&wakes);
        loop {
            match listener.try_recv().await {
                Ok(Some(notice)) => match queue_of(notice.payload()) {
                    Some(queue) => {
                        if let Some(wake) = wakes.get(&queue) {
                            wake.notify_one();
                        }
                    }
                    None => warn!(payload = notice.payload(), "unreadable job notification"),
                },
                // The connection dropped; the next call reconnects and listens again.
                Ok(None) => {
                    warn!("lost the connection listening for available jobs; reconnecting");
                    wake_all(&wakes);
                }
                Err(e) => {
                    warn!(error = %e, "failed listening for available jobs; reconnecting");
                    break;
                }
            }
        }
    }
}

/// A connection listening on the storage's channel.
async fn subscribe(storage: &Postgres) -> Result<PgListener, sqlx::Error> {
    let channel = storage.channel().await?;
    let mut listener = PgListener::connect_with(storage.pool()).await?;
    listener.listen(&channel).await?;
    info!(channel, "listening for available jobs");
    Ok(listener)
}

/// The queue a job notification names: `{"queue": "..."}`.
fn queue_of(payload: &str) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Notice {
        queue: String,
    }
    serde_json::from_str::<Notice>(payload)
        .ok()
        .map(|n| n.queue)
}

/// Give back jobs whose node stopped heartbeating.
async fn rescuer(
    storage: Arc<Postgres>,
    every: Duration,
    node_timeout: Duration,
    unowned_after: Duration,
) {
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
}

/// Insert cron slots as they fall due, starting `catch_up` in the past. Each entry goes on from
/// its last slot that is in, never further back than `catch_up`: older slots' jobs may have been
/// pruned, and inserting them again would run them twice.
async fn cron_task(
    storage: Arc<Postgres>,
    crons: Arc<Vec<Cron>>,
    every: Duration,
    catch_up: Duration,
) {
    let catch_up = chrono::Duration::from_std(catch_up).unwrap_or_default();
    let mut done = vec![chrono::Utc::now() - catch_up; crons.len()];
    let mut interval = tokio::time::interval(every);
    loop {
        interval.tick().await;
        let now = chrono::Utc::now();
        crate::cron::run_due(&crons, &storage, &mut done, now - catch_up, now).await;
    }
}

/// Delete old finished jobs, 1,000 at a time, until none are left, then wait for the next tick.
async fn pruner(storage: Arc<Postgres>, after: Duration, every: Duration) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn a_task_that_panics_is_restarted_with_backoff_until_it_returns() {
        let runs = Arc::new(AtomicUsize::new(0));
        let counted = runs.clone();
        let started = Instant::now();
        let backoff = (Duration::from_millis(20), Duration::from_millis(80));
        supervise_with("test", backoff, move || {
            let run = counted.fetch_add(1, Ordering::SeqCst);
            async move {
                if run < 3 {
                    panic!("run {run} panicked");
                }
            }
        })
        .await;
        assert_eq!(runs.load(Ordering::SeqCst), 4);
        // 20, 40 and 80 ms between the runs.
        assert!(started.elapsed() >= Duration::from_millis(140));
    }

    #[tokio::test]
    async fn a_spawned_task_passes_on_its_panic_and_stops_with_its_supervisor() {
        let runs = Arc::new(AtomicUsize::new(0));
        let counted = runs.clone();
        let (alive, gone) = tokio::sync::oneshot::channel::<()>();
        let mut alive = Some(alive);
        let backoff = (Duration::from_millis(10), Duration::from_millis(10));
        let supervisor = tokio::spawn(supervise_with("test", backoff, move || {
            let run = counted.fetch_add(1, Ordering::SeqCst);
            let alive = alive.take();
            spawned(tokio::spawn(async move {
                if run == 0 {
                    panic!("the first run panicked");
                }
                let _alive = alive;
                std::future::pending::<()>().await
            }))
        }));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(runs.load(Ordering::SeqCst), 2, "restarted once");

        supervisor.abort();
        tokio::time::timeout(Duration::from_secs(1), gone)
            .await
            .expect("the spawned task is aborted with its supervisor")
            .unwrap_err();
    }
}
