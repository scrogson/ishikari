mod postgres;
mod storage;

pub use postgres::Postgres;
pub use storage::Storage;

use crate::{queue::QueueBuilder, Stager, State};
use std::{marker::PhantomData, sync::Arc, time::Duration};

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
    #[allow(dead_code)]
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
///     let engine = Engine::<Postgres>::builder("my-engine")
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
pub struct EngineBuilder<S>
where
    S: Storage + 'static,
{
    name: EngineName,
    queues: Vec<QueueBuilder<S>>,
    stager_interval: Option<Duration>,
    stager_limit: Option<i32>,
    state: Option<State>,
    storage: PhantomData<S>,
}

impl<S> EngineBuilder<S>
where
    S: Storage + 'static,
{
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
    pub fn with_queue(mut self, queue_builder: QueueBuilder<S>) -> Self {
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

    /// Starts the engine with the provided storage backend.
    ///
    /// This will:
    /// 1. Initialize the stager with the configured interval and limit
    /// 2. Build and start all configured queues
    /// 3. Return an [`Engine`] instance that can be used to manage the queues
    pub fn start(self, storage: impl Into<Arc<S>>) -> Engine<S> {
        let storage = storage.into();
        let state = self.state.unwrap_or(Arc::new(()));

        let stager_interval = self.stager_interval.unwrap_or(Duration::from_secs(1));
        let stager_limit = self.stager_limit.unwrap_or(100);
        let stager = Stager::new(storage.clone(), stager_interval, stager_limit);
        let _stager_handle = stager.start();

        for queue in self.queues.into_iter() {
            let queue = queue.build(storage.clone(), state.clone());
            queue.start();
        }

        Engine {
            name: self.name,
            stager_interval,
            stager_limit,
            storage,
        }
    }
}

/// The core component of Ishikari that manages job queues and scheduling.
///
/// The engine is responsible for:
/// - Managing multiple job queues
/// - Running the stager to process scheduled jobs
/// - Coordinating between queues and the storage backend
///
/// An engine instance is created using the [`EngineBuilder`] and represents
/// a running job processing system. Once started, it will continue processing
/// jobs until the application terminates.
#[allow(dead_code)]
pub struct Engine<S>
where
    S: Storage + 'static,
{
    name: EngineName,
    storage: Arc<S>,
    stager_interval: Duration,
    stager_limit: i32,
}

impl<S> Engine<S>
where
    S: Storage + 'static,
{
    /// Creates a new builder for configuring an engine instance.
    ///
    /// The name parameter is used to identify this engine instance and should be
    /// unique within your application.
    pub fn builder(name: &str) -> EngineBuilder<S> {
        EngineBuilder {
            name: name.into(),
            queues: Vec::new(),
            stager_interval: None,
            stager_limit: None,
            state: None,
            storage: PhantomData,
        }
    }
}
