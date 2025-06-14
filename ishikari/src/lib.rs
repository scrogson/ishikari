use chrono::{DateTime, Duration, Utc};
use rand::Rng;
#[doc(hidden)]
pub use serde;
use serde::Serialize;
use sqlx::FromRow;
use sqlx::PgExecutor;

mod engine;
mod model;
mod queue;
mod result;
mod stager;

pub use ishikari_macros::{job, worker};

pub use engine::{Engine, EngineBuilder, Postgres, Storage};
pub use model::{Job, JobState};
pub use queue::Queue;
pub use result::{Cancel, Complete, PerformError, PerformResult, Snooze, Status};
pub use stager::Stager;

/// A prelude for building Ishikari workers.
pub mod prelude {
    pub use crate::{Cancel, Complete, Context, PerformResult, Snooze, Status, Worker};
}

pub(crate) type State = Arc<dyn std::any::Any + Send + Sync>;

/// A backoff strategy for retrying jobs.
pub enum Backoff {
    /// Fixed delay duration
    Fixed(Duration),
    /// Linear backoff based on attempt number
    Linear(Duration),
    /// Exponential backoff with a base duration
    Exponential(Duration),
    /// Exponential with jitter
    ExponentialJitter(Duration),
    /// Custom backoff strategy
    Custom(Box<dyn Fn(i32) -> DateTime<Utc> + Send + Sync>),
}

impl Backoff {
    pub fn next_retry(&self, attempt: i32) -> DateTime<Utc> {
        match self {
            Backoff::Fixed(duration) => Utc::now() + *duration,
            Backoff::Linear(base) => Utc::now() + (*base * attempt),
            Backoff::Exponential(base) => {
                let base_seconds = base.num_seconds();
                let exp_delay = base_seconds * 2_i64.pow(attempt as u32);
                Utc::now() + Duration::seconds(exp_delay)
            }
            Backoff::ExponentialJitter(base) => {
                let base_seconds = base.num_seconds();
                let exp_delay = base_seconds * 2_i64.pow(attempt as u32);
                let jitter = rand::thread_rng().gen_range(0..exp_delay);
                Utc::now() + Duration::seconds(exp_delay + jitter)
            }
            Backoff::Custom(strategy) => strategy(attempt),
        }
    }
}

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{info, instrument};

/// A context for a worker that provides access to job data and shared state.
///
/// The `Context` is passed to the `perform` method of a `Worker` and provides access to:
/// - The current job being processed
/// - Shared state that was registered with the `Engine`
///
/// # Example
///
/// ```rust,no_run
/// use ishikari::prelude::*;
/// use sqlx::PgPool;
///
/// // Define a simple state type
/// #[derive(Debug)]
/// struct AppState {
///     db_pool: PgPool,
/// }
///
/// // Define a job type
/// #[ishikari::job]
/// struct ProcessDataJob {
///     data: String,
/// }
///
/// #[ishikari::worker]
/// impl Worker for ProcessDataJob {
///     async fn perform(&self, ctx: Context) -> PerformResult {
///         // Access the job being processed
///         let job = ctx.job();
///         println!("Processing job {}", job.id);
///
///         // Access shared state
///         let state = ctx.state::<AppState>()?;
///         let _conn = state.db_pool.acquire().await?;
///
///         Complete::default().into()
///     }
/// }
/// ```
#[derive(Debug)]
pub struct Context {
    pub(crate) job: Arc<Job>,
    pub(crate) state: State,
}

/// Worker context.
///
/// The context provides access to the `Job` being executed and the state
/// registered with the `Engine`.
impl Context {
    pub(crate) fn new(job: Arc<Job>, state: State) -> Self {
        Self { job, state }
    }

    /// Returns a reference to the `Job` being executed.
    ///
    /// This provides access to all job metadata including:
    /// - Job ID
    /// - Current state
    /// - Queue name
    /// - Worker type
    /// - Arguments
    /// - Attempt count
    /// - Timestamps
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use ishikari::prelude::*;
    ///
    /// // Define a job type
    /// #[ishikari::job]
    /// struct LoggingJob {
    ///     message: String,
    /// }
    ///
    /// #[ishikari::worker]
    /// impl Worker for LoggingJob {
    ///     async fn perform(&self, ctx: Context) -> PerformResult {
    ///         let job = ctx.job();
    ///         
    ///         // Access job metadata
    ///         println!("Job ID: {}", job.id);
    ///         println!("Queue: {}", job.queue);
    ///         println!("Attempt: {}/{}", job.attempt, job.max_attempts);
    ///         
    ///         Complete::default().into()
    ///     }
    /// }
    /// ```
    pub fn job(&self) -> Arc<Job> {
        Arc::clone(&self.job)
    }

    /// Returns the shared state that was registered with `Engine::with_state`.
    ///
    /// This method allows access to application-wide state that was registered when
    /// building the engine. Common uses include:
    /// - Database connection pools
    /// - Configuration
    /// - Shared resources
    /// - Service clients
    ///
    /// # Arguments
    ///
    /// * `T` - The type of state to retrieve. Must match the type registered with the engine.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing an `Arc<T>` if the state exists and is of the correct type,
    /// or an error message if the state cannot be found or is of the wrong type.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use ishikari::prelude::*;
    /// use sqlx::PgPool;
    ///
    /// // Define configuration type
    /// #[derive(Debug)]
    /// struct Config {
    ///     api_key: String,
    /// }
    ///
    /// // Define application state
    /// #[derive(Debug)]
    /// struct AppState {
    ///     db_pool: PgPool,
    ///     config: Config,
    /// }
    ///
    /// // Define a job type
    /// #[ishikari::job]
    /// struct ApiJob {
    ///     endpoint: String,
    /// }
    ///
    /// #[ishikari::worker]
    /// impl Worker for ApiJob {
    ///     async fn perform(&self, ctx: Context) -> PerformResult {
    ///         // Get the shared state
    ///         let state = ctx.state::<AppState>()?;
    ///         
    ///         // Use the database pool
    ///         let conn = state.db_pool.acquire().await?;
    ///         
    ///         // Access configuration
    ///         let api_key = &state.config.api_key;
    ///         
    ///         Complete::default().into()
    ///     }
    /// }
    /// ```
    pub fn state<T: Any + Send + Sync + 'static>(&self) -> Result<Arc<T>, &'static str> {
        // Attempt to clone and downcast the Arc
        if let Ok(downcasted) = Arc::clone(&self.state).downcast::<T>() {
            Ok(downcasted)
        } else {
            Err("Failed to extract the specified type from the context")
        }
    }
}

#[typetag::serde(tag = "ishikari_worker")]
#[async_trait::async_trait]
/// A trait for implementing job workers in Ishikari.
///
/// The `Worker` trait defines the interface for job processing in Ishikari. It provides
/// methods for configuring job behavior and implementing the actual job processing logic.
///
/// # Example
///
/// ```rust,no_run
/// use ishikari::prelude::*;
///
/// // Define a job type
/// #[ishikari::job]
/// struct EmailJob {
///     to: String,
///     subject: String,
///     body: String,
/// }
///
/// #[ishikari::worker(queue = "emails", max_attempts = 3)]
/// impl Worker for EmailJob {
///     async fn perform(&self, ctx: Context) -> PerformResult {
///         // Send email logic here
///         println!("Sending email to: {}", self.to);
///         Complete::default().into()
///     }
/// }
/// ```
///
/// # Configuration Methods
///
/// The trait provides several methods for configuring job behavior:
///
/// - `queue()`: Specifies which queue the job should be processed in
/// - `max_attempts()`: Sets the maximum number of retry attempts
/// - `backoff()`: Configures the retry delay strategy
///
/// These methods can be overridden to customize job behavior, or they can be configured
/// using the `#[ishikari::worker]` attribute macro.
///
/// # Error Handling
///
/// The `perform` method returns a `PerformResult` which can be one of:
///
/// - `Ok(Status::Complete)`: Job completed successfully
/// - `Ok(Status::Cancel)`: Job was cancelled
/// - `Ok(Status::Snooze)`: Job should be retried after a delay
/// - `Err(PerformError)`: Job failed and should be retried according to backoff strategy
///
/// When returning errors, make sure to use error types that implement `Send` and `Sync`.
/// The `anyhow::Error` type is recommended for this purpose.
///
/// # Example with Error Handling
///
/// ```rust,no_run
/// use ishikari::prelude::*;
/// use sqlx::PgPool;
///
/// // Define application state
/// struct AppState {
///     db_pool: PgPool,
/// }
///
/// // Define a job type
/// #[ishikari::job]
/// struct DataProcessingJob {
///     data: String,
/// }
///
/// #[ishikari::worker]
/// impl Worker for DataProcessingJob {
///     async fn perform(&self, ctx: Context) -> PerformResult {
///         // Access job data
///         let job = ctx.job();
///         
///         // Access shared state
///         let state = ctx.state::<AppState>()?;
///         
///         // Process the data
///         let result = process_data(&self.data).await?;
///         
///         Complete::default().into()
///     }
/// }
///
/// // Helper function for the example
/// async fn process_data(data: &str) -> Result<(), anyhow::Error> {
///     // Process the data
///     Ok(())
/// }
/// ```
///
/// # Retry Behavior
///
/// When a job fails (returns an error), it will be retried according to the configured
/// backoff strategy. The job will be retried until either:
///
/// - It succeeds
/// - It reaches the maximum number of attempts
/// - It is explicitly cancelled
///
/// The backoff strategy can be customized by implementing the `backoff` method or
/// using the `#[ishikari::worker]` attribute macro.
pub trait Worker: Send + Sync {
    /// Returns the type name of the worker.
    ///
    /// This is used internally for serialization and deserialization of jobs.
    /// You typically don't need to override this method.
    fn worker() -> &'static str
    where
        Self: Sized,
    {
        std::any::type_name::<Self>()
    }

    /// Configures the queue the job should be processed in.
    ///
    /// # Returns
    ///
    /// The name of the queue as a static string. Jobs in the same queue are processed
    /// in the order they were received, subject to priority settings.
    ///
    /// # Default
    ///
    /// Returns `"default"` if not overridden.
    fn queue(&self) -> &'static str {
        "default"
    }

    /// Configures the maximum number of times a job can be retried before being discarded.
    ///
    /// # Returns
    ///
    /// The maximum number of attempts as an `i32`. If a job fails this many times,
    /// it will be marked as discarded and won't be retried again.
    ///
    /// # Default
    ///
    /// Returns `20` if not overridden.
    fn max_attempts(&self) -> i32 {
        20
    }

    /// Controls when the next retry attempt should be scheduled.
    ///
    /// This method is called when a job fails and needs to be retried. It determines
    /// how long to wait before the next attempt.
    ///
    /// # Arguments
    ///
    /// * `attempt` - The current attempt number (1-based)
    ///
    /// # Returns
    ///
    /// A `DateTime<Utc>` indicating when the next retry should occur.
    ///
    /// # Default
    ///
    /// Returns a time 5 seconds in the future with exponential backoff if not overridden.
    fn backoff(&self, attempt: i32) -> DateTime<Utc> {
        Backoff::Exponential(Duration::seconds(5)).next_retry(attempt)
    }

    /// Performs the actual job processing.
    ///
    /// This is the main method that implements the job's business logic. It receives
    /// a `Context` that provides access to the job's data and shared state.
    ///
    /// # Arguments
    ///
    /// * `context` - A `Context` containing the job data and shared state
    ///
    /// # Returns
    ///
    /// A `PerformResult` indicating the outcome of the job:
    /// - `Complete`: Job completed successfully
    /// - `Cancel`: Job was cancelled
    /// - `Snooze`: Job should be retried after a delay
    /// - `Error`: Job failed and should be retried according to backoff strategy
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use ishikari::prelude::*;
    /// use sqlx::PgPool;
    ///
    /// // Define application state
    /// struct AppState {
    ///     db_pool: PgPool,
    /// }
    ///
    /// // Define a job type
    /// #[ishikari::job]
    /// struct DataProcessingJob {
    ///     data: String,
    /// }
    ///
    /// #[ishikari::worker]
    /// impl Worker for DataProcessingJob {
    ///     async fn perform(&self, ctx: Context) -> PerformResult {
    ///         // Access job data
    ///         let job = ctx.job();
    ///         
    ///         // Access shared state
    ///         let state = ctx.state::<AppState>()?;
    ///         
    ///         // Process the data
    ///         let result = process_data(&self.data).await?;
    ///         
    ///         Complete::default().into()
    ///     }
    /// }
    ///
    /// // Helper function for the example
    /// async fn process_data(data: &str) -> Result<(), anyhow::Error> {
    ///     // Process the data
    ///     Ok(())
    /// }
    /// ```
    async fn perform(&self, context: Context) -> PerformResult;
}

/// Inserts a new job into the queue.
///
/// This function takes a job that implements the `Worker` trait and inserts it into the database.
/// The job will be processed according to its configuration (queue, max attempts, etc.).
///
/// # Arguments
///
/// * `job` - The job to insert. Must implement `Debug`, `Serialize`, and `Worker` traits.
/// * `executor` - A PostgreSQL executor (can be a connection pool or transaction).
///
/// # Returns
///
/// Returns a `Result` containing the inserted `Job` if successful, or a `sqlx::Error` if the
/// insertion fails.
///
/// # Example
///
/// ```rust,no_run
/// use ishikari::prelude::*;
/// use sqlx::PgPool;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     let pool = PgPool::connect("postgres://...").await?;
///     
///     // Define a job type
///     #[ishikari::job]
///     struct SimpleJob {
///         data: String,
///     }
///
///     #[ishikari::worker]
///     impl Worker for SimpleJob {
///         async fn perform(&self, _ctx: Context) -> PerformResult {
///             println!("Processing: {}", self.data);
///             Complete::default().into()
///         }
///     }
///     
///     // Create and insert a job
///     let job = SimpleJob { data: "test".to_string() };
///     let inserted = ishikari::insert(job, &pool).await?;
///     
///     println!("Job inserted with ID: {}", inserted.id);
/// #   Ok(())
/// # }
/// ```
///
/// # Transaction Support
///
/// The function can be used within a transaction:
///
/// ```rust,no_run
/// use ishikari::prelude::*;
/// use sqlx::PgPool;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
///     let pool = PgPool::connect("postgres://...").await?;
///     
///     // Define a job type
///     #[ishikari::job]
///     struct BatchJob {
///         id: i32,
///     }
///
///     #[ishikari::worker]
///     impl Worker for BatchJob {
///         async fn perform(&self, _ctx: Context) -> PerformResult {
///             println!("Processing batch {}", self.id);
///             Complete::default().into()
///         }
///     }
///     
///     // Start a transaction
///     let mut tx = pool.begin().await?;
///     
///     // Insert multiple jobs atomically
///     let job1 = ishikari::insert(BatchJob { id: 1 }, &mut *tx).await?;
///     let job2 = ishikari::insert(BatchJob { id: 2 }, &mut *tx).await?;
///     
///     // Commit the transaction
///     tx.commit().await?;
/// #   Ok(())
/// # }
/// ```
#[instrument(skip(executor))]
pub async fn insert<'a, J, E>(job: J, executor: E) -> Result<Job, sqlx::Error>
where
    J: Debug + Serialize + Worker + Send + Sync + 'static,
    E: PgExecutor<'a>,
{
    // TODO: remove this unwrap
    let args = serde_json::to_value(&job as &dyn Worker).unwrap();

    let row =
        sqlx::query(r#"insert into jobs (queue, worker, args, max_attempts) values ($1, $2, $3, $4) returning *"#)
            .bind(job.queue())
            .bind(J::worker())
            .bind(args)
            .bind(job.max_attempts())
            .fetch_one(executor)
            .await?;

    let inserted = Job::from_row(&row)?;

    info!("Job inserted id={}, args={:?}", inserted.id, job);

    Ok(inserted)
}
