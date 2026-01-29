//! Saga pattern for distributed transactions with compensation.
//!
//! Sagas provide a way to maintain data consistency across multiple steps
//! where each step can be compensated (rolled back) if a later step fails.
//!
//! # How it works
//!
//! 1. Steps execute in order, each waiting for the previous to complete
//! 2. If a step fails, compensation jobs run in reverse order
//! 3. Only completed steps are compensated (not the failed step)
//!
//! # Example
//!
//! ```rust,ignore
//! use ishikari_pro::Saga;
//!
//! // Book a trip - if any step fails, previous steps are rolled back
//! let workflow = Saga::new("book-trip")
//!     .step(BookFlight { flight_id: "UA123" })
//!         .compensate(CancelFlight { flight_id: "UA123" })
//!     .step(BookHotel { hotel_id: "H456" })
//!         .compensate(CancelHotel { hotel_id: "H456" })
//!     .step(BookCar { car_id: "C789" })
//!         .compensate(CancelCar { car_id: "C789" })
//!     .run(&pool)
//!     .await?;
//!
//! // If BookCar fails:
//! // 1. CancelHotel runs
//! // 2. CancelFlight runs
//! // 3. Workflow marked as "compensated"
//! ```

use super::error::{Error, Result};
use super::workflow::Workflow;
use crate::dependencies::Dependencies;
use crate::{Job, Worker};
use serde::Serialize;
use sqlx::PgPool;
use std::fmt::Debug;
use tracing::info;

/// A step in a saga with optional compensation.
struct SagaStep {
    /// The forward job to execute.
    forward: Box<dyn ErasedJob + Send + Sync>,
    /// The compensation job to run on rollback (optional).
    compensation: Option<Box<dyn ErasedJob + Send + Sync>>,
}

/// Trait for type-erased job insertion.
#[async_trait::async_trait]
trait ErasedJob: Debug + Send + Sync {
    /// Insert the job into the database and return the job.
    async fn insert_job(
        &self,
        pool: &PgPool,
        workflow_id: i64,
        schema: Option<&str>,
        has_dependencies: bool,
    ) -> std::result::Result<Job, sqlx::Error>;

    /// Insert a compensation job into the database.
    /// Compensation jobs are scheduled far in the future so they won't be
    /// automatically picked up by the Stager.
    async fn insert_as_compensation(
        &self,
        pool: &PgPool,
        workflow_id: i64,
        schema: Option<&str>,
    ) -> std::result::Result<Job, sqlx::Error>;
}

#[async_trait::async_trait]
impl<J> ErasedJob for J
where
    J: Debug + Serialize + Worker + Send + Sync + Clone + 'static,
{
    async fn insert_job(
        &self,
        pool: &PgPool,
        workflow_id: i64,
        schema: Option<&str>,
        has_dependencies: bool,
    ) -> std::result::Result<Job, sqlx::Error> {
        insert_job_with_workflow(pool, self.clone(), workflow_id, schema, has_dependencies).await
    }

    async fn insert_as_compensation(
        &self,
        pool: &PgPool,
        workflow_id: i64,
        schema: Option<&str>,
    ) -> std::result::Result<Job, sqlx::Error> {
        insert_compensation_job(pool, self.clone(), workflow_id, schema).await
    }
}

impl Debug for SagaStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SagaStep")
            .field("forward", &self.forward)
            .field("has_compensation", &self.compensation.is_some())
            .finish()
    }
}

/// Builder for adding a compensation to the current step.
#[must_use = "SagaStepBuilder does nothing until run() is called"]
pub struct SagaStepBuilder {
    saga: Saga,
    forward: Box<dyn ErasedJob + Send + Sync>,
}

impl SagaStepBuilder {
    /// Add a compensation job for this step.
    ///
    /// The compensation job will run if a later step fails.
    pub fn compensate<J>(mut self, job: J) -> Saga
    where
        J: Debug + Serialize + Worker + Send + Sync + Clone + 'static,
    {
        self.saga.steps.push(SagaStep {
            forward: self.forward,
            compensation: Some(Box::new(job)),
        });
        self.saga
    }

    /// Continue without a compensation job for this step.
    ///
    /// Use this when the step doesn't need rollback (e.g., read-only operations).
    pub fn no_compensate(mut self) -> Saga {
        self.saga.steps.push(SagaStep {
            forward: self.forward,
            compensation: None,
        });
        self.saga
    }

    /// Add another step to the saga.
    ///
    /// The current step will have no compensation.
    pub fn step<J>(self, job: J) -> SagaStepBuilder
    where
        J: Debug + Serialize + Worker + Send + Sync + Clone + 'static,
    {
        let mut saga = self.no_compensate();
        saga.step_impl(job)
    }

    /// Set the schema for multi-tenant support.
    pub fn schema(self, schema: impl Into<String>) -> Saga {
        let mut saga = self.no_compensate();
        saga.schema = Some(schema.into());
        saga
    }

    /// Execute the saga.
    pub async fn run(self, pool: &PgPool) -> Result<Workflow> {
        self.no_compensate().run(pool).await
    }
}

/// A builder for creating Saga workflows with compensation.
///
/// Sagas execute steps in sequence. If any step fails, compensation
/// jobs run in reverse order for all previously completed steps.
///
/// # Example
///
/// ```rust,ignore
/// use ishikari_pro::Saga;
///
/// let workflow = Saga::new("transfer-funds")
///     .step(DebitAccount { account: "A", amount: 100 })
///         .compensate(CreditAccount { account: "A", amount: 100 })
///     .step(CreditAccount { account: "B", amount: 100 })
///         .compensate(DebitAccount { account: "B", amount: 100 })
///     .run(&pool)
///     .await?;
/// ```
#[derive(Debug)]
pub struct Saga {
    name: String,
    steps: Vec<SagaStep>,
    schema: Option<String>,
    metadata: serde_json::Value,
}

impl Saga {
    /// Create a new saga with the given name.
    ///
    /// # Arguments
    ///
    /// * `name` - A descriptive name for the saga workflow
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
            schema: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    /// Add a step to the saga.
    ///
    /// Returns a builder that allows specifying a compensation job.
    ///
    /// # Arguments
    ///
    /// * `job` - The forward job to execute
    pub fn step<J>(mut self, job: J) -> SagaStepBuilder
    where
        J: Debug + Serialize + Worker + Send + Sync + Clone + 'static,
    {
        self.step_impl(job)
    }

    fn step_impl<J>(&mut self, job: J) -> SagaStepBuilder
    where
        J: Debug + Serialize + Worker + Send + Sync + Clone + 'static,
    {
        SagaStepBuilder {
            saga: std::mem::replace(
                self,
                Saga {
                    name: String::new(),
                    steps: Vec::new(),
                    schema: None,
                    metadata: serde_json::Value::Null,
                },
            ),
            forward: Box::new(job),
        }
    }

    /// Set the schema for multi-tenant support.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Attach metadata to the workflow.
    pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    /// Execute the saga.
    ///
    /// This creates a workflow, inserts all forward jobs with dependencies,
    /// and records compensation jobs for potential rollback.
    ///
    /// The first step starts immediately. Each subsequent step waits for
    /// the previous to complete. If any step fails, the DependencyResolver
    /// will trigger compensation jobs in reverse order.
    ///
    /// # Returns
    ///
    /// Returns the created workflow on success.
    pub async fn run(self, pool: &PgPool) -> Result<Workflow> {
        if self.steps.is_empty() {
            return Err(Error::InvalidWorkflowState {
                id: 0,
                state: "empty".to_string(),
                expected: "at least one step".to_string(),
            });
        }

        let schema = self.schema.as_deref();

        // Create the workflow
        let workflow = Workflow::create(pool, &self.name, self.metadata, schema).await?;

        info!(
            workflow_id = workflow.id,
            name = %self.name,
            steps = self.steps.len(),
            "starting saga"
        );

        // Insert all forward jobs and track saga steps
        let mut prev_job_id: Option<i64> = None;

        for (step_order, step) in self.steps.iter().enumerate() {
            let has_dependencies = prev_job_id.is_some();

            // Insert the forward job
            let forward_job = step
                .forward
                .insert_job(pool, workflow.id, schema, has_dependencies)
                .await?;

            info!(
                workflow_id = workflow.id,
                job_id = forward_job.id,
                step = step_order,
                "inserted saga forward step"
            );

            // If there's a previous job, add a dependency
            if let Some(prev_id) = prev_job_id {
                Dependencies::add(pool, forward_job.id, prev_id, schema).await?;
            }

            // Insert the compensation job (if any) scheduled far in the future.
            // It will only run if triggered by the resolver during rollback.
            let compensation_job_id = if let Some(ref comp) = step.compensation {
                // Compensation jobs are scheduled to year 3000 so they won't be
                // picked up by the Stager. Only released explicitly during rollback.
                let comp_job = comp.insert_as_compensation(pool, workflow.id, schema).await?;

                info!(
                    workflow_id = workflow.id,
                    job_id = comp_job.id,
                    step = step_order,
                    "inserted saga compensation step"
                );

                Some(comp_job.id)
            } else {
                None
            };

            // Record the saga step
            insert_saga_step(
                pool,
                workflow.id,
                forward_job.id,
                compensation_job_id,
                step_order as i32,
                schema,
            )
            .await?;

            prev_job_id = Some(forward_job.id);
        }

        Ok(workflow)
    }
}

/// Insert a saga step record.
async fn insert_saga_step(
    pool: &PgPool,
    workflow_id: i64,
    job_id: i64,
    compensation_job_id: Option<i64>,
    step_order: i32,
    schema: Option<&str>,
) -> Result<()> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_saga_steps", s),
        None => "ishikari_saga_steps".to_string(),
    };

    let query = format!(
        r#"
        INSERT INTO {} (workflow_id, job_id, compensation_job_id, step_order, state)
        VALUES ($1, $2, $3, $4, 'pending')
        "#,
        table
    );

    sqlx::query(&query)
        .bind(workflow_id)
        .bind(job_id)
        .bind(compensation_job_id)
        .bind(step_order)
        .execute(pool)
        .await?;

    Ok(())
}

/// Insert a job with a workflow ID.
async fn insert_job_with_workflow<J>(
    pool: &PgPool,
    job: J,
    workflow_id: i64,
    schema: Option<&str>,
    has_dependencies: bool,
) -> std::result::Result<Job, sqlx::Error>
where
    J: Debug + Serialize + Worker + Send + Sync + 'static,
{
    insert_job_impl(pool, job, workflow_id, schema, has_dependencies, false).await
}

/// Insert a compensation job with a workflow ID.
/// Compensation jobs are scheduled far in the future so they won't be picked up
/// by the Stager. They're only released explicitly during saga compensation.
async fn insert_compensation_job<J>(
    pool: &PgPool,
    job: J,
    workflow_id: i64,
    schema: Option<&str>,
) -> std::result::Result<Job, sqlx::Error>
where
    J: Debug + Serialize + Worker + Send + Sync + 'static,
{
    insert_job_impl(pool, job, workflow_id, schema, true, true).await
}

/// Internal job insertion with compensation flag.
async fn insert_job_impl<J>(
    pool: &PgPool,
    job: J,
    workflow_id: i64,
    schema: Option<&str>,
    has_dependencies: bool,
    is_compensation: bool,
) -> std::result::Result<Job, sqlx::Error>
where
    J: Debug + Serialize + Worker + Send + Sync + 'static,
{
    let table_name = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let state = if has_dependencies {
        "scheduled"
    } else {
        "available"
    };

    let enum_type = match schema {
        Some(s) => format!("{}.ishikari_job_state", s),
        None => "ishikari_job_state".to_string(),
    };

    let args =
        serde_json::to_value(&job as &dyn Worker).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;

    // Compensation jobs are scheduled far in the future (year 3000) so they won't
    // be picked up by the Stager. They're only released explicitly during rollback.
    if is_compensation {
        let query = format!(
            r#"
            INSERT INTO {} (queue, worker, args, max_attempts, workflow_id, state, scheduled_at)
            VALUES ($1, $2, $3, $4, $5, $6::{}, '3000-01-01'::timestamptz)
            RETURNING *
            "#,
            table_name, enum_type
        );

        sqlx::query_as::<_, Job>(&query)
            .bind(job.queue())
            .bind(J::worker())
            .bind(args)
            .bind(job.max_attempts())
            .bind(workflow_id)
            .bind(state)
            .fetch_one(pool)
            .await
    } else {
        let query = format!(
            r#"
            INSERT INTO {} (queue, worker, args, max_attempts, workflow_id, state)
            VALUES ($1, $2, $3, $4, $5, $6::{})
            RETURNING *
            "#,
            table_name, enum_type
        );

        sqlx::query_as::<_, Job>(&query)
            .bind(job.queue())
            .bind(J::worker())
            .bind(args)
            .bind(job.max_attempts())
            .bind(workflow_id)
            .bind(state)
            .fetch_one(pool)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Dummy job for testing - only implements ErasedJob directly
    #[derive(Debug, Clone)]
    struct DummyJob;

    #[async_trait::async_trait]
    impl ErasedJob for DummyJob {
        async fn insert_job(
            &self,
            _pool: &PgPool,
            _workflow_id: i64,
            _schema: Option<&str>,
            _has_dependencies: bool,
        ) -> std::result::Result<Job, sqlx::Error> {
            unimplemented!("DummyJob is only for validation tests")
        }

        async fn insert_as_compensation(
            &self,
            _pool: &PgPool,
            _workflow_id: i64,
            _schema: Option<&str>,
        ) -> std::result::Result<Job, sqlx::Error> {
            unimplemented!("DummyJob is only for validation tests")
        }
    }

    fn step_with_compensation() -> SagaStep {
        SagaStep {
            forward: Box::new(DummyJob),
            compensation: Some(Box::new(DummyJob)),
        }
    }

    fn step_without_compensation() -> SagaStep {
        SagaStep {
            forward: Box::new(DummyJob),
            compensation: None,
        }
    }

    #[test]
    fn test_saga_new() {
        let saga = Saga::new("test-saga");

        assert_eq!(saga.name, "test-saga");
        assert!(saga.steps.is_empty());
        assert!(saga.schema.is_none());
    }

    #[test]
    fn test_saga_step_with_compensation() {
        let saga = Saga {
            name: "booking".to_string(),
            steps: vec![step_with_compensation()],
            schema: None,
            metadata: serde_json::Value::Null,
        };

        assert_eq!(saga.steps.len(), 1);
        assert!(saga.steps[0].compensation.is_some());
    }

    #[test]
    fn test_saga_step_without_compensation() {
        let saga = Saga {
            name: "notification".to_string(),
            steps: vec![step_without_compensation()],
            schema: None,
            metadata: serde_json::Value::Null,
        };

        assert_eq!(saga.steps.len(), 1);
        assert!(saga.steps[0].compensation.is_none());
    }

    #[test]
    fn test_saga_multiple_steps_mixed_compensation() {
        let saga = Saga {
            name: "trip-booking".to_string(),
            steps: vec![
                step_with_compensation(),    // flight - has compensation
                step_with_compensation(),    // hotel - has compensation
                step_without_compensation(), // notification - no compensation
            ],
            schema: None,
            metadata: serde_json::Value::Null,
        };

        assert_eq!(saga.steps.len(), 3);
        assert!(saga.steps[0].compensation.is_some());
        assert!(saga.steps[1].compensation.is_some());
        assert!(saga.steps[2].compensation.is_none());
    }

    #[test]
    fn test_saga_with_schema() {
        let saga = Saga {
            name: "tenant-saga".to_string(),
            steps: vec![step_with_compensation()],
            schema: Some("tenant_456".to_string()),
            metadata: serde_json::Value::Null,
        };

        assert_eq!(saga.schema.as_deref(), Some("tenant_456"));
    }

    #[test]
    fn test_saga_with_metadata() {
        let metadata = json!({
            "booking_ref": "ABC123",
            "customer": "john@example.com"
        });

        let saga = Saga {
            name: "booking".to_string(),
            steps: vec![step_with_compensation()],
            schema: None,
            metadata: metadata.clone(),
        };

        assert_eq!(saga.metadata, metadata);
    }

    #[test]
    fn test_saga_schema_method() {
        let saga = Saga::new("test").schema("my_schema");

        assert_eq!(saga.schema.as_deref(), Some("my_schema"));
    }

    #[test]
    fn test_saga_metadata_method() {
        let metadata = json!({"key": "value"});
        let saga = Saga::new("test").metadata(metadata.clone());

        assert_eq!(saga.metadata, metadata);
    }

    #[test]
    fn test_saga_full_construction() {
        let metadata = json!({"order_id": 123});
        let saga = Saga {
            name: "full-saga".to_string(),
            steps: vec![step_with_compensation(), step_without_compensation()],
            schema: Some("tenant_1".to_string()),
            metadata,
        };

        assert_eq!(saga.name, "full-saga");
        assert_eq!(saga.steps.len(), 2);
        assert_eq!(saga.schema.as_deref(), Some("tenant_1"));
        assert_eq!(saga.metadata, json!({"order_id": 123}));
    }
}
