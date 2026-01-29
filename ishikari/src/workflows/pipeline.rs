//! Linear job pipelines.
//!
//! Pipelines allow you to chain jobs together in a sequence, where each job
//! depends on the previous one completing successfully.
//!
//! # Example
//!
//! ```rust,ignore
//! use ishikari_pro::Pipeline;
//!
//! // Define a pipeline that processes a video
//! let workflow = Pipeline::new("video-processing")
//!     .step(DownloadVideo { url: video_url })
//!     .step(TranscodeVideo { format: "mp4" })
//!     .step(UploadToCDN { bucket: "videos" })
//!     .run(&pool)
//!     .await?;
//! ```

use super::error::{Error, Result};
use super::workflow::Workflow;
use crate::dependencies::Dependencies;
use crate::{Job, Worker};
use serde::Serialize;
use sqlx::PgPool;
use std::fmt::Debug;
use tracing::info;

/// A step in a pipeline, holding a boxed job.
struct PipelineStep {
    /// The job to execute for this step.
    job: Box<dyn ErasedJob + Send + Sync>,
}

/// Trait for type-erased job insertion.
#[async_trait::async_trait]
trait ErasedJob: Debug + Send + Sync {
    /// Insert the job into the database and return the job ID.
    async fn insert_job(
        &self,
        pool: &PgPool,
        workflow_id: i64,
        schema: Option<&str>,
        has_dependencies: bool,
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
}

impl Debug for PipelineStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineStep")
            .field("job", &self.job)
            .finish()
    }
}

/// A builder for creating linear job pipelines.
///
/// Pipelines execute jobs in sequence, where each job waits for the
/// previous job to complete before starting.
#[derive(Debug)]
pub struct Pipeline {
    /// The name of the pipeline/workflow.
    name: String,
    /// The steps in the pipeline, in order.
    steps: Vec<PipelineStep>,
    /// Optional schema for multi-tenant support.
    schema: Option<String>,
    /// Optional metadata to attach to the workflow.
    metadata: serde_json::Value,
}

impl Pipeline {
    /// Create a new pipeline with the given name.
    ///
    /// # Arguments
    ///
    /// * `name` - A descriptive name for the pipeline (e.g., "video-processing")
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
            schema: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    /// Add a step to the pipeline.
    ///
    /// Steps are executed in the order they are added. Each step will wait
    /// for the previous step to complete before starting.
    ///
    /// # Arguments
    ///
    /// * `job` - The job to execute for this step
    pub fn step<J>(mut self, job: J) -> Self
    where
        J: Debug + Serialize + Worker + Send + Sync + Clone + 'static,
    {
        self.steps.push(PipelineStep { job: Box::new(job) });
        self
    }

    /// Set the schema for multi-tenant support.
    ///
    /// All jobs and the workflow will be created in this schema.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Attach metadata to the workflow.
    ///
    /// This metadata is stored with the workflow and can be retrieved later.
    pub fn metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    /// Execute the pipeline.
    ///
    /// This creates a workflow and inserts all jobs with their dependencies.
    /// The first job starts immediately, and each subsequent job waits for
    /// the previous one to complete.
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
            "starting pipeline"
        );

        // Insert all jobs and set up dependencies
        let mut prev_job_id: Option<i64> = None;

        for (i, step) in self.steps.into_iter().enumerate() {
            // First job has no dependencies, subsequent jobs depend on previous
            let has_dependencies = prev_job_id.is_some();

            // Insert the job
            let job = step
                .job
                .insert_job(pool, workflow.id, schema, has_dependencies)
                .await?;

            info!(
                workflow_id = workflow.id,
                job_id = job.id,
                step = i,
                "inserted pipeline step"
            );

            // If there's a previous job, add a dependency
            if let Some(prev_id) = prev_job_id {
                Dependencies::add(pool, job.id, prev_id, schema).await?;

                info!(
                    workflow_id = workflow.id,
                    job_id = job.id,
                    depends_on = prev_id,
                    "added dependency"
                );
            }

            prev_job_id = Some(job.id);
        }

        Ok(workflow)
    }
}

/// Insert a job with a workflow ID.
///
/// If `has_dependencies` is true, the job is inserted in 'scheduled' state
/// so it won't execute until its dependencies are resolved.
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
    let table_name = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    // Jobs with dependencies start in 'scheduled' state
    // The DependencyResolver will move them to 'available' when ready
    let state = if has_dependencies {
        "scheduled"
    } else {
        "available"
    };

    let enum_type = match schema {
        Some(s) => format!("{}.ishikari_job_state", s),
        None => "ishikari_job_state".to_string(),
    };

    let query = format!(
        r#"
        INSERT INTO {} (queue, worker, args, max_attempts, workflow_id, state)
        VALUES ($1, $2, $3, $4, $5, $6::{})
        RETURNING *
        "#,
        table_name, enum_type
    );

    let args =
        serde_json::to_value(&job as &dyn Worker).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;

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
    }

    fn dummy_step() -> PipelineStep {
        PipelineStep {
            job: Box::new(DummyJob),
        }
    }

    #[test]
    fn test_pipeline_new() {
        let pipeline = Pipeline::new("test-pipeline");

        assert_eq!(pipeline.name, "test-pipeline");
        assert!(pipeline.steps.is_empty());
        assert!(pipeline.schema.is_none());
    }

    #[test]
    fn test_pipeline_with_steps() {
        let pipeline = Pipeline {
            name: "multi-step".to_string(),
            steps: vec![dummy_step(), dummy_step(), dummy_step()],
            schema: None,
            metadata: serde_json::Value::Null,
        };

        assert_eq!(pipeline.steps.len(), 3);
    }

    #[test]
    fn test_pipeline_with_schema() {
        let pipeline = Pipeline {
            name: "tenant-pipeline".to_string(),
            steps: vec![dummy_step()],
            schema: Some("tenant_123".to_string()),
            metadata: serde_json::Value::Null,
        };

        assert_eq!(pipeline.schema.as_deref(), Some("tenant_123"));
    }

    #[test]
    fn test_pipeline_with_metadata() {
        let metadata = json!({
            "order_id": 12345,
            "priority": "high"
        });

        let pipeline = Pipeline {
            name: "order-processing".to_string(),
            steps: vec![dummy_step()],
            schema: None,
            metadata: metadata.clone(),
        };

        assert_eq!(pipeline.metadata, metadata);
    }

    #[test]
    fn test_pipeline_schema_method() {
        let pipeline = Pipeline::new("test").schema("my_schema");

        assert_eq!(pipeline.schema.as_deref(), Some("my_schema"));
    }

    #[test]
    fn test_pipeline_metadata_method() {
        let metadata = json!({"key": "value"});
        let pipeline = Pipeline::new("test").metadata(metadata.clone());

        assert_eq!(pipeline.metadata, metadata);
    }
}
