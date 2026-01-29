//! Directed Acyclic Graph (Dag) job orchestration.
//!
//! Dags allow complex job dependencies with fan-in and fan-out patterns.
//! Unlike pipelines (which are strictly linear), Dags can have jobs that
//! depend on multiple other jobs, and multiple jobs can depend on a single job.
//!
//! # Example
//!
//! ```rust,ignore
//! use ishikari_pro::Dag;
//!
//! // ETL pipeline with parallel extraction
//! let workflow = Dag::new("etl-pipeline")
//!     .add("extract_s3", ExtractFromS3 { bucket: "data" })
//!     .add("extract_api", ExtractFromAPI { endpoint: "..." })
//!     .add_after("transform", Transform { ... }, &["extract_s3", "extract_api"])
//!     .add_after("load", Load { ... }, &["transform"])
//!     .run(&pool)
//!     .await?;
//! ```

use super::error::{Error, Result};
use super::workflow::Workflow;
use crate::dependencies::Dependencies;
use crate::{Job, Worker};
use serde::Serialize;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use tracing::info;

/// A step in a Dag, holding a job and its dependencies.
struct DagStep {
    /// The name of this step (for referencing in dependencies).
    name: String,
    /// The job to execute for this step.
    job: Box<dyn ErasedJob + Send + Sync>,
    /// Names of steps this step depends on.
    depends_on: Vec<String>,
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

impl Debug for DagStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DagStep")
            .field("name", &self.name)
            .field("job", &self.job)
            .field("depends_on", &self.depends_on)
            .finish()
    }
}

/// A builder for creating Directed Acyclic Graph (Dag) job workflows.
///
/// Dags allow complex job dependencies where:
/// - Jobs can depend on multiple other jobs (fan-in)
/// - Multiple jobs can depend on a single job (fan-out)
/// - Jobs are identified by names within the Dag
///
/// # Example
///
/// ```rust,ignore
/// use ishikari_pro::Dag;
///
/// let workflow = Dag::new("etl")
///     .add("extract_s3", ExtractFromS3 { ... })
///     .add("extract_api", ExtractFromAPI { ... })
///     .add_after("transform", Transform { ... }, &["extract_s3", "extract_api"])
///     .add_after("load", Load { ... }, &["transform"])
///     .run(&pool)
///     .await?;
/// ```
#[derive(Debug)]
pub struct Dag {
    name: String,
    steps: Vec<DagStep>,
    schema: Option<String>,
    metadata: serde_json::Value,
}

impl Dag {
    /// Create a new Dag with the given name.
    ///
    /// # Arguments
    ///
    /// * `name` - A descriptive name for the Dag workflow
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
            schema: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    /// Add a step to the Dag with no dependencies.
    ///
    /// This step will start executing immediately when the Dag runs.
    ///
    /// # Arguments
    ///
    /// * `name` - A unique name for this step (used for dependency references)
    /// * `job` - The job to execute for this step
    pub fn add<J>(mut self, name: impl Into<String>, job: J) -> Self
    where
        J: Debug + Serialize + Worker + Send + Sync + Clone + 'static,
    {
        self.steps.push(DagStep {
            name: name.into(),
            job: Box::new(job),
            depends_on: Vec::new(),
        });
        self
    }

    /// Add a step to the Dag with dependencies.
    ///
    /// This step will not execute until all specified dependencies have completed.
    ///
    /// # Arguments
    ///
    /// * `name` - A unique name for this step (used for dependency references)
    /// * `job` - The job to execute for this step
    /// * `after` - Names of steps this step depends on
    pub fn add_after<J, I, S>(mut self, name: impl Into<String>, job: J, after: I) -> Self
    where
        J: Debug + Serialize + Worker + Send + Sync + Clone + 'static,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.steps.push(DagStep {
            name: name.into(),
            job: Box::new(job),
            depends_on: after.into_iter().map(|s| s.into()).collect(),
        });
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

    /// Validate the Dag has no cycles and all dependencies exist.
    fn validate(&self) -> Result<()> {
        let step_names: HashSet<&str> = self.steps.iter().map(|s| s.name.as_str()).collect();

        // Check for duplicate step names
        if step_names.len() != self.steps.len() {
            return Err(Error::InvalidWorkflowState {
                id: 0,
                state: "duplicate step names".to_string(),
                expected: "unique step names".to_string(),
            });
        }

        // Check all dependencies exist
        for step in &self.steps {
            for dep in &step.depends_on {
                if !step_names.contains(dep.as_str()) {
                    return Err(Error::DependencyNotFound(0));
                }
            }
        }

        // Check for cycles using Kahn's algorithm
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();

        for step in &self.steps {
            in_degree.entry(step.name.as_str()).or_insert(0);
            adjacency.entry(step.name.as_str()).or_default();

            for dep in &step.depends_on {
                *in_degree.entry(step.name.as_str()).or_insert(0) += 1;
                adjacency
                    .entry(dep.as_str())
                    .or_default()
                    .push(step.name.as_str());
            }
        }

        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(&name, _)| name)
            .collect();

        let mut visited = 0;

        while let Some(node) = queue.pop_front() {
            visited += 1;
            if let Some(neighbors) = adjacency.get(node) {
                for &neighbor in neighbors {
                    if let Some(deg) = in_degree.get_mut(neighbor) {
                        *deg -= 1;
                        if *deg == 0 {
                            queue.push_back(neighbor);
                        }
                    }
                }
            }
        }

        if visited != self.steps.len() {
            return Err(Error::CircularDependency);
        }

        Ok(())
    }

    /// Execute the Dag.
    ///
    /// This validates the Dag, creates a workflow, inserts all jobs with their
    /// dependencies, and returns the workflow.
    ///
    /// Jobs without dependencies will start immediately. Jobs with dependencies
    /// will be held in 'scheduled' state until the DependencyResolver releases them.
    ///
    /// # Returns
    ///
    /// Returns the created workflow on success.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Dag is empty
    /// - A dependency references a non-existent step
    /// - The Dag contains a cycle
    /// - Database operations fail
    pub async fn run(self, pool: &PgPool) -> Result<Workflow> {
        if self.steps.is_empty() {
            return Err(Error::InvalidWorkflowState {
                id: 0,
                state: "empty".to_string(),
                expected: "at least one step".to_string(),
            });
        }

        // Validate the Dag
        self.validate()?;

        let schema = self.schema.as_deref();

        // Create the workflow
        let workflow = Workflow::create(pool, &self.name, self.metadata, schema).await?;

        info!(
            workflow_id = workflow.id,
            name = %self.name,
            steps = self.steps.len(),
            "starting Dag"
        );

        // Insert all jobs and track their IDs by name
        let mut job_ids: HashMap<String, i64> = HashMap::new();

        for step in &self.steps {
            let has_dependencies = !step.depends_on.is_empty();

            // Insert the job
            let job = step
                .job
                .insert_job(pool, workflow.id, schema, has_dependencies)
                .await?;

            info!(
                workflow_id = workflow.id,
                job_id = job.id,
                step = %step.name,
                has_dependencies = has_dependencies,
                "inserted Dag step"
            );

            job_ids.insert(step.name.clone(), job.id);
        }

        // Create dependencies
        for step in &self.steps {
            let job_id = job_ids[&step.name];

            for dep_name in &step.depends_on {
                let dep_job_id = job_ids[dep_name];

                Dependencies::add(pool, job_id, dep_job_id, schema).await?;

                info!(
                    workflow_id = workflow.id,
                    job_id = job_id,
                    depends_on = dep_job_id,
                    step = %step.name,
                    dep_step = %dep_name,
                    "added dependency"
                );
            }
        }

        Ok(workflow)
    }
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

    #[test]
    fn test_cycle_detection() {
        // This test just validates the cycle detection logic
        // by manually creating DagSteps

        // Create a Dag with a cycle: A -> B -> C -> A
        let dag = Dag {
            name: "cyclic".to_string(),
            steps: vec![
                DagStep {
                    name: "a".to_string(),
                    job: Box::new(DummyJob),
                    depends_on: vec!["c".to_string()],
                },
                DagStep {
                    name: "b".to_string(),
                    job: Box::new(DummyJob),
                    depends_on: vec!["a".to_string()],
                },
                DagStep {
                    name: "c".to_string(),
                    job: Box::new(DummyJob),
                    depends_on: vec!["b".to_string()],
                },
            ],
            schema: None,
            metadata: serde_json::Value::Null,
        };

        assert!(matches!(dag.validate(), Err(Error::CircularDependency)));
    }

    #[test]
    fn test_missing_dependency() {
        let dag = Dag {
            name: "missing".to_string(),
            steps: vec![DagStep {
                name: "a".to_string(),
                job: Box::new(DummyJob),
                depends_on: vec!["nonexistent".to_string()],
            }],
            schema: None,
            metadata: serde_json::Value::Null,
        };

        assert!(matches!(dag.validate(), Err(Error::DependencyNotFound(_))));
    }

    #[test]
    fn test_valid_dag() {
        let dag = Dag {
            name: "valid".to_string(),
            steps: vec![
                DagStep {
                    name: "a".to_string(),
                    job: Box::new(DummyJob),
                    depends_on: vec![],
                },
                DagStep {
                    name: "b".to_string(),
                    job: Box::new(DummyJob),
                    depends_on: vec![],
                },
                DagStep {
                    name: "c".to_string(),
                    job: Box::new(DummyJob),
                    depends_on: vec!["a".to_string(), "b".to_string()],
                },
                DagStep {
                    name: "d".to_string(),
                    job: Box::new(DummyJob),
                    depends_on: vec!["c".to_string()],
                },
            ],
            schema: None,
            metadata: serde_json::Value::Null,
        };

        assert!(dag.validate().is_ok());
    }

    // Dummy job for testing
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
}
