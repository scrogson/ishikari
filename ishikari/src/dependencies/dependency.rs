//! Job dependency tracking.
//!
//! This module provides the foundation for job dependencies, allowing jobs
//! to wait for other jobs to complete before executing.

use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// The state of a job dependency.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyState {
    /// The dependency is pending (upstream job not yet complete).
    #[default]
    Pending,
    /// The dependency is satisfied (upstream job completed successfully).
    Satisfied,
    /// The dependency failed (upstream job failed or was cancelled).
    Failed,
}

impl DependencyState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Satisfied => "satisfied",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Self::Pending),
            "satisfied" => Some(Self::Satisfied),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

/// A dependency relationship between two jobs.
#[derive(Debug, Clone)]
pub struct Dependency {
    /// The job that has the dependency.
    pub job_id: i64,
    /// The job that must complete first.
    pub depends_on_job_id: i64,
    /// Current state of this dependency.
    pub state: DependencyState,
}

/// Operations for managing job dependencies.
pub struct Dependencies;

impl Dependencies {
    /// Add a dependency: `job_id` depends on `depends_on_job_id`.
    ///
    /// The job will not execute until all its dependencies are satisfied.
    pub async fn add(
        pool: &PgPool,
        job_id: i64,
        depends_on_job_id: i64,
        schema: Option<&str>,
    ) -> crate::workflows::error::Result<()> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_job_dependencies", s),
            None => "ishikari_job_dependencies".to_string(),
        };

        let query = format!(
            "INSERT INTO {} (job_id, depends_on_job_id, state) VALUES ($1, $2, 'pending')",
            table
        );

        sqlx::query(&query)
            .bind(job_id)
            .bind(depends_on_job_id)
            .execute(pool)
            .await?;

        Ok(())
    }

    /// Get all dependencies for a job.
    pub async fn for_job(
        pool: &PgPool,
        job_id: i64,
        schema: Option<&str>,
    ) -> crate::workflows::error::Result<Vec<Dependency>> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_job_dependencies", s),
            None => "ishikari_job_dependencies".to_string(),
        };

        let query = format!(
            "SELECT job_id, depends_on_job_id, state FROM {} WHERE job_id = $1",
            table
        );

        let rows: Vec<(i64, i64, String)> =
            sqlx::query_as(&query).bind(job_id).fetch_all(pool).await?;

        let deps = rows
            .into_iter()
            .map(|(job_id, depends_on_job_id, state)| Dependency {
                job_id,
                depends_on_job_id,
                state: DependencyState::parse(&state).unwrap_or_default(),
            })
            .collect();

        Ok(deps)
    }

    /// Check if all dependencies for a job are satisfied.
    pub async fn all_satisfied(
        pool: &PgPool,
        job_id: i64,
        schema: Option<&str>,
    ) -> crate::workflows::error::Result<bool> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_job_dependencies", s),
            None => "ishikari_job_dependencies".to_string(),
        };

        let query = format!(
            "SELECT COUNT(*) as count FROM {} WHERE job_id = $1 AND state != 'satisfied'",
            table
        );

        let count: (i64,) = sqlx::query_as(&query).bind(job_id).fetch_one(pool).await?;

        Ok(count.0 == 0)
    }

    /// Mark a dependency as satisfied.
    pub async fn mark_satisfied(
        pool: &PgPool,
        job_id: i64,
        depends_on_job_id: i64,
        schema: Option<&str>,
    ) -> crate::workflows::error::Result<()> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_job_dependencies", s),
            None => "ishikari_job_dependencies".to_string(),
        };

        let query = format!(
            "UPDATE {} SET state = 'satisfied' WHERE job_id = $1 AND depends_on_job_id = $2",
            table
        );

        sqlx::query(&query)
            .bind(job_id)
            .bind(depends_on_job_id)
            .execute(pool)
            .await?;

        Ok(())
    }

    /// Mark a dependency as failed.
    pub async fn mark_failed(
        pool: &PgPool,
        job_id: i64,
        depends_on_job_id: i64,
        schema: Option<&str>,
    ) -> crate::workflows::error::Result<()> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_job_dependencies", s),
            None => "ishikari_job_dependencies".to_string(),
        };

        let query = format!(
            "UPDATE {} SET state = 'failed' WHERE job_id = $1 AND depends_on_job_id = $2",
            table
        );

        sqlx::query(&query)
            .bind(job_id)
            .bind(depends_on_job_id)
            .execute(pool)
            .await?;

        Ok(())
    }

    /// Get all jobs that are waiting on a specific job to complete.
    pub async fn dependents(
        pool: &PgPool,
        job_id: i64,
        schema: Option<&str>,
    ) -> crate::workflows::error::Result<Vec<i64>> {
        let table = match schema {
            Some(s) => format!("{}.ishikari_job_dependencies", s),
            None => "ishikari_job_dependencies".to_string(),
        };

        let query = format!("SELECT job_id FROM {} WHERE depends_on_job_id = $1", table);

        let rows: Vec<(i64,)> = sqlx::query_as(&query).bind(job_id).fetch_all(pool).await?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}
