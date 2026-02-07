//! Error types for Ishikari Pro.

use thiserror::Error;

/// Errors that can occur in Ishikari Pro operations.
#[derive(Debug, Error)]
pub enum Error {
    /// A database error occurred.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// A job dependency was not found.
    #[error("dependency not found: job {0}")]
    DependencyNotFound(i64),

    /// A circular dependency was detected.
    #[error("circular dependency detected in workflow")]
    CircularDependency,

    /// A workflow was not found.
    #[error("workflow not found: {0}")]
    WorkflowNotFound(i64),

    /// A workflow is in an invalid state for the requested operation.
    #[error("workflow {id} is in state {state}, expected {expected}")]
    InvalidWorkflowState {
        id: i64,
        state: String,
        expected: String,
    },

    /// A saga compensation failed.
    #[error("saga compensation failed for workflow {workflow_id}: {reason}")]
    CompensationFailed { workflow_id: i64, reason: String },

    /// Serialization/deserialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Result type for Ishikari Pro operations.
pub type Result<T> = std::result::Result<T, Error>;
