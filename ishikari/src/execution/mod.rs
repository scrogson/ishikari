//! Workflow execution engine with proper data flow.
//!
//! This module provides a robust execution model inspired by Flow:
//! - DAG-based execution with topological levels
//! - Input/output tracking between nodes
//! - Expression interpolation (`{{inputs.x}}`, `{{nodes.y.z}}`)
//! - Pre-execution validation
//! - Real-time event broadcasting
//!
//! # Example
//!
//! ```rust,ignore
//! use ishikari_pro::execution::{WorkflowExecutor, ExecutionEvent};
//!
//! let executor = WorkflowExecutor::new(pool.clone());
//!
//! // Subscribe to events
//! let mut rx = executor.subscribe();
//! tokio::spawn(async move {
//!     while let Ok(event) = rx.recv().await {
//!         println!("Event: {:?}", event);
//!     }
//! });
//!
//! // Execute a workflow definition
//! let execution_id = executor.execute(definition_id, inputs).await?;
//! ```

mod context;
mod dag;
mod events;
mod executor;
mod interpolation;
mod storage;
mod types;
mod validation;

pub use context::NodeContext;
pub use events::{ExecutionEvent, EventSender};
pub use executor::{ExecutionError, NodeError, NodeHandler, WorkflowExecutor};
pub use interpolation::{Interpolator, InterpolationError};
pub use storage::{ExecutionStorage, NodeExecutionRow, WorkflowRunRow};
pub use types::*;
pub use validation::{ValidationError, ValidationResult, Validator};
