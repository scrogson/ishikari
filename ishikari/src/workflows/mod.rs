//! Workflow orchestration patterns for complex job sequencing.
//!
//! This module provides three patterns for orchestrating job execution:
//!
//! - **Pipeline**: Linear job sequencing where each job waits for the previous
//! - **DAG**: Directed Acyclic Graph for complex parallel execution with fan-in/fan-out
//! - **Saga**: Distributed transactions with compensation on failure
//!
//! # Example: Pipeline
//!
//! ```rust,ignore
//! use ishikari::workflows::Pipeline;
//!
//! let pipeline = Pipeline::builder("video-processing")
//!     .job(DownloadJob { url })
//!     .job(TranscodeJob { format: "mp4" })
//!     .job(UploadJob { destination })
//!     .build(&pool)
//!     .await?;
//! ```
//!
//! # Example: DAG
//!
//! ```rust,ignore
//! use ishikari::workflows::DAG;
//!
//! let dag = DAG::builder("etl-pipeline")
//!     .job("extract_users", ExtractUsersJob)
//!     .job("extract_orders", ExtractOrdersJob)
//!     .job("transform", TransformJob).depends_on(&["extract_users", "extract_orders"])
//!     .job("load", LoadJob).depends_on(&["transform"])
//!     .build(&pool)
//!     .await?;
//! ```
//!
//! # Example: Saga
//!
//! ```rust,ignore
//! use ishikari::workflows::Saga;
//!
//! let saga = Saga::builder("order-booking")
//!     .step(BookFlightJob, Some(CancelFlightJob))
//!     .step(BookHotelJob, Some(CancelHotelJob))
//!     .step(BookCarJob, Some(CancelCarJob))
//!     .build(&pool)
//!     .await?;
//! ```

mod dag;
pub mod error;
mod pipeline;
mod saga;
mod workflow;

pub use dag::Dag;
pub use error::{Error, Result};
pub use pipeline::Pipeline;
pub use saga::{Saga, SagaStepBuilder};
pub use workflow::{Workflow, WorkflowState};
