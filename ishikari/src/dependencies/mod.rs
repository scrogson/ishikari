//! Job dependency management and resolution.
//!
//! This module provides job-level dependency tracking and automatic resolution.
//! When a job completes, dependent jobs are automatically released for execution.
//!
//! # DependencyResolver
//!
//! The `DependencyResolver` is a background service that monitors completed jobs
//! and releases their dependents:
//!
//! ```rust,ignore
//! use ishikari::dependencies::DependencyResolver;
//! use std::time::Duration;
//!
//! // Start the resolver
//! let resolver = DependencyResolver::new(pool.clone(), Duration::from_millis(500))
//!     .start();
//!
//! // The resolver runs in the background, monitoring job completions
//! // and releasing dependent jobs automatically.
//!
//! // To stop the resolver
//! resolver.abort();
//! ```
//!
//! # Dependency States
//!
//! - **pending**: Waiting for upstream job to complete
//! - **satisfied**: Upstream job completed successfully
//! - **failed**: Upstream job failed, this dependency cannot be satisfied

mod dependency;
mod resolver;

pub use dependency::{Dependencies, DependencyState};
pub use resolver::DependencyResolver;
