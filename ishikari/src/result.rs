//! Job execution results and status types.
//!
//! This module provides types for representing the various outcomes of job execution.
//! Jobs can complete successfully, be cancelled, or be snoozed for later execution.
//!
//! # Examples
//!
//! ```rust
//! use ishikari::{Complete, Cancel, Snooze, Status};
//!
//! // Complete a job with a success message
//! let status = Complete::default()
//!     .message("Job processed successfully");
//!
//! // Cancel a job with a reason
//! let status = Cancel::default()
//!     .message("Job cancelled due to invalid data");
//!
//! // Snooze a job for 5 minutes
//! let status = Snooze(300);
//! ```

/// Represents a successfully completed job.
///
/// This type is used to indicate that a job has completed its execution successfully.
/// It can optionally include a message describing the completion.
///
/// # Examples
///
/// ```rust
/// use ishikari::Complete;
///
/// // Complete without a message
/// let complete = Complete::default();
///
/// // Complete with a message
/// let complete = Complete::default()
///     .message("Successfully processed 100 records");
/// ```
#[derive(Debug, Default)]
pub struct Complete(pub Option<String>);

/// Represents a cancelled job.
///
/// This type is used to indicate that a job has been cancelled before completion.
/// It can optionally include a message explaining why the job was cancelled.
///
/// # Examples
///
/// ```rust
/// use ishikari::Cancel;
///
/// // Cancel without a reason
/// let cancel = Cancel::default();
///
/// // Cancel with a reason
/// let cancel = Cancel::default()
///     .message("Cancelled due to invalid input data");
/// ```
#[derive(Debug, Default)]
pub struct Cancel(pub Option<String>);

/// Represents a job that should be executed later.
///
/// This type is used to indicate that a job should be rescheduled for later execution.
/// The contained value represents the number of seconds to wait before retrying.
///
/// # Examples
///
/// ```rust
/// use ishikari::Snooze;
///
/// // Snooze for 5 minutes
/// let snooze = Snooze(300);
///
/// // Snooze for 1 hour
/// let snooze = Snooze(3600);
/// ```
#[derive(Debug)]
pub struct Snooze(pub u64);

/// Represents the final status of a job execution.
///
/// This enum encapsulates all possible outcomes of a job execution:
/// - `Complete`: The job finished successfully
/// - `Cancel`: The job was cancelled
/// - `Snooze`: The job should be retried later
///
/// # Examples
///
/// ```rust
/// use ishikari::{Status, Complete, Cancel, Snooze};
///
/// // Complete a job
/// let status = Status::Complete(Complete::default());
///
/// // Cancel a job
/// let status = Status::Cancel(Cancel::default());
///
/// // Snooze a job
/// let status = Status::Snooze(Snooze(300));
/// ```
pub enum Status {
    /// The job completed successfully.
    Complete(Complete),
    /// The job was cancelled.
    Cancel(Cancel),
    /// The job should be retried later.
    Snooze(Snooze),
}

/// The error type returned by job execution.
///
/// This is a boxed error that can be sent between threads and shared safely.
/// It's recommended to use `anyhow::Error` for job errors as it implements
/// the required traits automatically.
pub type PerformError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The result type returned by job execution.
///
/// This type represents either a successful job status or an error that occurred
/// during execution.
pub type PerformResult = std::result::Result<Status, PerformError>;

impl From<Complete> for Status {
    fn from(s: Complete) -> Self {
        Self::Complete(s)
    }
}

impl From<Cancel> for Status {
    fn from(s: Cancel) -> Self {
        Self::Cancel(s)
    }
}

impl From<Snooze> for Status {
    fn from(s: Snooze) -> Self {
        Self::Snooze(s)
    }
}

impl Complete {
    /// Adds a message to the completion status.
    ///
    /// # Arguments
    ///
    /// * `message` - A message describing the completion. This can be any type that
    ///   implements `ToString`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ishikari::Complete;
    ///
    /// let complete = Complete::default()
    ///     .message("Processed 100 records successfully");
    /// ```
    pub fn message(mut self, message: impl ToString) -> Self {
        self.0 = Some(message.to_string());
        self
    }
}

impl Cancel {
    /// Adds a reason for the cancellation.
    ///
    /// # Arguments
    ///
    /// * `message` - A message explaining why the job was cancelled. This can be any
    ///   type that implements `ToString`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use ishikari::Cancel;
    ///
    /// let cancel = Cancel::default()
    ///     .message("Cancelled due to invalid input data");
    /// ```
    pub fn message(mut self, message: impl ToString) -> Self {
        self.0 = Some(message.to_string());
        self
    }
}

impl From<Complete> for PerformResult {
    fn from(complete: Complete) -> Self {
        Ok(Status::Complete(complete))
    }
}

impl From<Cancel> for PerformResult {
    fn from(cancel: Cancel) -> Self {
        Ok(Status::Cancel(cancel))
    }
}

impl From<Snooze> for PerformResult {
    fn from(snooze: Snooze) -> Self {
        Ok(Status::Snooze(snooze))
    }
}
