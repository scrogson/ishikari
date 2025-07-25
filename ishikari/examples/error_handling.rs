#![allow(unused_variables, clippy::uninlined_format_args)]

use ishikari::prelude::*;
use tracing::{error, info, instrument};

/// Example job that demonstrates various error handling patterns
#[ishikari::job]
pub struct ErrorHandlingJob {
    pub data: String,
    pub should_fail: bool,
    pub should_retry: bool,
}

#[ishikari::worker(queue = "error_demo", max_attempts = 5)]
impl Worker for ErrorHandlingJob {
    /// Custom backoff strategy for this job
    fn backoff(&self, attempt: i32) -> chrono::DateTime<chrono::Utc> {
        use chrono::{Duration, Utc};
        
        // Exponential backoff with jitter: 2^attempt seconds + random jitter
        let base_delay = 2_i64.pow(attempt as u32);
        let jitter = rand::random::<u64>() % 5; // 0-4 seconds of jitter
        
        Utc::now() + Duration::seconds(base_delay + jitter as i64)
    }

    #[instrument(skip(ctx))]
    async fn perform(&self, ctx: Context) -> PerformResult {
        let job = ctx.job();
        info!("Processing job {} with data: {}", job.id, self.data);

        // Simulate different error scenarios
        if self.should_fail && !self.should_retry {
            // This error will cause the job to be cancelled permanently
            return Cancel::default()
                .message("Job cancelled due to invalid data")
                .into();
        }

        if self.should_fail && self.should_retry {
            // This error will cause the job to be retried with backoff
            return Err("Temporary failure - will retry".into());
        }

        // Simulate some work that might fail
        match process_data(&self.data).await {
            Ok(result) => {
                info!("Successfully processed data: {}", result);
                Complete::default()
                    .message(format!("Processed: {}", result))
                    .into()
            }
            Err(e) if is_retryable_error(&e) => {
                error!("Retriable error occurred: {}", e);
                // Return error to trigger retry with backoff
                Err(e.into())
            }
            Err(e) => {
                error!("Non-retriable error occurred: {}", e);
                // Cancel the job permanently
                Cancel::default()
                    .message(format!("Permanent failure: {}", e))
                    .into()
            }
        }
    }
}

/// Example job that uses snooze for custom delay
#[ishikari::job]
pub struct SnoozeJob {
    pub retry_count: i32,
    pub max_retries: i32,
}

#[ishikari::worker(queue = "snooze_demo")]
impl Worker for SnoozeJob {
    #[instrument(skip(ctx))]
    async fn perform(&self, ctx: Context) -> PerformResult {
        let job = ctx.job();
        info!("Snooze job attempt {} of {}", self.retry_count, self.max_retries);

        if self.retry_count >= self.max_retries {
            Complete::default()
                .message("Max retries reached")
                .into()
        } else {
            // Snooze for 30 seconds before next attempt
            Snooze(30).into()
        }
    }
}

/// Simulate data processing that might fail
async fn process_data(data: &str) -> Result<String, anyhow::Error> {
    if data.is_empty() {
        return Err(anyhow::anyhow!("Empty data provided"));
    }
    
    if data.contains("poison") {
        return Err(anyhow::anyhow!("Poisoned data - permanent error"));
    }
    
    if data.contains("network") {
        return Err(anyhow::anyhow!("Network error - temporary"));
    }
    
    // Simulate processing
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    Ok(format!("Processed: {}", data.to_uppercase()))
}

/// Determine if an error should trigger a retry
fn is_retryable_error(error: &anyhow::Error) -> bool {
    let error_str = error.to_string().to_lowercase();
    
    // Network errors and timeouts are typically retryable
    error_str.contains("network") 
        || error_str.contains("timeout")
        || error_str.contains("connection")
        || error_str.contains("temporary")
}

#[allow(dead_code)]
fn main() {}