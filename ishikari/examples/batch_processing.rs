#![allow(
    unused_variables,
    clippy::uninlined_format_args,
    clippy::manual_div_ceil
)]

use ishikari::prelude::*;
use tracing::{info, instrument};

/// Example of batch processing with job spawning
#[ishikari::job]
pub struct BatchProcessorJob {
    pub batch_id: String,
    pub items: Vec<String>,
}

#[ishikari::worker(queue = "batch_processor", max_attempts = 3)]
impl Worker for BatchProcessorJob {
    #[instrument(skip(ctx))]
    async fn perform(&self, ctx: Context) -> PerformResult {
        info!(
            "Processing batch {} with {} items",
            self.batch_id,
            self.items.len()
        );

        // For demonstration, we'll process items in smaller batches
        const BATCH_SIZE: usize = 10;

        for (chunk_index, chunk) in self.items.chunks(BATCH_SIZE).enumerate() {
            // Create a sub-job for each chunk
            let chunk_job = ItemProcessorJob {
                batch_id: self.batch_id.clone(),
                chunk_index,
                items: chunk.to_vec(),
            };

            // In a real implementation, you would insert the job here
            // ishikari::insert(chunk_job, &pool).await?;

            info!(
                "Would spawn chunk {} with {} items",
                chunk_index,
                chunk.len()
            );
        }

        Complete::default()
            .message(format!(
                "Batch {} split into {} chunks",
                self.batch_id,
                (self.items.len() + BATCH_SIZE - 1) / BATCH_SIZE
            ))
            .into()
    }
}

/// Example of processing individual items in a batch
#[ishikari::job]
pub struct ItemProcessorJob {
    pub batch_id: String,
    pub chunk_index: usize,
    pub items: Vec<String>,
}

#[ishikari::worker(queue = "item_processor", max_attempts = 5)]
impl Worker for ItemProcessorJob {
    #[instrument(skip(ctx))]
    async fn perform(&self, ctx: Context) -> PerformResult {
        info!(
            "Processing chunk {} of batch {} with {} items",
            self.chunk_index,
            self.batch_id,
            self.items.len()
        );

        let mut processed_count = 0;
        let mut failed_items = Vec::new();

        for item in &self.items {
            match process_item(item).await {
                Ok(_) => {
                    processed_count += 1;
                    info!("Successfully processed item: {}", item);
                }
                Err(e) => {
                    failed_items.push(item.clone());
                    info!("Failed to process item {}: {}", item, e);
                }
            }
        }

        if failed_items.is_empty() {
            Complete::default()
                .message(format!("Processed {} items successfully", processed_count))
                .into()
        } else if failed_items.len() < self.items.len() {
            // Partial success - could create a retry job for failed items
            Complete::default()
                .message(format!(
                    "Processed {}/{} items, {} failed",
                    processed_count,
                    self.items.len(),
                    failed_items.len()
                ))
                .into()
        } else {
            // All items failed - retry the whole chunk
            Err(format!("All {} items failed processing", self.items.len()).into())
        }
    }
}

/// Example of a cleanup job that runs after batch processing
#[ishikari::job]
pub struct BatchCleanupJob {
    pub batch_id: String,
    pub total_items: usize,
}

#[ishikari::worker(queue = "cleanup", max_attempts = 2)]
impl Worker for BatchCleanupJob {
    #[instrument(skip(ctx))]
    async fn perform(&self, ctx: Context) -> PerformResult {
        info!(
            "Cleaning up batch {} (processed {} items)",
            self.batch_id, self.total_items
        );

        // Simulate cleanup operations
        cleanup_temp_files(&self.batch_id).await?;
        update_batch_status(&self.batch_id, "completed").await?;
        send_completion_notification(&self.batch_id).await?;

        Complete::default()
            .message(format!("Batch {} cleanup completed", self.batch_id))
            .into()
    }
}

/// Example of a scheduled job that processes batches periodically
#[ishikari::job]
pub struct PeriodicBatchJob {
    pub schedule_time: chrono::DateTime<chrono::Utc>,
}

#[ishikari::worker(queue = "scheduled", max_attempts = 2)]
impl Worker for PeriodicBatchJob {
    #[instrument(skip(ctx))]
    async fn perform(&self, ctx: Context) -> PerformResult {
        info!(
            "Running periodic batch job scheduled for {}",
            self.schedule_time
        );

        // Find pending batches to process
        let pending_batches = find_pending_batches().await?;

        for batch_id in pending_batches {
            let items = fetch_batch_items(&batch_id).await?;

            let batch_job = BatchProcessorJob {
                batch_id: batch_id.clone(),
                items,
            };

            // In a real implementation, insert the job
            // ishikari::insert(batch_job, &pool).await?;

            info!("Queued batch {} for processing", batch_id);
        }

        Complete::default()
            .message("Periodic batch processing completed")
            .into()
    }
}

// Simulated helper functions
async fn process_item(item: &str) -> Result<(), anyhow::Error> {
    if item.contains("error") {
        return Err(anyhow::anyhow!("Simulated processing error"));
    }

    // Simulate processing time
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    Ok(())
}

async fn cleanup_temp_files(batch_id: &str) -> Result<(), anyhow::Error> {
    info!("Cleaning up temp files for batch {}", batch_id);
    Ok(())
}

async fn update_batch_status(batch_id: &str, status: &str) -> Result<(), anyhow::Error> {
    info!("Updating batch {} status to {}", batch_id, status);
    Ok(())
}

async fn send_completion_notification(batch_id: &str) -> Result<(), anyhow::Error> {
    info!("Sending completion notification for batch {}", batch_id);
    Ok(())
}

async fn find_pending_batches() -> Result<Vec<String>, anyhow::Error> {
    // Simulate finding pending batches
    Ok(vec!["batch_001".to_string(), "batch_002".to_string()])
}

async fn fetch_batch_items(batch_id: &str) -> Result<Vec<String>, anyhow::Error> {
    // Simulate fetching batch items
    Ok((0..50).map(|i| format!("{}_{:03}", batch_id, i)).collect())
}

#[allow(dead_code)]
fn main() {}
