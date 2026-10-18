//! Worker definitions for the demo
//!
//! Contains all job and worker types used in demo scenarios.

use ishikari::prelude::*;
use std::time::Duration;
use tokio::time::sleep;

// ============ Email Processing ============

#[ishikari::job]
#[derive(Clone)]
pub struct ValidateEmail {
    pub email: String,
}

#[ishikari::worker(queue = "email")]
impl Worker for ValidateEmail {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_millis(500)).await;
        // Simple validation
        if self.email.contains('@') {
            Complete::default().message("valid").into()
        } else {
            Err("invalid email format".into())
        }
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct SendEmail {
    pub to: String,
    pub subject: String,
    pub body: String,
}

#[ishikari::worker(queue = "email")]
impl Worker for SendEmail {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        tracing::info!(to = %self.to, subject = %self.subject, "Email sent");
        Complete::default().message("sent").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct LogEmailEvent {
    pub event_type: String,
    pub email: String,
}

#[ishikari::worker(queue = "email")]
impl Worker for LogEmailEvent {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_millis(200)).await;
        Complete::default().into()
    }
}

// ============ ETL Processing ============

#[ishikari::job]
#[derive(Clone)]
pub struct ExtractFromSource {
    pub source: String,
    pub query: String,
}

#[ishikari::worker(queue = "etl")]
impl Worker for ExtractFromSource {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(2)).await;
        tracing::info!(source = %self.source, "Data extracted");
        Complete::default().message(r#"{"rows": 1000}"#).into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct TransformData {
    pub format: String,
    pub rules: String,
}

#[ishikari::worker(queue = "etl")]
impl Worker for TransformData {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(3)).await;
        Complete::default().message("transformed").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct LoadToDestination {
    pub destination: String,
    pub table: String,
}

#[ishikari::worker(queue = "etl")]
impl Worker for LoadToDestination {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        Complete::default().message("loaded").into()
    }
}

// ============ Order Saga ============

#[ishikari::job]
#[derive(Clone)]
pub struct ReserveInventory {
    pub order_id: String,
    pub product_id: String,
    pub quantity: i32,
}

#[ishikari::worker(queue = "orders", max_attempts = 3)]
impl Worker for ReserveInventory {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        Complete::default().message("reserved").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct ReleaseInventory {
    pub order_id: String,
    pub product_id: String,
    pub quantity: i32,
}

#[ishikari::worker(queue = "orders")]
impl Worker for ReleaseInventory {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_millis(500)).await;
        Complete::default().message("released").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct ChargePayment {
    pub order_id: String,
    pub amount: f64,
    pub currency: String,
}

#[ishikari::worker(queue = "payments", max_attempts = 3)]
impl Worker for ChargePayment {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(2)).await;
        Complete::default().message("charged").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct RefundPayment {
    pub order_id: String,
    pub amount: f64,
    pub reason: String,
}

#[ishikari::worker(queue = "payments")]
impl Worker for RefundPayment {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        Complete::default().message("refunded").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct ShipOrder {
    pub order_id: String,
    pub address: String,
    pub carrier: String,
}

#[ishikari::worker(queue = "shipping", max_attempts = 3)]
impl Worker for ShipOrder {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(1)).await;
        Complete::default().message("shipped").into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct CancelShipment {
    pub order_id: String,
    pub reason: String,
}

#[ishikari::worker(queue = "shipping")]
impl Worker for CancelShipment {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_millis(500)).await;
        Complete::default().message("cancelled").into()
    }
}

/// A ship order that always fails - used to test saga compensation.
/// Simulates a shipping request to an undeliverable address.
#[ishikari::job]
#[derive(Clone)]
pub struct FailingShipOrder {
    pub order_id: String,
    pub address: String,
}

#[ishikari::worker(queue = "shipping", max_attempts = 1)]
impl Worker for FailingShipOrder {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        // Simulate attempting to ship - discover the address is undeliverable
        sleep(Duration::from_millis(500)).await;

        tracing::error!(
            order_id = %self.order_id,
            address = %self.address,
            "Shipping failed: no carriers available for delivery area"
        );

        // Return Cancel with the error discovered during execution
        Cancel::default()
            .message("Shipping failed: no carriers available for delivery area")
            .into()
    }
}

// ============ Misc Jobs ============

#[ishikari::job]
#[derive(Clone)]
pub struct SlowJob {
    pub duration_secs: u64,
    pub label: String,
}

#[ishikari::worker(queue = "default")]
impl Worker for SlowJob {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        sleep(Duration::from_secs(self.duration_secs)).await;
        Complete::default().message(&self.label).into()
    }
}

#[ishikari::job]
#[derive(Clone)]
pub struct FailingJob {
    pub fail_until_attempt: i32,
    pub message: String,
}

#[ishikari::worker(queue = "default", max_attempts = 5)]
impl Worker for FailingJob {
    async fn perform(&self, ctx: Context) -> PerformResult {
        if ctx.job().attempt < self.fail_until_attempt {
            Err(format!("Failing on attempt {}", ctx.job().attempt).into())
        } else {
            Complete::default().message(&self.message).into()
        }
    }
}
