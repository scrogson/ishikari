//! Real-time execution events.
//!
//! Events are broadcast using Tokio channels, allowing multiple subscribers
//! to receive real-time updates about workflow execution.

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

use super::types::NodeData;

/// Channel capacity for event broadcasting.
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Execution events broadcast during workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ExecutionEvent {
    /// Workflow execution started
    WorkflowStarted {
        execution_id: Uuid,
        definition_id: i64,
        name: String,
    },

    /// Workflow execution completed successfully
    WorkflowCompleted {
        execution_id: Uuid,
        duration_ms: i64,
        outputs: serde_json::Value,
    },

    /// Workflow execution failed
    WorkflowFailed {
        execution_id: Uuid,
        error: String,
        duration_ms: i64,
    },

    /// Workflow execution was cancelled
    WorkflowCancelled {
        execution_id: Uuid,
        reason: Option<String>,
    },

    /// Node execution started
    NodeStarted {
        execution_id: Uuid,
        node_id: String,
        node_type: String,
    },

    /// Node execution completed successfully
    NodeCompleted {
        execution_id: Uuid,
        node_id: String,
        duration_ms: i64,
        output: serde_json::Value,
    },

    /// Node execution failed
    NodeFailed {
        execution_id: Uuid,
        node_id: String,
        error: String,
        duration_ms: i64,
    },

    /// Node was skipped
    NodeSkipped {
        execution_id: Uuid,
        node_id: String,
        reason: String,
    },

    /// Execution level started (nodes at this level execute in parallel)
    LevelStarted {
        execution_id: Uuid,
        level: usize,
        node_ids: Vec<String>,
    },

    /// Execution level completed
    LevelCompleted {
        execution_id: Uuid,
        level: usize,
        duration_ms: i64,
    },

    /// Progress update
    Progress {
        execution_id: Uuid,
        completed_nodes: usize,
        total_nodes: usize,
        percentage: f32,
    },
}

impl ExecutionEvent {
    /// Get the execution ID for this event.
    pub fn execution_id(&self) -> Uuid {
        match self {
            Self::WorkflowStarted { execution_id, .. }
            | Self::WorkflowCompleted { execution_id, .. }
            | Self::WorkflowFailed { execution_id, .. }
            | Self::WorkflowCancelled { execution_id, .. }
            | Self::NodeStarted { execution_id, .. }
            | Self::NodeCompleted { execution_id, .. }
            | Self::NodeFailed { execution_id, .. }
            | Self::NodeSkipped { execution_id, .. }
            | Self::LevelStarted { execution_id, .. }
            | Self::LevelCompleted { execution_id, .. }
            | Self::Progress { execution_id, .. } => *execution_id,
        }
    }

    /// Check if this is a terminal event (workflow completed/failed/cancelled).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::WorkflowCompleted { .. }
                | Self::WorkflowFailed { .. }
                | Self::WorkflowCancelled { .. }
        )
    }
}

/// Event sender for broadcasting execution events.
#[derive(Clone)]
pub struct EventSender {
    tx: broadcast::Sender<ExecutionEvent>,
}

impl EventSender {
    /// Create a new event sender.
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self { tx }
    }

    /// Send an event to all subscribers.
    pub fn send(&self, event: ExecutionEvent) {
        // Ignore send errors (no subscribers)
        let _ = self.tx.send(event);
    }

    /// Subscribe to events.
    pub fn subscribe(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.tx.subscribe()
    }

    /// Get the number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }

    // Convenience methods for sending specific events

    /// Send workflow started event.
    pub fn workflow_started(&self, execution_id: Uuid, definition_id: i64, name: &str) {
        self.send(ExecutionEvent::WorkflowStarted {
            execution_id,
            definition_id,
            name: name.to_string(),
        });
    }

    /// Send workflow completed event.
    pub fn workflow_completed(
        &self,
        execution_id: Uuid,
        duration_ms: i64,
        outputs: serde_json::Value,
    ) {
        self.send(ExecutionEvent::WorkflowCompleted {
            execution_id,
            duration_ms,
            outputs,
        });
    }

    /// Send workflow failed event.
    pub fn workflow_failed(&self, execution_id: Uuid, error: &str, duration_ms: i64) {
        self.send(ExecutionEvent::WorkflowFailed {
            execution_id,
            error: error.to_string(),
            duration_ms,
        });
    }

    /// Send node started event.
    pub fn node_started(&self, execution_id: Uuid, node_id: &str, node_type: &str) {
        self.send(ExecutionEvent::NodeStarted {
            execution_id,
            node_id: node_id.to_string(),
            node_type: node_type.to_string(),
        });
    }

    /// Send node completed event.
    pub fn node_completed(
        &self,
        execution_id: Uuid,
        node_id: &str,
        duration_ms: i64,
        output: &NodeData,
    ) {
        self.send(ExecutionEvent::NodeCompleted {
            execution_id,
            node_id: node_id.to_string(),
            duration_ms,
            output: output.to_json(),
        });
    }

    /// Send node failed event.
    pub fn node_failed(&self, execution_id: Uuid, node_id: &str, error: &str, duration_ms: i64) {
        self.send(ExecutionEvent::NodeFailed {
            execution_id,
            node_id: node_id.to_string(),
            error: error.to_string(),
            duration_ms,
        });
    }

    /// Send node skipped event.
    pub fn node_skipped(&self, execution_id: Uuid, node_id: &str, reason: &str) {
        self.send(ExecutionEvent::NodeSkipped {
            execution_id,
            node_id: node_id.to_string(),
            reason: reason.to_string(),
        });
    }

    /// Send level started event.
    pub fn level_started(&self, execution_id: Uuid, level: usize, node_ids: Vec<String>) {
        self.send(ExecutionEvent::LevelStarted {
            execution_id,
            level,
            node_ids,
        });
    }

    /// Send level completed event.
    pub fn level_completed(&self, execution_id: Uuid, level: usize, duration_ms: i64) {
        self.send(ExecutionEvent::LevelCompleted {
            execution_id,
            level,
            duration_ms,
        });
    }

    /// Send progress event.
    pub fn progress(&self, execution_id: Uuid, completed: usize, total: usize) {
        let percentage = if total > 0 {
            (completed as f32 / total as f32) * 100.0
        } else {
            0.0
        };
        self.send(ExecutionEvent::Progress {
            execution_id,
            completed_nodes: completed,
            total_nodes: total,
            percentage,
        });
    }
}

impl Default for EventSender {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_event_broadcast() {
        let sender = EventSender::new();
        let mut rx1 = sender.subscribe();
        let mut rx2 = sender.subscribe();

        let execution_id = Uuid::new_v4();
        sender.workflow_started(execution_id, 1, "test");

        let event1 = rx1.recv().await.unwrap();
        let event2 = rx2.recv().await.unwrap();

        assert_eq!(event1.execution_id(), execution_id);
        assert_eq!(event2.execution_id(), execution_id);
    }
}
