//! Database storage for workflow executions.
//!
//! Persists workflow and node execution state to PostgreSQL.

use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use super::types::{ExecutionStatus, NodeData, NodeExecution, NodeStatus, WorkflowExecution};

/// Storage for workflow executions.
pub struct ExecutionStorage {
    pool: PgPool,
    schema: Option<String>,
}

impl ExecutionStorage {
    /// Create a new execution storage.
    pub fn new(pool: PgPool) -> Self {
        Self { pool, schema: None }
    }

    /// Set the database schema for multi-tenancy.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    fn table_name(&self, table: &str) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, table),
            None => table.to_string(),
        }
    }

    // ===== Workflow Run Operations =====

    /// Create a new workflow run.
    pub async fn create_workflow_run(
        &self,
        execution_id: Uuid,
        definition_id: i64,
        definition_name: &str,
        definition_version: i32,
        workflow_id: i64,
        inputs: &Value,
    ) -> Result<i64, sqlx::Error> {
        let query = format!(
            r#"
            INSERT INTO {} (
                execution_id, definition_id, definition_name, definition_version,
                workflow_id, inputs, status, started_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, 'running', NOW())
            RETURNING id
            "#,
            self.table_name("ishikari_workflow_runs")
        );

        let row: (i64,) = sqlx::query_as(&query)
            .bind(execution_id)
            .bind(definition_id)
            .bind(definition_name)
            .bind(definition_version)
            .bind(workflow_id)
            .bind(inputs)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.0)
    }

    /// Get a workflow run by execution ID.
    pub async fn get_workflow_run_by_execution_id(
        &self,
        execution_id: Uuid,
    ) -> Result<Option<WorkflowRunRow>, sqlx::Error> {
        let query = format!(
            r#"
            SELECT id, execution_id, definition_id, definition_name, definition_version,
                   workflow_id, inputs, status, outputs, error, started_at, completed_at, created_at
            FROM {}
            WHERE execution_id = $1
            "#,
            self.table_name("ishikari_workflow_runs")
        );

        sqlx::query_as(&query)
            .bind(execution_id)
            .fetch_optional(&self.pool)
            .await
    }

    /// Update workflow run status.
    pub async fn update_workflow_run_status(
        &self,
        workflow_run_id: i64,
        status: ExecutionStatus,
    ) -> Result<(), sqlx::Error> {
        let query = format!(
            "UPDATE {} SET status = $1 WHERE id = $2",
            self.table_name("ishikari_workflow_runs")
        );

        sqlx::query(&query)
            .bind(status.to_string())
            .bind(workflow_run_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Complete a workflow run successfully.
    pub async fn complete_workflow_run(
        &self,
        workflow_run_id: i64,
        outputs: &Value,
    ) -> Result<(), sqlx::Error> {
        let query = format!(
            r#"
            UPDATE {}
            SET status = 'completed', outputs = $1, completed_at = NOW()
            WHERE id = $2
            "#,
            self.table_name("ishikari_workflow_runs")
        );

        sqlx::query(&query)
            .bind(outputs)
            .bind(workflow_run_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Fail a workflow run.
    pub async fn fail_workflow_run(
        &self,
        workflow_run_id: i64,
        error: &str,
    ) -> Result<(), sqlx::Error> {
        let query = format!(
            r#"
            UPDATE {}
            SET status = 'failed', error = $1, completed_at = NOW()
            WHERE id = $2
            "#,
            self.table_name("ishikari_workflow_runs")
        );

        sqlx::query(&query)
            .bind(error)
            .bind(workflow_run_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Cancel a workflow run.
    pub async fn cancel_workflow_run(&self, workflow_run_id: i64) -> Result<(), sqlx::Error> {
        let query = format!(
            r#"
            UPDATE {}
            SET status = 'cancelled', completed_at = NOW()
            WHERE id = $2
            "#,
            self.table_name("ishikari_workflow_runs")
        );

        sqlx::query(&query)
            .bind(workflow_run_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    // ===== Node Execution Operations =====

    /// Create a node execution record.
    pub async fn create_node_execution(
        &self,
        workflow_run_id: i64,
        node_id: &str,
        node_type: &str,
        inputs: &Value,
    ) -> Result<i64, sqlx::Error> {
        let query = format!(
            r#"
            INSERT INTO {} (workflow_run_id, node_id, node_type, status, inputs)
            VALUES ($1, $2, $3, 'pending', $4)
            RETURNING id
            "#,
            self.table_name("ishikari_node_executions")
        );

        let row: (i64,) = sqlx::query_as(&query)
            .bind(workflow_run_id)
            .bind(node_id)
            .bind(node_type)
            .bind(inputs)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.0)
    }

    /// Mark a node as started.
    pub async fn start_node_execution(
        &self,
        workflow_run_id: i64,
        node_id: &str,
    ) -> Result<(), sqlx::Error> {
        let query = format!(
            r#"
            UPDATE {}
            SET status = 'running', started_at = NOW()
            WHERE workflow_run_id = $1 AND node_id = $2
            "#,
            self.table_name("ishikari_node_executions")
        );

        sqlx::query(&query)
            .bind(workflow_run_id)
            .bind(node_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Complete a node execution successfully.
    pub async fn complete_node_execution(
        &self,
        workflow_run_id: i64,
        node_id: &str,
        output: &Value,
        duration_ms: i64,
    ) -> Result<(), sqlx::Error> {
        let query = format!(
            r#"
            UPDATE {}
            SET status = 'completed', output = $1, duration_ms = $2, completed_at = NOW()
            WHERE workflow_run_id = $3 AND node_id = $4
            "#,
            self.table_name("ishikari_node_executions")
        );

        sqlx::query(&query)
            .bind(output)
            .bind(duration_ms)
            .bind(workflow_run_id)
            .bind(node_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Fail a node execution.
    pub async fn fail_node_execution(
        &self,
        workflow_run_id: i64,
        node_id: &str,
        error: &str,
        duration_ms: i64,
    ) -> Result<(), sqlx::Error> {
        let query = format!(
            r#"
            UPDATE {}
            SET status = 'failed', error = $1, duration_ms = $2, completed_at = NOW()
            WHERE workflow_run_id = $3 AND node_id = $4
            "#,
            self.table_name("ishikari_node_executions")
        );

        sqlx::query(&query)
            .bind(error)
            .bind(duration_ms)
            .bind(workflow_run_id)
            .bind(node_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Skip a node execution.
    pub async fn skip_node_execution(
        &self,
        workflow_run_id: i64,
        node_id: &str,
        reason: &str,
    ) -> Result<(), sqlx::Error> {
        let query = format!(
            r#"
            UPDATE {}
            SET status = 'skipped', skip_reason = $1, completed_at = NOW()
            WHERE workflow_run_id = $2 AND node_id = $3
            "#,
            self.table_name("ishikari_node_executions")
        );

        sqlx::query(&query)
            .bind(reason)
            .bind(workflow_run_id)
            .bind(node_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    /// Increment retry count for a node.
    pub async fn increment_node_retry(
        &self,
        workflow_run_id: i64,
        node_id: &str,
    ) -> Result<i32, sqlx::Error> {
        let query = format!(
            r#"
            UPDATE {}
            SET retry_count = retry_count + 1, status = 'pending', started_at = NULL
            WHERE workflow_run_id = $1 AND node_id = $2
            RETURNING retry_count
            "#,
            self.table_name("ishikari_node_executions")
        );

        let row: (i32,) = sqlx::query_as(&query)
            .bind(workflow_run_id)
            .bind(node_id)
            .fetch_one(&self.pool)
            .await?;

        Ok(row.0)
    }

    /// Get all node executions for a workflow run.
    pub async fn get_node_executions(
        &self,
        workflow_run_id: i64,
    ) -> Result<Vec<NodeExecutionRow>, sqlx::Error> {
        let query = format!(
            r#"
            SELECT id, workflow_run_id, node_id, node_type, status, inputs, output,
                   error, skip_reason, retry_count, started_at, completed_at, duration_ms, created_at
            FROM {}
            WHERE workflow_run_id = $1
            ORDER BY created_at
            "#,
            self.table_name("ishikari_node_executions")
        );

        sqlx::query_as(&query)
            .bind(workflow_run_id)
            .fetch_all(&self.pool)
            .await
    }

    // ===== Node Output Operations =====

    /// Get a node's output from node executions.
    pub async fn get_node_output(
        &self,
        workflow_run_id: i64,
        node_id: &str,
    ) -> Result<Option<Value>, sqlx::Error> {
        let query = format!(
            "SELECT output FROM {} WHERE workflow_run_id = $1 AND node_id = $2 AND status = 'completed' AND output IS NOT NULL",
            self.table_name("ishikari_node_executions")
        );

        let row: Option<(Value,)> = sqlx::query_as(&query)
            .bind(workflow_run_id)
            .bind(node_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|(v,)| v))
    }

    /// Get all node outputs for a workflow run from node executions.
    pub async fn get_all_node_outputs(
        &self,
        workflow_run_id: i64,
    ) -> Result<std::collections::HashMap<String, NodeData>, sqlx::Error> {
        let query = format!(
            "SELECT node_id, output FROM {} WHERE workflow_run_id = $1 AND status = 'completed' AND output IS NOT NULL",
            self.table_name("ishikari_node_executions")
        );

        let rows: Vec<(String, Value)> = sqlx::query_as(&query)
            .bind(workflow_run_id)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .into_iter()
            .map(|(node_id, output)| (node_id, NodeData::Json(output)))
            .collect())
    }
}

/// Database row for workflow runs.
#[derive(Debug, sqlx::FromRow)]
pub struct WorkflowRunRow {
    pub id: i64,
    pub execution_id: Option<Uuid>,
    pub definition_id: i64,
    pub definition_name: String,
    pub definition_version: i32,
    pub workflow_id: i64,
    pub inputs: Value,
    pub status: String,
    pub outputs: Option<Value>,
    pub error: Option<String>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl WorkflowRunRow {
    /// Convert to WorkflowExecution.
    pub fn to_execution(&self) -> WorkflowExecution {
        WorkflowExecution {
            id: self.execution_id.unwrap_or_else(Uuid::nil),
            definition_id: self.definition_id,
            name: self.definition_name.clone(),
            status: match self.status.as_str() {
                "completed" => ExecutionStatus::Completed,
                "failed" => ExecutionStatus::Failed,
                "cancelled" => ExecutionStatus::Cancelled,
                _ => ExecutionStatus::Running,
            },
            inputs: std::collections::HashMap::new(), // Would need to deserialize
            outputs: std::collections::HashMap::new(), // Would need to deserialize
            error: self.error.clone(),
            started_at: self.started_at.map(|t| t.timestamp()),
            completed_at: self.completed_at.map(|t| t.timestamp()),
        }
    }
}

/// Database row for node executions.
#[derive(Debug, sqlx::FromRow)]
pub struct NodeExecutionRow {
    pub id: i64,
    pub workflow_run_id: i64,
    pub node_id: String,
    pub node_type: String,
    pub status: String,
    pub inputs: Value,
    pub output: Option<Value>,
    pub error: Option<String>,
    pub skip_reason: Option<String>,
    pub retry_count: i32,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
    pub duration_ms: Option<i64>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl NodeExecutionRow {
    /// Convert to NodeExecution.
    pub fn to_execution(&self, execution_id: Uuid) -> NodeExecution {
        NodeExecution {
            execution_id,
            node_id: self.node_id.clone(),
            node_type: self.node_type.clone(),
            status: match self.status.as_str() {
                "running" => NodeStatus::Running,
                "completed" => NodeStatus::Completed,
                "failed" => NodeStatus::Failed,
                "skipped" => NodeStatus::Skipped,
                _ => NodeStatus::Pending,
            },
            inputs: std::collections::HashMap::new(), // Would need to deserialize
            output: self.output.clone().map(NodeData::Json),
            error: self.error.clone(),
            skip_reason: self.skip_reason.clone(),
            started_at: self.started_at.map(|t| t.timestamp()),
            completed_at: self.completed_at.map(|t| t.timestamp()),
            duration_ms: self.duration_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_table_name_without_schema() {
        let pool = sqlx::PgPool::connect_lazy("postgres://invalid").unwrap();
        let storage = ExecutionStorage::new(pool);
        assert_eq!(
            storage.table_name("ishikari_workflow_runs"),
            "ishikari_workflow_runs"
        );
    }

    #[tokio::test]
    async fn test_table_name_with_schema() {
        let pool = sqlx::PgPool::connect_lazy("postgres://invalid").unwrap();
        let storage = ExecutionStorage::new(pool).schema("tenant_1");
        assert_eq!(
            storage.table_name("ishikari_workflow_runs"),
            "tenant_1.ishikari_workflow_runs"
        );
    }
}
