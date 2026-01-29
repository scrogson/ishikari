//! Workflow and job action handlers.

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Redirect, Response},
};
use sqlx::PgPool;
use tracing::{error, info};

use crate::AppState;

/// Cancel a workflow and all its pending jobs.
pub async fn cancel_workflow(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    // First check if workflow exists and is cancellable
    let workflow = get_workflow(&state.pool, schema, id).await;
    if workflow.is_none() {
        return Err((axum::http::StatusCode::NOT_FOUND, "Workflow not found").into_response());
    }

    let (_, workflow_state) = workflow.unwrap();
    if workflow_state == "completed"
        || workflow_state == "cancelled"
        || workflow_state == "compensated"
    {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            format!("Cannot cancel workflow in state: {}", workflow_state),
        )
            .into_response());
    }

    // Cancel all pending/scheduled/available jobs in the workflow
    if let Err(e) = cancel_workflow_jobs(&state.pool, schema, id).await {
        error!(workflow_id = id, error = %e, "failed to cancel workflow jobs");
        return Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to cancel workflow",
        )
            .into_response());
    }

    // Update workflow state
    if let Err(e) = set_workflow_state(&state.pool, schema, id, "cancelled").await {
        error!(workflow_id = id, error = %e, "failed to update workflow state");
        return Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to update workflow state",
        )
            .into_response());
    }

    info!(workflow_id = id, "workflow cancelled");
    Ok(Redirect::to(&format!("/workflows/{}", id)))
}

/// Retry failed jobs in a workflow.
pub async fn retry_workflow(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    // Check if workflow exists
    let workflow = get_workflow(&state.pool, schema, id).await;
    if workflow.is_none() {
        return Err((axum::http::StatusCode::NOT_FOUND, "Workflow not found").into_response());
    }

    let (_, workflow_state) = workflow.unwrap();
    if workflow_state != "failed" && workflow_state != "running" {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            format!("Cannot retry workflow in state: {}", workflow_state),
        )
            .into_response());
    }

    // Reset failed/discarded jobs to available
    if let Err(e) = retry_workflow_jobs(&state.pool, schema, id).await {
        error!(workflow_id = id, error = %e, "failed to retry workflow jobs");
        return Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to retry workflow",
        )
            .into_response());
    }

    // Set workflow back to running if it was failed
    if workflow_state == "failed" {
        if let Err(e) = set_workflow_state(&state.pool, schema, id, "running").await {
            error!(workflow_id = id, error = %e, "failed to update workflow state");
        }
    }

    info!(workflow_id = id, "workflow jobs retried");
    Ok(Redirect::to(&format!("/workflows/{}", id)))
}

/// Clone a workflow (create a new one with same jobs).
pub async fn clone_workflow(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    // Get original workflow
    let workflow = get_workflow_full(&state.pool, schema, id).await;
    if workflow.is_none() {
        return Err((axum::http::StatusCode::NOT_FOUND, "Workflow not found").into_response());
    }

    let (name, metadata) = workflow.unwrap();

    // Create new workflow
    let new_workflow_id = match create_workflow(&state.pool, schema, &name, &metadata).await {
        Ok(id) => id,
        Err(e) => {
            error!(workflow_id = id, error = %e, "failed to create cloned workflow");
            return Err((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to clone workflow",
            )
                .into_response());
        }
    };

    // Clone jobs
    if let Err(e) = clone_workflow_jobs(&state.pool, schema, id, new_workflow_id).await {
        error!(
            workflow_id = id,
            new_workflow_id = new_workflow_id,
            error = %e,
            "failed to clone workflow jobs"
        );
        return Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to clone workflow jobs",
        )
            .into_response());
    }

    info!(
        original_workflow_id = id,
        new_workflow_id = new_workflow_id,
        "workflow cloned"
    );
    Ok(Redirect::to(&format!("/workflows/{}", new_workflow_id)))
}

/// Retry a single job.
pub async fn retry_job(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    if let Err(e) = retry_single_job(&state.pool, schema, id).await {
        error!(job_id = id, error = %e, "failed to retry job");
        return Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to retry job",
        )
            .into_response());
    }

    info!(job_id = id, "job retried");
    Ok(Redirect::to(&format!("/jobs/{}", id)))
}

/// Cancel a single job.
pub async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    if let Err(e) = cancel_single_job(&state.pool, schema, id).await {
        error!(job_id = id, error = %e, "failed to cancel job");
        return Err((
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to cancel job",
        )
            .into_response());
    }

    info!(job_id = id, "job cancelled");
    Ok(Redirect::to(&format!("/jobs/{}", id)))
}

// Helper functions

async fn get_workflow(pool: &PgPool, schema: Option<&str>, id: i64) -> Option<(i64, String)> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    let query = format!("SELECT id, state FROM {} WHERE id = $1", table);
    sqlx::query_as(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}

async fn get_workflow_full(
    pool: &PgPool,
    schema: Option<&str>,
    id: i64,
) -> Option<(String, serde_json::Value)> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    let query = format!("SELECT name, metadata FROM {} WHERE id = $1", table);
    sqlx::query_as(&query)
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}

async fn set_workflow_state(
    pool: &PgPool,
    schema: Option<&str>,
    id: i64,
    state: &str,
) -> Result<(), sqlx::Error> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    let query = if state == "completed"
        || state == "failed"
        || state == "cancelled"
        || state == "compensated"
    {
        format!(
            "UPDATE {} SET state = $1, completed_at = now() WHERE id = $2",
            table
        )
    } else {
        format!("UPDATE {} SET state = $1 WHERE id = $2", table)
    };

    sqlx::query(&query)
        .bind(state)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

async fn cancel_workflow_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    workflow_id: i64,
) -> Result<(), sqlx::Error> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        r#"
        UPDATE {}
        SET state = 'cancelled', cancelled_at = now()
        WHERE workflow_id = $1
          AND state IN ('available', 'scheduled', 'retryable')
        "#,
        table
    );

    sqlx::query(&query).bind(workflow_id).execute(pool).await?;
    Ok(())
}

async fn retry_workflow_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    workflow_id: i64,
) -> Result<(), sqlx::Error> {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };

    // Reset discarded jobs to retryable, reset attempt count
    let query = format!(
        r#"
        UPDATE {}
        SET state = CASE
            WHEN EXISTS (SELECT 1 FROM {} d WHERE d.job_id = id AND d.state != 'satisfied')
            THEN 'scheduled'
            ELSE 'available'
        END,
        attempt = 0,
        errors = '[]'::jsonb
        WHERE workflow_id = $1
          AND state IN ('discarded', 'cancelled')
        "#,
        jobs_table, deps_table
    );

    sqlx::query(&query).bind(workflow_id).execute(pool).await?;

    // Also reset failed dependencies
    let reset_deps = format!(
        r#"
        UPDATE {} d
        SET state = 'pending'
        FROM {} j
        WHERE d.job_id = j.id
          AND j.workflow_id = $1
          AND d.state = 'failed'
        "#,
        deps_table, jobs_table
    );

    sqlx::query(&reset_deps)
        .bind(workflow_id)
        .execute(pool)
        .await?;

    Ok(())
}

async fn create_workflow(
    pool: &PgPool,
    schema: Option<&str>,
    name: &str,
    metadata: &serde_json::Value,
) -> Result<i64, sqlx::Error> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_workflows", s),
        None => "ishikari_workflows".to_string(),
    };

    let query = format!(
        r#"
        INSERT INTO {} (name, state, metadata)
        VALUES ($1, 'running', $2)
        RETURNING id
        "#,
        table
    );

    let (id,): (i64,) = sqlx::query_as(&query)
        .bind(name)
        .bind(metadata)
        .fetch_one(pool)
        .await?;

    Ok(id)
}

async fn clone_workflow_jobs(
    pool: &PgPool,
    schema: Option<&str>,
    original_id: i64,
    new_id: i64,
) -> Result<(), sqlx::Error> {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };

    // First, clone all jobs and create a mapping of old to new IDs
    let clone_jobs_query = format!(
        r#"
        WITH cloned AS (
            INSERT INTO {} (queue, worker, args, max_attempts, priority, workflow_id, state)
            SELECT queue, worker, args, max_attempts, priority, $2,
                   CASE WHEN state = 'completed' THEN 'available' ELSE state END
            FROM {}
            WHERE workflow_id = $1
            ORDER BY id
            RETURNING id
        ),
        original_ids AS (
            SELECT id, row_number() OVER (ORDER BY id) as rn
            FROM {}
            WHERE workflow_id = $1
        ),
        new_ids AS (
            SELECT id, row_number() OVER (ORDER BY id) as rn
            FROM cloned
        )
        SELECT o.id as old_id, n.id as new_id
        FROM original_ids o
        JOIN new_ids n ON o.rn = n.rn
        "#,
        jobs_table, jobs_table, jobs_table
    );

    let id_mapping: Vec<(i64, i64)> = sqlx::query_as(&clone_jobs_query)
        .bind(original_id)
        .bind(new_id)
        .fetch_all(pool)
        .await?;

    // Clone dependencies using the mapping
    for (old_job_id, new_job_id) in &id_mapping {
        let get_deps = format!(
            "SELECT depends_on_job_id FROM {} WHERE job_id = $1",
            deps_table
        );

        let deps: Vec<(i64,)> = sqlx::query_as(&get_deps)
            .bind(old_job_id)
            .fetch_all(pool)
            .await?;

        for (old_dep_id,) in deps {
            // Find the new ID for this dependency
            if let Some((_, new_dep_id)) = id_mapping.iter().find(|(o, _)| *o == old_dep_id) {
                let insert_dep = format!(
                    "INSERT INTO {} (job_id, depends_on_job_id, state) VALUES ($1, $2, 'pending')",
                    deps_table
                );
                sqlx::query(&insert_dep)
                    .bind(new_job_id)
                    .bind(new_dep_id)
                    .execute(pool)
                    .await?;
            }
        }
    }

    Ok(())
}

async fn retry_single_job(pool: &PgPool, schema: Option<&str>, id: i64) -> Result<(), sqlx::Error> {
    let jobs_table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };
    let deps_table = match schema {
        Some(s) => format!("{}.ishikari_job_dependencies", s),
        None => "ishikari_job_dependencies".to_string(),
    };

    let query = format!(
        r#"
        UPDATE {}
        SET state = CASE
            WHEN EXISTS (SELECT 1 FROM {} d WHERE d.job_id = $1 AND d.state != 'satisfied')
            THEN 'scheduled'
            ELSE 'available'
        END,
        attempt = 0,
        errors = '[]'::jsonb
        WHERE id = $1
          AND state IN ('discarded', 'cancelled', 'retryable')
        "#,
        jobs_table, deps_table
    );

    sqlx::query(&query).bind(id).execute(pool).await?;
    Ok(())
}

async fn cancel_single_job(
    pool: &PgPool,
    schema: Option<&str>,
    id: i64,
) -> Result<(), sqlx::Error> {
    let table = match schema {
        Some(s) => format!("{}.ishikari_jobs", s),
        None => "ishikari_jobs".to_string(),
    };

    let query = format!(
        r#"
        UPDATE {}
        SET state = 'cancelled', cancelled_at = now()
        WHERE id = $1
          AND state IN ('available', 'scheduled', 'retryable')
        "#,
        table
    );

    sqlx::query(&query).bind(id).execute(pool).await?;
    Ok(())
}
