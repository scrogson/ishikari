//! Workflow and job action handlers.

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Redirect, Response},
};
use sqlx::{PgConnection, PgPool};
use tracing::{error, info};

use crate::AppState;

/// Cancel a workflow and all its pending jobs.
pub async fn cancel_workflow(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    match cancel(&state.pool, schema, id).await {
        Ok(Outcome::Done) => {}
        Ok(Outcome::NotFound) => {
            return Err((axum::http::StatusCode::NOT_FOUND, "Workflow not found").into_response())
        }
        Ok(Outcome::Refused(workflow_state)) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!("Cannot cancel workflow in state: {}", workflow_state),
            )
                .into_response())
        }
        Err(e) => {
            error!(workflow_id = id, error = %e, "failed to cancel workflow");
            return Err((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to cancel workflow",
            )
                .into_response());
        }
    }

    info!(workflow_id = id, "workflow cancelled");
    Ok(Redirect::to(&format!(
        "{}/workflows/{}",
        state.base_path, id
    )))
}

/// Retry failed jobs in a workflow.
pub async fn retry_workflow(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    match retry(&state.pool, schema, id).await {
        Ok(Outcome::Done) => {}
        Ok(Outcome::NotFound) => {
            return Err((axum::http::StatusCode::NOT_FOUND, "Workflow not found").into_response())
        }
        Ok(Outcome::Refused(workflow_state)) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!("Cannot retry workflow in state: {}", workflow_state),
            )
                .into_response())
        }
        Err(e) => {
            error!(workflow_id = id, error = %e, "failed to retry workflow");
            return Err((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to retry workflow",
            )
                .into_response());
        }
    }

    info!(workflow_id = id, "workflow jobs retried");
    Ok(Redirect::to(&format!(
        "{}/workflows/{}",
        state.base_path, id
    )))
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
    Ok(Redirect::to(&format!(
        "{}/workflows/{}",
        state.base_path, new_workflow_id
    )))
}

/// Retry a single job.
pub async fn retry_job(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    let retried = match storage(&state.pool, schema).retry_job(id).await {
        Ok(retried) => retried,
        Err(e) => {
            error!(job_id = id, error = %e, "failed to retry job");
            return Err((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to retry job",
            )
                .into_response());
        }
    };

    if retried {
        info!(job_id = id, "job retried");
    }
    Ok(job_page(&state, id, retried, "retry"))
}

/// Cancel a single job.
pub async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Redirect, Response> {
    let schema = state.schema();

    let cancelled = match storage(&state.pool, schema)
        .cancel_job(id, Some("cancelled from the admin"))
        .await
    {
        Ok(cancelled) => cancelled,
        Err(e) => {
            error!(job_id = id, error = %e, "failed to cancel job");
            return Err((
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to cancel job",
            )
                .into_response());
        }
    };

    if cancelled {
        info!(job_id = id, "job cancelled");
    }
    Ok(job_page(&state, id, cancelled, "cancel"))
}

/// Back to the job's page, which says when the action changed nothing (the job had moved on).
fn job_page(state: &AppState, id: i64, changed: bool, action: &str) -> Redirect {
    if changed {
        Redirect::to(&format!("{}/jobs/{}", state.base_path, id))
    } else {
        Redirect::to(&format!(
            "{}/jobs/{}?unchanged={}",
            state.base_path, id, action
        ))
    }
}

// Helper functions

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

/// What became of a workflow action.
enum Outcome {
    Done,
    NotFound,
    /// The workflow's state, which the action doesn't apply to.
    Refused(String),
}

/// Cancel the workflow's unfinished jobs, then the workflow, in one transaction. The workflow's
/// row is locked after the jobs' (the order the engine takes them in) and its state checked
/// then, so a last step completing meanwhile leaves it completed.
async fn cancel(pool: &PgPool, schema: Option<&str>, id: i64) -> Result<Outcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    cancel_workflow_jobs(&mut tx, schema, id).await?;
    let outcome = match lock_workflow(&mut tx, schema, id).await? {
        None => Outcome::NotFound,
        Some(state) if matches!(state.as_str(), "completed" | "cancelled" | "compensated") => {
            Outcome::Refused(state)
        }
        Some(_) => {
            set_workflow_state(&mut tx, schema, id, "cancelled").await?;
            Outcome::Done
        }
    };
    // Otherwise dropped, so rolled back.
    if let Outcome::Done = outcome {
        tx.commit().await?;
    }
    Ok(outcome)
}

/// Reset the workflow's discarded and cancelled jobs, then set a failed workflow running, in
/// one transaction: the reset jobs can't run before the workflow is running again, so the last
/// of them to complete completes it. Locks as `cancel` does.
async fn retry(pool: &PgPool, schema: Option<&str>, id: i64) -> Result<Outcome, sqlx::Error> {
    let mut tx = pool.begin().await?;
    retry_workflow_jobs(&mut tx, schema, id).await?;
    let outcome = match lock_workflow(&mut tx, schema, id).await? {
        None => Outcome::NotFound,
        Some(state) if state == "failed" => {
            set_workflow_state(&mut tx, schema, id, "running").await?;
            Outcome::Done
        }
        Some(state) if state == "running" => Outcome::Done,
        Some(state) => Outcome::Refused(state),
    };
    if let Outcome::Done = outcome {
        tx.commit().await?;
    }
    Ok(outcome)
}

/// The workflow's state, with its row locked until the transaction ends. `NO KEY`, so inserts
/// of its jobs (whose foreign key checks take `KEY SHARE`) aren't held up.
async fn lock_workflow(
    tx: &mut PgConnection,
    schema: Option<&str>,
    id: i64,
) -> Result<Option<String>, sqlx::Error> {
    let workflows = qualified(schema, "ishikari_workflows");
    sqlx::query_scalar(&format!(
        "SELECT state FROM {workflows} WHERE id = $1 FOR NO KEY UPDATE"
    ))
    .bind(id)
    .fetch_optional(&mut *tx)
    .await
}

async fn set_workflow_state(
    tx: &mut PgConnection,
    schema: Option<&str>,
    id: i64,
    state: &str,
) -> Result<(), sqlx::Error> {
    let table = qualified(schema, "ishikari_workflows");

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
        format!(
            "UPDATE {} SET state = $1, completed_at = NULL WHERE id = $2",
            table
        )
    };

    sqlx::query(&query)
        .bind(state)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    Ok(())
}

/// Cancel the workflow's unfinished jobs as `Postgres::cancel_job` cancels one: why recorded in
/// their errors, their unique keys released, the dependencies on them failed, and what depends
/// on them outside the workflow, which can never run now, cancelled too.
async fn cancel_workflow_jobs(
    tx: &mut PgConnection,
    schema: Option<&str>,
    workflow_id: i64,
) -> Result<(), sqlx::Error> {
    let (jobs, deps) = (
        qualified(schema, "ishikari_jobs"),
        qualified(schema, "ishikari_job_dependencies"),
    );

    // Locked in id order, as the engine locks a step's dependents.
    let ids: Vec<i64> = sqlx::query_scalar(&format!(
        r#"
        SELECT id FROM {jobs}
        WHERE workflow_id = $1
          AND state IN ('pending', 'available', 'scheduled', 'retryable', 'executing')
        ORDER BY id
        FOR UPDATE
        "#
    ))
    .bind(workflow_id)
    .fetch_all(&mut *tx)
    .await?;
    if ids.is_empty() {
        return Ok(());
    }
    // Each with an error record of why, as the engine's error records are made.
    sqlx::query(&format!(
        "UPDATE {jobs} SET state = 'cancelled', cancelled_at = now(), {release}, \
             errors = errors || jsonb_build_object('attempt', attempt, 'at', $2::jsonb, \
                                                   'kind', $3::text, 'error', $4::text) \
         WHERE id = ANY($1)",
        release = RELEASE_CANCELLED,
    ))
    .bind(&ids)
    .bind(serde_json::json!(chrono::Utc::now()))
    .bind(ishikari::ErrorKind::Cancelled.as_str())
    .bind("workflow cancelled from the admin")
    .execute(&mut *tx)
    .await?;

    let downstream: Vec<i64> = sqlx::query_scalar(&format!(
        r#"
        WITH RECURSIVE down(id) AS (
            SELECT job_id FROM {deps} WHERE depends_on_job_id = ANY($1)
            UNION
            SELECT d.job_id FROM {deps} d JOIN down ON d.depends_on_job_id = down.id
        )
        SELECT id FROM down ORDER BY id
        "#
    ))
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await?;
    if !downstream.is_empty() {
        let mut failed = ids;
        failed.extend(&downstream);
        sqlx::query(&format!(
            "UPDATE {deps} SET state = 'failed' \
             WHERE depends_on_job_id = ANY($1) AND state = 'pending'"
        ))
        .bind(&failed)
        .execute(&mut *tx)
        .await?;
        sqlx::query(&format!(
            "UPDATE {jobs} SET state = 'cancelled', cancelled_at = now(), {release} \
             WHERE id = ANY($1) AND state IN ('pending', 'scheduled')",
            release = RELEASE_CANCELLED,
        ))
        .bind(&downstream)
        .execute(&mut *tx)
        .await?;
    }
    Ok(())
}

/// `unique_key = …` for a job being cancelled: kept when cancelled is one of its unique states,
/// released otherwise (as the engine does).
const RELEASE_CANCELLED: &str =
    "unique_key = CASE WHEN 'cancelled' = ANY(unique_states::text[]) THEN unique_key END";

async fn retry_workflow_jobs(
    tx: &mut PgConnection,
    schema: Option<&str>,
    workflow_id: i64,
) -> Result<(), sqlx::Error> {
    let jobs_table = qualified(schema, "ishikari_jobs");
    let deps_table = qualified(schema, "ishikari_job_dependencies");
    let enum_type = qualified(schema, "ishikari_job_state");

    // Locked in id order, as `cancel_workflow_jobs` does.
    let ids: Vec<i64> = sqlx::query_scalar(&format!(
        r#"
        SELECT id FROM {}
        WHERE workflow_id = $1
          AND state IN ('discarded', 'cancelled')
        ORDER BY id
        FOR UPDATE
        "#,
        jobs_table
    ))
    .bind(workflow_id)
    .fetch_all(&mut *tx)
    .await?;

    // Start them over as `Postgres::retry_job` does: attempts reset, errors kept as the history
    // of their earlier runs, no longer discarded or cancelled.
    let query = format!(
        r#"
        UPDATE {}
        SET state = CASE
            WHEN EXISTS (SELECT 1 FROM {} d WHERE d.job_id = id AND d.state != 'satisfied')
            THEN 'pending'
            ELSE 'available'
        END::{},
        attempt = 0,
        scheduled_at = now(),
        discarded_at = NULL,
        cancelled_at = NULL
        WHERE id = ANY($1)
        "#,
        jobs_table, deps_table, enum_type
    );

    sqlx::query(&query).bind(&ids).execute(&mut *tx).await?;

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
        .execute(&mut *tx)
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

/// `schema.name`, or `name` when no schema is set.
fn qualified(schema: Option<&str>, name: &str) -> String {
    match schema {
        Some(schema) => format!("{schema}.{name}"),
        None => name.to_string(),
    }
}

/// The jobs storage for the admin's pool and schema.
fn storage(pool: &PgPool, schema: Option<&str>) -> ishikari::Postgres {
    let storage = ishikari::Postgres::new(pool.clone());
    match schema {
        Some(schema) => storage.with_schema(schema),
        None => storage,
    }
}
