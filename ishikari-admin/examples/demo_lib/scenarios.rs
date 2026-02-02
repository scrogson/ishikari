//! Scenario creators for the demo
//!
//! Functions that create sample workflows to demonstrate different patterns.

use crate::demo_lib::workers::*;
use ishikari::workflows::{Dag, Pipeline, Saga};
use sqlx::PgPool;

/// Create an email notification pipeline
pub async fn create_email_pipeline(
    pool: &PgPool,
) -> Result<i64, ishikari::workflows::Error> {
    let workflow = Pipeline::new("email-notification")
        .metadata(serde_json::json!({
            "triggered_by": "user_signup",
            "user_id": "usr_12345"
        }))
        .step(ValidateEmail {
            email: "user@example.com".into(),
        })
        .step(SendEmail {
            to: "user@example.com".into(),
            subject: "Welcome!".into(),
            body: "Thanks for signing up.".into(),
        })
        .step(LogEmailEvent {
            event_type: "welcome_sent".into(),
            email: "user@example.com".into(),
        })
        .run(pool)
        .await?;

    Ok(workflow.id)
}

/// Create an ETL DAG with fan-in/fan-out
pub async fn create_etl_dag(pool: &PgPool) -> Result<i64, ishikari::workflows::Error> {
    let workflow = Dag::new("daily-etl")
        .metadata(serde_json::json!({
            "schedule": "0 2 * * *",
            "environment": "production"
        }))
        // Fan-out: 3 parallel extractions
        .add(
            "extract_postgres",
            ExtractFromSource {
                source: "postgres".into(),
                query: "SELECT * FROM orders WHERE date = today()".into(),
            },
        )
        .add(
            "extract_mysql",
            ExtractFromSource {
                source: "mysql".into(),
                query: "SELECT * FROM customers".into(),
            },
        )
        .add(
            "extract_api",
            ExtractFromSource {
                source: "rest_api".into(),
                query: "/api/v1/products".into(),
            },
        )
        // Fan-in: transform waits for all extracts
        .add_after(
            "transform",
            TransformData {
                format: "parquet".into(),
                rules: "normalize_dates,dedupe".into(),
            },
            ["extract_postgres", "extract_mysql", "extract_api"],
        )
        // Fan-out: parallel loads
        .add_after(
            "load_warehouse",
            LoadToDestination {
                destination: "snowflake".into(),
                table: "analytics.daily_snapshot".into(),
            },
            ["transform"],
        )
        .add_after(
            "load_datalake",
            LoadToDestination {
                destination: "s3".into(),
                table: "datalake/daily/".into(),
            },
            ["transform"],
        )
        .run(pool)
        .await?;

    Ok(workflow.id)
}

/// Create an order processing saga with compensations
pub async fn create_order_saga(pool: &PgPool) -> Result<i64, ishikari::workflows::Error> {
    let order_id = format!("ord_{}", uuid::Uuid::new_v4().simple());

    let workflow = Saga::new("process-order")
        .metadata(serde_json::json!({
            "order_id": order_id,
            "customer_id": "cust_abc123",
            "total": 299.99
        }))
        .step(ReserveInventory {
            order_id: order_id.clone(),
            product_id: "prod_widget".into(),
            quantity: 2,
        })
        .compensate(ReleaseInventory {
            order_id: order_id.clone(),
            product_id: "prod_widget".into(),
            quantity: 2,
        })
        .step(ChargePayment {
            order_id: order_id.clone(),
            amount: 299.99,
            currency: "USD".into(),
        })
        .compensate(RefundPayment {
            order_id: order_id.clone(),
            amount: 299.99,
            reason: "Order cancelled".into(),
        })
        .step(ShipOrder {
            order_id: order_id.clone(),
            address: "123 Main St, City, ST 12345".into(),
            carrier: "fedex".into(),
        })
        .compensate(CancelShipment {
            order_id: order_id.clone(),
            reason: "Order cancelled".into(),
        })
        .run(pool)
        .await?;

    Ok(workflow.id)
}

/// Create a failing order saga to test compensation
pub async fn create_failing_order_saga(
    pool: &PgPool,
) -> Result<i64, ishikari::workflows::Error> {
    let order_id = format!("fail_ord_{}", uuid::Uuid::new_v4().simple());

    let workflow = Saga::new("process-order-failing")
        .metadata(serde_json::json!({
            "order_id": order_id,
            "customer_id": "cust_xyz789",
            "total": 599.99,
            "test_type": "compensation_test"
        }))
        // Step 1: Reserve inventory - will succeed
        .step(ReserveInventory {
            order_id: order_id.clone(),
            product_id: "prod_premium".into(),
            quantity: 3,
        })
        .compensate(ReleaseInventory {
            order_id: order_id.clone(),
            product_id: "prod_premium".into(),
            quantity: 3,
        })
        // Step 2: Charge payment - will succeed
        .step(ChargePayment {
            order_id: order_id.clone(),
            amount: 599.99,
            currency: "USD".into(),
        })
        .compensate(RefundPayment {
            order_id: order_id.clone(),
            amount: 599.99,
            reason: "Shipping unavailable".into(),
        })
        // Step 3: Ship order - WILL FAIL and trigger compensation
        .step(FailingShipOrder {
            order_id: order_id.clone(),
            address: "456 Remote Rd, Nowhere, XX 00000".into(),
        })
        .compensate(CancelShipment {
            order_id: order_id.clone(),
            reason: "Order cancelled due to shipping failure".into(),
        })
        .run(pool)
        .await?;

    Ok(workflow.id)
}

/// Create sample workflow definitions for the admin UI
pub async fn create_sample_workflow_definitions(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Check if table exists
    let table_exists: Option<bool> = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'ishikari_workflow_definitions'
        )
        "#,
    )
    .fetch_optional(pool)
    .await?;

    if table_exists != Some(true) {
        tracing::warn!(
            "ishikari_workflow_definitions table not found, skipping workflow definitions"
        );
        return Ok(());
    }

    // Hello World workflow
    let hello_world = serde_json::json!({
        "echo": {
            "type": "echo",
            "inputs": {
                "message": "{{inputs.greeting}}"
            }
        },
        "transform": {
            "type": "uppercase",
            "depends_on": ["echo"],
            "inputs": {
                "text": "{{echo.outputs.message}}"
            }
        }
    });

    let hello_inputs = serde_json::json!({
        "greeting": {
            "type": "string",
            "description": "The greeting message to echo",
            "required": true
        }
    });

    let hello_outputs = serde_json::json!({
        "result": "{{transform.outputs.result}}"
    });

    sqlx::query(
        r#"
        INSERT INTO ishikari_workflow_definitions (name, version, description, input_schema, nodes, output_schema, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (name, version) DO NOTHING
        "#,
    )
    .bind("hello-world")
    .bind(1)
    .bind("A simple hello world workflow that echoes and transforms text")
    .bind(&hello_inputs)
    .bind(&hello_world)
    .bind(&hello_outputs)
    .bind(serde_json::json!({"category": "demo", "author": "ishikari"}))
    .execute(pool)
    .await?;

    // Data processing workflow
    let data_pipeline = serde_json::json!({
        "fetch": {
            "type": "http/get",
            "inputs": {
                "url": "{{inputs.api_url}}",
                "headers": {
                    "Accept": "application/json"
                }
            }
        },
        "validate": {
            "type": "json/validate",
            "depends_on": ["fetch"],
            "inputs": {
                "data": "{{fetch.outputs.body}}",
                "schema": "{{inputs.schema}}"
            }
        },
        "transform": {
            "type": "json/transform",
            "depends_on": ["validate"],
            "inputs": {
                "data": "{{validate.outputs.data}}",
                "mapping": "{{inputs.field_mapping}}"
            }
        },
        "store": {
            "type": "db/insert",
            "depends_on": ["transform"],
            "inputs": {
                "table": "{{inputs.target_table}}",
                "records": "{{transform.outputs.records}}"
            }
        }
    });

    let data_inputs = serde_json::json!({
        "api_url": {
            "type": "string",
            "description": "The API URL to fetch data from",
            "required": true
        },
        "schema": {
            "type": "object",
            "description": "JSON schema to validate against",
            "required": false
        },
        "field_mapping": {
            "type": "object",
            "description": "Field mapping for transformation",
            "required": true
        },
        "target_table": {
            "type": "string",
            "description": "Database table to store results",
            "required": true
        }
    });

    let data_outputs = serde_json::json!({
        "record_count": "{{store.outputs.inserted_count}}",
        "status": "{{store.outputs.status}}"
    });

    sqlx::query(
        r#"
        INSERT INTO ishikari_workflow_definitions (name, version, description, input_schema, nodes, output_schema, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (name, version) DO NOTHING
        "#,
    )
    .bind("data-pipeline")
    .bind(1)
    .bind("Fetch data from an API, validate, transform, and store in database")
    .bind(&data_inputs)
    .bind(&data_pipeline)
    .bind(&data_outputs)
    .bind(serde_json::json!({"category": "etl", "author": "ishikari"}))
    .execute(pool)
    .await?;

    // Notification workflow with conditional branching
    let notification = serde_json::json!({
        "check_preferences": {
            "type": "db/query",
            "inputs": {
                "query": "SELECT email_enabled, sms_enabled FROM user_preferences WHERE user_id = $1",
                "params": ["{{inputs.user_id}}"]
            }
        },
        "send_email": {
            "type": "email/send",
            "depends_on": ["check_preferences"],
            "when": "{{check_preferences.outputs.email_enabled}}",
            "inputs": {
                "to": "{{inputs.email}}",
                "subject": "{{inputs.subject}}",
                "body": "{{inputs.message}}"
            }
        },
        "send_sms": {
            "type": "sms/send",
            "depends_on": ["check_preferences"],
            "when": "{{check_preferences.outputs.sms_enabled}}",
            "inputs": {
                "phone": "{{inputs.phone}}",
                "message": "{{inputs.message}}"
            }
        },
        "log_notification": {
            "type": "db/insert",
            "depends_on": ["send_email", "send_sms"],
            "inputs": {
                "table": "notification_log",
                "records": [{
                    "user_id": "{{inputs.user_id}}",
                    "type": "{{inputs.notification_type}}",
                    "sent_at": "{{now()}}"
                }]
            }
        }
    });

    let notification_inputs = serde_json::json!({
        "user_id": {
            "type": "string",
            "description": "User ID to notify",
            "required": true
        },
        "email": {
            "type": "string",
            "description": "Email address"
        },
        "phone": {
            "type": "string",
            "description": "Phone number for SMS"
        },
        "subject": {
            "type": "string",
            "description": "Email subject"
        },
        "message": {
            "type": "string",
            "description": "Notification message",
            "required": true
        },
        "notification_type": {
            "type": "string",
            "description": "Type of notification",
            "required": true
        }
    });

    sqlx::query(
        r#"
        INSERT INTO ishikari_workflow_definitions (name, version, description, input_schema, nodes, output_schema, metadata)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (name, version) DO NOTHING
        "#,
    )
    .bind("send-notification")
    .bind(1)
    .bind("Send notifications via email and/or SMS based on user preferences")
    .bind(&notification_inputs)
    .bind(&notification)
    .bind(serde_json::json!({}))
    .bind(serde_json::json!({"category": "notifications", "author": "ishikari"}))
    .execute(pool)
    .await?;

    Ok(())
}

/// Create standalone jobs in various states
pub async fn create_sample_jobs(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Some quick jobs
    for i in 1..=5 {
        ishikari::insert(
            SlowJob {
                duration_secs: 1,
                label: format!("Quick job {}", i),
            },
            pool,
        )
        .await?;
    }

    // Some slow jobs
    for i in 1..=3 {
        ishikari::insert(
            SlowJob {
                duration_secs: 10,
                label: format!("Slow job {}", i),
            },
            pool,
        )
        .await?;
    }

    // Jobs that will retry
    ishikari::insert(
        FailingJob {
            fail_until_attempt: 3,
            message: "Eventually succeeds".into(),
        },
        pool,
    )
    .await?;

    ishikari::insert(
        FailingJob {
            fail_until_attempt: 10, // Will exhaust retries
            message: "Always fails".into(),
        },
        pool,
    )
    .await?;

    Ok(())
}
