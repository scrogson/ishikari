use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use sqlx::postgres::PgPool;
use std::fs;
use std::path::PathBuf;

/// Current schema version
const CURRENT_VERSION: i32 = 4;

/// Default schema prefix
const DEFAULT_PREFIX: &str = "public";

#[derive(Parser)]
#[command(name = "ishikari")]
#[command(about = "Ishikari job processing framework CLI")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate database migration files
    #[command(visible_alias = "gen", visible_alias = "g")]
    Generate {
        #[command(subcommand)]
        target: GenerateTarget,
    },
}

#[derive(Subcommand)]
enum GenerateTarget {
    /// Generate migration files for Ishikari tables
    Migration {
        /// Output directory for migration files (default: ./migrations)
        #[arg(short, long, default_value = "migrations")]
        output: PathBuf,

        /// Custom schema name (default: public)
        #[arg(short, long)]
        schema: Option<String>,

        /// Target version to migrate to (default: latest)
        #[arg(short, long)]
        version: Option<i32>,

        /// Database URL to check current version (optional)
        #[arg(short, long, env = "DATABASE_URL")]
        database_url: Option<String>,

        /// Force overwrite existing files
        #[arg(short, long)]
        force: bool,
    },
    /// Check the current migrated version
    Version {
        /// Database URL to check
        #[arg(short, long, env = "DATABASE_URL")]
        database_url: String,

        /// Custom schema name (default: public)
        #[arg(short, long)]
        schema: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Generate { target } => handle_generate(target).await,
    }
}

async fn handle_generate(target: GenerateTarget) -> Result<()> {
    match target {
        GenerateTarget::Migration {
            output,
            schema,
            version,
            database_url,
            force,
        } => generate_migration(output, schema, version, database_url, force).await,
        GenerateTarget::Version {
            database_url,
            schema,
        } => check_version(&database_url, schema.as_deref()).await,
    }
}

async fn check_version(database_url: &str, schema: Option<&str>) -> Result<()> {
    let pool = PgPool::connect(database_url)
        .await
        .context("Failed to connect to database")?;

    let prefix = schema.unwrap_or(DEFAULT_PREFIX);
    let version = migrated_version(&pool, prefix).await?;

    if version == 0 {
        println!("No Ishikari tables found in schema '{}'", prefix);
    } else {
        println!("Current version: {} (latest: {})", version, CURRENT_VERSION);
        if version < CURRENT_VERSION {
            println!("Run 'ishikari generate migration' to upgrade");
        } else {
            println!("Schema is up to date");
        }
    }

    Ok(())
}

/// Check the migrated version by reading the table comment on ishikari_jobs
async fn migrated_version(pool: &PgPool, prefix: &str) -> Result<i32> {
    let escaped_prefix = prefix.replace('\'', "''");

    let query = format!(
        r#"
        SELECT pg_catalog.obj_description(pg_class.oid, 'pg_class')
        FROM pg_class
        LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE pg_class.relname = 'ishikari_jobs'
        AND pg_namespace.nspname = '{}'
        "#,
        escaped_prefix
    );

    let result: Option<(Option<String>,)> = sqlx::query_as(&query)
        .fetch_optional(pool)
        .await
        .context("Failed to query table comment")?;

    match result {
        Some((Some(version_str),)) => version_str
            .parse::<i32>()
            .map_err(|_| anyhow::anyhow!("Invalid version in table comment: {}", version_str)),
        _ => Ok(0), // Table doesn't exist or no comment
    }
}

async fn generate_migration(
    output_dir: PathBuf,
    schema: Option<String>,
    target_version: Option<i32>,
    database_url: Option<String>,
    force: bool,
) -> Result<()> {
    let prefix = schema.as_deref().unwrap_or(DEFAULT_PREFIX);
    let target = target_version.unwrap_or(CURRENT_VERSION);

    if !(1..=CURRENT_VERSION).contains(&target) {
        anyhow::bail!(
            "Invalid target version: {}. Valid range is 1-{}",
            target,
            CURRENT_VERSION
        );
    }

    // Determine starting version
    let initial_version = if let Some(ref url) = database_url {
        let pool = PgPool::connect(url)
            .await
            .context("Failed to connect to database")?;
        migrated_version(&pool, prefix).await?
    } else {
        0
    };

    if initial_version >= target {
        println!(
            "Already at version {} (target: {}). Nothing to generate.",
            initial_version, target
        );
        return Ok(());
    }

    // Create output directory if it doesn't exist
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)
            .with_context(|| format!("Failed to create directory: {}", output_dir.display()))?;
        println!("📁 Created directory: {}", output_dir.display());
    }

    let start_version = initial_version + 1;
    let timestamp = generate_migration_timestamp();

    // Build the migration SQL
    let mut migration_sql = String::new();
    migration_sql.push_str("-- Ishikari Migration\n");
    migration_sql.push_str(&format!(
        "-- Generated by ishikari-cli (v{} -> v{})\n",
        initial_version, target
    ));
    migration_sql.push_str(&format!("-- Schema: {}\n\n", prefix));

    // Add schema creation for non-public schemas
    if prefix != DEFAULT_PREFIX {
        migration_sql.push_str(&format!("CREATE SCHEMA IF NOT EXISTS {};\n\n", prefix));
    }

    // Concatenate all version migrations
    for version in start_version..=target {
        let version_sql = get_version_sql(version)?;
        migration_sql.push_str(&format!(
            "-- ============ Version {} ============\n\n",
            version
        ));
        migration_sql.push_str(&version_sql);
        migration_sql.push_str("\n\n");
    }

    // Add version comment update
    migration_sql.push_str(&format!(
        "-- Record the migrated version\nCOMMENT ON TABLE {}.ishikari_jobs IS '{}';\n",
        prefix, target
    ));

    // Apply schema replacements
    let migration_sql = apply_schema_replacements(&migration_sql, prefix);

    // Determine filename
    let filename = if initial_version == 0 {
        format!("{}_create_ishikari_tables.sql", timestamp)
    } else {
        format!(
            "{}_upgrade_ishikari_v{}_to_v{}.sql",
            timestamp, initial_version, target
        )
    };

    let output_path = output_dir.join(&filename);

    // Check if file exists
    if output_path.exists() && !force {
        anyhow::bail!(
            "Migration file already exists: {}\nUse --force to overwrite",
            output_path.display()
        );
    }

    // Write migration file
    fs::write(&output_path, migration_sql)
        .with_context(|| format!("Failed to write migration file: {}", output_path.display()))?;

    println!("✅ Generated migration: {}", output_path.display());

    // Print summary
    println!("\n📋 Migration summary:");
    println!("   From version: {}", initial_version);
    println!("   To version:   {}", target);
    println!("   Schema:       {}", prefix);

    if initial_version == 0 {
        println!("\n   Tables created:");
        println!("   • ishikari_jobs (core job queue)");
        if target >= 2 {
            println!("   • ishikari_workflows (workflow tracking)");
        }
        if target >= 3 {
            println!("   • ishikari_job_dependencies (job dependencies)");
            println!("   • ishikari_saga_steps (saga compensation)");
        }
        if target >= 4 {
            println!("   • ishikari_workflow_definitions (workflow templates)");
            println!("   • ishikari_workflow_runs (execution tracking)");
            println!("   • ishikari_node_executions (node-level tracking)");
        }
    }

    println!("\n📋 Next steps:");
    println!("   1. Review the generated migration file");
    println!("   2. Run: sqlx migrate run --database-url $DATABASE_URL");

    Ok(())
}

fn get_version_sql(version: i32) -> Result<String> {
    match version {
        1 => Ok(include_str!("../migrations/v01.sql").to_string()),
        2 => Ok(include_str!("../migrations/v02.sql").to_string()),
        3 => Ok(include_str!("../migrations/v03.sql").to_string()),
        4 => Ok(include_str!("../migrations/v04.sql").to_string()),
        _ => anyhow::bail!("Unknown migration version: {}", version),
    }
}

fn apply_schema_replacements(sql: &str, prefix: &str) -> String {
    let escaped_prefix = prefix.replace('\'', "''");

    let sql = if prefix == DEFAULT_PREFIX {
        // For public schema, remove the schema prefix entirely
        sql.replace("{SCHEMA}.", "")
            .replace("{SCHEMA}", "public")
            .replace("{CREATE_SCHEMA}", "")
            .replace("{ESCAPED_PREFIX}", &escaped_prefix)
    } else {
        // For custom schema, apply the prefix
        sql.replace("{SCHEMA}", prefix)
            .replace(
                "{CREATE_SCHEMA}",
                &format!("CREATE SCHEMA IF NOT EXISTS {};", prefix),
            )
            .replace("{ESCAPED_PREFIX}", &escaped_prefix)
    };

    sql
}

fn generate_migration_timestamp() -> String {
    Utc::now().format("%Y%m%d%H%M%S").to_string()
}
