use anyhow::{Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use std::fs;
use std::path::PathBuf;

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
    /// Generate a new migration file for Ishikari tables
    Migration {
        /// Output directory for migration files (default: ./migrations)
        #[arg(short, long, default_value = "migrations")]
        output: PathBuf,

        /// Custom schema name (default: public)
        #[arg(short, long)]
        schema: Option<String>,

        /// Migration name prefix (auto-timestamped)
        #[arg(short, long, default_value = "create_ishikari_tables")]
        name: String,

        /// Force overwrite existing files
        #[arg(short, long)]
        force: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Generate { target } => handle_generate(target),
    }
}

fn handle_generate(target: GenerateTarget) -> Result<()> {
    match target {
        GenerateTarget::Migration {
            output,
            schema,
            name,
            force,
        } => generate_migration(output, schema, name, force),
    }
}

fn generate_migration(
    output_dir: PathBuf,
    schema: Option<String>,
    name: String,
    force: bool,
) -> Result<()> {
    // Create output directory if it doesn't exist
    if !output_dir.exists() {
        fs::create_dir_all(&output_dir)
            .with_context(|| format!("Failed to create directory: {}", output_dir.display()))?;
        println!("📁 Created directory: {}", output_dir.display());
    }

    // Generate timestamp for migration
    let timestamp = generate_migration_timestamp();

    // Load template and generate migration content
    let template = include_str!("../migration_template.sql");
    let (migration_content, filename, has_schema) = if let Some(ref schema_name) = schema {
        let content = generate_migration_from_template(template, Some(schema_name));
        let filename = format!("{}_{}.sql", timestamp, name);
        (content, filename, true)
    } else {
        let content = generate_migration_from_template(template, None);
        let filename = format!("{}_{}.sql", timestamp, name);
        (content, filename, false)
    };

    let output_path = output_dir.join(&filename);

    // Check if file exists and handle force flag
    if output_path.exists() && !force {
        anyhow::bail!(
            "Migration file already exists: {}\nUse --force to overwrite",
            output_path.display()
        );
    }

    // Write migration file
    fs::write(&output_path, migration_content)
        .with_context(|| format!("Failed to write migration file: {}", output_path.display()))?;

    println!("✅ Generated migration: {}", output_path.display());

    // Print next steps
    println!("\n📋 Next steps:");
    println!("   1. Review the generated migration file");
    println!("   2. Run: sqlx migrate run --database-url $DATABASE_URL");
    println!("   3. Your Ishikari tables will be ready!");

    if has_schema {
        println!("\n💡 Note: This migration creates tables in a custom schema.");
        println!("   Make sure your application is configured to use the same schema.");
    }

    Ok(())
}

fn generate_migration_timestamp() -> String {
    // Generate timestamp in the format YYYYMMDDHHMMSS (e.g., 20231124165159)
    Utc::now().format("%Y%m%d%H%M%S").to_string()
}

fn generate_migration_from_template(template: &str, schema: Option<&str>) -> String {
    match schema {
        Some(schema_name) => {
            // For custom schema: replace {SCHEMA} with schema name and {CREATE_SCHEMA} with CREATE SCHEMA statement
            let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {};", schema_name);
            template
                .replace("{SCHEMA}", schema_name)
                .replace("{CREATE_SCHEMA}", &create_schema)
        }
        None => {
            // For public schema: replace {SCHEMA} with empty string and {CREATE_SCHEMA} with empty string
            template
                .replace("{SCHEMA}.", "")
                .replace("{CREATE_SCHEMA}", "")
        }
    }
}
