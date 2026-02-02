# Migrations

This directory is where you should place your generated migrations.

## Generating Migrations

Use the ishikari CLI to generate migrations:

```bash
# Install the CLI
cargo install ishikari-cli

# Generate all migrations (fresh install)
ishikari generate migration

# Generate with database URL to check current version
ishikari generate migration --database-url $DATABASE_URL

# Generate for a specific schema (multi-tenant)
ishikari generate migration --schema my_tenant

# Generate only up to a specific version
ishikari generate migration --version 2

# Check current migrated version
ishikari generate version --database-url $DATABASE_URL
```

## Running Migrations

After generating, run migrations with sqlx:

```bash
sqlx migrate run --database-url $DATABASE_URL
```

## Schema Versions

The CLI tracks schema versions via table comments on `ishikari_jobs`:

| Version | Tables Added |
|---------|--------------|
| 1 | `ishikari_jobs` (core job queue) |
| 2 | `ishikari_workflows` (workflow tracking) |
| 3 | `ishikari_job_dependencies`, `ishikari_saga_steps` |
| 4 | `ishikari_workflow_definitions`, `ishikari_workflow_runs`, `ishikari_node_executions` |

When you provide `--database-url`, the CLI queries the current version and generates only the migrations needed to reach the target version.

## Upgrading

To upgrade an existing installation:

```bash
# Check current version
ishikari generate version --database-url $DATABASE_URL

# Generate upgrade migration
ishikari generate migration --database-url $DATABASE_URL

# Apply the migration
sqlx migrate run --database-url $DATABASE_URL
```
