# ishikari-cli

Command-line tool for the [Ishikari](https://github.com/scrogson/ishikari) job processing framework.

## Installation

```bash
cargo install ishikari-cli
```

## Usage

### Generate Database Migrations

Generate sqlx-compatible migration files for setting up Ishikari job processing tables:

```bash
# Generate migration in default ./migrations directory
ishikari migration generate

# Generate with custom schema
ishikari migration generate --schema jobs

# Generate in custom directory
ishikari migration generate --output db/migrations

# Custom migration name
ishikari migration generate --name setup_job_processing
```

### Command Options

- `--output, -o <DIR>` - Output directory for migration files (default: `migrations/`)
- `--schema, -s <NAME>` - Custom PostgreSQL schema name (default: public schema)
- `--name, -n <PREFIX>` - Migration name prefix (default: `create_ishikari_tables`)
- `--force, -f` - Force overwrite existing migration files

## Examples

### Basic Setup

```bash
# Generate migration
ishikari migration generate

# Run migration
sqlx migrate run --database-url $DATABASE_URL
```

Output: `migrations/20231124165159_create_ishikari_tables.sql`

### Multi-tenant Setup

```bash
# Generate migrations for different tenants
ishikari migration generate --schema tenant_a --name setup_tenant_a_jobs
ishikari migration generate --schema tenant_b --name setup_tenant_b_jobs

# Run migrations
sqlx migrate run --database-url $DATABASE_URL
```

Output:
- `migrations/20231124165159_setup_tenant_a_jobs_in_tenant_a_schema.sql`
- `migrations/20231124170245_setup_tenant_b_jobs_in_tenant_b_schema.sql`

### Integration with Existing Projects

```bash
# Check existing migrations
ls migrations/
# 003_create_user_profiles.sql

# Generate Ishikari migration (automatically timestamped)
ishikari migration generate

# Result: migrations/20231124165159_create_ishikari_tables.sql
```

## Integration with sqlx-cli

The generated migrations are fully compatible with [sqlx-cli](https://github.com/launchbadge/sqlx/tree/main/sqlx-cli):

```bash
# Install sqlx-cli if you haven't already
cargo install sqlx-cli --no-default-features --features native-tls,postgres

# Generate Ishikari migration
ishikari migration generate

# Apply migrations
sqlx migrate run --database-url $DATABASE_URL

# Check migration status
sqlx migrate info --database-url $DATABASE_URL
```

## Features

- **Timestamp-based naming** - Uses YYYYMMDDHHMMSS format (e.g., `20231124165159_create_ishikari_tables.sql`)
- **Schema-aware** - Generates schema-specific SQL for multi-tenant applications
- **Directory creation** - Creates migration directories if they don't exist
- **Collision detection** - Prevents accidental overwrites without `--force`
- **Clear guidance** - Provides next steps after generation

## Why Use ishikari-cli?

- **No manual copying** - No need to manually copy SQL files
- **Perfect integration** - Works seamlessly with existing sqlx-cli workflows
- **Production ready** - Generates migration files suitable for CI/CD pipelines
- **Multi-tenant friendly** - Easy schema-specific migration generation
- **Consistent naming** - Follows standard migration naming conventions