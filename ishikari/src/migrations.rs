//! Ishikari's database schema, as versioned SQL.
//!
//! Hosts usually write the SQL into their own migrations with `ishikari generate migration`
//! (the `ishikari-cli` crate), so it runs with the rest of their schema. [`migrate`] applies it
//! directly instead, which suits tests and apps without a migration tool.
//!
//! The installed version is recorded as the comment on the `ishikari_jobs` table, so an upgrade
//! only applies the versions after it.
//!
//! ```rust,no_run
//! # async fn example(pool: sqlx::PgPool) -> Result<(), sqlx::Error> {
//! // Everything up to the current version, in the public schema.
//! ishikari::migrations::migrate(&pool, "public").await?;
//!
//! // The SQL for versions 3 and 4 in a tenant schema, to write into a migration file.
//! let sql = ishikari::migrations::sql("tenant_a", 2, 4);
//! # Ok(())
//! # }
//! ```

use sqlx::{PgExecutor, PgPool};

/// The newest schema version.
pub const CURRENT_VERSION: i32 = 7;

/// The schema used when none is given.
pub const DEFAULT_SCHEMA: &str = "public";

const VERSIONS: [&str; CURRENT_VERSION as usize] = [
    include_str!("../migrations/v01.sql"),
    include_str!("../migrations/v02.sql"),
    include_str!("../migrations/v03.sql"),
    include_str!("../migrations/v04.sql"),
    include_str!("../migrations/v05.sql"),
    include_str!("../migrations/v06.sql"),
    include_str!("../migrations/v07.sql"),
];

/// The SQL that takes `schema` from version `from` (0 for a fresh install) to version `to`,
/// creating the schema first when it isn't `public` and recording `to` as the installed
/// version.
///
/// # Panics
///
/// Panics unless `0 <= from < to <= CURRENT_VERSION`.
pub fn sql(schema: &str, from: i32, to: i32) -> String {
    assert!(
        0 <= from && from < to && to <= CURRENT_VERSION,
        "invalid migration range {from}..{to} (current version is {CURRENT_VERSION})"
    );
    let mut out = String::new();
    out.push_str(&format!(
        "-- Ishikari migration v{from} -> v{to}\n-- Schema: {schema}\n\n"
    ));
    if schema != DEFAULT_SCHEMA {
        out.push_str(&format!("CREATE SCHEMA IF NOT EXISTS {schema};\n\n"));
    }
    for version in from + 1..=to {
        out.push_str(&format!(
            "-- ============ Version {version} ============\n\n"
        ));
        out.push_str(VERSIONS[(version - 1) as usize]);
        out.push_str("\n\n");
    }
    out.push_str(&format!(
        "-- Record the migrated version\nCOMMENT ON TABLE {{SCHEMA}}.ishikari_jobs IS '{to}';\n"
    ));
    apply_schema(&out, schema)
}

/// The version installed in `schema`: 0 when ishikari's tables aren't there.
pub async fn migrated_version<'e>(
    executor: impl PgExecutor<'e>,
    schema: &str,
) -> Result<i32, sqlx::Error> {
    let comment: Option<Option<String>> = sqlx::query_scalar(
        "SELECT obj_description(c.oid, 'pg_class') \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = 'ishikari_jobs' AND n.nspname = $1",
    )
    .bind(schema)
    .fetch_optional(executor)
    .await?;
    Ok(comment
        .flatten()
        .and_then(|c| c.trim().parse().ok())
        .unwrap_or(0))
}

/// Bring `schema` up to [`CURRENT_VERSION`], applying only the versions it doesn't have yet.
/// Returns the version it started from.
pub async fn migrate(pool: &PgPool, schema: &str) -> Result<i32, sqlx::Error> {
    let from = migrated_version(pool, schema).await?;
    if from < CURRENT_VERSION {
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(&sql(schema, from, CURRENT_VERSION))
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }
    Ok(from)
}

fn apply_schema(sql: &str, schema: &str) -> String {
    let escaped = schema.replace('\'', "''");
    if schema == DEFAULT_SCHEMA {
        sql.replace("{SCHEMA}.", "")
            .replace("{SCHEMA}", DEFAULT_SCHEMA)
            .replace("{ESCAPED_PREFIX}", &escaped)
    } else {
        sql.replace("{SCHEMA}", schema)
            .replace("{ESCAPED_PREFIX}", &escaped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_schema_has_no_prefix_or_create() {
        let sql = sql("public", 0, CURRENT_VERSION);
        assert!(!sql.contains("{SCHEMA}") && !sql.contains("{ESCAPED_PREFIX}"));
        assert!(!sql.contains("CREATE SCHEMA"));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS ishikari_jobs"));
        assert!(sql.ends_with(&format!(
            "COMMENT ON TABLE ishikari_jobs IS '{CURRENT_VERSION}';\n"
        )));
    }

    #[test]
    fn a_tenant_schema_is_created_and_prefixed() {
        let sql = sql("tenant_a", 2, 4);
        assert!(sql.starts_with("-- Ishikari migration v2 -> v4"));
        assert!(sql.contains("CREATE SCHEMA IF NOT EXISTS tenant_a;"));
        assert!(sql.contains("tenant_a.ishikari_job_dependencies"));
        assert!(!sql.contains("ishikari_workflows ("), "v02 is not included");
        assert!(sql.contains("connamespace = 'tenant_a'::regnamespace"));
    }

    #[test]
    #[should_panic(expected = "invalid migration range")]
    fn an_empty_range_panics() {
        sql("public", CURRENT_VERSION, CURRENT_VERSION);
    }
}
