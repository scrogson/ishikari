//! Inserting jobs: options, unique jobs and batches.
//!
//! ```rust,no_run
//! use ishikari::{InsertOpts, Unique};
//! use std::time::Duration;
//! # use ishikari::prelude::*;
//! # #[ishikari::job] struct SyncFeeds { town: i64 }
//! # #[ishikari::worker] impl Worker for SyncFeeds {
//! #     async fn perform(&self, _: Context) -> PerformResult { Complete::default().into() }
//! # }
//!
//! # async fn example(pool: sqlx::PgPool) -> Result<(), sqlx::Error> {
//! // In 10 minutes, unless a sync for this town is already waiting or running.
//! let opts = InsertOpts::new()
//!     .schedule_in(Duration::from_secs(600))
//!     .unique(Unique::new().args(["town"]));
//! let inserted = ishikari::insert_with(SyncFeeds { town: 7 }, opts, &pool).await?;
//! if inserted.is_duplicate() {
//!     println!("already queued as job {}", inserted.job().id);
//! }
//! # Ok(())
//! # }
//! ```

use crate::{engine::qualified, Job, JobState, Worker};
use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{Acquire, Postgres as Pg};
use std::fmt::Debug;
use std::time::Duration;
use tracing::info;

/// Options for one insert. Anything left unset comes from the worker (queue, max attempts,
/// unique) or the table's defaults.
#[derive(Clone, Debug, Default)]
pub struct InsertOpts {
    queue: Option<String>,
    priority: Option<i32>,
    scheduled_at: Option<DateTime<Utc>>,
    max_attempts: Option<i32>,
    tags: Vec<String>,
    meta: Option<serde_json::Value>,
    unique: Option<Unique>,
    schema: Option<String>,
}

impl InsertOpts {
    pub fn new() -> Self {
        Self::default()
    }

    /// The queue to run in, instead of the worker's.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// 0 (first) to 3 (last). Defaults to 0.
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Run no earlier than `at`.
    pub fn schedule_at(mut self, at: DateTime<Utc>) -> Self {
        self.scheduled_at = Some(at);
        self
    }

    /// Run no earlier than `delay` from now.
    pub fn schedule_in(mut self, delay: Duration) -> Self {
        self.scheduled_at =
            Some(Utc::now() + chrono::Duration::from_std(delay).unwrap_or_default());
        self
    }

    /// How many attempts, instead of the worker's.
    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    /// Add a tag (shown in the admin, searchable).
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Free-form data stored with the job.
    pub fn meta(mut self, meta: serde_json::Value) -> Self {
        self.meta = Some(meta);
        self
    }

    /// Don't insert when a matching job exists; see [`Unique`]. Overrides the worker's.
    pub fn unique(mut self, unique: Unique) -> Self {
        self.unique = Some(unique);
        self
    }

    /// Insert into `schema` rather than the connection's search path.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub(crate) fn schema_or(mut self, schema: Option<&str>) -> Self {
        if self.schema.is_none() {
            self.schema = schema.map(str::to_string);
        }
        self
    }
}

/// What makes two jobs "the same" for uniqueness, and for how long.
///
/// A unique job holds a key while it is in one of its unique states (by default, until it
/// completes, is discarded or is cancelled). Inserting a job with a key that is held returns the
/// existing job instead. The key is made of the worker, the queue and the job's arguments (all
/// of them, or the ones named with [`args`](Unique::args)); with a [`period`](Unique::period) it
/// also includes the current period, so "once per hour" is `period(1h)` with
/// [`all_states`](Unique::all_states).
#[derive(Clone, Debug)]
pub struct Unique {
    args: Option<Vec<String>>,
    period: Option<Duration>,
    states: Vec<JobState>,
    key: Option<String>,
}

impl Default for Unique {
    fn default() -> Self {
        Self::new()
    }
}

impl Unique {
    /// Unique by worker, queue and all arguments, while waiting, running or retrying.
    pub fn new() -> Self {
        Self {
            args: None,
            period: None,
            states: vec![
                JobState::Pending,
                JobState::Scheduled,
                JobState::Available,
                JobState::Executing,
                JobState::Retryable,
            ],
            key: None,
        }
    }

    /// Unique by an explicit key, regardless of worker, queue and arguments.
    pub fn key(key: impl Into<String>) -> Self {
        Self {
            key: Some(key.into()),
            ..Self::new()
        }
    }

    /// Compare only these argument fields.
    pub fn args<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.args = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Unique within each period of this length (counted from the Unix epoch).
    pub fn period(mut self, period: Duration) -> Self {
        self.period = Some(period);
        self
    }

    /// Hold the key while the job is in one of these states.
    pub fn states(mut self, states: impl IntoIterator<Item = JobState>) -> Self {
        self.states = states.into_iter().collect();
        self
    }

    /// Hold the key in every state, until the job is pruned: with a period, at most one job
    /// per period ever runs.
    pub fn all_states(self) -> Self {
        self.states([
            JobState::Pending,
            JobState::Scheduled,
            JobState::Available,
            JobState::Executing,
            JobState::Retryable,
            JobState::Completed,
            JobState::Discarded,
            JobState::Cancelled,
        ])
    }

    /// The key's text (hashed with md5 in the database).
    fn text(
        &self,
        worker: &str,
        queue: &str,
        args: &serde_json::Value,
        now: DateTime<Utc>,
    ) -> String {
        let mut text = match &self.key {
            Some(key) => key.clone(),
            None => {
                let args = match (&self.args, args) {
                    (Some(fields), serde_json::Value::Object(map)) => serde_json::Value::Object(
                        fields
                            .iter()
                            .filter_map(|f| map.get(f).map(|v| (f.clone(), v.clone())))
                            .collect(),
                    ),
                    _ => args.clone(),
                };
                // serde_json's map is ordered by key, so this is canonical.
                format!("{worker}|{queue}|{args}")
            }
        };
        if let Some(period) = self.period.filter(|p| p.as_secs() > 0) {
            let bucket = now.timestamp().div_euclid(period.as_secs() as i64);
            text.push_str(&format!("|{}:{bucket}", period.as_secs()));
        }
        text
    }
}

/// The result of an insert: the new job, or the existing job a unique insert matched.
#[derive(Clone, Debug)]
pub enum Inserted {
    New(Job),
    Duplicate(Job),
}

impl Inserted {
    pub fn job(&self) -> &Job {
        match self {
            Inserted::New(job) | Inserted::Duplicate(job) => job,
        }
    }

    pub fn into_job(self) -> Job {
        match self {
            Inserted::New(job) | Inserted::Duplicate(job) => job,
        }
    }

    pub fn is_duplicate(&self) -> bool {
        matches!(self, Inserted::Duplicate(_))
    }
}

/// Insert a job with options. With [`Unique`] (from the options or the worker), an existing
/// matching job is returned as [`Inserted::Duplicate`] and nothing is inserted.
///
/// `conn` can be a pool, a connection or a transaction (`&mut *tx`), so the job can be
/// inserted atomically with the caller's own writes.
pub async fn insert_with<'c, J, A>(
    job: J,
    opts: InsertOpts,
    conn: A,
) -> Result<Inserted, sqlx::Error>
where
    J: Debug + Serialize + Worker + Send + Sync + 'static,
    A: Acquire<'c, Database = Pg>,
{
    let mut conn = conn.acquire().await?;
    let row = Row::new(&job, &opts)?;
    insert_row(&mut conn, opts.schema.as_deref(), &row, None).await
}

/// Insert several jobs of one type in one statement, with the same options. Returns the jobs
/// inserted; unique duplicates are skipped.
pub async fn insert_all<'c, J, A>(
    jobs: impl IntoIterator<Item = J>,
    opts: InsertOpts,
    conn: A,
) -> Result<Vec<Job>, sqlx::Error>
where
    J: Debug + Serialize + Worker + Send + Sync + 'static,
    A: Acquire<'c, Database = Pg>,
{
    let rows = jobs
        .into_iter()
        .map(|job| Row::new(&job, &opts))
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Ok(vec![]);
    }
    let mut conn = conn.acquire().await?;
    let schema = opts.schema.as_deref();
    let (jobs, state) = (
        qualified(schema, "ishikari_jobs"),
        qualified(schema, "ishikari_job_state"),
    );
    let col = |f: fn(&Row) -> String| rows.iter().map(f).collect::<Vec<_>>();
    let inserted = sqlx::query_as::<_, Job>(&format!(
        r#"
        INSERT INTO {jobs} (queue, worker, args, max_attempts, priority, tags, meta, state,
                            scheduled_at, unique_key, unique_states)
        SELECT r.queue, r.worker, r.args, r.max_attempts, r.priority, $6::varchar[], $7,
               CASE WHEN r.scheduled_at > now() THEN 'scheduled' ELSE 'available' END::{state},
               coalesce(r.scheduled_at, now()), md5(r.unique_key), $8::text[]::{state}[]
        FROM unnest($1::text[], $2::text[], $3::jsonb[], $4::int[], $5::int[], $9::timestamptz[],
                    $10::text[])
             AS r(queue, worker, args, max_attempts, priority, scheduled_at, unique_key)
        ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL DO NOTHING
        RETURNING *
        "#
    ))
    .bind(col(|r| r.queue.clone()))
    .bind(col(|r| r.worker.to_string()))
    .bind(rows.iter().map(|r| r.args.clone()).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.max_attempts).collect::<Vec<_>>())
    .bind(rows.iter().map(|r| r.priority).collect::<Vec<_>>())
    .bind(&rows[0].tags)
    .bind(&rows[0].meta)
    .bind(&rows[0].unique_states)
    .bind(rows.iter().map(|r| r.scheduled_at).collect::<Vec<_>>())
    .bind(
        rows.iter()
            .map(|r| r.unique_key.clone())
            .collect::<Vec<_>>(),
    )
    .fetch_all(&mut *conn)
    .await?;
    info!(count = inserted.len(), of = rows.len(), "jobs inserted");
    Ok(inserted)
}

/// One job's column values.
pub(crate) struct Row {
    queue: String,
    worker: &'static str,
    args: serde_json::Value,
    max_attempts: i32,
    priority: i32,
    tags: Vec<String>,
    meta: serde_json::Value,
    /// When the caller scheduled it; `None` is the database's `now()`.
    scheduled_at: Option<DateTime<Utc>>,
    unique_key: Option<String>,
    unique_states: Option<Vec<String>>,
    debug: String,
}

impl Row {
    /// The same row with no unique key.
    pub(crate) fn without_unique(mut self) -> Row {
        self.unique_key = None;
        self.unique_states = None;
        self
    }

    pub(crate) fn new<J>(job: &J, opts: &InsertOpts) -> Result<Row, sqlx::Error>
    where
        J: Debug + Serialize + Worker + Send + Sync + 'static,
    {
        let args = serde_json::to_value(job as &dyn Worker)
            .map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
        let queue = opts
            .queue
            .clone()
            .unwrap_or_else(|| job.queue().to_string());
        let worker = J::worker();
        let unique = opts.unique.clone().or_else(|| job.unique());
        let now = Utc::now();
        Ok(Row {
            unique_key: unique.as_ref().map(|u| u.text(worker, &queue, &args, now)),
            unique_states: unique
                .as_ref()
                .map(|u| u.states.iter().map(|s| s.as_str().to_string()).collect()),
            queue,
            worker,
            args,
            max_attempts: opts.max_attempts.unwrap_or_else(|| job.max_attempts()),
            priority: opts.priority.unwrap_or(0),
            tags: opts.tags.clone(),
            meta: opts.meta.clone().unwrap_or_else(|| serde_json::json!({})),
            scheduled_at: opts.scheduled_at,
            debug: format!("{job:?}"),
        })
    }
}

/// Insert one row, returning the existing job when its unique key is held. `state` overrides
/// the state (workflow steps go in `pending`); `workflow_id` and step names come through
/// `meta` and the extra columns of the caller.
pub(crate) async fn insert_row(
    conn: &mut sqlx::PgConnection,
    schema: Option<&str>,
    row: &Row,
    state: Option<(JobState, Option<i64>)>,
) -> Result<Inserted, sqlx::Error> {
    let (jobs, state_type) = (
        qualified(schema, "ishikari_jobs"),
        qualified(schema, "ishikari_job_state"),
    );
    let (forced, workflow_id) = match state {
        Some((s, w)) => (Some(s.as_str()), w),
        None => (None, None),
    };
    let sql = format!(
        r#"
        WITH ins AS (
            INSERT INTO {jobs} (queue, worker, args, max_attempts, priority, tags, meta, state,
                                scheduled_at, unique_key, unique_states, workflow_id)
            VALUES ($1, $2, $3, $4, $5, $6::varchar[], $7,
                    coalesce($8, CASE WHEN $9 > now() THEN 'scheduled' ELSE 'available' END)
                        ::{state_type},
                    coalesce($9, now()), md5($10), $11::text[]::{state_type}[], $12)
            ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL DO NOTHING
            RETURNING *
        )
        SELECT *, false AS duplicate FROM ins
        UNION ALL
        SELECT j.*, true AS duplicate FROM {jobs} j
        WHERE $10 IS NOT NULL AND j.unique_key = md5($10) AND NOT EXISTS (SELECT 1 FROM ins)
        "#
    );
    // A conflict with a job committed after this statement began isn't visible to it; trying
    // again sees it (or inserts, if it was released meanwhile).
    for _ in 0..3 {
        let found: Option<(Job, bool)> = sqlx::query(&sql)
            .bind(&row.queue)
            .bind(row.worker)
            .bind(&row.args)
            .bind(row.max_attempts)
            .bind(row.priority)
            .bind(&row.tags)
            .bind(&row.meta)
            .bind(forced)
            .bind(row.scheduled_at)
            .bind(&row.unique_key)
            .bind(&row.unique_states)
            .bind(workflow_id)
            .fetch_optional(&mut *conn)
            .await?
            .map(|r| {
                use sqlx::{FromRow, Row as _};
                Ok::<_, sqlx::Error>((Job::from_row(&r)?, r.try_get("duplicate")?))
            })
            .transpose()?;
        match found {
            Some((job, false)) => {
                info!("Job inserted id={}, args={}", job.id, row.debug);
                return Ok(Inserted::New(job));
            }
            Some((job, true)) => {
                info!(
                    "Job not inserted: duplicate of id={}, args={}",
                    job.id, row.debug
                );
                return Ok(Inserted::Duplicate(job));
            }
            None => continue,
        }
    }
    Err(sqlx::Error::Protocol(
        "unique insert kept conflicting with a job it couldn't see".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn the_key_uses_worker_queue_and_chosen_args() {
        let now = Utc::now();
        let args = serde_json::json!({"town": 7, "page": 2});
        let all = Unique::new().text("W", "q", &args, now);
        assert_eq!(all, r#"W|q|{"page":2,"town":7}"#);
        let some = Unique::new().args(["town"]).text("W", "q", &args, now);
        assert_eq!(some, r#"W|q|{"town":7}"#);
        assert_eq!(Unique::key("cron:x").text("W", "q", &args, now), "cron:x");
    }

    #[test]
    fn a_period_buckets_the_key() {
        let args = serde_json::json!({});
        let hour = Unique::new().period(Duration::from_secs(3600));
        let at = |h, m| Utc.with_ymd_and_hms(2026, 10, 11, h, m, 0).unwrap();
        assert_eq!(
            hour.text("W", "q", &args, at(9, 0)),
            hour.text("W", "q", &args, at(9, 59))
        );
        assert_ne!(
            hour.text("W", "q", &args, at(9, 59)),
            hour.text("W", "q", &args, at(10, 0))
        );
    }
}
