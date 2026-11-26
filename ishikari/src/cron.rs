//! Periodic jobs.
//!
//! Each [`Cron`] entry inserts a job on a schedule. Every node of an application runs the cron
//! check, and each slot's job is inserted with a unique key naming the entry and the slot, so it
//! runs exactly once however many nodes are up, with no leader election. Slots missed while no
//! node was running are caught up within [`EngineBuilder::cron_catch_up`](crate::EngineBuilder::cron_catch_up).
//!
//! ```rust,no_run
//! use ishikari::{Cron, Engine, Postgres, Queue};
//! # use ishikari::prelude::*;
//! # #[ishikari::job] struct Digest;
//! # #[ishikari::worker] impl Worker for Digest {
//! #     async fn perform(&self, _: Context) -> PerformResult { Complete::default().into() }
//! # }
//!
//! # async fn example(pool: sqlx::PgPool) -> Result<(), ishikari::CronError> {
//! let engine = Engine::builder("app")
//!     .with_queue(Queue::builder("default"))
//!     // Every day at 7:00 in Chicago.
//!     .cron(Cron::new("daily-digest", "0 7 * * *", || Digest)?.timezone(ishikari::chrono_tz::America::Chicago))
//!     .start(Postgres::new(pool));
//! # Ok(())
//! # }
//! ```
//!
//! Entries kept in the database, put and deleted while engines run, are [`Crons`].

use crate::insert::{insert_row, Row};
use crate::{engine::qualified, InsertOpts, Inserted, Postgres, Unique, Worker};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde::Serialize;
use sqlx::{PgConnection, PgPool};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

type InsertFn = Arc<
    dyn Fn(
            Arc<Postgres>,
            InsertOpts,
        ) -> Pin<Box<dyn Future<Output = Result<Inserted, sqlx::Error>> + Send>>
        + Send
        + Sync,
>;

/// A cron pattern, read in a timezone.
#[derive(Clone)]
struct Schedule {
    pattern: String,
    cron: croner::Cron,
    timezone: Tz,
}

impl Schedule {
    fn parse(pattern: &str) -> Result<Schedule, CronError> {
        let cron = croner::Cron::from_str(pattern).map_err(|e| CronError {
            pattern: pattern.to_string(),
            reason: e.to_string(),
        })?;
        Ok(Schedule {
            pattern: pattern.to_string(),
            cron,
            timezone: Tz::UTC,
        })
    }

    /// The first `limit` slots after `after`, up to and including `until`.
    fn slots(
        &self,
        after: DateTime<Utc>,
        until: DateTime<Utc>,
        limit: usize,
    ) -> Vec<DateTime<Utc>> {
        let mut slots = Vec::new();
        let mut from = after.with_timezone(&self.timezone);
        while slots.len() < limit {
            let Ok(next) = self.cron.find_next_occurrence(&from, false) else {
                break;
            };
            let next_utc = next.with_timezone(&Utc);
            if next_utc > until {
                break;
            }
            slots.push(next_utc);
            from = next;
        }
        slots
    }
}

/// The options a slot's job is inserted with: scheduled at the slot, with
/// `{"cron": name, "slot": slot}` in its meta, unique by the entry's name and the slot.
fn slot_opts(name: &str, slot: DateTime<Utc>) -> InsertOpts {
    let slot_text = slot.to_rfc3339();
    InsertOpts::new()
        .schedule_at(slot)
        .meta(serde_json::json!({"cron": name, "slot": slot_text}))
        .unique(Unique::key(format!("ishikari:cron:{name}:{slot_text}")).all_states())
}

/// A job inserted on a schedule.
#[derive(Clone)]
pub struct Cron {
    name: String,
    schedule: Schedule,
    insert: InsertFn,
}

impl Debug for Cron {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cron")
            .field("name", &self.name)
            .field("pattern", &self.schedule.pattern)
            .field("timezone", &self.schedule.timezone)
            .finish()
    }
}

/// A cron pattern that couldn't be parsed, or a cron entry that couldn't be made.
#[derive(Debug, thiserror::Error)]
#[error("invalid cron pattern {pattern:?}: {reason}")]
pub struct CronError {
    pub pattern: String,
    pub reason: String,
}

impl Cron {
    /// Insert the job `make` returns on `pattern`: standard five-field crontab syntax (minute,
    /// hour, day of month, month, day of week), in UTC unless a [`timezone`](Cron::timezone) is
    /// set. `name` identifies the entry; keep it stable, since each slot is unique by name.
    pub fn new<J, F>(name: impl Into<String>, pattern: &str, make: F) -> Result<Cron, CronError>
    where
        J: Debug + Serialize + Worker + Send + Sync + 'static,
        F: Fn() -> J + Send + Sync + 'static,
    {
        let schedule = Schedule::parse(pattern)?;
        let make = Arc::new(make);
        let insert: InsertFn = Arc::new(move |storage: Arc<Postgres>, opts: InsertOpts| {
            let job = make();
            Box::pin(async move {
                let opts = opts.schema_or(storage.schema());
                crate::insert_with(job, opts, &*storage.pool).await
            })
        });
        Ok(Cron {
            name: name.into(),
            schedule,
            insert,
        })
    }

    /// Read the pattern in `timezone` (for example `chrono_tz::America::Chicago`).
    pub fn timezone(mut self, timezone: Tz) -> Self {
        self.schedule.timezone = timezone;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The slots after `after`, up to and including `until` (at most 100).
    pub fn due(&self, after: DateTime<Utc>, until: DateTime<Utc>) -> Vec<DateTime<Utc>> {
        self.schedule.slots(after, until, MAX_SLOTS)
    }

    /// Insert the job for `slot`, unless some node already has. The job is scheduled at the slot
    /// and carries `{"cron": name, "slot": slot}` in its meta.
    pub async fn insert_slot(
        &self,
        storage: Arc<Postgres>,
        slot: DateTime<Utc>,
    ) -> Result<Inserted, sqlx::Error> {
        (self.insert)(storage, slot_opts(&self.name, slot)).await
    }
}

/// A cron entry to keep in the database with [`Crons::put`]: a job, serialized now, inserted on a
/// schedule by every engine on the database until it's deleted.
#[derive(Clone)]
pub struct CronEntry {
    name: String,
    schedule: Schedule,
    worker: &'static str,
    args: serde_json::Value,
    queue: String,
    priority: i32,
    max_attempts: i32,
    tags: Vec<String>,
}

impl Debug for CronEntry {
    // The args may hold personal data; they aren't shown.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CronEntry")
            .field("name", &self.name)
            .field("pattern", &self.schedule.pattern)
            .field("timezone", &self.schedule.timezone)
            .field("worker", &self.worker)
            .field("queue", &self.queue)
            .finish()
    }
}

impl CronEntry {
    /// Insert `job` (a copy of it as it is now) on `pattern`, which reads as [`Cron::new`]'s, in
    /// the job's queue with its max attempts unless they're set here. `name` identifies the
    /// entry: putting another of the same name replaces it.
    pub fn new<J>(name: impl Into<String>, pattern: &str, job: &J) -> Result<CronEntry, CronError>
    where
        J: Debug + Serialize + Worker + Send + Sync + 'static,
    {
        let name = name.into();
        let schedule = Schedule::parse(pattern)?;
        let refused = |reason: String| CronError {
            pattern: pattern.to_string(),
            reason,
        };
        if name.is_empty() {
            return Err(refused("a cron entry needs a name".into()));
        }
        if name.trim() != name || name.chars().any(char::is_control) {
            return Err(refused(format!(
                "a cron entry's name can't start or end with whitespace, nor hold control characters: {name:?}"
            )));
        }
        let args = serde_json::to_value(job as &dyn Worker)
            .map_err(|e| refused(format!("the job couldn't be serialized: {e}")))?;
        Ok(CronEntry {
            name,
            schedule,
            worker: J::worker(),
            args,
            queue: job.queue().to_string(),
            priority: 0,
            max_attempts: job.max_attempts(),
            tags: Vec::new(),
        })
    }

    /// Read the pattern in `timezone` (for example `"America/Chicago".parse()?`).
    pub fn timezone(mut self, timezone: Tz) -> Self {
        self.schedule.timezone = timezone;
        self
    }

    /// Insert the jobs into `queue`, rather than the job's.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    /// The jobs' priority, 0 (the first claimed, and the default) to 3.
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// How many times a job is attempted, rather than the job's own.
    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Tag the jobs.
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A cron entry kept in the database, as [`Crons`] reads it.
#[derive(Clone)]
pub struct StoredCron {
    name: String,
    schedule: Schedule,
    worker: String,
    args: serde_json::Value,
    queue: String,
    priority: i32,
    max_attempts: i32,
    tags: Vec<String>,
    inserted_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Debug for StoredCron {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredCron")
            .field("name", &self.name)
            .field("pattern", &self.schedule.pattern)
            .field("timezone", &self.schedule.timezone)
            .field("worker", &self.worker)
            .field("queue", &self.queue)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

impl StoredCron {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn pattern(&self) -> &str {
        &self.schedule.pattern
    }

    pub fn timezone(&self) -> Tz {
        self.schedule.timezone
    }

    /// The job's type: its `worker`, as its jobs are stored.
    pub fn worker(&self) -> &str {
        &self.worker
    }

    /// The job, serialized: its jobs' `args`.
    pub fn args(&self) -> &serde_json::Value {
        &self.args
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn priority(&self) -> i32 {
        self.priority
    }

    pub fn max_attempts(&self) -> i32 {
        self.max_attempts
    }

    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// When the entry was first put.
    pub fn inserted_at(&self) -> DateTime<Utc> {
        self.inserted_at
    }

    /// When the entry last changed. Its slots are those after this.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// The slots after `after`, up to and including `until` (at most 100).
    pub fn due(&self, after: DateTime<Utc>, until: DateTime<Utc>) -> Vec<DateTime<Utc>> {
        self.schedule.slots(after, until, MAX_SLOTS)
    }

    /// The first slot after `after`, if the pattern has one.
    pub fn next(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.schedule
            .slots(after, DateTime::<Utc>::MAX_UTC, 1)
            .into_iter()
            .next()
    }

    /// Insert the job for `slot`, unless some node already has, as long as the entry is still
    /// as it was read: `None` once it's been deleted or changed. The entry's row is locked
    /// (shared) until the job is in, so a delete waits for it, and no slot is inserted after.
    /// A row another transaction is changing (a `put_in` or `delete_in` not yet committed) is
    /// skipped, not waited on, and reads as changed: its slot waits for the next check, and no
    /// other entry waits for it.
    async fn insert_slot(
        &self,
        storage: &Postgres,
        slot: DateTime<Utc>,
    ) -> Result<Option<Inserted>, sqlx::Error> {
        let schema = storage.schema();
        let mut tx = storage.pool.begin().await?;
        let current: Option<i32> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT 1 FROM {} WHERE name = $1 AND updated_at = $2 FOR SHARE SKIP LOCKED",
            qualified(schema, "ishikari_crons")
        )))
        .bind(&self.name)
        .bind(self.updated_at)
        .fetch_optional(&mut *tx)
        .await?;
        if current.is_none() {
            return Ok(None);
        }
        let mut opts = slot_opts(&self.name, slot).priority(self.priority);
        for tag in &self.tags {
            opts = opts.tag(tag.clone());
        }
        let row = Row::stored(
            self.worker.clone(),
            self.args.clone(),
            self.queue.clone(),
            self.max_attempts,
            &opts,
        );
        let inserted = insert_row(&mut tx, schema, &row, None).await?;
        tx.commit().await?;
        Ok(Some(inserted))
    }
}

/// A row of `ishikari_crons`.
#[derive(sqlx::FromRow)]
struct CronRow {
    name: String,
    pattern: String,
    timezone: String,
    worker: String,
    args: serde_json::Value,
    queue: String,
    priority: i32,
    max_attempts: i32,
    tags: Vec<String>,
    inserted_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// A stored cron entry whose pattern or timezone can't be read (edited by hand): it never runs,
/// and [`Crons::delete`] deletes it.
#[derive(Debug)]
pub struct UnreadableCron {
    name: String,
    updated_at: DateTime<Utc>,
    error: CronError,
}

impl UnreadableCron {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Why it can't be read.
    pub fn error(&self) -> &CronError {
        &self.error
    }
}

impl CronRow {
    /// The entry, unless its pattern or timezone can't be read.
    fn read(self) -> Result<StoredCron, UnreadableCron> {
        let timezone = Tz::from_str(&self.timezone).map_err(|e| CronError {
            pattern: self.pattern.clone(),
            reason: format!("unknown timezone {:?}: {e}", self.timezone),
        });
        let (schedule, timezone) =
            match Schedule::parse(&self.pattern).and_then(|schedule| Ok((schedule, timezone?))) {
                Ok(read) => read,
                Err(error) => {
                    return Err(UnreadableCron {
                        name: self.name,
                        updated_at: self.updated_at,
                        error,
                    })
                }
            };
        Ok(StoredCron {
            name: self.name,
            schedule: Schedule {
                timezone,
                ..schedule
            },
            worker: self.worker,
            args: self.args,
            queue: self.queue,
            priority: self.priority,
            max_attempts: self.max_attempts,
            tags: self.tags,
            inserted_at: self.inserted_at,
            updated_at: self.updated_at,
        })
    }
}

/// Cron entries kept in the database (`ishikari_crons`, schema v11), which the host, or an agent
/// it gives a tool to, puts and deletes while its engines run.
///
/// The entries given to the engine ([`EngineBuilder::cron`](crate::EngineBuilder::cron)) are
/// fixed while it runs. Every engine on the database also reads these on each cron check, so a
/// change is picked up within [`cron_interval`](crate::EngineBuilder::cron_interval), with no
/// restart; a slot's job is inserted once however many nodes are up, as for the engine's own.
/// Each entry holds its job serialized, so its type must be one the engines know (a worker of the
/// host's, or a [`ModuleJob`](crate::execution::ModuleJob)).
///
/// ```rust,no_run
/// use ishikari::{CronEntry, Crons};
/// # use ishikari::prelude::*;
/// # #[ishikari::job] struct Digest { team: i64 }
/// # #[ishikari::worker] impl Worker for Digest {
/// #     async fn perform(&self, _: Context) -> PerformResult { Complete::default().into() }
/// # }
///
/// # async fn example(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// let crons = Crons::new(pool);
/// let entry = CronEntry::new("team-7-digest", "30 8 * * 1-5", &Digest { team: 7 })?
///     .timezone("Europe/Paris".parse()?);
/// crons.put(&entry).await?;
/// // ...
/// crons.delete("team-7-digest").await?;
/// # Ok(())
/// # }
/// ```
///
/// An entry's slots start when it's put: none from before is caught up. A change to it applies
/// to the slots after the change, and once [`delete`](Crons::delete) returns, no node inserts
/// another of its slots. An entry named as one of an engine's own is ignored by that engine,
/// which runs its own.
#[derive(Clone, Debug)]
pub struct Crons {
    pool: PgPool,
    schema: Option<String>,
}

impl Crons {
    /// Entries kept in `pool`'s search path.
    pub fn new(pool: PgPool) -> Self {
        Self { pool, schema: None }
    }

    /// Keep entries in `schema`, as the engine's `Postgres` keeps jobs.
    ///
    /// Panics unless `schema` is [valid](crate::migrations::is_valid_schema).
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(crate::migrations::checked_schema(schema));
        self
    }

    /// The entries an engine's storage runs.
    pub(crate) fn of(storage: &Postgres) -> Self {
        Self {
            pool: (*storage.pool).clone(),
            schema: storage.schema().map(str::to_string),
        }
    }

    fn table(&self) -> String {
        qualified(self.schema.as_deref(), "ishikari_crons")
    }

    /// Keep `entry`, replacing the one of its name. A change applies from now on: the slots
    /// after it insert the new job, on the new pattern. Putting an entry as it already is
    /// changes nothing.
    pub async fn put(&self, entry: &CronEntry) -> Result<StoredCron, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        self.put_in(&mut conn, entry).await
    }

    /// [`put`](Self::put) in the caller's transaction (`&mut tx`), so the entry is kept or not
    /// with the caller's own rows. The engines see it once the transaction commits; while it's
    /// open, a changed entry's slots wait (no other entry's do), and those still due when it
    /// commits are the new version's. It starts from when it was put, not from the commit.
    ///
    /// Under `REPEATABLE READ` or `SERIALIZABLE`, a put racing another of the same name fails
    /// with a serialization error, for the caller to retry its transaction.
    pub async fn put_in(
        &self,
        conn: &mut PgConnection,
        entry: &CronEntry,
    ) -> Result<StoredCron, sqlx::Error> {
        let table = self.table();
        let sql = format!(
            r#"
            WITH put AS (
                INSERT INTO {table} AS c (name, pattern, timezone, worker, args, queue, priority,
                                          max_attempts, tags, inserted_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, clock_timestamp(), clock_timestamp())
                ON CONFLICT (name) DO UPDATE SET
                    pattern = EXCLUDED.pattern, timezone = EXCLUDED.timezone,
                    worker = EXCLUDED.worker, args = EXCLUDED.args, queue = EXCLUDED.queue,
                    priority = EXCLUDED.priority, max_attempts = EXCLUDED.max_attempts,
                    tags = EXCLUDED.tags, updated_at = clock_timestamp()
                WHERE (c.pattern, c.timezone, c.worker, c.args, c.queue, c.priority,
                       c.max_attempts, c.tags)
                      IS DISTINCT FROM
                      (EXCLUDED.pattern, EXCLUDED.timezone, EXCLUDED.worker, EXCLUDED.args,
                       EXCLUDED.queue, EXCLUDED.priority, EXCLUDED.max_attempts, EXCLUDED.tags)
                RETURNING c.*
            )
            SELECT * FROM put
            UNION ALL
            SELECT * FROM {table} WHERE name = $1 AND NOT EXISTS (SELECT 1 FROM put)
            "#
        );
        // An entry put by a statement that began after this one isn't visible to its second
        // half; trying again sees it. (Under READ COMMITTED each try is a new snapshot, in a
        // transaction too; under REPEATABLE READ the conflict is an error instead.) `updated_at`
        // is `clock_timestamp()`, not `now()`: in a long transaction that's the transaction's
        // start, and slots between it and the put would follow the new version.
        for _ in 0..3 {
            let row: Option<CronRow> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(&entry.name)
                .bind(&entry.schedule.pattern)
                .bind(entry.schedule.timezone.name())
                .bind(entry.worker)
                .bind(&entry.args)
                .bind(&entry.queue)
                .bind(entry.priority)
                .bind(entry.max_attempts)
                .bind(&entry.tags)
                .fetch_optional(&mut *conn)
                .await?;
            if let Some(row) = row {
                return row
                    .read()
                    .map_err(|u| sqlx::Error::Decode(Box::new(u.error)));
            }
        }
        Err(sqlx::Error::Protocol(
            "cron entry put kept conflicting with one it couldn't see".into(),
        ))
    }

    /// Delete the entry `name`: whether there was one. Once this returns, no node inserts
    /// another of its slots (a job already inserted still runs).
    pub async fn delete(&self, name: &str) -> Result<bool, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        self.delete_in(&mut conn, name).await
    }

    /// [`delete`](Self::delete) in the caller's transaction (`&mut tx`). Once it commits, no
    /// node inserts another of the entry's slots; while it's open, they wait (no other entry's
    /// do), and if it rolls back, those still due are inserted.
    pub async fn delete_in(
        &self,
        conn: &mut PgConnection,
        name: &str,
    ) -> Result<bool, sqlx::Error> {
        let deleted = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {} WHERE name = $1",
            self.table()
        )))
        .bind(name)
        .execute(conn)
        .await?;
        Ok(deleted.rows_affected() > 0)
    }

    /// The entry `name`, if there is one (and it can be read).
    pub async fn get(&self, name: &str) -> Result<Option<StoredCron>, sqlx::Error> {
        Ok(self
            .read(Some(name))
            .await?
            .into_iter()
            .find_map(Result::ok))
    }

    /// Every entry, by name. One whose pattern or timezone can't be read (edited by hand) is
    /// left out, with a warning.
    pub async fn list(&self) -> Result<Vec<StoredCron>, sqlx::Error> {
        Ok(self
            .read(None)
            .await?
            .into_iter()
            .filter_map(|entry| match entry {
                Ok(entry) => Some(entry),
                Err(u) => {
                    tracing::warn!(cron = %u.name, error = %u.error, "cron entry can't be read; skipped");
                    None
                }
            })
            .collect())
    }

    /// Every entry, by name, with those that can't be read (edited by hand) as errors: they
    /// never run, and [`delete`](Self::delete) deletes them.
    pub async fn list_all(&self) -> Result<Vec<Result<StoredCron, UnreadableCron>>, sqlx::Error> {
        self.read(None).await
    }

    /// The entries (or the one named), by name, each read or not.
    async fn read(
        &self,
        name: Option<&str>,
    ) -> Result<Vec<Result<StoredCron, UnreadableCron>>, sqlx::Error> {
        let rows: Vec<CronRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT * FROM {} WHERE $1::text IS NULL OR name = $1 ORDER BY name",
            self.table()
        )))
        .bind(name)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(CronRow::read).collect())
    }
}

/// The most slots one check inserts for an entry; the next check goes on from the last.
const MAX_SLOTS: usize = 100;

/// An entry a check runs: one of the engine's, or one kept in the database.
#[derive(Clone, Copy)]
enum Entry<'a> {
    Engine(&'a Cron),
    Stored(&'a StoredCron),
}

impl Entry<'_> {
    fn name(&self) -> &str {
        match self {
            Entry::Engine(cron) => &cron.name,
            Entry::Stored(cron) => &cron.name,
        }
    }

    fn schedule(&self) -> &Schedule {
        match self {
            Entry::Engine(cron) => &cron.schedule,
            Entry::Stored(cron) => &cron.schedule,
        }
    }

    /// No slot at or before this is inserted: a stored entry's start, when it last changed.
    fn since(&self) -> Option<DateTime<Utc>> {
        match self {
            Entry::Engine(_) => None,
            Entry::Stored(cron) => Some(cron.updated_at),
        }
    }

    /// What its progress is kept by: an engine's entry by its place among them (two may share a
    /// name), a stored one by its name and version, so a changed entry starts afresh from its
    /// change, not from how far the old one got.
    fn key(&self, index: usize) -> Key {
        match self {
            Entry::Engine(_) => Key::Engine(index),
            Entry::Stored(cron) => Key::Stored(cron.name.clone(), cron.updated_at),
        }
    }

    /// The slot's job, inserted or found; `None` when the entry is no longer as it was read.
    async fn insert_slot(
        &self,
        storage: &Arc<Postgres>,
        slot: DateTime<Utc>,
    ) -> Result<Option<Inserted>, sqlx::Error> {
        match self {
            Entry::Engine(cron) => cron.insert_slot(storage.clone(), slot).await.map(Some),
            Entry::Stored(cron) => cron.insert_slot(storage, slot).await,
        }
    }

    /// Insert the slots that fell due after `after`, up to `until`, in order, stopping at the
    /// first that fails and after [`MAX_SLOTS`]. Returns how many jobs were inserted (slots
    /// another node had inserted don't count) and the time through which every slot is in:
    /// `until`, or the last slot done before stopping. That is the next check's `after`.
    async fn insert_due(
        &self,
        storage: &Arc<Postgres>,
        after: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> (usize, DateTime<Utc>) {
        let name = self.name();
        let mut slots = self.schedule().slots(after, until, MAX_SLOTS + 1);
        let more = slots.len() > MAX_SLOTS;
        slots.truncate(MAX_SLOTS);
        let mut inserted = 0;
        let mut done = after;
        for slot in slots {
            match self.insert_slot(storage, slot).await {
                Ok(Some(Inserted::New(job))) => {
                    tracing::info!(cron = %name, %slot, id = job.id, "cron job inserted");
                    inserted += 1;
                }
                Ok(Some(Inserted::Duplicate(_))) => {}
                Ok(None) => {
                    tracing::debug!(cron = %name, "cron entry changed or deleted; reading it again next check");
                    return (inserted, done);
                }
                Err(e) => {
                    tracing::error!(cron = %name, %slot, error = %e, "failed to insert cron job; retrying next check");
                    return (inserted, done);
                }
            }
            done = slot;
        }
        if more {
            tracing::warn!(cron = %name, through = %done, "more than {MAX_SLOTS} cron slots due; inserting the rest next check");
            return (inserted, done);
        }
        (inserted, until)
    }
}

/// What an entry's progress is kept by; see [`Entry::key`].
#[derive(Clone, PartialEq, Eq, Hash)]
enum Key {
    Engine(usize),
    Stored(String, DateTime<Utc>),
}

/// A node's cron checks: the engine's entries and those kept in the database, and how far each
/// is in.
pub(crate) struct Checks {
    engine: Vec<Cron>,
    stored: Crons,
    /// The time through which each entry's slots are in.
    done: HashMap<Key, DateTime<Utc>>,
    /// Stored entries named as one of the engine's, and those that can't be read (by name and
    /// version), as of the last read: each is warned of once, not on every check.
    shadowed: HashSet<String>,
    unreadable: HashSet<(String, DateTime<Utc>)>,
    /// Whether the last read of the stored entries failed.
    read_failed: bool,
}

impl Checks {
    pub(crate) fn new(engine: Vec<Cron>, storage: &Postgres) -> Self {
        Self {
            engine,
            stored: Crons::of(storage),
            done: HashMap::new(),
            shadowed: HashSet::new(),
            unreadable: HashSet::new(),
            read_failed: false,
        }
    }

    /// Read the stored entries, then insert each entry's slots that fell due after its `done`
    /// time (but not before `floor`, nor a stored entry's last change), up to `until`, and move
    /// `done` on through the slots that are in. A slot that fails to insert stays due, so the
    /// next check tries it again. If the stored entries can't be read, the engine's still run.
    /// Returns how many jobs were inserted.
    pub(crate) async fn run(
        &mut self,
        storage: &Arc<Postgres>,
        floor: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> usize {
        let stored = self.read_stored().await;
        let engine_names: HashSet<&str> = self.engine.iter().map(|c| c.name.as_str()).collect();
        let mut entries: Vec<Entry<'_>> = self.engine.iter().map(Entry::Engine).collect();
        if let Some(stored) = &stored {
            let mut shadowed = HashSet::new();
            for cron in stored {
                if engine_names.contains(cron.name.as_str()) {
                    if !self.shadowed.contains(&cron.name) {
                        tracing::warn!(cron = %cron.name, "a stored cron entry is named as one of the engine's; it's ignored");
                    }
                    shadowed.insert(cron.name.clone());
                } else {
                    entries.push(Entry::Stored(cron));
                }
            }
            self.shadowed = shadowed;
        }

        let mut inserted = 0;
        let mut live = HashSet::new();
        for (index, entry) in entries.iter().enumerate() {
            let key = entry.key(index);
            let done = self.done.get(&key).copied().unwrap_or(floor);
            let after = done.max(floor).max(entry.since().unwrap_or(floor));
            let (new, through) = entry.insert_due(storage, after, until).await;
            inserted += new;
            self.done.insert(key.clone(), through);
            live.insert(key);
        }
        // Forget the entries that are gone or changed, once the stored ones could be read.
        if stored.is_some() {
            self.done.retain(|key, _| live.contains(key));
        }
        inserted
    }

    /// The stored entries that can be read; `None` if none could be read at all.
    async fn read_stored(&mut self) -> Option<Vec<StoredCron>> {
        let rows = match self.stored.read(None).await {
            Ok(rows) => rows,
            Err(e) => {
                if !self.read_failed {
                    tracing::error!(error = %e, "failed to read the stored cron entries (is the schema at v11?); running the engine's own until they can be");
                    self.read_failed = true;
                }
                return None;
            }
        };
        if self.read_failed {
            tracing::info!("the stored cron entries can be read again");
            self.read_failed = false;
        }
        let mut entries = Vec::new();
        let mut unreadable = HashSet::new();
        for row in rows {
            match row {
                Ok(entry) => entries.push(entry),
                Err(u) => {
                    let key = (u.name, u.updated_at);
                    if !self.unreadable.contains(&key) {
                        tracing::warn!(cron = %key.0, error = %u.error, "a stored cron entry can't be read; skipped");
                    }
                    unreadable.insert(key);
                }
            }
        }
        self.unreadable = unreadable;
        Some(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[crate::job]
    struct Tick;

    #[crate::worker]
    impl Worker for Tick {
        async fn perform(&self, _ctx: crate::Context) -> crate::PerformResult {
            crate::Complete::default().into()
        }
    }

    fn at(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 11, h, m, 0).unwrap()
    }

    #[test]
    fn slots_after_and_up_to() {
        let every_minute = Cron::new("t", "* * * * *", || Tick).unwrap();
        assert_eq!(
            every_minute.due(at(10, 0), at(10, 3)),
            vec![at(10, 1), at(10, 2), at(10, 3)]
        );
        let hourly = Cron::new("h", "0 * * * *", || Tick).unwrap();
        assert_eq!(
            hourly.due(at(9, 30), at(11, 59)),
            vec![at(10, 0), at(11, 0)]
        );
    }

    #[test]
    fn a_timezone_moves_the_slots() {
        // 7:00 in Chicago (CDT, UTC-5) on 2026-10-11 is 12:00 UTC.
        let daily = Cron::new("d", "0 7 * * *", || Tick)
            .unwrap()
            .timezone(chrono_tz::America::Chicago);
        assert_eq!(daily.due(at(0, 0), at(23, 59)), vec![at(12, 0)]);
    }

    #[test]
    fn a_bad_pattern_is_an_error() {
        let err = Cron::new("x", "every day", || Tick).unwrap_err();
        assert_eq!(err.pattern, "every day");
    }

    /// Against a real Postgres, as the integration tests: a migrated schema of its own (dropped
    /// by [`Db::finish`]), or `None` when database tests are switched off.
    struct Db {
        admin: PgPool,
        schema: String,
        storage: Arc<Postgres>,
        crons: Crons,
    }

    impl Db {
        async fn new() -> Option<Db> {
            if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
                return None;
            }
            let url = std::env::var("ISHIKARI_TEST_DATABASE_URL")
                .or_else(|_| std::env::var("DATABASE_URL"))
                .expect(
                    "set ISHIKARI_TEST_DATABASE_URL (or DATABASE_URL), or ISHIKARI_SKIP_DB_TESTS=1",
                );
            let schema = format!(
                "ishikari_t_{}",
                &uuid::Uuid::new_v4().simple().to_string()[..12]
            );
            let admin = PgPool::connect(&url).await.unwrap();
            crate::migrations::migrate(&admin, &schema).await.unwrap();
            Some(Db {
                storage: Arc::new(Postgres::new(admin.clone()).with_schema(&schema)),
                crons: Crons::new(admin.clone()).schema(&schema),
                admin,
                schema,
            })
        }

        /// The jobs' slots of the entry `name`.
        async fn slots(&self, name: &str) -> Vec<DateTime<Utc>> {
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT scheduled_at FROM {}.ishikari_jobs WHERE meta->>'cron' = $1 ORDER BY 1",
                self.schema
            )))
            .bind(name)
            .fetch_all(&self.admin)
            .await
            .unwrap()
        }

        async fn finish(self) {
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "DROP SCHEMA {} CASCADE",
                self.schema
            )))
            .execute(&self.admin)
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn an_entry_read_before_a_delete_or_change_inserts_nothing_after_it() {
        let Some(db) = Db::new().await else {
            return;
        };
        let entry = CronEntry::new("e", "* * * * *", &Tick).unwrap();
        let read = db.crons.put(&entry).await.unwrap();
        let slot = at(10, 1);

        // Changed since it was read: the slot is the new version's to insert.
        let changed = db.crons.put(&entry.clone().priority(1)).await.unwrap();
        assert!(read.insert_slot(&db.storage, slot).await.unwrap().is_none());
        // Deleted since it was read.
        assert!(db.crons.delete("e").await.unwrap());
        assert!(changed
            .insert_slot(&db.storage, slot)
            .await
            .unwrap()
            .is_none());
        assert!(db.slots("e").await.is_empty());

        // As it is, it inserts.
        let current = db.crons.put(&entry).await.unwrap();
        assert!(current
            .insert_slot(&db.storage, slot)
            .await
            .unwrap()
            .is_some());
        assert_eq!(db.slots("e").await, [slot]);

        db.finish().await;
    }

    /// A node's clock ahead of the database's: its check before an entry's change runs past the
    /// change's time, so the old version's progress is ahead of the new version's start. The
    /// new version still inserts every slot after its change.
    #[tokio::test]
    async fn a_changed_entry_starts_from_its_change_however_far_the_old_one_got() {
        let Some(db) = Db::new().await else {
            return;
        };
        let yearly = CronEntry::new("chg", "0 0 1 1 *", &Tick).unwrap();
        db.crons.put(&yearly).await.unwrap();
        let mut checks = Checks::new(vec![], &db.storage);
        let ahead = chrono::Duration::seconds(3);
        let catch_up = chrono::Duration::seconds(10);
        let until = Utc::now() + ahead;
        checks.run(&db.storage, until - catch_up, until).await;

        let every_second = CronEntry::new("chg", "* * * * * *", &Tick).unwrap();
        let changed = db.crons.put(&every_second).await.unwrap();
        assert!(changed.updated_at() < until);
        let until = Utc::now() + ahead;
        checks.run(&db.storage, until - catch_up, until).await;

        assert_eq!(
            db.slots("chg").await,
            changed.due(changed.updated_at(), until)
        );

        db.finish().await;
    }

    /// A transaction holding one entry's row (a `delete_in` not yet committed) holds up no other
    /// entry's slots, on any node: its own wait for the next check after it ends.
    #[tokio::test]
    async fn an_open_transaction_on_one_entry_holds_up_no_other() {
        let Some(db) = Db::new().await else {
            return;
        };
        // Read in name order: the held entry first.
        for name in ["a-held", "b-free"] {
            let entry = CronEntry::new(name, "* * * * * *", &Tick).unwrap();
            db.crons.put(&entry).await.unwrap();
        }
        let mut tx = db.admin.begin().await.unwrap();
        assert!(db.crons.delete_in(&mut tx, "a-held").await.unwrap());

        let mut checks = Checks::new(vec![], &db.storage);
        let until = Utc::now() + chrono::Duration::seconds(2);
        let floor = until - chrono::Duration::seconds(10);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            checks.run(&db.storage, floor, until),
        )
        .await
        .expect("the check waited on the held entry");
        assert!(!db.slots("b-free").await.is_empty());
        assert!(db.slots("a-held").await.is_empty());

        // Rolled back: the held entry's slots are still due.
        tx.rollback().await.unwrap();
        checks.run(&db.storage, floor, until).await;
        assert!(!db.slots("a-held").await.is_empty());

        db.finish().await;
    }

    #[test]
    fn the_next_slot() {
        let hourly = Cron::new("h", "0 * * * *", || Tick).unwrap();
        let stored = StoredCron {
            name: "h".into(),
            schedule: hourly.schedule.clone(),
            worker: "w".into(),
            args: serde_json::json!({}),
            queue: "default".into(),
            priority: 0,
            max_attempts: 1,
            tags: vec![],
            inserted_at: at(0, 0),
            updated_at: at(0, 0),
        };
        assert_eq!(stored.next(at(9, 30)), Some(at(10, 0)));
        assert_eq!(stored.next(at(10, 0)), Some(at(11, 0)), "after, not at");
        // Months away, past where `due` would stop looking.
        let yearly = Cron::new("y", "0 0 1 1 *", || Tick).unwrap();
        let stored = StoredCron {
            schedule: yearly.schedule.clone(),
            ..stored
        };
        assert_eq!(
            stored.next(at(9, 30)),
            Some(Utc.with_ymd_and_hms(2027, 1, 1, 0, 0, 0).unwrap())
        );
    }
}
