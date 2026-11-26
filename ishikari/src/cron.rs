//! Periodic jobs.
//!
//! Each [`Cron`] entry inserts a job on a schedule. Every node of an application runs the cron
//! check, and each slot's job is inserted with a unique key naming the entry and the slot, so it
//! runs exactly once however many nodes are up, with no leader election. Slots missed while no
//! node was running are caught up within [`EngineBuilder::cron_catch_up`](crate::EngineBuilder::cron_catch_up):
//! all of them, the latest or none, by each entry's [`CatchUp`].
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

impl Schedule {
    /// The last slot after `after`, up to and including `until`.
    fn last_slot(&self, after: DateTime<Utc>, until: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let mut last = None;
        let mut from = after.with_timezone(&self.timezone);
        while let Ok(next) = self.cron.find_next_occurrence(&from, false) {
            if next.with_timezone(&Utc) > until {
                break;
            }
            last = Some(next.with_timezone(&Utc));
            from = next;
        }
        last
    }
}

/// What an entry does with the slots it missed: those that fell due while no node was checking it
/// (all down, or not yet started), within
/// [`cron_catch_up`](crate::EngineBuilder::cron_catch_up). A slot that falls due while a node is
/// up isn't missed, however many fall due between two checks. Nor is one of an entry kept in the
/// database ([`Crons`]) that a running node sees late: put, changed or resumed in a transaction
/// that committed after the node's check, its slots from when it was put run on that node's next.
/// But once a check of a node failed to read the stored entries (or before its first check), an
/// entry it hadn't read before (put, changed or resumed since its last read) missed the slots due
/// until then, as one the node couldn't see.
///
/// A slot a node tried to insert and couldn't (the database refused it, the entry's row was
/// held by a transaction, or the stored entries couldn't be read, so the node ran those it read
/// last) is tried again on its next check: with [`All`](CatchUp::All) as it is, with the others
/// only while it's the newest late slot and no slot falls due on time, so an outage of the
/// database inserts at most one late slot. Once an entry is changed, the slots of its old version
/// still to insert never are: the new version runs from its change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum CatchUp {
    /// Insert every missed slot, in order: for cheap, idempotent jobs, where each run counts.
    #[default]
    All,
    /// Insert only the newest missed slot (none if a slot falls due on time): for a sync, a
    /// report, a digest, where only the latest run matters.
    Latest,
    /// Insert no missed slot: the next runs on its own slot. For a job whose late run is worse
    /// than none, or an expensive one.
    None,
}

impl CatchUp {
    /// As it's stored: `all`, `latest` or `none`.
    pub fn as_str(self) -> &'static str {
        match self {
            CatchUp::All => "all",
            CatchUp::Latest => "latest",
            CatchUp::None => "none",
        }
    }
}

impl std::fmt::Display for CatchUp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CatchUp {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "all" => Ok(CatchUp::All),
            "latest" => Ok(CatchUp::Latest),
            "none" => Ok(CatchUp::None),
            other => Err(format!("unknown catch-up {other:?}: all, latest or none")),
        }
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
    catch_up: CatchUp,
    insert: InsertFn,
}

impl Debug for Cron {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cron")
            .field("name", &self.name)
            .field("pattern", &self.schedule.pattern)
            .field("timezone", &self.schedule.timezone)
            .field("catch_up", &self.catch_up)
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
            catch_up: CatchUp::All,
            insert,
        })
    }

    /// Read the pattern in `timezone` (for example `chrono_tz::America::Chicago`).
    pub fn timezone(mut self, timezone: Tz) -> Self {
        self.schedule.timezone = timezone;
        self
    }

    /// What to do with the slots missed while no node ran: [`CatchUp::All`] (the default) unless
    /// set here.
    pub fn catch_up(mut self, catch_up: CatchUp) -> Self {
        self.catch_up = catch_up;
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
    catch_up: CatchUp,
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
            .field("catch_up", &self.catch_up)
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
            catch_up: CatchUp::All,
        })
    }

    /// What to do with the slots missed while no node ran: [`CatchUp::All`] (the default) unless
    /// set here.
    pub fn catch_up(mut self, catch_up: CatchUp) -> Self {
        self.catch_up = catch_up;
        self
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
    catch_up: CatchUp,
    paused: bool,
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
            .field("catch_up", &self.catch_up)
            .field("paused", &self.paused)
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

    /// When the entry was first put: when the transaction that put it began, as for a job's
    /// `inserted_at`, so a job inserted with it (as by [`run_now_in`](Crons::run_now_in)) is
    /// inserted since it.
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

    /// What it does with the slots missed while no node ran.
    pub fn catch_up(&self) -> CatchUp {
        self.catch_up
    }

    /// Whether it's [paused](Crons::pause): it inserts no slot until it's resumed.
    pub fn paused(&self) -> bool {
        self.paused
    }

    /// The first slot after `after` (and after the entry's last change, since no slot before
    /// that runs), if the pattern has one. This is the pattern's, paused or not: a paused entry
    /// runs none (see [`paused`](Self::paused)).
    ///
    /// Its last run is a job: of those whose meta has `{"cron": name}` (`meta @> ...`, which
    /// the jobs' `meta` index serves) inserted since its [`inserted_at`](Self::inserted_at) (not
    /// those of a deleted entry of the same name), the one with the latest `"slot"`. Slots are
    /// RFC 3339 in UTC, so their text sorts as their time does; not `scheduled_at`, which a
    /// retry or a snooze moves on.
    pub fn next(&self, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.schedule
            .slots(after.max(self.updated_at), DateTime::<Utc>::MAX_UTC, 1)
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
    catch_up: String,
    paused: bool,
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
        let catch_up = CatchUp::from_str(&self.catch_up).map_err(|reason| CronError {
            pattern: self.pattern.clone(),
            reason,
        });
        let read = Schedule::parse(&self.pattern)
            .and_then(|schedule| Ok((schedule, timezone?, catch_up?)));
        let (schedule, timezone, catch_up) = match read {
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
            catch_up,
            paused: self.paused,
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
/// to the slots after the change, and once [`delete`](Crons::delete) (or
/// [`pause`](Crons::pause)) returns, no node inserts another of its slots. An entry named as one
/// of an engine's own is ignored by that engine, which runs its own.
///
/// # Names
///
/// A name is unique in the schema, across every owner of entries. Where entries belong to
/// tenants or users, start each name with its owner's (`"tenant-42:digest"`), so two never
/// collide, and [`list_by_prefix`](Crons::list_by_prefix) lists one owner's.
///
/// # With the caller's own rows
///
/// [`put_in`](Crons::put_in), [`delete_in`](Crons::delete_in), [`pause_in`](Crons::pause_in),
/// [`resume_in`](Crons::resume_in) and [`run_now_in`](Crons::run_now_in) take the caller's
/// transaction, so an entry is kept or dropped with the rows it belongs to (a subscription and
/// its renewal):
///
/// ```rust,no_run
/// # use ishikari::{CronEntry, Crons};
/// # use ishikari::prelude::*;
/// # #[ishikari::job] struct Renew { subscription: i64 }
/// # #[ishikari::worker] impl Worker for Renew {
/// #     async fn perform(&self, _: Context) -> PerformResult { Complete::default().into() }
/// # }
/// # async fn example(pool: sqlx::PgPool, crons: Crons) -> Result<(), Box<dyn std::error::Error>> {
/// let mut tx = pool.begin().await?;
/// let id: i64 = sqlx::query_scalar("INSERT INTO subscriptions (plan) VALUES ('monthly') RETURNING id")
///     .fetch_one(&mut *tx)
///     .await?;
/// let renew = CronEntry::new(format!("subscription-{id}:renew"), "0 9 1 * *", &Renew { subscription: id })?;
/// crons.put_in(&mut tx, &renew).await?;
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
///
/// # A job already inserted still runs
///
/// A slot's job inserted just before a delete or a pause (it may be waiting for a free slot in its
/// queue) still runs. A worker for whom that matters checks its entry first, which the job's meta
/// names (`"cron"`). This one lets a job [run now](Crons::run_now) (`"run_now"`) run anyway:
///
/// ```rust,no_run
/// use ishikari::Crons;
/// # use ishikari::prelude::*;
/// # #[ishikari::job] struct Digest { team: i64 }
///
/// #[ishikari::worker]
/// impl Worker for Digest {
///     async fn perform(&self, ctx: Context) -> PerformResult {
///         let job = ctx.job();
///         let meta = job.meta.as_ref();
///         let entry = meta.and_then(|meta| meta["cron"].as_str());
///         let run_now = meta.is_some_and(|meta| meta["run_now"] == true);
///         if let (Some(name), false) = (entry, run_now) {
///             let crons = ctx.state::<Crons>()?;
///             match crons.get(name).await? {
///                 Some(entry) if !entry.paused() => {}
///                 _ => return Cancel(Some(format!("{name} was deleted or paused"))).into(),
///             }
///         }
///         // ... send the digest
///         Complete::default().into()
///     }
/// }
/// ```
///
/// # Cost
///
/// Every node reads every entry on each check, and each slot it inserts is a transaction of its
/// own: fine for thousands of entries.
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
    /// changes nothing. A paused entry stays paused.
    pub async fn put(&self, entry: &CronEntry) -> Result<StoredCron, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        self.put_in(&mut conn, entry).await
    }

    /// [`put`](Self::put) in the caller's transaction (`&mut tx`), so the entry is kept or not
    /// with the caller's own rows. The engines see it once the transaction commits; while it's
    /// open, the entry's slots wait (even if the put changes nothing; no other entry's do), and
    /// those still due when it commits are the new version's. It starts from when it was put,
    /// not from the commit: the slots between are inserted when it commits (those within
    /// [`cron_catch_up`](crate::EngineBuilder::cron_catch_up)), whatever its [`CatchUp`], as
    /// they fell due while the nodes were up; but on a node that failed to read the stored
    /// entries between the put and its first read of it, those due until then were missed (see
    /// [`CatchUp`]). Keep the transaction short.
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
                                          max_attempts, tags, catch_up, inserted_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), clock_timestamp())
                ON CONFLICT (name) DO UPDATE SET
                    pattern = EXCLUDED.pattern, timezone = EXCLUDED.timezone,
                    worker = EXCLUDED.worker, args = EXCLUDED.args, queue = EXCLUDED.queue,
                    priority = EXCLUDED.priority, max_attempts = EXCLUDED.max_attempts,
                    tags = EXCLUDED.tags, catch_up = EXCLUDED.catch_up,
                    updated_at = clock_timestamp()
                WHERE (c.pattern, c.timezone, c.worker, c.args, c.queue, c.priority,
                       c.max_attempts, c.tags, c.catch_up)
                      IS DISTINCT FROM
                      (EXCLUDED.pattern, EXCLUDED.timezone, EXCLUDED.worker, EXCLUDED.args,
                       EXCLUDED.queue, EXCLUDED.priority, EXCLUDED.max_attempts, EXCLUDED.tags,
                       EXCLUDED.catch_up)
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
                .bind(entry.catch_up.as_str())
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

    /// Pause the entry `name`: whether there is one. Once this returns, no node inserts another
    /// of its slots (a job already inserted still runs), as after a [`delete`](Self::delete), but
    /// it's kept, listed, and [`resume`](Self::resume)d as it was. Pausing a paused entry changes
    /// nothing.
    pub async fn pause(&self, name: &str) -> Result<bool, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        self.pause_in(&mut conn, name).await
    }

    /// [`pause`](Self::pause) in the caller's transaction (`&mut tx`). Until it commits, the
    /// entry's slots wait, and another pause or resume of it does too.
    pub async fn pause_in(&self, conn: &mut PgConnection, name: &str) -> Result<bool, sqlx::Error> {
        self.set_paused(conn, name, true).await
    }

    /// Resume the entry `name`: whether there is one. It runs from now, as if it had just been
    /// put: none of the slots while it was paused is caught up. Resuming an entry that isn't
    /// paused changes nothing.
    pub async fn resume(&self, name: &str) -> Result<bool, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        self.resume_in(&mut conn, name).await
    }

    /// [`resume`](Self::resume) in the caller's transaction (`&mut tx`). Until it commits, the
    /// entry's slots wait (even if it wasn't paused), and another pause or resume of it does
    /// too.
    pub async fn resume_in(
        &self,
        conn: &mut PgConnection,
        name: &str,
    ) -> Result<bool, sqlx::Error> {
        self.set_paused(conn, name, false).await
    }

    /// Pause or resume: a new `updated_at`, so a slot read before it isn't inserted (the guard in
    /// [`StoredCron::insert_slot`]) and none from before it is caught up. Only a change moves it.
    async fn set_paused(
        &self,
        conn: &mut PgConnection,
        name: &str,
        paused: bool,
    ) -> Result<bool, sqlx::Error> {
        let table = self.table();
        // One statement that always updates the row (moving `updated_at` only on a change): it
        // waits for a transaction holding the row, and finds none if that deleted it. A check of
        // existence apart could see a row another put since, and say an entry this didn't pause
        // was.
        let updated = sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {table} SET paused = $2,
                 updated_at = CASE WHEN paused <> $2 THEN clock_timestamp() ELSE updated_at END
             WHERE name = $1"
        )))
        .bind(name)
        .bind(paused)
        .execute(conn)
        .await?;
        Ok(updated.rows_affected() > 0)
    }

    /// Insert the job of the entry `name` now, outside its schedule (to try it, or for a "run
    /// it now" button): its worker and args, in its queue, with its priority, max attempts and
    /// tags, and `{"cron": name, "slot": now, "run_now": true}` in its meta, so it's found with
    /// the entry's own. `None` if there's no such entry. A paused entry runs too. It's no slot:
    /// the entry's next still runs on its own.
    pub async fn run_now(&self, name: &str) -> Result<Option<Inserted>, sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        self.run_now_in(&mut conn, name).await
    }

    /// [`run_now`](Self::run_now) in the caller's transaction (`&mut tx`).
    pub async fn run_now_in(
        &self,
        conn: &mut PgConnection,
        name: &str,
    ) -> Result<Option<Inserted>, sqlx::Error> {
        // As stored, read or not: the job doesn't need the pattern.
        let row: Option<CronRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT * FROM {} WHERE name = $1",
            self.table()
        )))
        .bind(name)
        .fetch_optional(&mut *conn)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let now = Utc::now();
        let mut opts = InsertOpts::new()
            .priority(row.priority)
            .meta(serde_json::json!({"cron": row.name, "slot": now.to_rfc3339(), "run_now": true}));
        for tag in row.tags {
            opts = opts.tag(tag);
        }
        let job = Row::stored(row.worker, row.args, row.queue, row.max_attempts, &opts);
        insert_row(conn, self.schema.as_deref(), &job, None)
            .await
            .map(Some)
    }

    /// The entry `name`, if there is one (and it can be read).
    pub async fn get(&self, name: &str) -> Result<Option<StoredCron>, sqlx::Error> {
        Ok(self
            .read(Filter::Name(name))
            .await?
            .into_iter()
            .find_map(Result::ok))
    }

    /// Every entry, by name. One whose pattern or timezone can't be read (edited by hand) is
    /// left out, with a warning.
    pub async fn list(&self) -> Result<Vec<StoredCron>, sqlx::Error> {
        Ok(readable(self.read(Filter::All).await?))
    }

    /// The entries whose name starts with `prefix`, by name, as [`list`](Self::list): one
    /// owner's, when their names start with it (`"tenant-42:"`).
    pub async fn list_by_prefix(&self, prefix: &str) -> Result<Vec<StoredCron>, sqlx::Error> {
        Ok(readable(self.read(Filter::Prefix(prefix)).await?))
    }

    /// The entries tagged `tag`, by name, as [`list`](Self::list).
    pub async fn list_by_tag(&self, tag: &str) -> Result<Vec<StoredCron>, sqlx::Error> {
        Ok(readable(self.read(Filter::Tag(tag)).await?))
    }

    /// Every entry, by name, with those that can't be read (edited by hand) as errors: they
    /// never run, and [`delete`](Self::delete) deletes them.
    pub async fn list_all(&self) -> Result<Vec<Result<StoredCron, UnreadableCron>>, sqlx::Error> {
        self.read(Filter::All).await
    }

    /// The entries `filter` picks, by name, each read or not.
    async fn read(
        &self,
        filter: Filter<'_>,
    ) -> Result<Vec<Result<StoredCron, UnreadableCron>>, sqlx::Error> {
        // Each served by an index: the primary key, `name text_pattern_ops` for a prefix
        // (`LIKE` with no wildcard but the last), and the GIN index on `tags`.
        let (clause, value) = match filter {
            Filter::All => ("$1::text IS NULL", None),
            Filter::Name(name) => ("name = $1", Some(name.to_string())),
            Filter::Prefix(prefix) => (
                r"name LIKE $1 ESCAPE '\'",
                Some(format!("{}%", escape_like(prefix))),
            ),
            Filter::Tag(tag) => ("tags @> ARRAY[$1]::varchar[]", Some(tag.to_string())),
        };
        let rows: Vec<CronRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT * FROM {} WHERE {clause} ORDER BY name",
            self.table()
        )))
        .bind(value)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(CronRow::read).collect())
    }
}

/// Which entries [`Crons::read`] reads.
#[derive(Clone, Copy)]
enum Filter<'a> {
    All,
    Name(&'a str),
    Prefix(&'a str),
    Tag(&'a str),
}

/// `text` matched as it is by `LIKE … ESCAPE '\'`.
fn escape_like(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        if matches!(c, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(c);
    }
    escaped
}

/// The entries that can be read; each that can't is left out, with a warning.
fn readable(entries: Vec<Result<StoredCron, UnreadableCron>>) -> Vec<StoredCron> {
    entries
        .into_iter()
        .filter_map(|entry| match entry {
            Ok(entry) => Some(entry),
            Err(u) => {
                tracing::warn!(cron = %u.name, error = %u.error, "cron entry can't be read; skipped");
                None
            }
        })
        .collect()
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

    fn catch_up(&self) -> CatchUp {
        match self {
            Entry::Engine(cron) => cron.catch_up,
            Entry::Stored(cron) => cron.catch_up,
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

    /// The slots to insert this check, and the time through which the slots are decided (the
    /// next check's `after`): those after `after` up to `until`, but at most [`MAX_SLOTS`]
    /// (the next check goes on from the last), and, by the entry's [`CatchUp`], of those at or
    /// before `on_time` (missed) and those it owes (tried and not inserted), all, the newest or
    /// none.
    fn plan(
        &self,
        after: DateTime<Utc>,
        on_time: DateTime<Utc>,
        until: DateTime<Utc>,
        owed: &[DateTime<Utc>],
    ) -> (Vec<DateTime<Utc>>, DateTime<Utc>) {
        let schedule = self.schedule();
        let capped = |from: DateTime<Utc>| {
            let mut slots = schedule.slots(from, until, MAX_SLOTS + 1);
            if slots.len() > MAX_SLOTS {
                slots.truncate(MAX_SLOTS);
                let through = slots[MAX_SLOTS - 1];
                tracing::warn!(cron = %self.name(), %through, "more than {MAX_SLOTS} cron slots due; inserting the rest next check");
                (slots, through)
            } else {
                (slots, until)
            }
        };
        match self.catch_up() {
            CatchUp::All => {
                let (slots, through) = capped(after);
                (owed.iter().copied().chain(slots).collect(), through)
            }
            policy @ (CatchUp::Latest | CatchUp::None) => {
                let on_time = on_time.max(after);
                let (slots, through) = capped(on_time);
                if !slots.is_empty() {
                    return (slots, through);
                }
                let missed = match policy {
                    CatchUp::Latest => schedule.last_slot(after, on_time),
                    _ => None,
                };
                let latest = owed.iter().copied().chain(missed).max();
                (latest.into_iter().collect(), through)
            }
        }
    }

    /// Insert `slots`, in order, stopping at the first that fails or the entry no longer as it
    /// was read. Returns how many jobs were inserted (slots another node had inserted don't
    /// count) and how many slots are in.
    async fn insert(&self, storage: &Arc<Postgres>, slots: &[DateTime<Utc>]) -> (usize, usize) {
        let name = self.name();
        let mut inserted = 0;
        for (done, slot) in slots.iter().copied().enumerate() {
            match self.insert_slot(storage, slot).await {
                Ok(Some(Inserted::New(job))) => {
                    tracing::info!(cron = %name, %slot, id = job.id, "cron job inserted");
                    inserted += 1;
                }
                Ok(Some(Inserted::Duplicate(_))) => {}
                Ok(None) => {
                    tracing::debug!(cron = %name, "cron entry changed, deleted or held; reading it again next check");
                    return (inserted, done);
                }
                Err(e) => {
                    tracing::error!(cron = %name, %slot, error = %e, "failed to insert cron job; retrying next check");
                    return (inserted, done);
                }
            }
        }
        (inserted, slots.len())
    }
}

/// What an entry's progress is kept by; see [`Entry::key`].
#[derive(Clone, PartialEq, Eq, Hash)]
enum Key {
    Engine(usize),
    Stored(String, DateTime<Utc>),
}

/// How far an entry is in, on a node.
struct Progress {
    /// The time through which its slots are decided: inserted, owed, or skipped by its
    /// [`CatchUp`].
    through: DateTime<Utc>,
    /// The slots it was to insert and didn't (refused, or held), to try again.
    owed: Vec<DateTime<Utc>>,
}

/// A node's cron checks: the engine's entries and those kept in the database, and how far each
/// is in.
pub(crate) struct Checks {
    engine: Vec<Cron>,
    stored: Crons,
    /// How far each entry is in.
    done: HashMap<Key, Progress>,
    /// The `until` of the last check that couldn't see the stored entries: the node's first, or
    /// one whose read of them failed. An entry new to the node missed the slots at or before it,
    /// and runs on time after it.
    unseen_through: Option<DateTime<Utc>>,
    /// The stored entries as last read, run again while they can't be read: the guard on each
    /// slot's insert refuses those of an entry since deleted, changed or paused.
    last_read: Vec<StoredCron>,
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
            unseen_through: None,
            last_read: Vec::new(),
            shadowed: HashSet::new(),
            unreadable: HashSet::new(),
            read_failed: false,
        }
    }

    /// Read the stored entries, then insert each entry's slots that fell due after its `done`
    /// time (but not before `floor`, nor a stored entry's last change), up to `until`, and move
    /// `done` on through the slots that are in. A slot that fails to insert stays due, so the
    /// next check tries it again. If the stored entries can't be read, those last read run (and
    /// are owed what they can't insert), so a failed read is as an outage of the database is
    /// for the engine's own. Returns how many jobs were inserted.
    pub(crate) async fn run(
        &mut self,
        storage: &Arc<Postgres>,
        floor: DateTime<Utc>,
        until: DateTime<Utc>,
    ) -> usize {
        let read = self.read_stored().await;
        let unseen = read.is_none() || self.unseen_through.is_none();
        if let Some(stored) = read {
            self.last_read = stored;
        }
        let stored = &self.last_read;
        let engine_names: HashSet<&str> = self.engine.iter().map(|c| c.name.as_str()).collect();
        let mut entries: Vec<Entry<'_>> = self.engine.iter().map(Entry::Engine).collect();
        let mut shadowed = HashSet::new();
        for cron in stored {
            if engine_names.contains(cron.name.as_str()) {
                if !self.shadowed.contains(&cron.name) {
                    tracing::warn!(cron = %cron.name, "a stored cron entry is named as one of the engine's; it's ignored");
                }
                shadowed.insert(cron.name.clone());
            } else if !cron.paused {
                entries.push(Entry::Stored(cron));
            }
        }
        self.shadowed = shadowed;

        let mut inserted = 0;
        let mut live = HashSet::new();
        for (index, entry) in entries.iter().enumerate() {
            let key = entry.key(index);
            let start = floor.max(entry.since().unwrap_or(floor));
            let progress = self.done.get(&key);
            let after = progress.map_or(start, |p| p.through.max(start));
            // Slots after this weren't missed: the last check decided on the entry through
            // `through`, so the rest fell due since. An entry new to the node (put, changed or
            // resumed since its last check, or seen late: its transaction committed after a
            // check) missed only those due before the node could see it; on its first check, all.
            let on_time = match progress {
                Some(p) => p.through,
                None => self.unseen_through.unwrap_or(until),
            };
            let owed: Vec<_> = progress
                .map(|p| {
                    p.owed
                        .iter()
                        .copied()
                        .filter(|slot| *slot > start)
                        .collect()
                })
                .unwrap_or_default();
            let (slots, through) = entry.plan(after, on_time, until, &owed);
            let (new, done) = entry.insert(storage, &slots).await;
            inserted += new;
            let progress = Progress {
                through,
                owed: slots[done..].to_vec(),
            };
            self.done.insert(key.clone(), progress);
            live.insert(key);
        }
        if unseen {
            self.unseen_through = Some(until);
        }
        // Forget the entries that are gone, changed or paused.
        self.done.retain(|key, _| live.contains(key));
        inserted
    }

    /// The stored entries that can be read; `None` if none could be read at all.
    async fn read_stored(&mut self) -> Option<Vec<StoredCron>> {
        let rows = match self.stored.read(Filter::All).await {
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

        /// The jobs' slots of the entry `name` from `h0` on.
        async fn slots_from(&self, name: &str, h0: DateTime<Utc>) -> Vec<DateTime<Utc>> {
            let mut slots = self.slots(name).await;
            slots.retain(|slot| *slot >= h0);
            slots
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

        // Changed since it was read: not inserted. The new version runs only from its change, so
        // a slot due before the change that no node inserted in time never is.
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

    /// An hourly entry with `catch_up` through a node's start (five hours to catch up, so
    /// 8:00 to 12:00 missed), a check on time (13:00), a slot the database refuses then accepts
    /// on the next check (14:00), and one it refuses until a newer slot is due (15:00, then
    /// 16:00): the slots inserted.
    async fn hourly_through_misses(catch_up: CatchUp) -> Vec<DateTime<Utc>> {
        let db = Db::new().await.expect("database tests");
        let hourly = Cron::new("h", "0 * * * *", || Tick)
            .unwrap()
            .catch_up(catch_up);
        let mut checks = Checks::new(vec![hourly], &db.storage);
        let window = chrono::Duration::hours(5);
        let refuse = |refused: bool| {
            let sql = if refused {
                "ALTER TABLE {s}.ishikari_jobs ADD CONSTRAINT no_cron CHECK (meta->>'cron' IS NULL) NOT VALID"
            } else {
                "ALTER TABLE {s}.ishikari_jobs DROP CONSTRAINT no_cron"
            };
            let sql = sql.replace("{s}", &db.schema);
            let admin = db.admin.clone();
            async move {
                sqlx::query(sqlx::AssertSqlSafe(sql))
                    .execute(&admin)
                    .await
                    .unwrap();
            }
        };
        for (until, refused) in [
            (at(12, 30), false),
            (at(13, 10), false),
            (at(14, 10), true),
            (at(14, 20), false),
            (at(15, 10), true),
            (at(16, 10), false),
        ] {
            if refused {
                refuse(true).await;
            }
            checks.run(&db.storage, until - window, until).await;
            if refused {
                refuse(false).await;
            }
        }
        let slots = db.slots("h").await;
        db.finish().await;
        slots
    }

    #[tokio::test]
    async fn all_missed_slots_are_caught_up_by_default() {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return;
        }
        let hours = [8, 9, 10, 11, 12, 13, 14, 15, 16].map(|h| at(h, 0));
        assert_eq!(hourly_through_misses(CatchUp::All).await, hours);
    }

    #[tokio::test]
    async fn only_the_latest_missed_slot_is_caught_up() {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return;
        }
        // 15:00 was refused, and 16:00 fell due before it was accepted.
        let hours = [12, 13, 14, 16].map(|h| at(h, 0));
        assert_eq!(hourly_through_misses(CatchUp::Latest).await, hours);
    }

    #[tokio::test]
    async fn no_missed_slot_is_caught_up_but_a_refused_one_is_tried_again() {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return;
        }
        let hours = [13, 14, 16].map(|h| at(h, 0));
        assert_eq!(hourly_through_misses(CatchUp::None).await, hours);
    }

    /// The top of an hour after now, so that an entry put now runs from before it.
    fn an_hour_ahead() -> DateTime<Utc> {
        use chrono::DurationRound;
        let hour = chrono::Duration::hours(1);
        (Utc::now() + hour).duration_trunc(hour).unwrap() + hour
    }

    fn minutes(m: i64) -> chrono::Duration {
        chrono::Duration::minutes(m)
    }

    /// A check of `checks` through `until`, catching up five hours.
    async fn check(checks: &mut Checks, db: &Db, until: DateTime<Utc>) {
        checks
            .run(&db.storage, until - chrono::Duration::hours(5), until)
            .await;
    }

    /// An hourly entry put with `catch_up`, then a node checking from 10 minutes before `h0`
    /// (that check reading the stored entries if `read_first`); they can't be read by the checks
    /// `failing` minutes after `h0`, and can by those `after` it: the slots inserted from `h0`.
    async fn through_failed_reads(
        catch_up: CatchUp,
        h0: DateTime<Utc>,
        read_first: bool,
        failing: &[i64],
        after: &[i64],
    ) -> Vec<DateTime<Utc>> {
        let db = Db::new().await.expect("database tests");
        let entry = CronEntry::new("rf", "0 * * * *", &Tick)
            .unwrap()
            .catch_up(catch_up);
        db.crons.put(&entry).await.unwrap();
        let mut checks = Checks::new(vec![], &db.storage);
        let rename =
            |from: &str, to: &str| format!("ALTER TABLE {s}.{from} RENAME TO {to}", s = db.schema);
        if read_first {
            check(&mut checks, &db, h0 - minutes(10)).await;
        }
        sqlx::raw_sql(sqlx::AssertSqlSafe(rename("ishikari_crons", "crons_away")))
            .execute(&db.admin)
            .await
            .unwrap();
        if !read_first {
            check(&mut checks, &db, h0 - minutes(10)).await;
        }
        for m in failing {
            check(&mut checks, &db, h0 + minutes(*m)).await;
        }
        sqlx::raw_sql(sqlx::AssertSqlSafe(rename("crons_away", "ishikari_crons")))
            .execute(&db.admin)
            .await
            .unwrap();
        for m in after {
            check(&mut checks, &db, h0 + minutes(*m)).await;
        }
        let slots = db.slots_from("rf", h0).await;
        db.finish().await;
        slots
    }

    #[tokio::test]
    async fn a_failed_read_of_the_stored_entries_loses_no_slot_due_while_the_node_was_up() {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return;
        }
        let h0 = an_hour_ahead();
        for catch_up in [CatchUp::All, CatchUp::Latest, CatchUp::None] {
            let slots = through_failed_reads(catch_up, h0, true, &[10], &[20, 30]).await;
            assert_eq!(slots, [h0], "{catch_up}");
        }
    }

    #[tokio::test]
    async fn stored_entries_unread_across_slots_catch_up_as_their_own_do() {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return;
        }
        let h0 = an_hour_ahead();
        let h = |n: i64| h0 + chrono::Duration::hours(n);
        // Each slot was due while they couldn't be read, so tried and owed: all of them, or only
        // the newest, as an engine's entry would be through an outage of the database.
        let all = through_failed_reads(CatchUp::All, h0, true, &[10, 70, 130], &[140]).await;
        assert_eq!(all, [h(0), h(1), h(2)]);
        for catch_up in [CatchUp::Latest, CatchUp::None] {
            let slots = through_failed_reads(catch_up, h0, true, &[10, 70, 130], &[140]).await;
            assert_eq!(slots, [h(2)], "{catch_up}");
        }
    }

    #[tokio::test]
    async fn an_entry_first_read_after_an_outage_missed_the_slots_due_through_it() {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return;
        }
        let h0 = an_hour_ahead();
        let h = |n: i64| h0 + chrono::Duration::hours(n);
        // The node started while they couldn't be read: it never checked the entry, so the slots
        // until it could were missed.
        let all = through_failed_reads(CatchUp::All, h0, false, &[10, 70, 130], &[140]).await;
        assert_eq!(all, [h(0), h(1), h(2)]);
        let latest = through_failed_reads(CatchUp::Latest, h0, false, &[10, 70, 130], &[140]).await;
        assert_eq!(latest, [h(2)]);
        let none =
            through_failed_reads(CatchUp::None, h0, false, &[10, 70, 130], &[140, 190]).await;
        assert_eq!(none, [h(3)], "the next on time");
    }

    #[tokio::test]
    async fn an_entry_deleted_while_unread_inserts_nothing_more() {
        let Some(db) = Db::new().await else {
            return;
        };
        let h0 = an_hour_ahead();
        let entry = CronEntry::new("gone", "0 * * * *", &Tick).unwrap();
        db.crons.put(&entry).await.unwrap();
        let mut checks = Checks::new(vec![], &db.storage);
        check(&mut checks, &db, h0 - minutes(10)).await;
        // Unread, and deleted meanwhile.
        let sql = |sql: &str| sql.replace("{s}", &db.schema);
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql(
            "ALTER TABLE {s}.ishikari_crons RENAME TO crons_away;
             DELETE FROM {s}.crons_away WHERE name = 'gone';",
        )))
        .execute(&db.admin)
        .await
        .unwrap();
        check(&mut checks, &db, h0 + minutes(10)).await;
        assert!(
            db.slots_from("gone", h0).await.is_empty(),
            "the copy last read isn't run"
        );
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql(
            "ALTER TABLE {s}.crons_away RENAME TO ishikari_crons",
        )))
        .execute(&db.admin)
        .await
        .unwrap();
        // Read again (another entry runs), and still nothing of the deleted one.
        db.crons
            .put(&CronEntry::new("other", "0 * * * *", &Tick).unwrap())
            .await
            .unwrap();
        check(&mut checks, &db, h0 + minutes(20)).await;
        assert_eq!(db.slots_from("other", h0).await, [h0]);
        assert!(db.slots_from("gone", h0).await.is_empty());
        db.finish().await;
    }

    #[tokio::test]
    async fn a_change_held_open_across_a_slot_loses_none_of_it() {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return;
        }
        for catch_up in [CatchUp::All, CatchUp::Latest, CatchUp::None] {
            let db = Db::new().await.unwrap();
            let h0 = an_hour_ahead();
            let v0 = CronEntry::new("held", "0 * * * *", &Tick)
                .unwrap()
                .catch_up(catch_up);
            db.crons.put(&v0).await.unwrap();
            let mut checks = Checks::new(vec![], &db.storage);
            check(&mut checks, &db, h0 - minutes(10)).await;
            let mut tx = db.admin.begin().await.unwrap();
            let v1 = db
                .crons
                .put_in(&mut tx, &v0.clone().priority(1))
                .await
                .unwrap();
            assert!(v1.updated_at() < h0);
            check(&mut checks, &db, h0 + minutes(10)).await;
            assert!(
                db.slots_from("held", h0).await.is_empty(),
                "held: {catch_up}"
            );
            tx.commit().await.unwrap();
            check(&mut checks, &db, h0 + minutes(20)).await;
            check(&mut checks, &db, h0 + minutes(30)).await;
            assert_eq!(
                db.slots_from("held", h0).await,
                [h0],
                "the new version's: {catch_up}"
            );
            db.finish().await;
        }
    }

    #[tokio::test]
    async fn an_entry_put_or_resumed_in_a_transaction_runs_from_then_not_from_its_commit() {
        if std::env::var_os("ISHIKARI_SKIP_DB_TESTS").is_some() {
            return;
        }
        for resumed in [false, true] {
            for catch_up in [CatchUp::All, CatchUp::Latest, CatchUp::None] {
                let db = Db::new().await.unwrap();
                let h0 = an_hour_ahead();
                let entry = CronEntry::new("late", "0 * * * *", &Tick)
                    .unwrap()
                    .catch_up(catch_up);
                if resumed {
                    db.crons.put(&entry).await.unwrap();
                    db.crons.pause("late").await.unwrap();
                }
                let mut checks = Checks::new(vec![], &db.storage);
                check(&mut checks, &db, h0 - minutes(10)).await;
                let mut tx = db.admin.begin().await.unwrap();
                if resumed {
                    assert!(db.crons.resume_in(&mut tx, "late").await.unwrap());
                } else {
                    db.crons.put_in(&mut tx, &entry).await.unwrap();
                }
                check(&mut checks, &db, h0 + minutes(10)).await;
                tx.commit().await.unwrap();
                check(&mut checks, &db, h0 + minutes(20)).await;
                assert_eq!(
                    db.slots_from("late", h0).await,
                    [h0],
                    "resumed {resumed}: {catch_up}"
                );
                db.finish().await;
            }
        }
    }

    #[tokio::test]
    async fn an_entry_deleted_while_it_was_being_paused_or_resumed_is_none() {
        let Some(db) = Db::new().await else {
            return;
        };
        for pause in [true, false] {
            let entry = CronEntry::new("racing", "0 * * * *", &Tick).unwrap();
            db.crons.put(&entry).await.unwrap();
            if !pause {
                db.crons.pause("racing").await.unwrap();
            }
            let mut tx = db.admin.begin().await.unwrap();
            assert!(db.crons.delete_in(&mut tx, "racing").await.unwrap());
            let crons = db.crons.clone();
            let setting = tokio::spawn(async move {
                if pause {
                    crons.pause("racing").await
                } else {
                    crons.resume("racing").await
                }
            });
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            tx.commit().await.unwrap();
            assert!(!setting.await.unwrap().unwrap(), "pause {pause}");
        }
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
            catch_up: CatchUp::All,
            paused: false,
            inserted_at: at(0, 0),
            updated_at: at(0, 0),
        };
        assert_eq!(stored.next(at(9, 30)), Some(at(10, 0)));
        assert_eq!(stored.next(at(10, 0)), Some(at(11, 0)), "after, not at");
        // None before its last change runs.
        let changed = StoredCron {
            updated_at: at(10, 30),
            ..stored.clone()
        };
        assert_eq!(changed.next(at(9, 30)), Some(at(11, 0)));
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
