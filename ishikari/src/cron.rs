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
//!     .cron(Cron::new("daily-digest", "0 7 * * *", || Digest)?.timezone(chrono_tz::America::Chicago))
//!     .start(Postgres::new(pool));
//! # Ok(())
//! # }
//! ```

use crate::{InsertOpts, Inserted, Postgres, Unique, Worker};
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde::Serialize;
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

/// A job inserted on a schedule.
#[derive(Clone)]
pub struct Cron {
    name: String,
    pattern: String,
    schedule: croner::Cron,
    timezone: Tz,
    insert: InsertFn,
}

impl Debug for Cron {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cron")
            .field("name", &self.name)
            .field("pattern", &self.pattern)
            .field("timezone", &self.timezone)
            .finish()
    }
}

/// A cron pattern that couldn't be parsed.
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
        let schedule = croner::Cron::from_str(pattern).map_err(|e| CronError {
            pattern: pattern.to_string(),
            reason: e.to_string(),
        })?;
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
            pattern: pattern.to_string(),
            schedule,
            timezone: Tz::UTC,
            insert,
        })
    }

    /// Read the pattern in `timezone` (for example `chrono_tz::America::Chicago`).
    pub fn timezone(mut self, timezone: Tz) -> Self {
        self.timezone = timezone;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// The slots after `after`, up to and including `until` (at most 100).
    pub fn due(&self, after: DateTime<Utc>, until: DateTime<Utc>) -> Vec<DateTime<Utc>> {
        let mut slots = Vec::new();
        let mut from = after.with_timezone(&self.timezone);
        while slots.len() < 100 {
            let Ok(next) = self.schedule.find_next_occurrence(&from, false) else {
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

    /// Insert the job for `slot`, unless some node already has. The job is scheduled at the slot
    /// and carries `{"cron": name, "slot": slot}` in its meta.
    pub async fn insert_slot(
        &self,
        storage: Arc<Postgres>,
        slot: DateTime<Utc>,
    ) -> Result<Inserted, sqlx::Error> {
        let slot_text = slot.to_rfc3339();
        let opts = InsertOpts::new()
            .schedule_at(slot)
            .meta(serde_json::json!({"cron": self.name, "slot": slot_text}))
            .unique(Unique::key(format!("ishikari:cron:{}:{slot_text}", self.name)).all_states());
        (self.insert)(storage, opts).await
    }
}

/// Insert every entry's slots that fell due after `after`, up to `until`. Returns how many
/// jobs were inserted (slots another node had inserted don't count).
pub(crate) async fn run_due(
    entries: &[Cron],
    storage: &Arc<Postgres>,
    after: DateTime<Utc>,
    until: DateTime<Utc>,
) -> usize {
    let mut inserted = 0;
    for entry in entries {
        for slot in entry.due(after, until) {
            match entry.insert_slot(storage.clone(), slot).await {
                Ok(Inserted::New(job)) => {
                    tracing::info!(cron = %entry.name, %slot, id = job.id, "cron job inserted");
                    inserted += 1;
                }
                Ok(Inserted::Duplicate(_)) => {}
                Err(e) => {
                    tracing::error!(cron = %entry.name, %slot, error = %e, "failed to insert cron job")
                }
            }
        }
    }
    inserted
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
}
