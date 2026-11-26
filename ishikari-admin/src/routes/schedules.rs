//! The Schedules page: the cron entries kept in the database (`ishikari::Crons`), put by the host
//! or its agents while its engines run, each with its next and last run. Operators pause, resume,
//! run now and delete them here, and, with the host's modules (`AppState::with_node_modules`),
//! schedule a module.
//! Entries given to an engine in code aren't kept there, so they aren't listed.

use std::collections::HashMap;
use std::future::Future;
use std::str::FromStr;

use axum::extract::rejection::FormRejection;
use axum::extract::{Form, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use chrono::{DateTime, Utc};
use ishikari::chrono_tz::Tz;
use ishikari::execution::ModuleJob;
use ishikari::{CatchUp, CronEntry, Crons, StoredCron};
use serde::Deserialize;

use crate::templates::SchedulesTemplate;
use crate::AppState;

/// A schedule, as the page lists it.
#[derive(Debug, Clone)]
pub struct ScheduleRow {
    pub name: String,
    pub pattern: String,
    pub timezone: String,
    /// What it runs: a module job's module, else the job's type.
    pub job: String,
    /// Whether `job` is a module.
    pub module: bool,
    /// The job's type, as its jobs are stored.
    pub worker: String,
    pub queue: String,
    pub priority: i32,
    pub max_attempts: i32,
    pub tags: Vec<String>,
    /// Which missed slots a node catches up: `all`, `latest` or `none`.
    pub catch_up: String,
    /// Paused: none of its slots is inserted until it's resumed.
    pub paused: bool,
    /// Its next slot, if the pattern has one (none while it's paused).
    pub next: Option<DateTime<Utc>>,
    /// The job of its latest slot, while it's kept.
    pub last: Option<LastRun>,
    pub updated_at: DateTime<Utc>,
}

/// The job of a schedule's latest slot.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct LastRun {
    pub id: i64,
    pub state: String,
    /// Its slot (or, for a job whose meta doesn't give one, when it's scheduled).
    pub slot: DateTime<Utc>,
    /// Inserted by "Run now" (`Crons::run_now`), not on a slot of the pattern.
    pub run_now: bool,
}

impl LastRun {
    /// The badge class for its state.
    pub fn state_class(&self) -> &'static str {
        match self.state.as_str() {
            "available" => "ik-tone-primary",
            "scheduled" => "ik-tone-violet",
            "executing" => "ik-tone-info ik-live",
            "completed" => "ik-tone-success",
            "discarded" => "ik-tone-danger",
            "retryable" => "ik-tone-warning",
            _ => "ik-tone-neutral",
        }
    }
}

/// A schedule whose pattern or timezone can't be read (edited by hand): it never runs.
#[derive(Debug, Clone)]
pub struct UnreadableRow {
    pub name: String,
    pub error: String,
    pub updated_at: DateTime<Utc>,
}

/// What the page says after a change.
#[derive(Debug, Deserialize)]
pub struct ChangedQuery {
    pub put: Option<String>,
    pub deleted: Option<String>,
    pub paused: Option<String>,
    pub resumed: Option<String>,
    pub ran: Option<String>,
    /// The job "Run now" inserted (said only if it's an id: the query can be edited).
    pub job: Option<String>,
}

impl ChangedQuery {
    fn notice(self) -> Option<String> {
        let said = |done: &str, name: Option<String>| name.map(|name| format!("{done} {name}"));
        said("Scheduled", self.put)
            .or_else(|| said("Deleted", self.deleted))
            .or_else(|| said("Paused", self.paused))
            .or_else(|| said("Resumed", self.resumed))
            .or_else(|| {
                let ran = said("Ran", self.ran)?;
                Some(match self.job.and_then(|job| job.parse::<i64>().ok()) {
                    Some(id) => format!("{ran}: job #{id}"),
                    None => ran,
                })
            })
    }
}

/// The form scheduling a module.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ScheduleForm {
    pub name: String,
    pub module: String,
    pub pattern: String,
    /// An IANA timezone, as `America/Chicago`; UTC when empty.
    pub timezone: String,
    /// The module's inputs: a JSON object, `{}` when empty.
    pub args: String,
    /// The queue, `modules` when empty.
    pub queue: String,
    /// How many times each job is attempted, the job's own when empty.
    pub max_attempts: String,
    /// Which missed slots a node catches up: `all` (when empty), `latest` or `none`.
    pub catch_up: String,
}

impl ScheduleForm {
    /// The form as the page first shows it.
    fn defaults() -> Self {
        ScheduleForm {
            pattern: "0 * * * *".into(),
            timezone: "UTC".into(),
            args: "{}".into(),
            catch_up: CatchUp::All.to_string(),
            ..ScheduleForm::default()
        }
    }
}

/// The form naming the schedule to pause, resume, run now or delete.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub struct NameForm {
    pub name: String,
}

type Refused = (StatusCode, String);

/// The Schedules page.
pub async fn list(State(state): State<AppState>, Query(query): Query<ChangedQuery>) -> Response {
    let notice = query.notice();
    page(
        &state,
        StatusCode::OK,
        notice,
        None,
        ScheduleForm::defaults(),
    )
    .await
}

/// Schedule a module, an `application/x-www-form-urlencoded` [`ScheduleForm`]: a
/// [`ModuleJob`] of it, put under the form's name (replacing the schedule of that name).
pub async fn put(
    State(state): State<AppState>,
    form: Result<Form<ScheduleForm>, FormRejection>,
) -> Response {
    if state.node_modules.is_none() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let form = match form {
        Ok(Form(form)) => form,
        Err(rejection) => return (rejection.status(), rejection.body_text()).into_response(),
    };
    match putting(&state, &form).await {
        Ok(name) => changed(&state, "put", &name, None),
        Err((status, error)) => page(&state, status, None, Some(error), form).await,
    }
}

/// Put the form's schedule: its name, or why it can't be put.
async fn putting(state: &AppState, form: &ScheduleForm) -> Result<String, Refused> {
    let bad = |why: String| (StatusCode::BAD_REQUEST, why);
    let name = form.name.trim();
    if name.is_empty() {
        return Err(bad("Name the schedule".into()));
    }
    let module = form.module.trim();
    let modules = state.node_modules.as_ref().expect("checked by the handler");
    let known = modules
        .modules()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .iter()
        .any(|m| m.spec.name == module);
    if !known {
        return Err(bad(format!("No module {module}")));
    }
    let args = super::node_types::object_args(&form.args).map_err(|why| bad(why.into()))?;
    let max_attempts =
        super::node_types::max_attempts(&form.max_attempts).map_err(|why| bad(why.into()))?;
    let timezone = match form.timezone.trim() {
        "" => Tz::UTC,
        tz => Tz::from_str(tz).map_err(|_| bad(format!("Unknown timezone {tz}")))?,
    };
    let catch_up = match form.catch_up.trim() {
        "" => CatchUp::All,
        catch_up => CatchUp::from_str(catch_up).map_err(|e| bad(e.to_string()))?,
    };
    let job = ModuleJob::new(module, args);
    let mut entry = CronEntry::new(name, form.pattern.trim(), &job)
        .map_err(|e| bad(e.to_string()))?
        .timezone(timezone)
        .catch_up(catch_up);
    if let Some(attempts) = max_attempts {
        entry = entry.max_attempts(attempts);
    }
    match form.queue.trim() {
        "" => {}
        queue => entry = entry.queue(queue),
    }
    crons(state).put(&entry).await.map_err(|e| {
        // A value the table refuses (too long, a NUL...) is the form's to fix.
        let refused = matches!(&e, sqlx::Error::Database(db)
            if db.code().is_some_and(|code| code.starts_with("22") || code.starts_with("23")));
        let status = if refused {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        (status, format!("Couldn't put the schedule: {e}"))
    })?;
    Ok(name.to_string())
}

/// Delete a schedule, an `application/x-www-form-urlencoded` [`NameForm`], by its name exactly
/// as given (one edited by hand may have odd whitespace). No slot of it is inserted once it's
/// deleted; a job already inserted still runs.
pub async fn delete(
    State(state): State<AppState>,
    form: Result<Form<NameForm>, FormRejection>,
) -> Response {
    act(state, form, "deleted", "delete", |crons, name| async move {
        crons.delete(&name).await.map(|found| found.then_some(None))
    })
    .await
}

/// Pause a schedule, a [`NameForm`]: none of its slots is inserted until it's resumed.
pub async fn pause(
    State(state): State<AppState>,
    form: Result<Form<NameForm>, FormRejection>,
) -> Response {
    act(state, form, "paused", "pause", |crons, name| async move {
        crons.pause(&name).await.map(|found| found.then_some(None))
    })
    .await
}

/// Resume a schedule, a [`NameForm`]: it runs from now, catching up none of the slots while it
/// was paused.
pub async fn resume(
    State(state): State<AppState>,
    form: Result<Form<NameForm>, FormRejection>,
) -> Response {
    act(state, form, "resumed", "resume", |crons, name| async move {
        crons.resume(&name).await.map(|found| found.then_some(None))
    })
    .await
}

/// Run a schedule's job now, a [`NameForm`], outside its pattern (paused or not).
pub async fn run_now(
    State(state): State<AppState>,
    form: Result<Form<NameForm>, FormRejection>,
) -> Response {
    act(state, form, "ran", "run", |crons, name| async move {
        let inserted = crons.run_now(&name).await?;
        Ok(inserted.map(|inserted| Some(inserted.job().id)))
    })
    .await
}

/// Do `action` to the form's schedule, then back to the page saying it was `done`: `action`
/// gives `None` when there's no such schedule, else the job it inserted, if any.
async fn act<F, Fut>(
    state: AppState,
    form: Result<Form<NameForm>, FormRejection>,
    done: &str,
    verb: &str,
    action: F,
) -> Response
where
    F: FnOnce(Crons, String) -> Fut,
    Fut: Future<Output = Result<Option<Option<i64>>, sqlx::Error>>,
{
    let name = match form {
        Ok(Form(form)) => form.name,
        Err(rejection) => return (rejection.status(), rejection.body_text()).into_response(),
    };
    let (status, error) = match action(crons(&state), name.clone()).await {
        Ok(Some(job)) => return changed(&state, done, &name, job),
        Ok(None) => (StatusCode::NOT_FOUND, format!("No schedule {name}")),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Couldn't {verb} {name}: {e}"),
        ),
    };
    page(&state, status, None, Some(error), ScheduleForm::defaults()).await
}

/// The schedules of the admin's schema.
fn crons(state: &AppState) -> Crons {
    let crons = Crons::new(state.pool.clone());
    match state.schema() {
        Some(schema) => crons.schema(schema),
        None => crons,
    }
}

/// Back to the page, which says what was `done` to `name` (and the job it inserted).
fn changed(state: &AppState, done: &str, name: &str, job: Option<i64>) -> Response {
    let job = job.map(|id| id.to_string());
    let mut query = vec![(done, name)];
    if let Some(job) = &job {
        query.push(("job", job));
    }
    match serde_urlencoded::to_string(query) {
        Ok(query) => {
            Redirect::to(&format!("{}/schedules?{query}", state.base_path)).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// The job of each schedule's latest slot, by schedule name, in one query: of the jobs inserted
/// since the schedule was (so not one of a deleted schedule of the same name), the one with the
/// latest slot. Not the latest `scheduled_at`: a retry or a snooze moves that on.
async fn last_runs(
    state: &AppState,
    schedules: &[(String, DateTime<Utc>)],
) -> Result<HashMap<String, LastRun>, sqlx::Error> {
    let (names, since): (Vec<String>, Vec<DateTime<Utc>>) = schedules.iter().cloned().unzip();
    if schedules.is_empty() {
        return Ok(HashMap::new());
    }
    let jobs = match state.schema() {
        Some(schema) => format!("{schema}.ishikari_jobs"),
        None => "ishikari_jobs".to_string(),
    };
    // `meta @>` is what the GIN index on `meta` serves. Slots are RFC 3339 in UTC, so their text
    // sorts as their time does.
    // The schedule's name, then its job's id, state, slot, scheduled time and whether it was run
    // now.
    type LastRow = (String, i64, String, Option<String>, DateTime<Utc>, bool);
    let rows: Vec<LastRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT c.name, j.id, j.state::text, j.slot, j.scheduled_at, j.run_now
             FROM unnest($1::text[], $2::timestamptz[]) AS c(name, since)
             CROSS JOIN LATERAL (
                 SELECT id, state, meta->>'slot' AS slot, scheduled_at,
                     meta @> '{{\"run_now\": true}}' AS run_now
                 FROM {jobs}
                 WHERE meta @> jsonb_build_object('cron', c.name) AND inserted_at >= c.since
                 ORDER BY meta->>'slot' DESC NULLS LAST, id DESC LIMIT 1
             ) j"
    )))
    .bind(&names)
    .bind(&since)
    .fetch_all(&state.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(name, id, state, slot, scheduled_at, run_now)| {
            let slot = slot
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map_or(scheduled_at, |s| s.with_timezone(&Utc));
            (
                name,
                LastRun {
                    id,
                    state,
                    slot,
                    run_now,
                },
            )
        })
        .collect())
}

fn row(cron: StoredCron, now: DateTime<Utc>, last: Option<LastRun>) -> ScheduleRow {
    let module = (cron.worker() == ModuleJob::WORKER)
        .then(|| cron.args().get("module").and_then(|m| m.as_str()))
        .flatten();
    let job = match module {
        Some(module) => module.to_string(),
        None => cron
            .worker()
            .rsplit("::")
            .next()
            .unwrap_or(cron.worker())
            .to_string(),
    };
    ScheduleRow {
        name: cron.name().to_string(),
        pattern: cron.pattern().to_string(),
        timezone: cron.timezone().name().to_string(),
        job,
        module: module.is_some(),
        worker: cron.worker().to_string(),
        queue: cron.queue().to_string(),
        priority: cron.priority(),
        max_attempts: cron.max_attempts(),
        tags: cron.tags().to_vec(),
        catch_up: cron.catch_up().to_string(),
        paused: cron.paused(),
        next: (!cron.paused()).then(|| cron.next(now)).flatten(),
        last,
        updated_at: cron.updated_at(),
    }
}

async fn page(
    state: &AppState,
    status: StatusCode,
    notice: Option<String>,
    mut error: Option<String>,
    form: ScheduleForm,
) -> Response {
    let now = Utc::now();
    let mut schedules = Vec::new();
    let mut unreadable = Vec::new();
    match crons(state).list_all().await {
        Ok(all) => {
            let named: Vec<(String, DateTime<Utc>)> = all
                .iter()
                .filter_map(|c| c.as_ref().ok())
                .map(|c| (c.name().to_string(), c.inserted_at()))
                .collect();
            let mut last = last_runs(state, &named).await.unwrap_or_else(|e| {
                error.get_or_insert(format!("Couldn't find the schedules' last runs: {e}"));
                HashMap::new()
            });
            for cron in all {
                match cron {
                    Ok(cron) => {
                        let last = last.remove(cron.name());
                        schedules.push(row(cron, now, last));
                    }
                    Err(u) => unreadable.push(UnreadableRow {
                        name: u.name().to_string(),
                        error: u.error().to_string(),
                        updated_at: u.updated_at(),
                    }),
                }
            }
        }
        Err(e) => {
            error.get_or_insert(format!(
                "Couldn't list the schedules (is the schema at v11?): {e}"
            ));
        }
    }
    let modules = match &state.node_modules {
        Some(modules) => match modules.modules().await {
            Ok(modules) => Some(modules.into_iter().map(|m| m.spec.name).collect()),
            Err(e) => {
                error.get_or_insert(format!("Couldn't list the modules: {e}"));
                Some(Vec::new())
            }
        },
        None => None,
    };
    let template = SchedulesTemplate {
        base_path: state.base_path.to_string(),
        nav_items: state.nav_items.to_vec(),
        schedules,
        unreadable,
        modules,
        notice,
        error,
        form,
    };
    (status, template).into_response()
}
