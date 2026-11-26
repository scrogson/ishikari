//! The Schedules page lists the cron entries kept in the database (`ishikari::Crons`), with each
//! one's next and last run, pauses, resumes, runs now and deletes them, and, with the host's
//! modules, schedules a module.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use axum::http::{header, StatusCode};
use axum::Router;
use chrono::Utc;
use common::{body, get, post_form, TestDb};
use ishikari::execution::{
    ModuleError, ModuleGrants, ModuleHandler, ModuleJob, ModuleUpload, ModuleVersion, NodeModule,
    NodeModules, NodeSpec, TestRun,
};
use ishikari::{CatchUp, CronEntry, Crons};
use ishikari_admin::AppState;

/// Modules that are only listed: `test/kit`.
struct Listed;

#[async_trait::async_trait]
impl NodeModules for Listed {
    async fn handler(
        &self,
        _node_type: &str,
        _as_of: ModuleVersion,
    ) -> Result<Option<ModuleHandler>, ModuleError> {
        Ok(None)
    }

    async fn modules(&self) -> Result<Vec<NodeModule>, ModuleError> {
        Ok(vec![NodeModule {
            spec: NodeSpec::new("test/kit"),
            version: 1,
            sha256: "00".into(),
            size: 1,
            source: None,
            grants: ModuleGrants::default(),
            uploaded_at: Utc::now(),
        }])
    }

    async fn describe(&self, _bytes: &[u8]) -> Result<NodeSpec, ModuleError> {
        Err(ModuleError::Invalid("only listed".into()))
    }

    async fn upload(&self, _upload: ModuleUpload) -> Result<NodeModule, ModuleError> {
        Err(ModuleError::Invalid("only listed".into()))
    }

    async fn test_run(
        &self,
        _upload: ModuleUpload,
        _inputs: HashMap<String, serde_json::Value>,
    ) -> Result<TestRun, ModuleError> {
        Err(ModuleError::Invalid("only listed".into()))
    }
}

/// The admin at `/admin` on the test's schema, with [`Listed`] modules or none.
fn admin(db: &TestDb, modules: bool) -> Router {
    let mut state = AppState::with_schema(db.pool.clone(), &db.schema).with_base_path("/admin");
    if modules {
        state = state.with_node_modules(Arc::new(Listed));
    }
    Router::new().nest("/admin", ishikari_admin::app(state))
}

fn crons(db: &TestDb) -> Crons {
    Crons::new(db.pool.clone()).schema(&db.schema)
}

fn location(res: &axum::response::Response) -> String {
    res.headers()[header::LOCATION]
        .to_str()
        .unwrap()
        .to_string()
}

#[tokio::test]
async fn schedules_are_listed_with_their_next_and_last_run() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let digest = CronEntry::new(
        "team-7-digest",
        "0 7 * * *",
        &ModuleJob::new("test/kit", serde_json::json!({"team": 7})),
    )
    .unwrap()
    .timezone(ishikari::chrono_tz::America::Chicago)
    .tag("agent");
    let stored = crons(&db).put(&digest).await.unwrap();
    let last: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {}.ishikari_jobs (worker, state, meta, scheduled_at) \
         VALUES ($1, 'completed', '{{\"cron\": \"team-7-digest\"}}', now()) RETURNING id",
        db.schema
    )))
    .bind(ModuleJob::WORKER)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    let app = admin(&db, false);
    let res = get(&app, "/admin/schedules", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let html = body(res).await;
    assert!(html.contains("team-7-digest"));
    assert!(html.contains("0 7 * * *"));
    assert!(html.contains("America/Chicago"));
    assert!(html.contains("test/kit"), "a module job by its module");
    assert!(html.contains("agent"));
    let next = stored.next(Utc::now()).unwrap();
    assert!(
        html.contains(&next.format("%Y-%m-%d %H:%M").to_string()),
        "its next run"
    );
    assert!(
        html.contains(&format!("/admin/jobs/{last}")),
        "its last run"
    );
    assert!(html.contains(r#"href="/admin/schedules""#), "in the nav");
    // No modules: nothing to schedule here.
    assert!(!html.contains("Schedule a module"));

    db.drop().await;
}

#[tokio::test]
async fn with_none_the_page_says_so() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let html = body(get(&admin(&db, false), "/admin/schedules", &[]).await).await;
    assert!(html.contains("No schedules"));

    db.drop().await;
}

#[tokio::test]
async fn a_schedule_is_deleted() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let entry = CronEntry::new(
        "gone",
        "* * * * *",
        &ModuleJob::new("test/kit", serde_json::json!({})),
    )
    .unwrap();
    crons(&db).put(&entry).await.unwrap();
    let app = admin(&db, false);

    let res = post_form(&app, "/admin/schedules/delete", &[("name", "gone")]).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(&res), "/admin/schedules?deleted=gone");
    assert!(crons(&db).get("gone").await.unwrap().is_none());
    let html = body(get(&app, &location(&res), &[]).await).await;
    assert!(html.contains("Deleted gone"));

    let res = post_form(&app, "/admin/schedules/delete", &[("name", "gone")]).await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(body(res).await.contains("No schedule gone"));

    db.drop().await;
}

#[tokio::test]
async fn an_unreadable_schedule_is_shown_and_can_be_deleted() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {}.ishikari_crons (name, pattern, worker, args, queue, max_attempts) \
         VALUES ('broken', 'not a pattern', 'x', '{{}}', 'default', 1)",
        db.schema
    )))
    .execute(&db.pool)
    .await
    .unwrap();
    let app = admin(&db, false);

    let html = body(get(&app, "/admin/schedules", &[]).await).await;
    assert!(html.contains("broken"));
    assert!(html.contains("not a pattern"));
    assert!(html.contains("Never runs"));

    let res = post_form(&app, "/admin/schedules/delete", &[("name", "broken")]).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert!(crons(&db).list_all().await.unwrap().is_empty());

    db.drop().await;
}

#[tokio::test]
async fn a_schema_without_the_table_shows_an_error_not_a_failure() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE {}.ishikari_crons",
        db.schema
    )))
    .execute(&db.pool)
    .await
    .unwrap();

    let res = get(&admin(&db, false), "/admin/schedules", &[]).await;
    assert_eq!(res.status(), StatusCode::OK);
    let html = body(res).await;
    assert!(html.contains("Couldn&#39;t list the schedules"), "{html}");

    db.drop().await;
}

#[tokio::test]
async fn a_module_is_scheduled() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = admin(&db, true);
    let html = body(get(&app, "/admin/schedules", &[]).await).await;
    assert!(html.contains("Schedule a module"));
    assert!(html.contains(r#"value="test/kit""#), "the module to pick");

    let res = post_form(
        &app,
        "/admin/schedules",
        &[
            ("name", "weekly-kit"),
            ("module", "test/kit"),
            ("pattern", "30 8 * * 1"),
            ("timezone", "Europe/Paris"),
            ("args", r#"{"n": 1}"#),
            ("queue", ""),
            ("max_attempts", "3"),
            ("catch_up", "latest"),
        ],
    )
    .await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(&res), "/admin/schedules?put=weekly-kit");
    let stored = crons(&db).get("weekly-kit").await.unwrap().unwrap();
    assert_eq!(stored.pattern(), "30 8 * * 1");
    assert_eq!(stored.timezone(), ishikari::chrono_tz::Europe::Paris);
    assert_eq!(stored.worker(), ModuleJob::WORKER);
    assert_eq!(stored.queue(), ModuleJob::QUEUE);
    assert_eq!(stored.max_attempts(), 3);
    assert_eq!(stored.catch_up(), CatchUp::Latest);
    assert_eq!(
        stored.args(),
        &serde_json::json!({"ishikari_worker": ModuleJob::WORKER, "module": "test/kit", "args": {"n": 1}})
    );

    db.drop().await;
}

#[tokio::test]
async fn a_bad_schedule_is_refused_with_the_form_kept() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = admin(&db, true);
    for (field, value, why) in [
        ("pattern", "every day", "invalid cron pattern"),
        ("timezone", "Mars/Olympus", "Unknown timezone"),
        ("args", "[1]", "The args must be a JSON object"),
        ("module", "nope", "No module nope"),
        ("name", " ", "Name the schedule"),
    ] {
        let mut form = vec![
            ("name", "kept-name"),
            ("module", "test/kit"),
            ("pattern", "0 * * * *"),
            ("timezone", "UTC"),
            ("args", "{}"),
        ];
        form.iter_mut().find(|(k, _)| *k == field).unwrap().1 = value;
        let res = post_form(&app, "/admin/schedules", &form).await;
        assert_eq!(res.status(), StatusCode::BAD_REQUEST, "{field}");
        let html = body(res).await;
        assert!(html.contains(why), "{field}: {why}");
        if field != "name" {
            assert!(
                html.contains(r#"value="kept-name""#),
                "{field}: the form is kept"
            );
        }
    }
    assert!(crons(&db).list().await.unwrap().is_empty());

    // Without modules there's nothing to schedule here.
    let res = post_form(
        &admin(&db, false),
        "/admin/schedules",
        &[
            ("name", "x"),
            ("module", "test/kit"),
            ("pattern", "* * * * *"),
        ],
    )
    .await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);

    db.drop().await;
}

/// Insert a job of the schedule `name` for `slot`, in `state`, scheduled at `scheduled_at`.
async fn slot_job(
    db: &TestDb,
    name: &str,
    slot: &str,
    state: &str,
    scheduled_at: chrono::DateTime<Utc>,
) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {}.ishikari_jobs (worker, state, meta, scheduled_at) \
         VALUES ($1, $2::{}.ishikari_job_state, jsonb_build_object('cron', $3::text, 'slot', $4::text), $5) \
         RETURNING id",
        db.schema, db.schema
    )))
    .bind(ModuleJob::WORKER)
    .bind(state)
    .bind(name)
    .bind(slot)
    .bind(scheduled_at)
    .fetch_one(&db.pool)
    .await
    .unwrap()
}

fn hourly(name: &str) -> CronEntry {
    CronEntry::new(
        name,
        "0 * * * *",
        &ModuleJob::new("test/kit", serde_json::json!({})),
    )
    .unwrap()
}

#[tokio::test]
async fn the_last_run_is_the_latest_slot_even_when_an_older_one_is_retried_later() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    crons(&db).put(&hourly("hourly")).await.unwrap();
    let now = Utc::now();
    // The 10:00 job failed and retries in half an hour; the 11:00 one is done.
    let older = slot_job(
        &db,
        "hourly",
        "2026-11-26T10:00:00+00:00",
        "retryable",
        now + chrono::Duration::minutes(30),
    )
    .await;
    let latest = slot_job(
        &db,
        "hourly",
        "2026-11-26T11:00:00+00:00",
        "completed",
        now - chrono::Duration::minutes(1),
    )
    .await;

    let html = body(get(&admin(&db, false), "/admin/schedules", &[]).await).await;
    assert!(html.contains(&format!("/admin/jobs/{latest}")), "{html}");
    assert!(!html.contains(&format!("/admin/jobs/{older}")));
    assert!(html.contains("2026-11-26 11:00"), "the slot's time");

    db.drop().await;
}

#[tokio::test]
async fn a_schedule_put_again_after_a_delete_has_no_last_run_of_the_old_one() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    crons(&db).put(&hourly("hourly")).await.unwrap();
    let old = slot_job(
        &db,
        "hourly",
        "2026-11-26T10:00:00+00:00",
        "completed",
        Utc::now(),
    )
    .await;
    assert!(crons(&db).delete("hourly").await.unwrap());
    crons(&db).put(&hourly("hourly")).await.unwrap();

    let html = body(get(&admin(&db, false), "/admin/schedules", &[]).await).await;
    assert!(!html.contains(&format!("/admin/jobs/{old}")), "{html}");
    assert!(html.contains("none yet"));

    db.drop().await;
}

#[tokio::test]
async fn a_schedule_with_odd_whitespace_in_its_name_can_be_deleted() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    // Only by hand: `CronEntry::new` refuses such a name.
    assert!(CronEntry::new(
        " padded ",
        "0 * * * *",
        &ModuleJob::new("test/kit", serde_json::json!({}))
    )
    .is_err());
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {}.ishikari_crons (name, pattern, worker, args, queue, max_attempts) \
         VALUES (' padded ', '0 * * * *', 'x', '{{}}', 'default', 1)",
        db.schema
    )))
    .execute(&db.pool)
    .await
    .unwrap();

    let app = admin(&db, false);
    let res = post_form(&app, "/admin/schedules/delete", &[("name", " padded ")]).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert!(crons(&db).list_all().await.unwrap().is_empty());

    db.drop().await;
}

#[tokio::test]
async fn a_value_the_table_refuses_is_a_bad_request() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = admin(&db, true);
    let queue = "q".repeat(200);
    let res = post_form(
        &app,
        "/admin/schedules",
        &[
            ("name", "long-queue"),
            ("module", "test/kit"),
            ("pattern", "0 * * * *"),
            ("queue", &queue),
        ],
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    assert!(body(res).await.contains(r#"value="long-queue""#));

    db.drop().await;
}

#[tokio::test]
async fn a_failed_delete_keeps_the_forms_defaults() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let res = post_form(
        &admin(&db, true),
        "/admin/schedules/delete",
        &[("name", "gone")],
    )
    .await;
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    assert!(body(res).await.contains(r#"value="0 * * * *""#));

    db.drop().await;
}

#[tokio::test]
async fn a_catch_up_the_table_has_no_name_for_is_refused_with_the_form_kept() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let app = admin(&db, true);
    let res = post_form(
        &app,
        "/admin/schedules",
        &[
            ("name", "kit"),
            ("module", "test/kit"),
            ("pattern", "0 * * * *"),
            ("catch_up", "sometimes"),
        ],
    )
    .await;
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let html = body(res).await;
    assert!(html.contains("sometimes"), "why it's refused");
    assert!(html.contains(r#"value="kit""#), "the form kept");
    assert!(crons(&db).get("kit").await.unwrap().is_none());

    // The page first offers every missed slot.
    let html = body(get(&app, "/admin/schedules", &[]).await).await;
    assert!(html.contains(r#"<option value="all" selected>"#));

    db.drop().await;
}

#[tokio::test]
async fn a_schedule_is_paused_and_resumed() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    crons(&db).put(&hourly("nap")).await.unwrap();
    let app = admin(&db, false);

    let res = post_form(&app, "/admin/schedules/pause", &[("name", "nap")]).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    assert_eq!(location(&res), "/admin/schedules?paused=nap");
    assert!(crons(&db).get("nap").await.unwrap().unwrap().paused());
    let html = body(get(&app, &location(&res), &[]).await).await;
    assert!(html.contains("Paused nap"));
    assert!(
        html.contains(r#"<span class="ik-none">paused</span>"#),
        "no next run"
    );
    assert!(html.contains("/admin/schedules/resume"));
    assert!(!html.contains("/admin/schedules/pause"));

    // Pausing it again changes nothing, and says so as before.
    let res = post_form(&app, "/admin/schedules/pause", &[("name", "nap")]).await;
    assert_eq!(location(&res), "/admin/schedules?paused=nap");

    let res = post_form(&app, "/admin/schedules/resume", &[("name", "nap")]).await;
    assert_eq!(location(&res), "/admin/schedules?resumed=nap");
    assert!(!crons(&db).get("nap").await.unwrap().unwrap().paused());
    let html = body(get(&app, &location(&res), &[]).await).await;
    assert!(html.contains("Resumed nap"));
    assert!(html.contains("/admin/schedules/pause"));

    for action in ["pause", "resume", "run"] {
        let res = post_form(
            &app,
            &format!("/admin/schedules/{action}"),
            &[("name", "nope")],
        )
        .await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND, "{action}");
        assert!(body(res).await.contains("No schedule nope"), "{action}");
    }

    db.drop().await;
}

#[tokio::test]
async fn a_schedule_is_run_now_and_its_job_is_its_last_run() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    crons(&db).put(&hourly("now")).await.unwrap();
    crons(&db).pause("now").await.unwrap();
    let app = admin(&db, false);

    let res = post_form(&app, "/admin/schedules/run", &[("name", "now")]).await;
    assert_eq!(res.status(), StatusCode::SEE_OTHER);
    let to = location(&res);
    let id: i64 = to
        .strip_prefix("/admin/schedules?ran=now&job=")
        .expect("the job it inserted")
        .parse()
        .unwrap();
    let meta: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT meta FROM {}.ishikari_jobs WHERE id = $1",
        db.schema
    )))
    .bind(id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(meta["cron"], "now");
    assert_eq!(meta["run_now"], true);

    let html = body(get(&app, &to, &[]).await).await;
    assert!(html.contains(&format!("Ran now: job #{id}")));
    assert!(
        html.contains(&format!(r#"href="/admin/jobs/{id}""#)),
        "its last run"
    );
    assert!(html.contains("· run now"));

    db.drop().await;
}
