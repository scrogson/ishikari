//! The job lists page by id (keyset pages) and count cheaply on a large table.

mod common;

use common::{body, get, TestDb};
use ishikari_admin::AppState;

/// `n` jobs of `worker` in `queue` and `state`, inserted in id order; their ids.
async fn jobs(db: &TestDb, n: i64, queue: &str, state: &str) -> Vec<i64> {
    sqlx::query_scalar(&format!(
        "INSERT INTO {s}.ishikari_jobs (worker, queue, state)
         SELECT 'app::workers::Work', $1, $2::{s}.ishikari_job_state FROM generate_series(1, $3)
         RETURNING id",
        s = db.schema
    ))
    .bind(queue)
    .bind(state)
    .bind(n)
    .fetch_all(&db.pool)
    .await
    .expect("insert jobs")
}

/// The ids of the jobs a list shows, in order.
fn listed(html: &str) -> Vec<i64> {
    html.split(r#"<a class="ik-link ik-mono" href="/jobs/"#)
        .skip(1)
        .map(|s| s.split('"').next().unwrap().parse().unwrap())
        .collect()
}

/// The URL query of the pager's link labelled `label` ("Newest", "Newer" or "Older" jobs).
fn link(html: &str, label: &str) -> Option<String> {
    let label = format!(r#"aria-label="{label} jobs""#);
    let a = html.split("<a ").find(|a| a.contains(&label))?;
    let href = a.split(r#"href="?"#).nth(1)?.split('"').next()?;
    Some(href.replace("&#38;", "&").replace("&amp;", "&"))
}

/// Ids `from` down to `to`, as `ids` (oldest first) has them.
fn newest_first(ids: &[i64], range: std::ops::RangeInclusive<usize>) -> Vec<i64> {
    ids[range].iter().rev().copied().collect()
}

#[tokio::test]
async fn the_job_list_pages_by_id_both_ways() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let ids = jobs(&db, 120, "default", "available").await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let html = body(get(&app, "/jobs", &[]).await).await;
    assert_eq!(listed(&html), newest_first(&ids, 70..=119));
    assert!(html.contains("120</span> jobs"), "an exact count");
    assert_eq!(link(&html, "Newer"), None);
    assert_eq!(link(&html, "Newest"), None);
    let older = link(&html, "Older").expect("older jobs");
    assert_eq!(older, format!("before={}", ids[70]));

    let html = body(get(&app, &format!("/jobs?{older}"), &[]).await).await;
    assert_eq!(listed(&html), newest_first(&ids, 20..=69));
    assert_eq!(link(&html, "Newest").as_deref(), Some(""));
    assert_eq!(link(&html, "Newer"), Some(format!("after={}", ids[69])));
    let older = link(&html, "Older").expect("older jobs");

    let html = body(get(&app, &format!("/jobs?{older}"), &[]).await).await;
    assert_eq!(listed(&html), newest_first(&ids, 0..=19));
    assert_eq!(link(&html, "Older"), None, "the oldest page");
    let newer = link(&html, "Newer").expect("newer jobs");

    let html = body(get(&app, &format!("/jobs?{newer}"), &[]).await).await;
    assert_eq!(listed(&html), newest_first(&ids, 20..=69));
    let newer = link(&html, "Newer").expect("newer jobs");

    // Back at the top: the newest page, whole, with nothing newer.
    let html = body(get(&app, &format!("/jobs?{newer}"), &[]).await).await;
    assert_eq!(listed(&html), newest_first(&ids, 70..=119));
    assert_eq!(link(&html, "Newer"), None);
    assert_eq!(link(&html, "Newest"), None);

    db.drop().await;
}

/// Each link of a filtered list keeps its filters, as the htmx fragment's links do.
#[tokio::test]
async fn the_job_lists_pages_keep_its_filters() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let mut wanted = Vec::new();
    for _ in 0..3 {
        wanted.extend(jobs(&db, 8, "mailers", "completed").await);
        jobs(&db, 5, "mailers", "available").await;
        jobs(&db, 5, "default", "completed").await;
    }
    wanted.reverse();
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));
    let filters = "state=completed&queue=mailers&per_page=10";

    let html = body(
        get(
            &app,
            &format!("/api/jobs?{filters}"),
            &[("hx-request", "true")],
        )
        .await,
    )
    .await;
    assert_eq!(listed(&html), wanted[..10]);
    let older = link(&html, "Older").expect("older jobs");
    assert_eq!(older, format!("before={}&{filters}", wanted[9]));
    assert!(
        html.contains(&format!(
            r#"hx-get="/api/jobs?before={}&#38;{}""#,
            wanted[9],
            filters.replace('&', "&#38;")
        )),
        "the htmx link keeps them too: {html}"
    );

    let html = body(get(&app, &format!("/api/jobs?{older}"), &[]).await).await;
    assert_eq!(listed(&html), wanted[10..20]);
    assert_eq!(link(&html, "Newest").as_deref(), Some(filters));
    let newer = link(&html, "Newer").expect("newer jobs");
    assert_eq!(newer, format!("after={}&{filters}", wanted[10]));
    let older = link(&html, "Older").expect("older jobs");

    let html = body(get(&app, &format!("/jobs?{older}"), &[]).await).await;
    assert_eq!(listed(&html), wanted[20..]);
    assert_eq!(link(&html, "Older"), None);
    assert!(html.contains("24</span> jobs"), "the filtered count");

    let html = body(get(&app, &format!("/api/jobs?{newer}"), &[]).await).await;
    assert_eq!(listed(&html), wanted[..10]);

    db.drop().await;
}

#[tokio::test]
async fn a_queues_jobs_page_by_id_keeping_the_state() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let mut wanted = jobs(&db, 15, "mailers", "completed").await;
    jobs(&db, 10, "mailers", "available").await;
    jobs(&db, 10, "default", "completed").await;
    wanted.extend(jobs(&db, 15, "mailers", "completed").await);
    wanted.reverse();
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let html = body(get(&app, "/queues/mailers?state=completed", &[]).await).await;
    assert_eq!(listed(&html), wanted[..25]);
    assert!(html.contains("30</span> jobs"), "{html}");
    let older = link(&html, "Older").expect("older jobs");
    assert_eq!(older, format!("before={}&state=completed", wanted[24]));

    let html = body(get(&app, &format!("/queues/mailers?{older}"), &[]).await).await;
    assert_eq!(listed(&html), wanted[25..]);
    assert_eq!(link(&html, "Older"), None);
    assert_eq!(
        link(&html, "Newer"),
        Some(format!("after={}&state=completed", wanted[25]))
    );

    db.drop().await;
}

/// Past 10,000 jobs, a count is the planner's estimate (for that state, of that queue), shown
/// as one; below it, exact.
#[tokio::test]
async fn large_counts_are_estimated() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    jobs(&db, 12_000, "mailers", "completed").await;
    jobs(&db, 3, "mailers", "available").await;
    jobs(&db, 2, "default", "available").await;
    sqlx::query(&format!("ANALYZE {}.ishikari_jobs", db.schema))
        .execute(&db.pool)
        .await
        .unwrap();
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let stat = |html: &str, label: &str| {
        let after = html.split(&format!(">{label}</div>")).nth(1).unwrap();
        let value = after.split("</div>").next().unwrap();
        value.rsplit('>').next().unwrap().trim().to_string()
    };
    let html = body(get(&app, "/", &[]).await).await;
    assert_eq!(stat(&html, "Completed"), "≈12k");
    assert_eq!(stat(&html, "Available"), "5");
    assert!(html.contains("≈12k</span> in total"), "{html}");

    let html = body(get(&app, "/jobs?state=completed", &[]).await).await;
    assert!(html.contains("≈12k</span> jobs"), "{html}");
    let html = body(get(&app, "/jobs?state=available", &[]).await).await;
    assert!(html.contains(">5</span> jobs"), "{html}");

    let html = body(get(&app, "/queues/mailers", &[]).await).await;
    assert_eq!(stat(&html, "Completed"), "≈12k");
    assert_eq!(stat(&html, "Available"), "3");
    let html = body(get(&app, "/queues", &[]).await).await;
    assert!(html.contains(">≈12k</td>"), "the mailers' total: {html}");

    db.drop().await;
}

/// The filters' options come from the state index and the planner's statistics, not from
/// reading every job, and always have the one the list is filtered by.
#[tokio::test]
async fn the_job_filters_list_queues_and_workers() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    jobs(&db, 3, "alpha", "completed").await;
    jobs(&db, 3, "beta", "available").await;
    jobs(&db, 3, "alpha", "discarded").await;
    db.job("app::workers::Other", "scheduled").await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let html = body(get(&app, "/jobs", &[]).await).await;
    for option in [
        r#"<option value="alpha" "#,
        r#"<option value="beta" "#,
        r#"<option value="default" "#,
        r#"<option value="app::workers::Work" "#,
        r#"<option value="app::workers::Other" "#,
    ] {
        assert_eq!(html.matches(option).count(), 1, "{option}");
    }

    let html = body(get(&app, "/jobs?queue=gamma&worker=app::workers::Gone", &[]).await).await;
    assert!(
        html.contains(r#"<option value="gamma" selected>"#),
        "{html}"
    );
    assert!(
        html.contains(r#"<option value="app::workers::Gone" title="app::workers::Gone" selected>"#)
    );

    db.drop().await;
}

#[tokio::test]
async fn the_job_list_filters_by_workflow() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let workflow: i64 = sqlx::query_scalar(&format!(
        "INSERT INTO {}.ishikari_workflows (name) VALUES ('wf') RETURNING id",
        db.schema
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();
    let ids = jobs(&db, 3, "default", "completed").await;
    jobs(&db, 2, "default", "completed").await;
    sqlx::query(&format!(
        "UPDATE {}.ishikari_jobs SET workflow_id = $1 WHERE id = ANY($2)",
        db.schema
    ))
    .bind(workflow)
    .bind(&ids)
    .execute(&db.pool)
    .await
    .unwrap();
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let html = body(get(&app, &format!("/jobs?workflow_id={workflow}"), &[]).await).await;
    assert_eq!(listed(&html), ids.iter().rev().copied().collect::<Vec<_>>());

    db.drop().await;
}
