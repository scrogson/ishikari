//! The graphs: each page's nodes carry what admin.js needs to lay them out and draw the edges.

mod common;

use common::{body, get, TestDb};
use ishikari::prelude::*;
use ishikari::workflows::Dag;
use ishikari_admin::AppState;

#[ishikari::job]
#[derive(Clone)]
struct Step;

#[ishikari::worker]
impl Worker for Step {
    async fn perform(&self, _ctx: Context) -> PerformResult {
        Complete::default().into()
    }
}

/// A diamond: `fetch` fans out to `left` and `right`, which fan in to `merge`.
async fn diamond(db: &TestDb) -> (i64, [i64; 4]) {
    let wf = Dag::new("diamond")
        .schema(&db.schema)
        .add("fetch", Step)
        .add_after("left", Step, ["fetch"])
        .add_after("right", Step, ["fetch"])
        .add_after("merge", Step, ["left", "right"])
        .run(&db.pool)
        .await
        .unwrap();
    let mut ids = [0; 4];
    for (i, step) in ["fetch", "left", "right", "merge"].iter().enumerate() {
        ids[i] = sqlx::query_scalar(&format!(
            "SELECT id FROM {}.ishikari_jobs WHERE workflow_id = $1 AND meta->>'ishikari_step' = $2",
            db.schema
        ))
        .bind(wf.id)
        .bind(step)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    }
    (wf.id, ids)
}

/// The node for `id`: its opening tag up to the next node.
fn node(html: &str, id: i64) -> &str {
    let attr = html
        .find(&format!(r#"data-node="{id}""#))
        .unwrap_or_else(|| panic!("no node for #{id}"));
    let start = html[..attr].rfind('<').unwrap();
    let end = html[attr + 1..]
        .find("data-node=")
        .map_or(html.len(), |i| attr + 1 + i);
    &html[start..end]
}

fn deps(node: &str) -> Vec<i64> {
    let start = node.find(r#"data-deps=""#).map(|i| i + 11);
    let Some(start) = start else { return vec![] };
    let end = start + node[start..].find('"').unwrap();
    let mut ids: Vec<i64> = node[start..end]
        .split_whitespace()
        .map(|id| id.parse().unwrap())
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn a_workflows_graph_names_each_steps_upstreams() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (wf, [fetch, left, right, merge]) = diamond(&db).await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    let html = body(get(&app, &format!("/workflows/{wf}"), &[]).await).await;
    assert!(html.contains("data-ik-graph"));
    assert_eq!(deps(node(&html, fetch)), Vec::<i64>::new());
    assert_eq!(deps(node(&html, left)), vec![fetch]);
    assert_eq!(deps(node(&html, right)), vec![fetch]);
    assert_eq!(deps(node(&html, merge)), vec![left, right]);
    // Titled by step, linked to the job, with its state for the edges out of it.
    let tag = node(&html, merge);
    assert!(tag.contains(r#"<span class="ik-graph-node-title">merge</span>"#));
    assert!(tag.contains(&format!(r#"href="/jobs/{merge}""#)));
    assert!(tag.contains(r#"data-state="pending""#));

    db.drop().await;
}

#[tokio::test]
async fn a_jobs_graph_puts_it_between_its_upstreams_and_dependents() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let (_, [fetch, left, right, merge]) = diamond(&db).await;
    let app = ishikari_admin::app(AppState::with_schema(db.pool.clone(), &db.schema));

    for page in [format!("/jobs/{left}"), format!("/dependencies/{left}")] {
        let html = body(get(&app, &page, &[]).await).await;
        assert!(html.contains("data-ik-graph"), "{page}");
        let current = node(&html, left);
        assert!(current.contains("is-current"), "{page}");
        assert_eq!(deps(current), vec![fetch], "{page}");
        assert_eq!(deps(node(&html, fetch)), Vec::<i64>::new(), "{page}");
        assert_eq!(deps(node(&html, merge)), vec![left], "{page}");
        assert!(!html.contains(&format!(r#"data-node="{right}""#)), "{page}");
    }

    db.drop().await;
}
