//! A host app's own layout around the dashboard, as `app_with_layout` shows it: the host's
//! header and theme (shadcn/ui-style variables, here a stone and emerald palette), the
//! dashboard's pages in its main area.

use axum::http::request::Parts;
use axum::response::{Html, IntoResponse, Response};
use ishikari_admin::{embed_stylesheet_href, Page};

pub fn layout(_parts: &Parts, page: Page) -> Response {
    let title = escape(&page.title);
    Html(format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} · Acme Ops</title>
<style>
  :root {{
    color-scheme: light;
    --background: oklch(0.985 0.001 106);
    --foreground: oklch(0.216 0.006 56);
    --card: oklch(1 0 0);
    --card-foreground: oklch(0.216 0.006 56);
    --muted-foreground: oklch(0.553 0.013 58);
    --border: oklch(0.923 0.003 49);
    --input: oklch(0.869 0.005 56);
    --primary: oklch(0.596 0.145 163);
    --primary-foreground: oklch(0.985 0.001 106);
    --radius: 0.375rem;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      color-scheme: dark;
      --background: oklch(0.147 0.004 49);
      --foreground: oklch(0.985 0.001 106);
      --card: oklch(0.216 0.006 56);
      --card-foreground: oklch(0.985 0.001 106);
      --muted-foreground: oklch(0.709 0.01 56);
      --border: oklch(1 0 0 / 10%);
      --input: oklch(1 0 0 / 15%);
      --primary: oklch(0.696 0.17 162);
      --primary-foreground: oklch(0.147 0.004 49);
    }}
  }}
  *, ::before, ::after {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    background: var(--background);
    color: var(--foreground);
    font: 15px/1.5 ui-serif, Georgia, serif;
  }}
  .host-bar {{
    display: flex; align-items: center; gap: 1rem;
    padding: 0.875rem 2rem; border-bottom: 1px solid var(--border);
  }}
  .host-bar b {{ font-size: 1.125rem; }}
  .host-bar span {{ margin-left: auto; color: var(--muted-foreground); font-size: 0.875rem; }}
  .host-main {{ max-width: 80rem; margin: 0 auto; padding: 1.5rem 2rem 3rem; }}
</style>
<link rel="stylesheet" href="{css}">
<script src="{base}/assets/htmx.min.js"></script>
</head>
<body>
<header class="host-bar"><b>Acme Ops</b><span>Signed in as ada</span></header>
<main class="host-main">{nav}{body}</main>
</body>
</html>"#,
        css = embed_stylesheet_href(&page.base_path),
        base = page.base_path,
        nav = page.nav_html(),
        body = page.body,
    ))
    .into_response()
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
