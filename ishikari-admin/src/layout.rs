//! The dashboard inside a host app's own layout.
//!
//! Every dashboard page is rendered as a [`Page`]: its title and the HTML of its main area. By
//! default ([`app`](crate::app)) the dashboard wraps it in its own standalone page (sidebar,
//! light/dark theme). A host that mounts the dashboard inside its admin implements [`Layout`]
//! and builds the router with [`app_with_layout`](crate::app_with_layout); each page then comes
//! inside the host's layout, colored by the host's theme variables through the
//! [embed stylesheet](embed_stylesheet_href).
//!
//! htmx fragments (the tables and counts that refresh in place) are never wrapped.

use std::sync::Arc;

use askama::Template;
use axum::extract::Request;
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{Html, IntoResponse, Response};

use crate::NavItem;

/// A host app's layout around each dashboard page.
///
/// ```rust
/// use askama::Template;
/// use axum::http::{request::Parts, StatusCode};
/// use axum::response::{Html, IntoResponse, Response};
/// use ishikari_admin::{embed_stylesheet_href, Layout, Page};
///
/// #[derive(Template)]
/// #[template(
///     ext = "html",
///     source = r#"<!doctype html>
/// <html data-theme="mine">
/// <head>
///   <title>{{ page.title }} · My App</title>
///   <link rel="stylesheet" href="/my/app.css">
///   <link rel="stylesheet" href="{{ css }}">
///   <script src="/my/htmx.min.js"></script>
/// </head>
/// <body>
///   <nav>{{ user }}'s menu</nav>
///   <main>{{ page.nav_html()|safe }}{{ page.body|safe }}</main>
/// </body>
/// </html>"#
/// )]
/// struct Shell<'a> {
///     page: &'a Page,
///     css: String,
///     user: &'a str,
/// }
///
/// /// Who the host's auth middleware let in.
/// #[derive(Clone)]
/// struct User(String);
///
/// struct MyAdmin;
///
/// impl Layout for MyAdmin {
///     fn render(&self, parts: &Parts, page: Page) -> Response {
///         let Some(User(user)) = parts.extensions.get::<User>() else {
///             return StatusCode::UNAUTHORIZED.into_response();
///         };
///         let css = embed_stylesheet_href(&page.base_path);
///         match (Shell { page: &page, css, user }).render() {
///             Ok(html) => Html(html).into_response(),
///             Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
///         }
///     }
/// }
/// ```
pub trait Layout: Send + Sync {
    /// The whole page: `page.body` inside the host's layout, for the request `parts` describe.
    /// Their extensions carry what the host's middleware put there (the signed-in user, say).
    ///
    /// A successful response gets the page's own status; any other is returned as it is.
    fn render(&self, parts: &Parts, page: Page) -> Response;
}

impl<F> Layout for F
where
    F: Fn(&Parts, Page) -> Response + Send + Sync,
{
    fn render(&self, parts: &Parts, page: Page) -> Response {
        self(parts, page)
    }
}

/// A dashboard page, for a host's [`Layout`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Page {
    /// The page's own title ("Jobs", "Job #42"), as text; the layout adds its app's name.
    pub title: String,
    /// The HTML for the layout's main area, escaped. It needs htmx 2 on the page and the
    /// [embed stylesheet](embed_stylesheet_href).
    pub body: String,
    /// Where the dashboard is mounted (`/jobs`), empty at the root.
    pub base_path: String,
    /// The dashboard's sections (and any extra [`NavItem`]s), their links prefixed.
    pub nav: Vec<NavItem>,
    /// Which of `nav` this page belongs to.
    pub active: Option<usize>,
}

impl Page {
    /// A page with this title and body (to test a layout with).
    pub fn new(title: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            body: body.into(),
            base_path: String::new(),
            nav: Vec::new(),
            active: None,
        }
    }

    /// The section of `nav` this page belongs to.
    pub fn active_item(&self) -> Option<&NavItem> {
        self.active.and_then(|i| self.nav.get(i))
    }

    /// The dashboard's sections as a row of links, this page's marked: for a layout that
    /// doesn't show `nav` itself.
    pub fn nav_html(&self) -> String {
        SectionNav { page: self }.render().unwrap_or_default()
    }
}

/// Where a host page links the embed stylesheet from: the dashboard's components (`ik-*`) and
/// utilities, with no theme, nothing on `:root` and a reset only inside the dashboard. Its colors
/// come from the host's CSS variables: `--ik-<token>`, else shadcn/ui's `--<token>`, else
/// Tailwind's `--color-<token>`, else daisyUI 5's, else a default that follows `color-scheme`.
pub fn embed_stylesheet_href(base_path: &str) -> String {
    asset(base_path, "admin-embed.css")
}

/// Appended to asset addresses, so an upgrade isn't hidden behind the browser's cache.
pub(crate) const ASSET_VERSION: &str = env!("CARGO_PKG_VERSION");

fn asset(base_path: &str, file: &str) -> String {
    format!(
        "{}/assets/{file}?v={ASSET_VERSION}",
        base_path.trim_end_matches('/')
    )
}

#[derive(Template)]
#[template(path = "partials/section_nav.html")]
struct SectionNav<'a> {
    page: &'a Page,
}

impl SectionNav<'_> {
    fn is_active(&self, i: &usize) -> bool {
        self.page.active == Some(*i)
    }
}

/// What a page template gives a layout. Rendered only when there is one.
pub(crate) trait Content: Send + Sync {
    fn title(&self) -> askama::Result<String>;
    fn content(&self) -> askama::Result<String>;
    fn base_path(&self) -> &str;
    fn nav_items(&self) -> &[crate::NavItem];
}

/// A page template, kept with its standalone response for a layout to render.
#[derive(Clone)]
struct Deferred(Arc<dyn Content>);

/// The standalone page, with the template kept for a layout.
pub(crate) fn respond<T: Template + Content + 'static>(page: T) -> Response {
    match page.render() {
        Ok(html) => {
            let mut res = Html(html).into_response();
            res.extensions_mut().insert(Deferred(Arc::new(page)));
            res
        }
        Err(e) => {
            tracing::error!(error = %e, "rendering an admin page");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Middleware: a page's response goes through `layout`; anything else as it is.
pub(crate) async fn wrap(layout: Arc<dyn Layout>, req: Request, next: Next) -> Response {
    let (parts, body) = req.into_parts();
    let kept = parts.clone();
    let res = next.run(Request::from_parts(parts, body)).await;
    let Some(Deferred(content)) = res.extensions().get::<Deferred>().cloned() else {
        return res;
    };
    let page = match page_of(&*content, &kept) {
        Ok(page) => page,
        Err(e) => {
            tracing::error!(error = %e, "rendering an admin page");
            return StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };
    let mut out = layout.render(&kept, page);
    if out.status().is_success() {
        *out.status_mut() = res.status();
    }
    out
}

fn page_of(content: &dyn Content, parts: &Parts) -> askama::Result<Page> {
    let base_path = content.base_path().to_string();
    let nav = content.nav_items().to_vec();
    let active = active(&nav, &base_path, parts.uri.path());
    let body = format!(
        r#"<div class="ishikari-admin space-y-6">{}</div><script src="{}" defer></script>"#,
        content.content()?,
        asset(&base_path, "admin.js")
    );
    Ok(Page {
        title: unescape(content.title()?.trim()),
        body,
        base_path,
        nav,
        active,
    })
}

/// Text askama escaped, as it was.
fn unescape(html: &str) -> String {
    html.replace("&#34;", "\"")
        .replace("&#39;", "'")
        .replace("&#60;", "<")
        .replace("&#62;", ">")
        .replace("&#38;", "&")
}

/// The item whose link is the longest prefix of the page's path (`path` is as the admin's
/// router sees it, under `base_path`). The dashboard's own link matches only itself.
fn active(nav: &[NavItem], base_path: &str, path: &str) -> Option<usize> {
    let root = base_path.trim_end_matches('/');
    let full = format!("{root}{path}");
    let full = full.trim_end_matches('/');
    nav.iter()
        .enumerate()
        .filter(|(_, item)| {
            let href = item.href.trim_end_matches('/');
            if href == root {
                full == root
            } else {
                full == href
                    || full
                        .strip_prefix(href)
                        .is_some_and(|rest| rest.starts_with('/'))
            }
        })
        .max_by_key(|(_, item)| item.href.trim_end_matches('/').len())
        .map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nav(base: &str) -> Vec<NavItem> {
        let p = |s: &str| format!("{base}{s}");
        vec![
            NavItem::new("Dashboard", if base.is_empty() { "/" } else { base }),
            NavItem::new("Jobs", p("/jobs")),
            NavItem::new("Queues", p("/queues")),
            NavItem::new("Docs", "https://example.com/docs"),
        ]
    }

    #[test]
    fn the_longest_matching_link_is_the_active_one() {
        let nav = nav("/jobs");
        assert_eq!(active(&nav, "/jobs", "/"), Some(0));
        assert_eq!(active(&nav, "/jobs", "/jobs"), Some(1));
        assert_eq!(active(&nav, "/jobs", "/jobs/42"), Some(1));
        assert_eq!(active(&nav, "/jobs", "/queues/default"), Some(2));
        assert_eq!(
            active(&nav, "/jobs", "/builder"),
            None,
            "not under any section"
        );
        assert_eq!(active(&nav, "/jobs", "/jobsx"), None);
    }

    #[test]
    fn at_the_root_too() {
        let nav = nav("");
        assert_eq!(active(&nav, "", "/"), Some(0));
        assert_eq!(active(&nav, "", "/queues"), Some(2));
    }

    #[test]
    fn a_title_is_text() {
        assert_eq!(
            unescape("A &#38; B &#60;i&#62; &#34;q&#34; &#39;s&#39; &#38;#60;"),
            "A & B <i> \"q\" 's' &#60;"
        );
    }

    #[test]
    fn the_section_nav_marks_the_active_page_and_escapes_labels() {
        let mut page = Page::new("Jobs", "");
        page.nav = vec![
            NavItem::new("Dashboard", "/jobs"),
            NavItem::new("<Jobs>", "/jobs/jobs"),
        ];
        page.active = Some(1);
        let html = page.nav_html();
        assert!(html.contains(r#"href="/jobs/jobs" class="ik-tab" aria-current="page""#));
        assert!(html.contains("&#60;Jobs&#62;") || html.contains("&lt;Jobs&gt;"));
        assert_eq!(page.active_item().unwrap().href, "/jobs/jobs");
    }
}
