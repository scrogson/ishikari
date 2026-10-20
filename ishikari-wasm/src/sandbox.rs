//! What a guest can reach through `host`: its node's logs, and HTTP to the hosts its module was
//! granted; and through `script`, the script the runtime of scripts runs.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::runtime::bindings::ishikari::node::host::{Host, Level, Request, Response};
use crate::runtime::bindings::ishikari::node::script;

/// The most lines a guest may log; the rest are dropped.
const MAX_LOG_LINES: usize = 1000;

/// The largest response body `fetch` reads.
const MAX_RESPONSE_BYTES: u64 = 10 * 1024 * 1024;

/// A call's view of the host.
pub(crate) struct Sandbox {
    allowed_hosts: Vec<String>,
    /// When the call runs out of time; `fetch` gets what's left as its own timeout.
    pub deadline: Option<Instant>,
    pub logs: Vec<String>,
    /// The script, when the guest is the runtime of scripts.
    pub script: Option<Arc<str>>,
}

impl Sandbox {
    pub fn new(allowed_hosts: Vec<String>) -> Self {
        Self {
            allowed_hosts,
            deadline: None,
            logs: Vec::new(),
            script: None,
        }
    }

    fn remaining(&self) -> Duration {
        self.deadline.map_or(Duration::ZERO, |deadline| {
            deadline.saturating_duration_since(Instant::now())
        })
    }
}

impl Host for Sandbox {
    fn log(&mut self, level: Level, message: String) {
        if self.logs.len() < MAX_LOG_LINES {
            let level = match level {
                Level::Debug => "debug",
                Level::Info => "info",
                Level::Warn => "warn",
                Level::Error => "error",
            };
            self.logs.push(format!("{level}: {message}"));
        }
    }

    fn fetch(&mut self, request: Request) -> Result<Response, String> {
        let uri: ureq::http::Uri = request
            .url
            .parse()
            .map_err(|e| format!("bad URL {}: {e}", request.url))?;
        let default_port = match uri.scheme_str() {
            Some("http") => 80,
            Some("https") => 443,
            _ => return Err(format!("{} isn't an http or https URL", request.url)),
        };
        let host = uri.host().unwrap_or_default();
        let port = uri.port_u16().unwrap_or(default_port);
        if !allows(&self.allowed_hosts, host, port) {
            return Err(format!("{host} isn't a host this module may reach"));
        }
        let remaining = self.remaining();
        if remaining.is_zero() {
            return Err("no time left for the request".into());
        }

        let mut builder = ureq::http::Request::builder()
            .method(request.method.as_str())
            .uri(uri);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        let request = builder
            .body(request.body.unwrap_or_default())
            .map_err(|e| format!("bad request: {e}"))?;
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .max_redirects(0)
            .timeout_global(Some(remaining))
            .build()
            .into();
        let mut response = agent.run(request).map_err(|e| e.to_string())?;
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    String::from_utf8_lossy(value.as_bytes()).into_owned(),
                )
            })
            .collect();
        let body = response
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BYTES)
            .read_to_vec()
            .map_err(|e| e.to_string())?;
        Ok(Response {
            status: response.status().as_u16(),
            headers,
            body,
        })
    }
}

impl script::Host for Sandbox {
    fn source(&mut self) -> String {
        self.script.as_deref().unwrap_or_default().to_string()
    }
}

/// Whether `allowed` lets a request reach `host` on `port`: an entry is a host (any port), a
/// `host:port`, or `*.domain` (its subdomains).
fn allows(allowed: &[String], host: &str, port: u16) -> bool {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    allowed.iter().any(|entry| {
        let entry = entry.trim().to_ascii_lowercase();
        let (pattern, entry_port) = match entry.rsplit_once(':') {
            Some((pattern, p)) if !pattern.ends_with(':') => match p.parse::<u16>() {
                Ok(p) => (pattern.to_string(), Some(p)),
                Err(_) => (entry.clone(), None),
            },
            _ => (entry.clone(), None),
        };
        if entry_port.is_some_and(|p| p != port) {
            return false;
        }
        match pattern.strip_prefix("*.") {
            Some(domain) => host.ends_with(&format!(".{domain}")),
            None => host == pattern,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::allows;

    fn allowed(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|e| e.to_string()).collect()
    }

    #[test]
    fn a_host_is_allowed_on_any_port() {
        let list = allowed(&["api.example.com"]);
        assert!(allows(&list, "api.example.com", 443));
        assert!(allows(&list, "API.example.com.", 8080));
        assert!(!allows(&list, "example.com", 443));
        assert!(!allows(&list, "evil-api.example.com", 443));
    }

    #[test]
    fn a_host_and_port_is_allowed_on_that_port_only() {
        let list = allowed(&["127.0.0.1:8080"]);
        assert!(allows(&list, "127.0.0.1", 8080));
        assert!(!allows(&list, "127.0.0.1", 8081));
    }

    #[test]
    fn a_wildcard_allows_subdomains_only() {
        let list = allowed(&["*.example.com"]);
        assert!(allows(&list, "api.example.com", 443));
        assert!(allows(&list, "a.b.example.com", 443));
        assert!(!allows(&list, "example.com", 443));
        assert!(!allows(&list, "notexample.com", 443));
    }

    #[test]
    fn nothing_is_allowed_by_default() {
        assert!(!allows(&[], "localhost", 80));
    }
}
