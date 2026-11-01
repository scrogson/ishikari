//! What a guest can reach through `host`: its node's logs, and HTTP to the hosts and the secrets
//! its module was granted; and through `script`, the script the runtime of scripts runs.

use std::collections::HashMap;
use std::sync::Arc;

use ishikari::secrets::Secret;
use std::time::{Duration, Instant};

use crate::redact::Redact;
use crate::runtime::bindings::ishikari::node::host::{Host, Level, Request, Response};
use crate::runtime::bindings::ishikari::node::script;

/// The most lines a guest may log; the rest are dropped.
const MAX_LOG_LINES: usize = 1000;

/// The most bytes a logged line keeps (its level included); the rest of it is dropped.
const MAX_LOG_LINE_BYTES: usize = 8 * 1024;

/// The most bytes of lines a guest may log; the rest are dropped.
const MAX_LOG_BYTES: usize = 1024 * 1024;

/// The largest response body `fetch` reads.
const MAX_RESPONSE_BYTES: u64 = 10 * 1024 * 1024;

/// A call's view of the host.
#[derive(Default)]
pub(crate) struct Sandbox {
    allowed_hosts: Vec<String>,
    /// The names of the secrets the module was granted.
    granted: Vec<String>,
    /// The host's secrets of those names there are; `None` when the host keeps none.
    secrets: Option<HashMap<String, Secret>>,
    /// When the call runs out of time; `fetch` gets what's left as its own timeout.
    pub deadline: Option<Instant>,
    pub logs: Vec<String>,
    /// The bytes of `logs`.
    log_bytes: usize,
    /// The values of `secrets`, taken out of a line before it's cut short.
    redact: Redact,
    /// The names of the granted secrets the guest read, in the order it first read them.
    pub read: Vec<String>,
    /// The script, when the guest is the runtime of scripts.
    pub script: Option<Arc<str>>,
}

impl Sandbox {
    /// A sandbox reaching `allowed_hosts`, and the secrets `granted`, of which `secrets` are
    /// the host's.
    pub fn new(
        allowed_hosts: Vec<String>,
        granted: Vec<String>,
        secrets: Option<HashMap<String, Secret>>,
    ) -> Self {
        Self {
            allowed_hosts,
            granted,
            redact: Redact::new(secrets.iter().flat_map(|secrets| secrets.values())),
            secrets,
            ..Self::default()
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
        if self.logs.len() >= MAX_LOG_LINES || self.log_bytes >= MAX_LOG_BYTES {
            return;
        }
        let level = match level {
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        };
        let room = MAX_LOG_LINE_BYTES.min(MAX_LOG_BYTES - self.log_bytes);
        // Redacted before it's cut short, where a secret's value cut in two couldn't be found.
        // What's past the room by more than a secret's length can't be kept, nor be part of one
        // that is, so it isn't searched.
        let searched = prefix(&message, room + self.redact.longest());
        let line = format!("{level}: {}", self.redact.text(searched));
        let line = prefix(&line, room).to_string();
        self.log_bytes += line.len();
        self.logs.push(line);
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
        // The Host is the URL's (ureq sets it): the guest's could reach the server's other names.
        for (name, value) in request
            .headers
            .iter()
            .filter(|(name, _)| !name.eq_ignore_ascii_case("host"))
        {
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

    fn secret(&mut self, name: String) -> Result<String, String> {
        if !self.granted.contains(&name) {
            return Err(format!("{name} isn't a secret this module was granted"));
        }
        if !self.read.contains(&name) {
            self.read.push(name.clone());
        }
        let secrets = self.secrets.as_ref().ok_or("this host keeps no secrets")?;
        match secrets.get(&name) {
            Some(secret) => Ok(secret.expose().to_string()),
            None => Err(format!("no secret {name}")),
        }
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

/// The longest start of `text` of at most `bytes` bytes that ends on a character.
pub(crate) fn prefix(text: &str, bytes: usize) -> &str {
    if text.len() <= bytes {
        return text;
    }
    let mut end = bytes;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests {
    use super::{allows, Sandbox, MAX_LOG_BYTES, MAX_LOG_LINE_BYTES};
    use crate::runtime::bindings::ishikari::node::host::{Host, Level, Request};
    use ishikari::secrets::Secret;
    use std::collections::HashMap;
    use std::io::{BufRead, BufReader, Write};
    use std::time::{Duration, Instant};

    fn allowed(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|e| e.to_string()).collect()
    }

    /// A guest logging a long line over and over is kept to a line's and a call's bytes.
    #[test]
    fn logs_are_kept_to_their_bytes() {
        let mut sandbox = Sandbox::default();
        let line = "x".repeat(64 * 1024);
        for _ in 0..1000 {
            sandbox.log(Level::Info, line.clone());
        }
        assert!(sandbox.logs.iter().all(|l| l.len() <= MAX_LOG_LINE_BYTES));
        let kept: usize = sandbox.logs.iter().map(String::len).sum();
        assert!(kept <= MAX_LOG_BYTES, "{kept}");
        assert!(sandbox.logs[0].starts_with("info: xxx"));

        // A line cut short at a character's boundary.
        let mut sandbox = Sandbox::default();
        sandbox.log(Level::Info, "é".repeat(MAX_LOG_LINE_BYTES));
        assert!(sandbox.logs[0].len() <= MAX_LOG_LINE_BYTES);
    }

    /// A line is redacted before it's cut short, so no part of a secret is left behind where it
    /// can't be recognized.
    #[test]
    fn a_long_line_is_redacted_before_it_is_cut() {
        let secret = "s3cr3t-value-0123456789";
        let secrets = HashMap::from([("k".to_string(), Secret::from(secret))]);
        let mut sandbox = Sandbox::new(vec![], vec!["k".into()], Some(secrets));
        let line = format!(
            "{}{secret}",
            "a".repeat(MAX_LOG_LINE_BYTES - "info: ".len() - 8)
        );
        sandbox.log(Level::Info, line);
        assert!(!sandbox.logs[0].contains("s3cr"), "{}", sandbox.logs[0]);
    }

    /// A request's `Host` is its URL's, whatever the guest says: another would reach the
    /// server's other names.
    #[test]
    fn a_request_is_sent_to_its_urls_host() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || {
            let (socket, _) = listener.accept().unwrap();
            let mut hosts = Vec::new();
            let mut lines = BufReader::new(&socket).lines();
            while let Some(Ok(line)) = lines.next() {
                if line.is_empty() {
                    break;
                }
                if let Some((name, value)) = line.split_once(':') {
                    if name.eq_ignore_ascii_case("host") {
                        hosts.push(value.trim().to_string());
                    }
                }
            }
            let body = hosts.join(",");
            write!(
                &socket,
                "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .unwrap();
        });

        let mut sandbox = Sandbox::new(vec![format!("127.0.0.1:{port}")], vec![], None);
        sandbox.deadline = Some(Instant::now() + Duration::from_secs(5));
        let response = sandbox
            .fetch(Request {
                method: "GET".into(),
                url: format!("http://127.0.0.1:{port}/"),
                headers: vec![("Host".into(), "internal.example".into())],
                body: None,
            })
            .unwrap();
        server.join().unwrap();
        assert_eq!(
            String::from_utf8(response.body).unwrap(),
            format!("127.0.0.1:{port}")
        );
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
