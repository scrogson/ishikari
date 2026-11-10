//! The host's secrets: values its nodes are given by name (API keys, tokens), from a backend
//! the host picks, rather than in their inputs, which are stored with their runs and jobs.
//!
//! A backend is a [`Secrets`]; one the admin can also write to is a [`SecretStore`]. ishikari
//! has [`PostgresSecrets`], kept encrypted with the host's key beside its jobs, [`EnvSecrets`],
//! the process's environment variables, and [`MemorySecrets`], for tests. Another backend
//! (Vault, OpenBao, a cloud's secret manager) is an implementation of [`Secrets`].
//!
//! A node registered in Rust reads them from its context ([`NodeContext::secret`]) once its
//! [`NodeTypes`] has them ([`NodeTypes::secrets`]). A module (`ishikari-wasm`) never reads a
//! value: it's given a placeholder for each secret it was granted, which the host replaces with
//! the value in a request's headers, and only in a request to one of the hosts the secret is
//! bound to ([`BoundSecret::hosts`], set with it in a [`SecretStore`]).
//!
//! ```rust,no_run
//! use ishikari::execution::NodeTypes;
//! use ishikari::secrets::EnvSecrets;
//! use std::sync::Arc;
//!
//! # fn f(pool: sqlx::PgPool) {
//! // `stripe/api-key` is the variable ISHIKARI_SECRET_STRIPE_API_KEY.
//! let secrets = Arc::new(EnvSecrets::new("ISHIKARI_SECRET_"));
//! let types = NodeTypes::new(pool).secrets(secrets);
//! # }
//! ```
//!
//! [`NodeContext::secret`]: crate::execution::NodeContext::secret
//! [`NodeTypes`]: crate::execution::NodeTypes
//! [`NodeTypes::secrets`]: crate::execution::NodeTypes::secrets

mod env;
mod memory;
mod postgres;

use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, Utc};

pub use env::EnvSecrets;
pub use memory::MemorySecrets;
pub use postgres::{PostgresSecrets, SecretKey};

/// The longest name a secret may have.
pub const MAX_NAME_LENGTH: usize = 200;

/// The most hosts a secret may be bound to.
pub const MAX_HOSTS: usize = 64;

/// The longest host a secret may be bound to (a name of 253 bytes, and a port).
const MAX_HOST_LENGTH: usize = 260;

/// A secret's value. It shows it only through [`expose`](Secret::expose): debugging one prints
/// `Secret(..)`.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The value.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(..)")
    }
}

impl From<String> for Secret {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Secret {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// A secret a [`SecretStore`] keeps, without its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretInfo {
    pub name: String,
    /// The hosts a module may send it to ([`BoundSecret::hosts`]).
    pub hosts: Vec<String>,
    /// When its value was last set.
    pub updated_at: DateTime<Utc>,
}

/// A secret's value, with the hosts a module may send it to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundSecret {
    pub value: Secret,
    /// The hosts a module's request may carry it to, in its headers: `api.example.com`,
    /// `127.0.0.1:8080` (that port only), or `*.example.com` (its subdomains), as a module's
    /// allowed hosts are. None, and no module can send it anywhere.
    pub hosts: Vec<String>,
}

/// Why a secret can't be given or kept.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    /// A name a secret can't have.
    #[error(
        "{0:?} isn't a secret's name: up to 200 letters, digits and `_ - . /`, without empty, \
         `.` or `..` segments"
    )]
    InvalidName(String),
    /// A host a secret can't be bound to.
    #[error("{0:?} isn't a host (a name, name:port or *.domain)")]
    InvalidHost(String),
    /// There's no secret of the name.
    #[error("no secret {0}")]
    Missing(String),
    /// The backend failed.
    #[error("{0}")]
    Backend(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Where the host's secrets come from.
#[async_trait::async_trait]
pub trait Secrets: Send + Sync {
    /// The secret `name`, or `None` when there's none of that name.
    /// [`SecretError::InvalidName`] for a name no secret can have ([`check_name`]).
    async fn get(&self, name: &str) -> Result<Option<Secret>, SecretError>;

    /// The secrets of `names` there are, by name. A backend that can read several at once
    /// overrides this.
    async fn get_all(&self, names: &[String]) -> Result<HashMap<String, Secret>, SecretError> {
        let mut found = HashMap::new();
        for name in names {
            if let Some(secret) = self.get(name).await? {
                found.insert(name.clone(), secret);
            }
        }
        Ok(found)
    }

    /// The secrets of `names` there are, by name, each with the hosts a module may send it to.
    /// A backend that keeps hosts overrides this; one that doesn't binds its secrets to none.
    async fn get_bound(
        &self,
        names: &[String],
    ) -> Result<HashMap<String, BoundSecret>, SecretError> {
        let found = self.get_all(names).await?;
        Ok(found
            .into_iter()
            .map(|(name, value)| {
                let bound = BoundSecret {
                    value,
                    hosts: Vec::new(),
                };
                (name, bound)
            })
            .collect())
    }
}

impl fmt::Debug for dyn Secrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secrets")
    }
}

/// Secrets the admin manages: listed by name, set and deleted, their values never read back
/// but by [`Secrets::get`].
#[async_trait::async_trait]
pub trait SecretStore: Secrets {
    /// Every secret, by name.
    async fn list(&self) -> Result<Vec<SecretInfo>, SecretError>;

    /// Set the secret `name` to `value`, bound to `hosts` ([`check_hosts`]), replacing any
    /// it had.
    async fn set(&self, name: &str, value: Secret, hosts: &[String]) -> Result<(), SecretError>;

    /// Bind the secret `name` to `hosts` ([`check_hosts`]), in place of those it was bound to,
    /// keeping its value: whether there was one.
    async fn bind(&self, name: &str, hosts: &[String]) -> Result<bool, SecretError>;

    /// Delete the secret `name`: whether there was one.
    async fn delete(&self, name: &str) -> Result<bool, SecretError>;
}

/// `name`, if a secret can have it: 1 to [`MAX_NAME_LENGTH`] ASCII letters, digits and
/// `_ - . /`, in segments split by `/` that aren't empty, `.` or `..` (so a backend that keeps
/// secrets by path, such as a vault, finds none outside its own).
pub fn check_name(name: &str) -> Result<&str, SecretError> {
    let allowed = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '/');
    let segment = |s: &str| !matches!(s, "" | "." | "..");
    if name.len() > MAX_NAME_LENGTH || !name.chars().all(allowed) || !name.split('/').all(segment) {
        return Err(SecretError::InvalidName(name.to_string()));
    }
    Ok(name)
}

/// `hosts`, if a secret can be bound to them: trimmed, without blanks or repeats, in order, each
/// a host, `host:port` or `*.domain` (no scheme, path, whitespace or comma), at most
/// [`MAX_HOSTS`] of them.
pub fn check_hosts(hosts: &[String]) -> Result<Vec<String>, SecretError> {
    let mut checked: Vec<String> = Vec::new();
    for host in hosts {
        let host = host.trim();
        if host.is_empty() {
            continue;
        }
        let allowed =
            |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '*' | ':' | '_');
        if host.len() > MAX_HOST_LENGTH || !host.chars().all(allowed) {
            return Err(SecretError::InvalidHost(host.to_string()));
        }
        checked.push(host.to_ascii_lowercase());
    }
    checked.sort();
    checked.dedup();
    if checked.len() > MAX_HOSTS {
        return Err(SecretError::InvalidHost(format!(
            "{} hosts, of at most {MAX_HOSTS}",
            checked.len()
        )));
    }
    Ok(checked)
}
