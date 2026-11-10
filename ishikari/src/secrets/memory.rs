use std::collections::BTreeMap;
use std::sync::Mutex;

use chrono::{DateTime, Utc};

use super::{
    check_hosts, check_name, BoundSecret, Secret, SecretError, SecretInfo, SecretStore, Secrets,
};

/// Secrets kept in memory, gone with the process: for tests.
#[derive(Debug, Default)]
pub struct MemorySecrets {
    kept: Mutex<BTreeMap<String, (BoundSecret, DateTime<Utc>)>>,
}

impl MemorySecrets {
    pub fn new() -> Self {
        Self::default()
    }

    /// These secrets, with `name` set to `value`, bound to no host.
    ///
    /// # Panics
    ///
    /// When `name` isn't a secret's name.
    pub fn with(self, name: &str, value: impl Into<Secret>) -> Self {
        self.with_hosts(name, value, &[])
    }

    /// These secrets, with `name` set to `value`, bound to `hosts`.
    ///
    /// # Panics
    ///
    /// When `name` isn't a secret's name, or a host isn't a host.
    pub fn with_hosts(self, name: &str, value: impl Into<Secret>, hosts: &[&str]) -> Self {
        let name = check_name(name).expect("a secret's name");
        let hosts: Vec<String> = hosts.iter().map(|h| h.to_string()).collect();
        let bound = BoundSecret {
            value: value.into(),
            hosts: check_hosts(&hosts).expect("hosts"),
        };
        let kept = (bound, Utc::now());
        self.kept.lock().unwrap().insert(name.to_string(), kept);
        self
    }
}

#[async_trait::async_trait]
impl Secrets for MemorySecrets {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SecretError> {
        let kept = self.kept.lock().unwrap();
        Ok(kept
            .get(check_name(name)?)
            .map(|(bound, _)| bound.value.clone()))
    }

    async fn get_bound(
        &self,
        names: &[String],
    ) -> Result<std::collections::HashMap<String, BoundSecret>, SecretError> {
        let kept = self.kept.lock().unwrap();
        let mut found = std::collections::HashMap::new();
        for name in names {
            if let Some((bound, _)) = kept.get(check_name(name)?) {
                found.insert(name.clone(), bound.clone());
            }
        }
        Ok(found)
    }
}

#[async_trait::async_trait]
impl SecretStore for MemorySecrets {
    async fn list(&self) -> Result<Vec<SecretInfo>, SecretError> {
        let kept = self.kept.lock().unwrap();
        let info = kept.iter().map(|(name, (bound, updated_at))| SecretInfo {
            name: name.clone(),
            hosts: bound.hosts.clone(),
            updated_at: *updated_at,
        });
        Ok(info.collect())
    }

    async fn set(&self, name: &str, value: Secret, hosts: &[String]) -> Result<(), SecretError> {
        let name = check_name(name)?.to_string();
        let hosts = check_hosts(hosts)?;
        let bound = BoundSecret { value, hosts };
        self.kept.lock().unwrap().insert(name, (bound, Utc::now()));
        Ok(())
    }

    async fn bind(&self, name: &str, hosts: &[String]) -> Result<bool, SecretError> {
        let name = check_name(name)?;
        let hosts = check_hosts(hosts)?;
        let mut kept = self.kept.lock().unwrap();
        match kept.get_mut(name) {
            Some((bound, _)) => {
                bound.hosts = hosts;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn delete(&self, name: &str) -> Result<bool, SecretError> {
        let mut kept = self.kept.lock().unwrap();
        Ok(kept.remove(check_name(name)?).is_some())
    }
}
