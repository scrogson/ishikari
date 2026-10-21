use std::collections::BTreeMap;
use std::sync::Mutex;

use chrono::{DateTime, Utc};

use super::{check_name, Secret, SecretError, SecretInfo, SecretStore, Secrets};

/// Secrets kept in memory, gone with the process: for tests.
#[derive(Debug, Default)]
pub struct MemorySecrets {
    kept: Mutex<BTreeMap<String, (Secret, DateTime<Utc>)>>,
}

impl MemorySecrets {
    pub fn new() -> Self {
        Self::default()
    }

    /// These secrets, with `name` set to `value`.
    ///
    /// # Panics
    ///
    /// When `name` isn't a secret's name.
    pub fn with(self, name: &str, value: impl Into<Secret>) -> Self {
        let name = check_name(name).expect("a secret's name");
        let kept = (value.into(), Utc::now());
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
            .map(|(secret, _)| secret.clone()))
    }
}

#[async_trait::async_trait]
impl SecretStore for MemorySecrets {
    async fn list(&self) -> Result<Vec<SecretInfo>, SecretError> {
        let kept = self.kept.lock().unwrap();
        let info = kept.iter().map(|(name, (_, updated_at))| SecretInfo {
            name: name.clone(),
            updated_at: *updated_at,
        });
        Ok(info.collect())
    }

    async fn set(&self, name: &str, value: Secret) -> Result<(), SecretError> {
        let name = check_name(name)?.to_string();
        self.kept.lock().unwrap().insert(name, (value, Utc::now()));
        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<bool, SecretError> {
        let mut kept = self.kept.lock().unwrap();
        Ok(kept.remove(check_name(name)?).is_some())
    }
}
