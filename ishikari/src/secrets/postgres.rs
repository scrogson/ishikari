use std::collections::HashMap;
use std::fmt;

use base64::Engine;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use super::{check_name, Secret, SecretError, SecretInfo, SecretStore, Secrets};

/// A key secrets are encrypted with: 32 bytes the host keeps (not in the database), and an id,
/// kept with each secret encrypted with it. Debugging one prints its id only.
#[derive(Clone)]
pub struct SecretKey {
    id: String,
    key: Key,
}

impl SecretKey {
    pub fn new(id: impl Into<String>, key: [u8; 32]) -> Self {
        Self {
            id: id.into(),
            key: key.into(),
        }
    }

    /// The key of 32 bytes `encoded` as base64 (`openssl rand -base64 32`).
    pub fn from_base64(id: impl Into<String>, encoded: &str) -> Result<Self, SecretError> {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .ok()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .ok_or_else(|| SecretError::Backend("a secret key is 32 bytes, as base64".into()))?;
        Ok(Self::new(id, bytes))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new(&self.key)
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretKey").field("id", &self.id).finish()
    }
}

/// Secrets kept in Postgres (`ishikari_secrets`, schema v09), each value encrypted with the
/// host's key (XChaCha20-Poly1305, with a random nonce, bound to the secret's name). The admin
/// can set and delete them.
///
/// To change keys, give the new one and the old one ([`old_key`](Self::old_key)), which secrets
/// are still read with, then [`rotate`](Self::rotate) them to the new one.
#[derive(Clone)]
pub struct PostgresSecrets {
    pool: PgPool,
    schema: Option<String>,
    key: SecretKey,
    old_keys: Vec<SecretKey>,
}

impl fmt::Debug for PostgresSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresSecrets")
            .field("schema", &self.schema)
            .field("key", &self.key)
            .field("old_keys", &self.old_keys)
            .finish()
    }
}

/// A secret's row: its value encrypted.
#[derive(sqlx::FromRow)]
struct Sealed {
    name: String,
    ciphertext: Vec<u8>,
    nonce: Vec<u8>,
    key_id: String,
}

impl PostgresSecrets {
    /// Secrets kept in `pool`'s search path, encrypted with `key`.
    pub fn new(pool: PgPool, key: SecretKey) -> Self {
        Self {
            pool,
            schema: None,
            key,
            old_keys: Vec::new(),
        }
    }

    /// Keep secrets in `schema`, as the engine's `Postgres` keeps jobs.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// A key secrets may still be encrypted with, until they're [rotated](Self::rotate).
    pub fn old_key(mut self, key: SecretKey) -> Self {
        self.old_keys.push(key);
        self
    }

    /// Encrypt every secret encrypted with an old key with the current one: how many were.
    pub async fn rotate(&self) -> Result<u64, SecretError> {
        let mut tx = self.pool.begin().await?;
        let sealed: Vec<Sealed> = sqlx::query_as(&format!(
            "SELECT name, ciphertext, nonce, key_id FROM {} WHERE key_id <> $1 FOR UPDATE",
            self.table()
        ))
        .bind(&self.key.id)
        .fetch_all(&mut *tx)
        .await?;
        for sealed in &sealed {
            let secret = self.open(sealed)?;
            let (ciphertext, nonce) = self.seal(&sealed.name, &secret)?;
            sqlx::query(&format!(
                "UPDATE {} SET ciphertext = $2, nonce = $3, key_id = $4 WHERE name = $1",
                self.table()
            ))
            .bind(&sealed.name)
            .bind(ciphertext)
            .bind(nonce)
            .bind(&self.key.id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(sealed.len() as u64)
    }

    fn table(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.ishikari_secrets"),
            None => "ishikari_secrets".to_string(),
        }
    }

    /// `secret` encrypted with the current key for `name`: its ciphertext and nonce.
    fn seal(&self, name: &str, secret: &Secret) -> Result<(Vec<u8>, Vec<u8>), SecretError> {
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let payload = Payload {
            msg: secret.expose().as_bytes(),
            aad: name.as_bytes(),
        };
        let ciphertext =
            self.key.cipher().encrypt(&nonce, payload).map_err(|_| {
                SecretError::Backend(format!("the secret {name} can't be encrypted"))
            })?;
        Ok((ciphertext, nonce.to_vec()))
    }

    /// The secret `sealed` holds, decrypted with the key it was encrypted with.
    fn open(&self, sealed: &Sealed) -> Result<Secret, SecretError> {
        let Sealed {
            name,
            ciphertext,
            nonce,
            key_id,
        } = sealed;
        let key = std::iter::once(&self.key)
            .chain(&self.old_keys)
            .find(|key| key.id == *key_id)
            .ok_or_else(|| {
                SecretError::Backend(format!(
                    "the secret {name} is encrypted with the key {key_id}, which isn't given"
                ))
            })?;
        let undecryptable = || {
            SecretError::Backend(format!(
                "the secret {name} can't be decrypted with the key {key_id}"
            ))
        };
        if nonce.len() != 24 {
            return Err(undecryptable());
        }
        let payload = Payload {
            msg: ciphertext,
            aad: name.as_bytes(),
        };
        let value = key
            .cipher()
            .decrypt(XNonce::from_slice(nonce), payload)
            .map_err(|_| undecryptable())?;
        String::from_utf8(value)
            .map(Secret::from)
            .map_err(|_| undecryptable())
    }
}

#[async_trait::async_trait]
impl Secrets for PostgresSecrets {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SecretError> {
        let sealed: Option<Sealed> = sqlx::query_as(&format!(
            "SELECT name, ciphertext, nonce, key_id FROM {} WHERE name = $1",
            self.table()
        ))
        .bind(check_name(name)?)
        .fetch_optional(&self.pool)
        .await?;
        sealed.map(|sealed| self.open(&sealed)).transpose()
    }

    async fn get_all(&self, names: &[String]) -> Result<HashMap<String, Secret>, SecretError> {
        for name in names {
            check_name(name)?;
        }
        let sealed: Vec<Sealed> = sqlx::query_as(&format!(
            "SELECT name, ciphertext, nonce, key_id FROM {} WHERE name = ANY($1)",
            self.table()
        ))
        .bind(names)
        .fetch_all(&self.pool)
        .await?;
        sealed
            .iter()
            .map(|sealed| Ok((sealed.name.clone(), self.open(sealed)?)))
            .collect()
    }
}

#[async_trait::async_trait]
impl SecretStore for PostgresSecrets {
    async fn list(&self) -> Result<Vec<SecretInfo>, SecretError> {
        let rows: Vec<(String, DateTime<Utc>)> = sqlx::query_as(&format!(
            "SELECT name, updated_at FROM {} ORDER BY name",
            self.table()
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(name, updated_at)| SecretInfo { name, updated_at })
            .collect())
    }

    async fn set(&self, name: &str, value: Secret) -> Result<(), SecretError> {
        let name = check_name(name)?;
        let (ciphertext, nonce) = self.seal(name, &value)?;
        sqlx::query(&format!(
            "INSERT INTO {} (name, ciphertext, nonce, key_id) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (name) DO UPDATE SET ciphertext = EXCLUDED.ciphertext, \
             nonce = EXCLUDED.nonce, key_id = EXCLUDED.key_id, updated_at = now()",
            self.table()
        ))
        .bind(name)
        .bind(ciphertext)
        .bind(nonce)
        .bind(&self.key.id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<bool, SecretError> {
        let deleted = sqlx::query(&format!("DELETE FROM {} WHERE name = $1", self.table()))
            .bind(check_name(name)?)
            .execute(&self.pool)
            .await?;
        Ok(deleted.rows_affected() > 0)
    }
}
