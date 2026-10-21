//! The host's secrets: values its nodes are given by name, from the backend the host picks.

mod common;

use common::TestDb;
use ishikari::secrets::{
    EnvSecrets, MemorySecrets, PostgresSecrets, Secret, SecretError, SecretKey, SecretStore,
    Secrets,
};
use sqlx::Row;

/// A secret shows its value only when asked to, never when debugged.
#[test]
fn a_secret_hides_its_value() {
    let secret = Secret::from("s3cret");
    assert_eq!(secret.expose(), "s3cret");
    assert!(!format!("{secret:?}").contains("s3cret"));
}

/// A secret's name is up to 200 letters, digits and `_ - . /`; a backend refuses any other.
#[tokio::test]
async fn names_are_checked() {
    let secrets = MemorySecrets::new();
    let long = "a".repeat(201);
    for name in ["", "has space", "ünïcode", "semi;colon", &long] {
        let got = secrets.get(name).await;
        assert!(matches!(got, Err(SecretError::InvalidName(_))), "{name:?}");
        let set = secrets.set(name, Secret::from("x")).await;
        assert!(matches!(set, Err(SecretError::InvalidName(_))), "{name:?}");
    }
    for name in ["stripe/api-key", "A_B.c-1", &"a".repeat(200)] {
        assert_eq!(secrets.get(name).await.unwrap(), None, "{name:?}");
    }
}

/// The environment's secrets are its variables of a prefix: a secret's name upper-cased, with
/// `_` for each character not a letter or digit.
#[tokio::test]
async fn env_secrets_are_variables() {
    std::env::set_var("ISHIKARI_TEST_SECRET_STRIPE_API_KEY", "sk_test");
    let secrets = EnvSecrets::new("ISHIKARI_TEST_SECRET_");
    assert_eq!(
        secrets.get("stripe/api-key").await.unwrap(),
        Some(Secret::from("sk_test"))
    );
    assert_eq!(secrets.get("stripe/other").await.unwrap(), None);
    let names = ["stripe/api-key".to_string(), "nope".to_string()];
    let found = secrets.get_all(&names).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found["stripe/api-key"].expose(), "sk_test");
}

/// Secrets kept in memory are set, listed by name (never with their values) and deleted.
#[tokio::test]
async fn memory_secrets_are_kept() {
    let secrets = MemorySecrets::new().with("b", "2");
    secrets.set("a", Secret::from("1")).await.unwrap();
    secrets.set("b", Secret::from("3")).await.unwrap();
    let names: Vec<String> = secrets
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.name)
        .collect();
    assert_eq!(names, ["a", "b"]);
    assert_eq!(secrets.get("b").await.unwrap(), Some(Secret::from("3")));
    assert!(secrets.delete("a").await.unwrap());
    assert!(!secrets.delete("a").await.unwrap());
    assert_eq!(secrets.get("a").await.unwrap(), None);
}

fn key(id: &str, byte: u8) -> SecretKey {
    SecretKey::new(id, [byte; 32])
}

fn backend_error<T: std::fmt::Debug>(result: Result<T, SecretError>) -> String {
    match result {
        Err(SecretError::Backend(why)) => why,
        other => panic!("expected the backend to fail, got {other:?}"),
    }
}

/// Postgres keeps secrets encrypted with the host's key: set, read back, listed by name, replaced
/// and deleted, their values never in the table.
#[tokio::test]
async fn postgres_secrets_are_kept_encrypted() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let secrets = PostgresSecrets::new(db.pool.clone(), key("k1", 1));

    secrets
        .set("stripe/api-key", Secret::from("sk_live_0123456789"))
        .await
        .unwrap();
    secrets
        .set("slack/token", Secret::from("xoxb"))
        .await
        .unwrap();
    assert_eq!(
        secrets.get("stripe/api-key").await.unwrap(),
        Some(Secret::from("sk_live_0123456789"))
    );
    assert_eq!(secrets.get("nope").await.unwrap(), None);

    let rows = sqlx::query("SELECT ciphertext, nonce, key_id FROM ishikari_secrets")
        .fetch_all(&db.pool)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    for row in rows {
        let ciphertext: Vec<u8> = row.get("ciphertext");
        assert!(!ciphertext.windows(4).any(|w| w == b"sk_l" || w == b"xoxb"));
        assert_eq!(row.get::<Vec<u8>, _>("nonce").len(), 24);
        assert_eq!(row.get::<String, _>("key_id"), "k1");
    }

    let listed = secrets.list().await.unwrap();
    let names: Vec<&str> = listed.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["slack/token", "stripe/api-key"]);

    let names = [
        "stripe/api-key".to_string(),
        "slack/token".to_string(),
        "nope".to_string(),
    ];
    let found = secrets.get_all(&names).await.unwrap();
    assert_eq!(found.len(), 2);
    assert_eq!(found["slack/token"].expose(), "xoxb");

    secrets
        .set("slack/token", Secret::from("xoxp"))
        .await
        .unwrap();
    assert_eq!(
        secrets.get("slack/token").await.unwrap(),
        Some(Secret::from("xoxp"))
    );
    let updated = secrets.list().await.unwrap();
    assert!(updated[0].updated_at > listed[0].updated_at);

    assert!(secrets.delete("slack/token").await.unwrap());
    assert!(!secrets.delete("slack/token").await.unwrap());
    assert_eq!(secrets.get("slack/token").await.unwrap(), None);
    assert!(matches!(
        secrets.set("not a name", Secret::from("x")).await,
        Err(SecretError::InvalidName(_))
    ));

    db.finish().await;
}

/// A secret's ciphertext is bound to its name: moved to another name, it can't be read.
#[tokio::test]
async fn a_secret_is_bound_to_its_name() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let secrets = PostgresSecrets::new(db.pool.clone(), key("k1", 1));
    secrets.set("a", Secret::from("for a")).await.unwrap();
    secrets.set("b", Secret::from("for b")).await.unwrap();

    sqlx::query(
        "UPDATE ishikari_secrets SET (ciphertext, nonce) = \
         (SELECT ciphertext, nonce FROM ishikari_secrets WHERE name = 'a') WHERE name = 'b'",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    let why = backend_error(secrets.get("b").await);
    assert_eq!(why, "the secret b can't be decrypted with the key k1");
    assert_eq!(secrets.get("a").await.unwrap(), Some(Secret::from("for a")));

    db.finish().await;
}

/// Secrets encrypted with a key the host no longer gives can't be read; given as an old key,
/// they are, and rotating encrypts them with the current key.
#[tokio::test]
async fn keys_are_rotated() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let before = PostgresSecrets::new(db.pool.clone(), key("k1", 1));
    before.set("a", Secret::from("1")).await.unwrap();
    before.set("b", Secret::from("2")).await.unwrap();

    let without = PostgresSecrets::new(db.pool.clone(), key("k2", 2));
    let why = backend_error(without.get("a").await);
    assert_eq!(
        why,
        "the secret a is encrypted with the key k1, which isn't given"
    );
    let wrong = PostgresSecrets::new(db.pool.clone(), key("k1", 9));
    let why = backend_error(wrong.get("a").await);
    assert_eq!(why, "the secret a can't be decrypted with the key k1");

    let during = PostgresSecrets::new(db.pool.clone(), key("k2", 2)).old_key(key("k1", 1));
    assert_eq!(during.get("a").await.unwrap(), Some(Secret::from("1")));
    during.set("c", Secret::from("3")).await.unwrap();
    assert_eq!(during.rotate().await.unwrap(), 2);
    assert_eq!(during.rotate().await.unwrap(), 0);

    let after = PostgresSecrets::new(db.pool.clone(), key("k2", 2));
    let names: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
    let found = after.get_all(&names).await.unwrap();
    let values: Vec<&str> = names.iter().map(|n| found[n].expose()).collect();
    assert_eq!(values, ["1", "2", "3"]);

    db.finish().await;
}

/// A key is 32 bytes, given as base64 too.
#[test]
fn a_key_is_32_bytes() {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode([5u8; 32]);
    let key = SecretKey::from_base64("k", &encoded).unwrap();
    assert_eq!(key.id(), "k");
    assert!(!format!("{key:?}").contains(&encoded));
    for bad in ["", "not base64!", "AAAA"] {
        assert!(SecretKey::from_base64("k", bad).is_err(), "{bad:?}");
    }
}

/// Secrets are kept in the schema they're given, as jobs are.
#[tokio::test]
async fn postgres_secrets_are_kept_in_a_schema() {
    let Some(db) = TestDb::new().await else {
        return;
    };
    let secrets = PostgresSecrets::new(db.plain_pool().await, key("k1", 1)).schema(&db.schema);
    secrets.set("a", Secret::from("1")).await.unwrap();
    let in_schema = PostgresSecrets::new(db.pool.clone(), key("k1", 1));
    assert_eq!(in_schema.get("a").await.unwrap(), Some(Secret::from("1")));

    db.finish().await;
}
