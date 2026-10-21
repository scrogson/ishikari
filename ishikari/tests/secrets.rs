//! The host's secrets: values its nodes are given by name, from the backend the host picks.

use ishikari::secrets::{EnvSecrets, MemorySecrets, Secret, SecretError, SecretStore, Secrets};

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
