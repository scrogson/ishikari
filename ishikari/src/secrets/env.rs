use super::{check_name, Secret, SecretError, Secrets};

/// Secrets from the process's environment variables of a prefix: the secret `stripe/api-key`
/// is `{prefix}STRIPE_API_KEY`, its name upper-cased with `_` for each character not a letter
/// or digit (so `a-b` and `a_b` are the same variable). Read-only.
#[derive(Debug, Clone)]
pub struct EnvSecrets {
    prefix: String,
}

impl EnvSecrets {
    /// Secrets from the variables named `{prefix}...`.
    ///
    /// Panics if `prefix` is empty: every variable of the process (its database's URL among
    /// them) would be a secret a module may be granted.
    pub fn new(prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        assert!(
            !prefix.is_empty(),
            "EnvSecrets needs a prefix, or every environment variable is a secret"
        );
        Self { prefix }
    }

    /// The variable of the secret `name`.
    pub fn variable(&self, name: &str) -> String {
        let name: String = name
            .chars()
            .map(|c| match c {
                c if c.is_ascii_alphanumeric() => c.to_ascii_uppercase(),
                _ => '_',
            })
            .collect();
        format!("{}{name}", self.prefix)
    }
}

#[async_trait::async_trait]
impl Secrets for EnvSecrets {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SecretError> {
        let variable = self.variable(check_name(name)?);
        match std::env::var(&variable) {
            Ok(value) => Ok(Some(Secret::from(value))),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(SecretError::Backend(format!("{variable} isn't UTF-8")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EnvSecrets;

    #[test]
    #[should_panic(expected = "EnvSecrets needs a prefix")]
    fn an_empty_prefix_is_refused() {
        EnvSecrets::new("");
    }

    #[test]
    fn a_secret_is_its_prefixed_variable() {
        assert_eq!(
            EnvSecrets::new("APP_").variable("stripe/api-key"),
            "APP_STRIPE_API_KEY"
        );
    }
}
