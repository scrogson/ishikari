use std::collections::HashMap;

use super::{check_hosts, check_name, BoundSecret, Secret, SecretError, Secrets};

/// Secrets from the process's environment variables of a prefix: the secret `stripe/api-key`
/// is `{prefix}STRIPE_API_KEY`, its name upper-cased with `_` for each character not a letter
/// or digit (so `a-b` and `a_b` are the same variable). Read-only.
///
/// The hosts a module may send it to are `{prefix}STRIPE_API_KEY_HOSTS`, split by commas
/// (`api.stripe.com`); without it, none.
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

    fn var(variable: &str) -> Result<Option<String>, SecretError> {
        match std::env::var(variable) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(SecretError::Backend(format!("{variable} isn't UTF-8")))
            }
        }
    }
}

#[async_trait::async_trait]
impl Secrets for EnvSecrets {
    async fn get(&self, name: &str) -> Result<Option<Secret>, SecretError> {
        let variable = self.variable(check_name(name)?);
        Ok(Self::var(&variable)?.map(Secret::from))
    }

    async fn get_bound(
        &self,
        names: &[String],
    ) -> Result<HashMap<String, BoundSecret>, SecretError> {
        let mut found = HashMap::new();
        for name in names {
            let variable = self.variable(check_name(name)?);
            let Some(value) = Self::var(&variable)? else {
                continue;
            };
            let hosts: Vec<String> = Self::var(&format!("{variable}_HOSTS"))?
                .iter()
                .flat_map(|hosts| hosts.split(','))
                .map(str::to_string)
                .collect();
            let bound = BoundSecret {
                value: Secret::from(value),
                hosts: check_hosts(&hosts)?,
            };
            found.insert(name.clone(), bound);
        }
        Ok(found)
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
