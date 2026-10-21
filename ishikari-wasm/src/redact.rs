//! Secrets' values taken out of what a module's node returns, fails with and logs.

use ishikari::secrets::Secret;

/// What a secret's value is replaced with.
const REDACTED: &str = "[redacted]";

/// The values to redact, longest first, so one that contains another goes whole.
pub(crate) struct Redact(Vec<String>);

impl Redact {
    pub fn new<'a>(secrets: impl IntoIterator<Item = &'a Secret>) -> Self {
        let mut values: Vec<String> = secrets
            .into_iter()
            .map(|secret| secret.expose().to_string())
            .filter(|value| !value.is_empty())
            .collect();
        values.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
        values.dedup();
        Self(values)
    }

    /// `text` without the values.
    pub fn text(&self, text: &str) -> String {
        self.0.iter().fold(text.to_string(), |text, value| {
            if text.contains(value.as_str()) {
                text.replace(value.as_str(), REDACTED)
            } else {
                text
            }
        })
    }

    /// `json` without the values in its strings and keys.
    pub fn json(&self, json: serde_json::Value) -> serde_json::Value {
        use serde_json::Value;
        if self.0.is_empty() {
            return json;
        }
        match json {
            Value::String(string) => Value::String(self.text(&string)),
            Value::Array(values) => {
                Value::Array(values.into_iter().map(|v| self.json(v)).collect())
            }
            Value::Object(object) => Value::Object(
                object
                    .into_iter()
                    .map(|(key, value)| (self.text(&key), self.json(value)))
                    .collect(),
            ),
            json => json,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Redact;
    use ishikari::secrets::Secret;
    use serde_json::json;

    #[test]
    fn values_are_redacted_longest_first() {
        let secrets = [
            Secret::from("abc"),
            Secret::from("abcdef"),
            Secret::from(""),
        ];
        let redact = Redact::new(&secrets);
        assert_eq!(
            redact.text("x abcdef y abc z"),
            "x [redacted] y [redacted] z"
        );
        assert_eq!(
            redact.json(json!({ "abc": ["abcdef", 1, null], "n": "none" })),
            json!({ "[redacted]": ["[redacted]", 1, null], "n": "none" })
        );
    }

    #[test]
    fn nothing_to_redact_changes_nothing() {
        let redact = Redact::new(&[]);
        assert_eq!(redact.text("abc"), "abc");
        assert_eq!(redact.json(json!({ "a": "b" })), json!({ "a": "b" }));
    }
}
