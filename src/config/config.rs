use anyhow::{Result, bail};
use std::env::var;

pub fn read_value_with_default(key: &str, default: &str) -> Result<String> {
    let val = var(key).unwrap_or(default.to_string());

    if val.is_empty() {
        bail!("Value for '{}' cannot be empty", key);
    }

    if val.len() > 63 {
        bail!("Value for '{}' cannot be longer than 63 characters", key);
    }

    let mut chars = val.chars();
    let first = chars.next().unwrap();

    if !first.is_ascii_lowercase() {
        bail!("Value for '{}' must start with a lowercase letter", key);
    }

    if !val
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        bail!(
            "Value for '{}' can only contain lowercase letters, digits, and underscore",
            key
        );
    }

    Ok(val)
}
