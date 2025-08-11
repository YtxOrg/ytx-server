use anyhow::{Result, bail};
use std::env::var;
use url::Url;

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

pub fn build_postgres_url(
    base_url: &str,
    user: &str,
    password: &str,
    database_name: &str,
) -> Result<String> {
    let mut url = Url::parse(base_url)?;

    url.set_username(user)
        .map_err(|()| anyhow::anyhow!("Invalid username"))?;

    url.set_password(Some(password))
        .map_err(|()| anyhow::anyhow!("Invalid password"))?;

    url.set_path(database_name);

    Ok(url.into())
}
