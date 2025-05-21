use anyhow::Result;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

pub async fn read_vault_data(vault_addr: &str, token: &str, secret_path: &str) -> Result<Value> {
    let url = format!("{}/v1/{}", vault_addr.trim_end_matches('/'), secret_path);

    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", token))?,
    );

    let client = reqwest::Client::new();
    let resp = client.get(&url).headers(headers).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("HTTP error {}", resp.status());
    }

    let json: Value = resp.json().await?;
    Ok(json["data"]["data"].clone())
}

pub async fn renew_vault_token(vault_addr: &str, token: &str) -> Result<()> {
    let url = format!(
        "{}/v1/auth/token/renew-self",
        vault_addr.trim_end_matches('/')
    );
    let client = reqwest::Client::new();
    let resp = client.post(&url).bearer_auth(token).send().await?;

    if resp.status().is_success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("HTTP {}", resp.status()))
    }
}

pub async fn periodic_renewal(vault_addr: String, token: String) -> Result<()> {
    let renew_interval = Duration::from_secs(20 * 24 * 60 * 60);
    loop {
        match renew_vault_token(&vault_addr, &token).await {
            Ok(_) => info!("[Vault] Token renewed successfully"),
            Err(e) => error!("[Vault] Failed to renew token: {}", e),
        }

        sleep(renew_interval).await;
    }
}

pub fn get_vault_password(data: &serde_json::Value, key: &str) -> Result<String> {
    data.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow::anyhow!("Vault key '{}' not found or not a string", key))
}
