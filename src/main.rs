mod config;
mod constant;
mod dbhub;
mod message;
mod vault;
mod websocket;

use chrono::Local;
use dotenvy::dotenv;
use std::{env::var, sync::Arc};
use tokio::net::TcpListener;

use crate::config::*;
use crate::constant::YTX_SECRET_PATH;
use crate::vault::*;
use anyhow::{Context, Result};
use dbhub::DbHub;
use websocket::WebSocket;

use crate::dbhub::sql_factory::SqlFactory;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables
    dotenv().ok();

    // Read data from .env file
    let base_postgres_url =
        var("BASE_POSTGRES_URL").unwrap_or_else(|_| "postgres://localhost:5432".to_string());
    let vault_addr = var("VAULT_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8200".to_string());
    let vault_token = var("VAULT_TOKEN").ok().filter(|t| !t.is_empty());
    let listen_addr = var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());

    let admin_user = read_value_with_default("ADMIN_USER", "ytx_admin")?;
    let mut admin_password = var("ADMIN_PASSWORD").unwrap_or_default();

    let readonly_user = read_value_with_default("READONLY_USER", "ytx_readonly")?;
    let mut readonly_password = var("READONLY_PASSWORD").unwrap_or_default();

    let readwrite_user = read_value_with_default("READWRITE_USER", "ytx_readwrite")?;
    let mut readwrite_password = var("READWRITE_PASSWORD").unwrap_or_default();

    if let Some(token) = &vault_token {
        renew_vault_token(&vault_addr, token).await?;

        let ytx_data = read_vault_data(&vault_addr, &token, YTX_SECRET_PATH)
            .await
            .context("Failed to read YTX role passwords from Vault")?;

        admin_password = get_vault_password(&ytx_data, &admin_user)?;
        readonly_password = get_vault_password(&ytx_data, &readonly_user)?;
        readwrite_password = get_vault_password(&ytx_data, &readwrite_user)?;

        let vault_addr_clone = vault_addr.clone();
        let token_clone = token.clone();
        tokio::spawn(async move {
            if let Err(e) = periodic_renewal(vault_addr_clone, token_clone).await {
                eprintln!("Vault token periodic renewal failed: {:?}", e);
            }
        });
    }

    let db_hub = Arc::new(DbHub::new(base_postgres_url, vault_addr, vault_token));

    {
        let mut passwords = db_hub.role_passwords.lock().await;
        passwords.insert(admin_user.clone(), admin_password.clone());
        passwords.insert(readonly_user.clone(), readonly_password.clone());
        passwords.insert(readwrite_user.clone(), readwrite_password.clone());
    }

    let sql_factory = Arc::new(SqlFactory::new());

    {
        let hub = Arc::clone(&db_hub);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(86400));
            loop {
                interval.tick().await;
                hub.cleanup_idle_resource(86400).await;
            }
        });
    }

    let listener = TcpListener::bind(listen_addr.clone()).await?;

    println!(
        "[{}] WebSocket: Start listening on: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        listen_addr
    );

    while let Ok((stream, _)) = listener.accept().await {
        let db_hub = db_hub.clone();
        let sql_factory = sql_factory.clone();

        tokio::spawn(async move {
            WebSocket::new(stream, db_hub, sql_factory).handle().await;
        });
    }

    Ok(())
}
