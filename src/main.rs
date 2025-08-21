mod constant;
mod dbhub;
mod message;
mod utils;
mod websocket;

use crate::constant::YTX_SECRET_PATH;
use crate::dbhub::*;
use crate::utils::*;

use anyhow::{Context, Result};
use dotenvy::dotenv;
use std::{env::var, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tracing::info;

use websocket::WebSocket;

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables
    dotenv().ok();

    let rust_log = var("RUST_LOG").unwrap_or_else(|_| "info".to_string());

    let _guard = init_tracing(&rust_log);

    // Read data from .env file
    let base_postgres_url =
        var("BASE_POSTGRES_URL").unwrap_or_else(|_| "postgres://localhost:5432".to_string());
    let vault_addr = var("VAULT_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8200".to_string());
    let vault_token = var("VAULT_TOKEN").ok().filter(|t| !t.is_empty());
    let listen_addr = var("LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:7749".to_string());

    let auth_db = read_value_with_default("AUTH_DB", "ytx_auth")?;
    let auth_readwrite_role = read_value_with_default("AUTH_READWRITE_ROLE", "ytx_auth_readwrite")?;

    let mut auth_readwrite_password = var("AUTH_READWRITE_PASSWORD").unwrap_or_default();

    if let Some(token) = &vault_token {
        let vault_addr_clone = vault_addr.clone();
        let token_clone = token.clone();

        tokio::spawn(async move {
            let _ = periodic_renewal(vault_addr_clone, token_clone).await;
        });

        let ytx_data = read_vault_data(&vault_addr, &token, YTX_SECRET_PATH)
            .await
            .context("Failed to read YTX role passwords from Vault")?;

        auth_readwrite_password = get_vault_password(&ytx_data, &auth_readwrite_role)?;
    }

    let auth_url = build_url(
        &base_postgres_url,
        &auth_readwrite_role,
        &auth_readwrite_password,
        &auth_db,
    )?;
    let auth_pool = create_pool(&auth_url).await?;

    sqlx::query("SELECT 1")
        .execute(&auth_pool)
        .await
        .context("Failed to connect to auth DB")?;

    info!("Connected to auth DB successfully.");

    let db_hub = Arc::new(DbHub::new(
        base_postgres_url,
        vault_addr,
        vault_token,
        auth_pool,
    ));

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

    while let Ok((mut stream, _)) = listener.accept().await {
        let db_hub = db_hub.clone();
        let sql_factory = sql_factory.clone();

        tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            let n = match stream.peek(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };

            let request = String::from_utf8_lossy(&buf[..n]);

            if request.starts_with("GET / ") || request.starts_with("GET / HTTP/") {
                if request.to_ascii_lowercase().contains("upgrade: websocket") {
                    info!("Incoming WebSocket handshake, upgrading connection.");
                    WebSocket::new(stream, db_hub, sql_factory).handle().await;
                } else {
                    info!("Incoming HTTP request, responding with 200 OK.");

                    let response = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK";
                    let _ = stream.write_all(response).await;
                    let _ = stream.flush().await;
                }
                return;
            }
        });
    }

    Ok(())
}
