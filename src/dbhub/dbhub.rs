use crate::YTX_SECRET_PATH;

use crate::utils::get_vault_password;
use crate::utils::read_vault_data;

use anyhow::Result;
use dotenvy::dotenv;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, broadcast};
use tracing::info;
use url::Url;

// 数据库连接池管理器
pub struct DbHub {
    pub base_postgres_url: String,
    pub vault_addr: String,
    pub vault_token: Option<String>,
    pub auth_pool: PgPool,
    pools: Mutex<HashMap<(String, String), (PgPool, Instant)>>,
    pub senders: Mutex<HashMap<(String, String), (broadcast::Sender<String>, Instant)>>,
}

impl DbHub {
    pub fn new(
        base_postgres_url: String,
        vault_addr: String,
        vault_token: Option<String>,
        auth_pool: PgPool,
    ) -> Self {
        Self {
            base_postgres_url,
            vault_addr,
            vault_token,
            auth_pool,
            pools: Mutex::new(HashMap::new()),
            senders: Mutex::new(HashMap::new()),
        }
    }

    pub async fn init_pool(&self, db_url: &str, database: &str, role: &str) -> Result<PgPool> {
        let mut pools = self.pools.lock().await;

        let key = (database.to_string(), role.to_string());
        let now = Instant::now();

        if let Some((pool, last_used)) = pools.get_mut(&key) {
            *last_used = now;
            return Ok(pool.clone());
        }

        let pool = create_pool(&db_url).await?;
        pools.insert(key, (pool.clone(), now));

        Ok(pool)
    }

    pub async fn get_role_password(&self, role: &str) -> Result<String> {
        if let Some(token) = &self.vault_token {
            let data = read_vault_data(&self.vault_addr, token, YTX_SECRET_PATH).await?;
            let password = get_vault_password(&data, role)?;
            Ok(password)
        } else {
            dotenv().ok();
            let var_name = format!("{}_PASSWORD", role.to_uppercase());
            let pw = std::env::var(&var_name)?;
            Ok(pw)
        }
    }

    pub async fn cleanup_idle_resource(&self, timeout_secs: u64) {
        let now = Instant::now();

        let mut pools = self.pools.lock().await;
        pools.retain(|key, (_, last_used)| {
            let keep = now.duration_since(*last_used) <= Duration::from_secs(timeout_secs);
            if !keep {
                info!("Cleaning idle pool: {:?}", key);
            }
            keep
        });

        let mut senders = self.senders.lock().await;
        senders.retain(|key, (_, last_used)| {
            let keep = now.duration_since(*last_used) <= Duration::from_secs(timeout_secs);
            if !keep {
                info!("Cleaning idle sender: {:?}", key);
            }
            keep
        });
    }

    pub async fn get_broadcast_sender(
        &self,
        database: &str,
        role: &str,
    ) -> Result<broadcast::Sender<String>> {
        let mut senders = self.senders.lock().await;
        let key = (database.to_string(), role.to_string());
        let now = Instant::now();

        if let Some((sender, last_used)) = senders.get_mut(&key) {
            *last_used = now;
            return Ok(sender.clone());
        }

        let (sender, _) = broadcast::channel(100);
        senders.insert(key, (sender.clone(), now));

        Ok(sender)
    }
}

pub async fn create_pool(db_url: &str) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(3600))
        .test_before_acquire(true)
        .connect(db_url)
        .await?;

    Ok(pool)
}

pub fn build_url(
    base_postgres_url: &str,
    user: &str,
    password: &str,
    database: &str,
) -> Result<String> {
    let mut url = Url::parse(base_postgres_url)?;

    url.set_username(user)
        .map_err(|()| anyhow::anyhow!("Invalid username"))?;

    url.set_password(Some(password))
        .map_err(|()| anyhow::anyhow!("Invalid password"))?;

    url.set_path(database);

    Ok(url.into())
}
