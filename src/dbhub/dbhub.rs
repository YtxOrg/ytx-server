use sqlx::PgConnection;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, broadcast};
use uuid::Uuid;

// 数据库连接池管理器
pub struct DbHub {
    pub base_postgres_url: String,
    pub vault_addr: String,
    pub vault_token: Option<String>,
    pub role_passwords: Mutex<HashMap<String, String>>,
    pools: Mutex<HashMap<(String, String), (PgPool, Instant)>>,
    pub senders: Mutex<HashMap<(String, String), (broadcast::Sender<String>, Instant)>>,
}

impl DbHub {
    pub fn new(base_postgres_url: String, vault_addr: String, vault_token: Option<String>) -> Self {
        Self {
            base_postgres_url,
            vault_addr,
            vault_token,
            role_passwords: Mutex::new(HashMap::new()),
            pools: Mutex::new(HashMap::new()),
            senders: Mutex::new(HashMap::new()),
        }
    }

    pub async fn init_pool(
        &self,
        db_url: &str,
        database: String,
        role: String,
    ) -> Result<PgPool, String> {
        let mut pools = self.pools.lock().await;
        let key = (database, role);
        let now = Instant::now();

        if let Some((pool, last_used)) = pools.get_mut(&key) {
            println!("Old connection pool: {:?}", key);
            *last_used = now;
            return Ok(pool.clone());
        }

        let pool = create_pool(&db_url).await?;

        println!("New connection pool: {:?}", key);
        pools.insert(key, (pool.clone(), now));
        Ok(pool)
    }

    pub async fn cleanup_idle_resource(&self, timeout_secs: u64) {
        let now = Instant::now();

        let mut pools = self.pools.lock().await;
        pools.retain(|key, (_, last_used)| {
            let keep = now.duration_since(*last_used) <= Duration::from_secs(timeout_secs);
            if !keep {
                println!("Cleaning idle pool: {:?}", key);
            }
            keep
        });

        let mut senders = self.senders.lock().await;
        senders.retain(|key, (_, last_used)| {
            let keep = now.duration_since(*last_used) <= Duration::from_secs(timeout_secs);
            if !keep {
                println!("Cleaning idle sender: {:?}", key);
            }
            keep
        });
    }

    pub async fn get_sender(&self, database: String, role: String) -> broadcast::Sender<String> {
        let mut senders = self.senders.lock().await;
        let key = (database, role);
        let now = Instant::now();

        if let Some((sender, last_used)) = senders.get_mut(&key) {
            println!("Old broadcast: {:?}", key);
            *last_used = now;
            return sender.clone();
        }

        println!("New broadcast: {:?}", key);

        let (sender, _) = broadcast::channel(100);
        senders.insert(key, (sender.clone(), now));
        sender
    }
}

pub async fn is_ytx_managed(conn: &mut PgConnection) -> Result<bool, String> {
    let result = sqlx::query_scalar::<_, bool>(
        "SELECT value FROM ytx_meta WHERE key = 'ytx_managed' LIMIT 1",
    )
    .fetch_optional(conn)
    .await;

    match result {
        Ok(Some(value)) => Ok(value),
        Ok(None) => Ok(false),
        Err(e)
            if e.to_string().contains("ytx_meta") && e.to_string().contains("does not exist") =>
        {
            Ok(false)
        }
        Err(e) => Err(format!("Failed to query meta: {e}")),
    }
}

pub async fn get_role(conn: &mut PgConnection, username: &str) -> Result<String, String> {
    let (rolsuper, rolcreaterole, rolcreatedb, rolcanlogin): (bool, bool, bool, bool) =
        sqlx::query_as(
            "SELECT rolsuper, rolcreaterole, rolcreatedb, rolcanlogin FROM pg_roles WHERE rolname = $1"
        )
        .bind(username)
        .fetch_one(conn)
        .await
        .map_err(|e| format!("Failed to get role info: {e}"))?;

    // 顺序：rolsuper(3) | rolcreaterole(2) | rolcreatedb(1) | rolcanlogin(0)
    let role_flags = (rolsuper as u8) << 3
        | (rolcreaterole as u8) << 2
        | (rolcreatedb as u8) << 1
        | (rolcanlogin as u8);

    let role_str = match role_flags {
        0b1000..=0b1111 => "super",  // rolsuper 为真
        0b0111 => "admin",           // 非super，拥有其他三项
        0b0110 => "db_creator",      // 有createrole + createdb
        0b0101 => "role_creator",    // 有createrole + login
        0b0100 => "createrole_only", // 仅有createrole
        0b0010 => "db_only",         // 仅有createdb
        0b0001 => "login",           // 仅有login
        _ => "other",                // 没有任何权限
    };

    println!("Role info: {}@{}", username, role_str);
    Ok(role_str.to_string())
}

pub async fn get_user_id(conn: &mut PgConnection, username: &str) -> Result<Uuid, String> {
    match sqlx::query_scalar::<_, Uuid>(
        r#"SELECT user_id FROM ytx_user WHERE username = $1 AND is_valid = true LIMIT 1"#,
    )
    .bind(username)
    .fetch_optional(conn)
    .await
    {
        Ok(Some(id)) => Ok(id),
        Ok(None) => Err(format!("User '{}' not found or invalid", username)),
        Err(e) => Err(format!("Failed to fetch user_id: {e}")),
    }
}

pub async fn create_pool(db_url: &str) -> Result<PgPool, String> {
    PgPoolOptions::new()
        .max_connections(4)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(10))
        .idle_timeout(Duration::from_secs(300))
        .max_lifetime(Duration::from_secs(3600))
        .test_before_acquire(true)
        .connect(db_url)
        .await
        .map_err(|e| format!("Failed to create pool for {}: {}", db_url, e))
}
