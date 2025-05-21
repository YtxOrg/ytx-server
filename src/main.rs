use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use std::{collections::HashMap, sync::Arc};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Serialize, Deserialize)]
struct LoginInfo {
    user: String,
    password: String,
    database: String,
}

// 数据库连接池管理器
struct DatabaseManager {
    pools: RwLock<HashMap<String, Arc<sqlx::PgPool>>>,
}

impl DatabaseManager {
    fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
        }
    }

    async fn get_pool(
        &self,
        database: &str,
        user: &str,
        password: &str,
    ) -> Result<Arc<sqlx::PgPool>, String> {
        // 先检查是否已有连接池
        {
            let pools = self.pools.read().await;
            if let Some(pool) = pools.get(database) {
                return Ok(pool.clone());
            }
        }

        // 创建新的连接池
        let database_url = format!(
            "postgres://{}:{}@localhost:5432/{}",
            user, password, database
        );
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .map_err(|e| e.to_string())?;

        let pool = Arc::new(pool);

        // 存储到集合中
        {
            let mut pools = self.pools.write().await;
            pools.insert(database.to_string(), pool.clone());
        }

        Ok(pool)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建数据库管理器
    let db_manager = Arc::new(DatabaseManager::new());

    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;
    println!("WebSocket server listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let db_manager = db_manager.clone();
        tokio::spawn(handle_connection(stream, db_manager));
    }

    Ok(())
}

async fn handle_connection(stream: TcpStream, db_manager: Arc<DatabaseManager>) {
    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during WebSocket handshake");

    let (mut write, mut read) = ws_stream.split();

    // 处理接收到的消息
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                // 尝试解析登录信息
                match serde_json::from_str::<LoginInfo>(&text) {
                    Ok(login_info) => {
                        // 获取或创建数据库连接池
                        match db_manager
                            .get_pool(&login_info.database, &login_info.user, &login_info.password)
                            .await
                        {
                            Ok(pool) => {
                                // 验证用户名和密码
                                match verify_user(&pool, &login_info).await {
                                    Ok(_) => {
                                        let response = serde_json::json!({
                                            "status": "success",
                                            "message": "Login successful"
                                        });

                                        if let Err(_) =
                                            write.send(Message::Text(response.to_string())).await
                                        {
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let response = serde_json::json!({
                                            "status": "error",
                                            "message": format!("Login failed: {}", e)
                                        });

                                        if let Err(_) =
                                            write.send(Message::Text(response.to_string())).await
                                        {
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let response = serde_json::json!({
                                    "status": "error",
                                    "message": format!("Database connection failed: {}", e)
                                });

                                if let Err(_) =
                                    write.send(Message::Text(response.to_string())).await
                                {
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        let response = serde_json::json!({
                            "status": "error",
                            "message": "Invalid login format"
                        });

                        if let Err(_) = write.send(Message::Text(response.to_string())).await {
                            break;
                        }
                    }
                }
            }
            Ok(Message::Close(_)) => {
                break;
            }
            Err(_) => {
                break;
            }
            _ => {}
        }
    }
}

async fn verify_user(pool: &sqlx::PgPool, login_info: &LoginInfo) -> Result<(), String> {
    // TODO: 实现实际的用户验证逻辑
    // 这里应该查询数据库验证用户名和密码
    // 示例：
    // let user = sqlx::query!(
    //     "SELECT * FROM users WHERE username = $1 AND password = $2",
    //     login_info.user,
    //     login_info.password
    // )
    // .fetch_optional(pool)
    // .await
    // .map_err(|e| e.to_string())?;

    // if user.is_none() {
    //     return Err("Invalid username or password".to_string());
    // }

    Ok(())
}
