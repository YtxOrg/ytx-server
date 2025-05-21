mod constant;
mod dbhub;
mod message;
mod websocket;

use chrono::Local;
use dbhub::DbHub;
use std::sync::Arc;
use tokio::net::TcpListener;
use websocket::WebSocket;

use crate::dbhub::sql_factory::SqlFactory;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_hub = Arc::new(DbHub::new());
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

    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(addr).await?;

    println!(
        "[{}] WebSocket: Start listening on: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        addr
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
