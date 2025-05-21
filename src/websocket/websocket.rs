use crate::constant::*;
use crate::dbhub::DbHub;
use crate::dbhub::SqlFactory;
use crate::websocket::Session;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, broadcast};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use tracing::{error, info};

use crate::message::MsgType;

pub struct WebSocket {
    stream: TcpStream,
    db_hub: Arc<DbHub>,
    sql_factory: Arc<SqlFactory>,
}

impl WebSocket {
    pub fn new(stream: TcpStream, db_hub: Arc<DbHub>, sql_factory: Arc<SqlFactory>) -> Self {
        Self {
            stream,
            db_hub,
            sql_factory,
        }
    }

    pub async fn handle(self) {
        match tokio_tungstenite::accept_async(self.stream).await {
            Ok(ws_stream) => {
                info!("WebSocket handshake successful.");

                let (write, read) = ws_stream.split();
                let mut session = Session::new(write, read, self.db_hub, self.sql_factory);
                session.run().await;
            }
            Err(e) => {
                error!("WebSocket handshake failed: {:?}", e);
            }
        }
    }
}

pub async fn broadcast_public_message(
    sender: broadcast::Sender<String>,
    msg_type: MsgType,
    value: serde_json::Value,
) -> Result<()> {
    let message = serde_json::json!({
        MSG_TYPE: msg_type,
        VALUE: value,
    })
    .to_string();

    sender
        .send(message)
        .with_context(|| "Failed to broadcast public message")?;

    Ok(())
}

pub async fn send_private_message(
    ws_writer: Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
    msg_type: MsgType,
    value: serde_json::Value,
) -> Result<()> {
    let message = serde_json::json!({
        MSG_TYPE: msg_type,
        VALUE: value
    })
    .to_string();

    ws_writer
        .lock()
        .await
        .send(Message::Text(message.into()))
        .await
        .with_context(|| "Failed to send private message")
}
