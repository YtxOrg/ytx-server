use crate::constant::*;
use crate::dbhub::DbHub;
use crate::dbhub::sql_factory::SqlFactory;
use crate::websocket::Session;

use crate::Local;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, broadcast};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

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
                println!(
                    "[{}] WebSocket handshake successful.",
                    Local::now().format("%Y-%m-%d %H:%M:%S")
                );

                let (write, read) = ws_stream.split();
                let mut session = Session::new(write, read, self.db_hub, self.sql_factory);
                session.run().await;
            }
            Err(e) => eprintln!(
                "[{}] WebSocket handshake failed: {}",
                Local::now().to_rfc3339(),
                e
            ),
        }
    }
}

pub async fn send_public_message(
    sender: broadcast::Sender<String>,
    msg_type: MsgType,
    value: serde_json::Value,
) -> Result<()> {
    let message = serde_json::json!({
        MSG_TYPE: msg_type,
        VALUE: value,
    })
    .to_string();

    sender.send(message).with_context(|| {
        format!(
            "[{}] Failed to broadcast public message",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
        )
    })?;

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
        .with_context(|| {
            format!(
                "[{}] Failed to broadcast private message",
                Local::now().format("%Y-%m-%d %H:%M:%S")
            )
        })
}
