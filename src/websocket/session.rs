use crate::constant::*;
use crate::dbhub::sql_factory::SqlFactory;
use crate::dbhub::*;
use crate::message::*;
use crate::websocket::websocket::{send_private_message, send_public_message};

use std::{collections::HashMap, str::FromStr, sync::Arc};

use chrono::{DateTime, Local, Utc};
use futures::future::join_all;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use rust_decimal::Decimal;
use serde_json::{Map, Number, Value, from_value, json};
use sqlx::{
    Column, Connection, Executor, PgConnection, PgPool, Postgres, Row, TypeInfo,
    postgres::{PgArguments, PgRow},
    query::Query,
    types::Json,
};
use tokio::{
    net::TcpStream,
    sync::{Mutex, broadcast, watch},
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use urlencoding::encode;
use uuid::Uuid;

pub struct Session {
    ws_writer: Arc<Mutex<SplitSink<WebSocketStream<TcpStream>, Message>>>,
    ws_reader: SplitStream<WebSocketStream<TcpStream>>,
    dbhub: Arc<DbHub>,
    pgpool: Option<PgPool>,
    sender: Option<broadcast::Sender<String>>,
    user_id: Option<Uuid>,
    forwarder: tokio::task::JoinHandle<()>,
    stop_sender: watch::Sender<()>,
    stop_receiver: watch::Receiver<()>,
    sql_factory: Arc<SqlFactory>,
    session_id: Uuid,
}

impl Session {
    pub fn new(
        ws_writer: SplitSink<WebSocketStream<TcpStream>, Message>,
        ws_reader: SplitStream<WebSocketStream<TcpStream>>,
        dbhub: Arc<DbHub>,
        sql_factory: Arc<SqlFactory>,
    ) -> Self {
        let (tx, rx) = watch::channel(());
        Self {
            ws_writer: Arc::new(Mutex::new(ws_writer)),
            ws_reader,
            dbhub,
            pgpool: None,
            user_id: None,
            forwarder: tokio::spawn(async {}),
            stop_sender: tx,
            stop_receiver: rx,
            sender: None,
            sql_factory,
            session_id: Uuid::now_v7(),
        }
    }

    pub async fn run(&mut self) {
        while let Some(msg) = self.ws_reader.next().await {
            match self.handle_message(msg).await {
                Ok(()) => {}
                Err(e) => {
                    eprintln!(
                        "[{}] Session run Err: {}",
                        Local::now().format("%Y-%m-%d %H:%M:%S"),
                        e
                    );
                }
            }
        }
        self.cleanup().await;
    }

    async fn cleanup(&mut self) {
        let _ = self.stop_sender.send(());
        let handle = std::mem::replace(&mut self.forwarder, tokio::spawn(async {}));
        let _ = handle.await;
    }
}

impl Session {
    async fn handle_message(
        &mut self,
        msg: Result<Message, tokio_tungstenite::tungstenite::Error>,
    ) -> Result<(), String> {
        let text = match msg {
            Ok(Message::Text(text)) => text,
            Ok(_) => return Ok(()),
            Err(e) => {
                return Err(format!(
                    "[{}] Session handle_message Err: {}",
                    Local::now().format("%Y-%m-%d %H:%M:%S"),
                    e
                ));
            }
        };

        let msg: Msg = serde_json::from_str(&text).map_err(|e| format!("Invalid JSON: {e}"))?;

        if self.user_id.is_none() && msg.msg_type != MsgType::Login {
            return Err("Unauthorized: must login first".to_string());
        }

        match msg.msg_type {
            // User-related
            MsgType::Login => self.handle_login(&msg).await,

            // Node insertion and movement
            MsgType::NodeInsert => self.handle_node_insert(&msg).await,
            MsgType::NodeDrag => self.handle_node_drag(&msg).await,
            MsgType::NodeUpdate => self.handle_node_update(&msg).await,

            // Node update
            MsgType::UpdateNodeDirectionRule => self.handle_node_update(&msg).await,
            MsgType::UpdateNodeName => self.handle_node_update(&msg).await,

            // Node removal and replacement
            MsgType::LeafRemove => self.handle_leaf_remove(&msg).await,
            MsgType::BranchRemove => self.handle_branch_remove(&msg).await,
            MsgType::SupportRemove => self.handle_support_remove(&msg).await,
            MsgType::LeafReplace => self.handle_leaf_replace(&msg).await,
            MsgType::SupportReplace => self.handle_support_replace(&msg).await,

            // Node pre-checks before removal
            MsgType::LeafCheckBeforeRemove => self.handle_leaf_check_before_remove(&msg).await,
            MsgType::SupportCheckBeforeRemove => {
                self.handle_support_check_before_remove(&msg).await
            }

            // Entry operations
            MsgType::EntryInsert => self.handle_entry_insert(&msg).await,
            MsgType::EntryUpdate => self.handle_entry_update(&msg).await,
            MsgType::EntryRemove => self.handle_entry_remove(&msg).await,
            MsgType::UpdateEntryRhsNode => self.handle_update_entry_rhs_node(&msg).await,
            MsgType::UpdateEntrySupportNode => self.handle_update_entry_support_node(&msg).await,
            MsgType::UpdateEntryRate => self.handle_update_entry_value(&msg).await,
            MsgType::UpdateEntryNumeric => self.handle_update_entry_value(&msg).await,

            // Update global config
            MsgType::UpdateDefaultUnit => self.handle_update_default_unit(&msg).await,
            MsgType::UpdateDocumentDir => self.handle_update_document_dir(&msg).await,

            // Settlement operations
            MsgType::UpdateSettlement => self.handle_update_settlement(&msg).await,

            // Fetch data
            MsgType::FetchTable => self.handle_fetch_table(&msg).await,
            MsgType::FetchTree => self.handle_fetch_tree(&msg).await,

            // Action check
            MsgType::CheckAction => self.handle_check_action(&msg).await,

            // Unknown or unhandled message types
            _ => Ok(()),
        }
    }
}

impl Session {
    async fn handle_login(&mut self, msg: &Msg) -> Result<(), String> {
        // Check if already logged in
        if self.user_id.is_some() {
            return Ok(());
        }

        // Parse login information
        let value: LoginInfo =
            from_value(msg.value.clone()).map_err(|e| format!("Failed to parse LoginInfo: {e}"))?;

        let user = &value.user;
        println!("Login info: {}@{}", user, value.workspace);

        // Build connection string and test connection
        let conn_str = build_pg_url(user, &value.password, &value.workspace);
        let mut temp_conn = match PgConnection::connect(&conn_str).await {
            Ok(conn) => conn,
            Err(e) => {
                println!("{e}");

                send_private_message(self.ws_writer.clone(), MsgType::LoginFailed, json!({}))
                    .await?;

                return Ok(());
            }
        };

        // Check if database is YTX managed
        let managed = is_ytx_managed(&mut temp_conn).await.unwrap_or(false);
        if !managed {
            send_private_message(self.ws_writer.clone(), MsgType::LoginFailed, json!({})).await?;
            return Ok(());
        }

        // Get user role and user_id
        let role = get_role(&mut temp_conn, user).await?;
        self.user_id = Some(get_user_id(&mut temp_conn, &value.user).await?);

        // Initialize connection pool
        let pool = self
            .dbhub
            .init_pool(&conn_str, value.workspace.clone(), role.clone())
            .await
            .map_err(|e| format!("DbHub init_pool error: {e}"))?;

        self.pgpool = Some(pool.clone());

        self.start_broadcast(value.workspace, role).await?;
        self.push_global_config().await?;

        // Send login success message with session ID
        send_private_message(
            self.ws_writer.clone(),
            MsgType::LoginSuccess,
            json!({
                SESSION_ID: self.session_id.to_string(),
            }),
        )
        .await?;

        self.push_tree().await?;
        Ok(())
    }
}

impl Session {
    async fn push_tree(&self) -> Result<(), String> {
        let pool = self
            .pgpool
            .as_ref()
            .ok_or("pgpool not initialized")?
            .clone();

        let writer = self.ws_writer.clone();

        let tasks = SECTIONS.iter().map(|section| {
            let pool = pool.clone();
            let writer = writer.clone();
            let section = section.to_string();

            async move {
                let sql_gen = self.sql_factory.get(&section).ok_or_else(|| {
                    format!("No SqlGen implementation found for section: '{}'", section)
                })?;

                let mut section_obj = serde_json::Map::new();
                section_obj.insert(SECTION.into(), Value::String(section.clone()));

                let node_sql = sql_gen.select_node(&section);
                let node_rows = sqlx::query(&node_sql)
                    .fetch_all(&pool)
                    .await
                    .map_err(|e| format!("Query failed for table '{}_node': {}", section, e))?;

                if node_rows.is_empty() {
                    return Ok(());
                }

                let path_table = format!("{}_path", section);
                let path_sql = format!("SELECT * FROM {}", path_table);

                let path_rows = sqlx::query(&path_sql)
                    .fetch_all(&pool)
                    .await
                    .map_err(|e| format!("Query failed for table '{}': {}", path_table, e))?;

                if path_rows.is_empty() {
                    return Err(
                        "Data inconsistency detected: node_rows and path_rows are not synchronized"
                            .to_string(),
                    );
                }

                let node_array = pg_to_json_rows(&node_rows)?;
                section_obj.insert(NODE.into(), node_array);

                let path_array = pg_to_json_rows(&path_rows)?;
                section_obj.insert(PATH.into(), path_array);

                send_private_message(writer, MsgType::PushTree, Value::Object(section_obj)).await?;
                Ok::<(), String>(())
            }
        });

        let results = join_all(tasks).await;

        for result in results {
            if let Err(e) = result {
                println!("Error fetching tree data: {}", e);
            }
        }

        println!(
            "[{}] push_tree: completed.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn push_global_config(&self) -> Result<(), String> {
        let pool = self
            .pgpool
            .as_ref()
            .ok_or("pgpool not initialized")?
            .clone();

        let writer = self.ws_writer.clone();

        let rows = sqlx::query(r#"SELECT section, default_unit, document_dir FROM global_config"#)
            .fetch_all(&pool)
            .await
            .map_err(|e| e.to_string())?;

        let configs_json: Vec<_> = rows
            .into_iter()
            .map(|row| {
                json!({
                    "section": row.try_get::<String, _>("section").unwrap_or_default(),
                    "default_unit": row.try_get::<i32, _>("default_unit").unwrap_or(0),
                    "document_dir": row.try_get::<String, _>("document_dir").unwrap_or_default(),
                })
            })
            .collect();

        send_private_message(
            writer,
            MsgType::PushGlobalConfig,
            Value::Array(configs_json),
        )
        .await?;

        println!(
            "[{}] push_global_config: completed.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }
}

impl Session {
    async fn handle_update_document_dir(&self, msg: &Msg) -> Result<(), String> {
        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: UpdateDocumentDir = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse UpdateDocumentDir: {e}"))?;

        value.session_id = self.session_id.to_string();

        sqlx::query(
            "UPDATE global_config SET document_dir = $1, updated_time = now(), updated_by = $2 WHERE section = $3",
        )
        .bind(&value.document_dir)
        .bind(user_id)
        .bind(&value.section)
        .execute(pool)
        .await
        .map_err(|e| format!("Failed to update document_dir: {e}"))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] handle_update_document_dir: successful.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        Ok(())
    }

    async fn handle_update_default_unit(&self, msg: &Msg) -> Result<(), String> {
        let (user_id, pool, sender) = self.resolve_context()?;

        let value: UpdateDefaultUnit = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse UpdateDefaultUnit: {e}"))?;

        let section = &value.section;
        validate_section(section)?;

        if section == FINANCE {
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM finance_entry WHERE is_valid = TRUE)",
            )
            .fetch_one(pool)
            .await
            .map_err(|e| format!("Failed to check if finance_entry exists: {e}"))?;

            if exists {
                send_private_message(
                    self.ws_writer.clone(),
                    MsgType::UpdateDefaultUnitFailed,
                    json!({STATUS: false}),
                )
                .await?;

                return Err(
                    "Cannot change default unit: finance entries already exist.".to_string()
                );
            }
        }

        sqlx::query(
                "UPDATE global_config SET default_unit = $1, updated_time = now(), updated_by = $2 WHERE section = $3",
            )
            .bind(&value.default_unit)
            .bind(user_id)
            .bind(section)
            .execute(pool)
            .await
            .map_err(|e| format!("Failed to update default_unit: {e}"))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] handle_update_default_unit: successful.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        Ok(())
    }
}

impl Session {
    /// Starts a broadcast task for a specific database.
    /// This task listens for database notifications and forwards them to the WebSocket client.
    ///
    /// Broadcast mechanism overview:
    /// 1. Each client gets its own `Receiver` from a shared broadcast channel.
    /// 2. When the database updates, all receivers receive the same notification.
    /// 3. Each client independently processes the messages in its own background task.
    ///
    /// # Arguments
    /// * `database` - The name of the database to subscribe to.
    async fn start_broadcast(&mut self, database: String, role: String) -> Result<(), String> {
        // Get the broadcast sender first
        self.sender = Some(self.dbhub.get_sender(database, role).await);

        // Then get a receiver for this specific client
        let mut receiver = self
            .sender
            .as_ref()
            .ok_or("Broadcast sender not initialized")?
            .subscribe();

        // Clone the WebSocket writer for use in the spawned task
        let ws_writer = self.ws_writer.clone();

        // Clone the stop signal receiver for graceful shutdown
        let mut stop_receiver = self.stop_receiver.clone();

        // Spawn a new task to handle the broadcast
        self.forwarder = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Handle shutdown signal
                    _ = stop_receiver.changed() => break,
                    // Handle database notifications
                    msg = receiver.recv() => match msg {
                        Ok(data) => {
                            // Forward the message to the WebSocket client
                            if ws_writer.lock().await.send(Message::Text(data.into())).await.is_err() {
                                break;
                            }
                        }
                        // Skip missed messages if we're too slow
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        // Break on other errors
                        Err(_) => break,
                    }
                }
            }

            // Cleanup: close the WebSocket connection
            let _ = ws_writer.lock().await.close().await;
        });

        Ok(())
    }

    pub fn resolve_context(&self) -> Result<(Uuid, &PgPool, &broadcast::Sender<String>), String> {
        let user_id = self.user_id.ok_or("user_id is missing")?;
        let pool = self.pgpool.as_ref().ok_or("pgpool not initialized")?;
        let sender = self.sender.as_ref().ok_or("sender not initialized")?;
        Ok((user_id, pool, sender))
    }
}

impl Session {
    async fn handle_fetch_tree(&self, msg: &Msg) -> Result<(), String> {
        let pool = self.pgpool.as_ref().ok_or("pgpool not initialized")?;

        let value: FetchTree =
            from_value(msg.value.clone()).map_err(|e| format!("Failed to parse FetchTree: {e}"))?;

        let section = &value.section;
        validate_section(section)?;

        match section.as_str() {
            TASK | SALE | PURCHASE => {}
            _ => return Err(format!("Invalid section: '{}'", section)),
        }

        let start = &value.start;
        let end = &value.end;

        let sql_gen = self
            .sql_factory
            .get(&section)
            .ok_or_else(|| format!("No SqlGen implementation found for section: '{}'", section))?;

        let sql = sql_gen
            .fetch_tree(section)
            .ok_or_else(|| format!("fetch_tree returned None for section: '{}'", section))?;

        let node_rows = sqlx::query(&sql)
            .bind(start)
            .bind(end)
            .fetch_all(&*pool)
            .await
            .map_err(|e| format!("Database query failed: {e}"))?;

        let mut obj = serde_json::Map::new();
        obj.insert(SECTION.into(), Value::String(section.clone()));

        if node_rows.is_empty() {
            obj.insert(NODE.into(), Value::Array(vec![]));
            obj.insert(PATH.into(), Value::Array(vec![]));
        } else {
            let path_table = format!("{}_path", section);
            let path_sql = format!("SELECT * FROM {}", path_table);

            let path_rows = sqlx::query(&path_sql)
                .fetch_all(pool)
                .await
                .map_err(|e| format!("Query failed for table '{}': {}", path_table, e))?;

            if path_rows.is_empty() {
                return Err(
                    "Data inconsistency detected: node_rows and path_rows are not synchronized"
                        .to_string(),
                );
            }

            let node_array = pg_to_json_rows(&node_rows)?;
            obj.insert(NODE.into(), node_array);

            let path_array = pg_to_json_rows(&path_rows)?;
            obj.insert(PATH.into(), path_array);
        }

        send_private_message(
            self.ws_writer.clone(),
            MsgType::FetchTree,
            Value::Object(obj),
        )
        .await?;

        println!(
            "[{}] handle_fetch_tree: successful.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        Ok(())
    }

    async fn handle_node_insert(&self, msg: &Msg) -> Result<(), String> {
        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: NodeInsert = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse NodeInsert: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let node = &mut value.node;
        let path = &mut value.path;

        let now = Utc::now().to_rfc3339();

        let ancestor_id = path
            .get(ANCESTOR)
            .and_then(|v| v.as_str())
            .ok_or("Missing or invalid 'ancestor'")?
            .parse()
            .map_err(|e| format!("Invalid ancestor UUID: {}", e))?;

        let descendant_id = path
            .get(DESCENDANT)
            .and_then(|v| v.as_str())
            .ok_or("Missing or invalid 'descendant'")?
            .parse()
            .map_err(|e| format!("Invalid descendant UUID: {}", e))?;

        let node_table = format!("{}_node", section);

        value.session_id = self.session_id.to_string();

        node.insert(USER_ID.to_string(), Value::String(user_id.to_string()));
        node.insert(CREATED_BY.to_string(), Value::String(user_id.to_string()));
        node.insert(CREATED_TIME.to_string(), Value::String(now.clone()));

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        insert_row(node_table.as_str(), &node, &mut tx).await?;
        insert_path(&section, ancestor_id, descendant_id, &mut tx).await?;

        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] handle_node_insert: successful.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        Ok(())
    }

    async fn handle_node_update(&self, msg: &Msg) -> Result<(), String> {
        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: Update =
            from_value(msg.value.clone()).map_err(|e| format!("Failed to parse Update: {}", e))?;

        value.session_id = self.session_id.to_string();
        let table_name = format!("{}_node", value.section);

        let now = Utc::now().to_rfc3339();
        let cache = &mut value.cache;

        cache.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
        cache.insert(UPDATED_TIME.to_string(), Value::String(now));

        update_row(&table_name, value.id, cache, pool).await?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] handle_node_update: successful.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        Ok(())
    }

    async fn handle_node_drag(&self, msg: &Msg) -> Result<(), String> {
        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: NodeDrag = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse NodeDrag: {}", e))?;

        value.session_id = self.session_id.to_string();
        let section = &value.section;
        validate_section(section)?;

        let path = &mut value.path;
        let node = &mut value.node;

        let node_table = format!("{}_node", section);

        let now = Utc::now();

        let ancestor_id: Uuid = path
            .get(ANCESTOR)
            .and_then(|v| v.as_str())
            .ok_or("Missing or invalid 'ancestor'")?
            .parse()
            .map_err(|e| format!("Invalid ancestor UUID: {}", e))?;

        let descendant_id: Uuid = path
            .get(DESCENDANT)
            .and_then(|v| v.as_str())
            .ok_or("Missing or invalid 'descendant'")?
            .parse()
            .map_err(|e| format!("Invalid descendant UUID: {}", e))?;

        node.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
        node.insert(UPDATED_TIME.to_string(), Value::String(now.to_rfc3339()));

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        delete_descendant_path(&section, descendant_id, &mut tx).await?;
        insert_path(&section, ancestor_id, descendant_id, &mut tx).await?;
        update_row(&node_table, descendant_id, &node, tx.as_mut()).await?;

        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] handle_node_drag: successful.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        Ok(())
    }

    async fn handle_leaf_remove(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_leaf_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: LeafRemove = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse LeafRemove: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let id = value.id;
        let leaf_entry = &mut value.leaf_entry;
        let support_entry = &mut value.support_entry;

        value.session_id = self.session_id.to_string();
        let now = Utc::now();

        let sql_gen = self
            .sql_factory
            .get(section)
            .ok_or_else(|| format!("No SqlGen found for section: {}", section))?;

        let mut delta_hash: HashMap<Uuid, (Decimal, Decimal)> = HashMap::new();

        if let Some(sql) = sql_gen.collect_leaf_entry(section) {
            let rows = sqlx::query(&sql)
                .bind(id)
                .fetch_all(pool)
                .await
                .map_err(|e| format!("Failed to query collect_leaf_reference: {}", e))?;

            for row in rows {
                let node_id: Uuid = row
                    .try_get(NODE_ID)
                    .map_err(|e| format!("Failed to query {NODE_ID}: {}", e))?;

                let entry_id: Uuid = row
                    .try_get(ENTRY_ID)
                    .map_err(|e| format!("Failed to query {ENTRY_ID}: {}", e))?;

                let support_id: Option<Uuid> = row
                    .try_get(SUPPORT_ID)
                    .map_err(|e| format!("Failed to query {SUPPORT_ID}: {}", e))?;

                leaf_entry
                    .entry(node_id)
                    .or_insert_with(Vec::new)
                    .push(entry_id);

                if let Some(support) = support_id {
                    support_entry
                        .entry(support)
                        .or_insert_with(Vec::new)
                        .push(entry_id);
                }

                let debit: Decimal = row
                    .try_get(DEBIT)
                    .map_err(|e| format!("Failed to query debit: {}", e))?;

                let credit: Decimal = row
                    .try_get(CREDIT)
                    .map_err(|e| format!("Failed to query credit: {}", e))?;

                let rate: Decimal = row
                    .try_get(RATE)
                    .map_err(|e| format!("Failed to query rate: {}", e))?;

                let delta = delta_hash
                    .entry(node_id)
                    .or_insert((Decimal::ZERO, Decimal::ZERO));

                delta.0 += credit - debit;
                delta.1 += rate * (credit - debit);
            }
        }

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        if !delta_hash.is_empty() {
            let count = delta_hash.len();

            let mut node_ids = Vec::with_capacity(count);
            let mut initial_deltas = Vec::with_capacity(count);
            let mut final_deltas = Vec::with_capacity(count);
            let mut node_delta = Vec::with_capacity(count);

            for (id, (initial_delta, final_delta)) in delta_hash {
                node_ids.push(id);
                initial_deltas.push(initial_delta);
                final_deltas.push(final_delta);
                node_delta.push(NodeDelta {
                    id,
                    initial_delta,
                    final_delta,
                });
            }

            value.node_delta = node_delta;

            sqlx::query(&format!(
                r#"
                UPDATE {section}_node AS n
                SET
                    initial_total = initial_total + CASE WHEN direction_rule THEN u.initial_delta ELSE -u.initial_delta END,
                    final_total = final_total + CASE WHEN direction_rule THEN u.final_delta ELSE -u.final_delta END,
                    updated_by = $1,
                    updated_time = $2
                FROM (
                    SELECT
                        UNNEST($3::UUID[])    AS node_id,
                        UNNEST($4::NUMERIC[]) AS initial_delta,
                        UNNEST($5::NUMERIC[]) AS final_delta
                ) AS u
                WHERE n.id = u.node_id
                "#
            ))
            .bind(user_id)
            .bind(now)
            .bind(&node_ids)
            .bind(&initial_deltas)
            .bind(&final_deltas)
            .fetch_all(tx.as_mut())
            .await
            .map_err(|e| format!("Failed to batch update node totals: {}", e))?;
        }

        println!("Removing node...");
        remove_node(&section, id, user_id, now, sql_gen, &mut tx).await?;

        println!("Removing leaf entry...");
        remove_leaf_reference(&section, id, user_id, now, sql_gen, &mut tx)
            .await
            .map_err(|e| e.to_string())?;

        println!("Deleting path...");
        delete_descendant_path(&section, id, &mut tx).await?;

        println!("Committing transaction...");
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        println!("Sending public message...");
        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_leaf_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_support_check_before_remove(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_support_check_before_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let value: SupportCheckBeforeRemove = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse SupportCheckBeforeRemove request: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let id = value.id;

        let now = Utc::now();

        let sql_gen = self
            .sql_factory
            .get(section)
            .ok_or_else(|| format!("No SqlGen found for section: {}", section))?;

        let support_reference = has_support_reference(section, id, sql_gen, pool).await?;

        if !support_reference {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| format!("Failed to begin transaction: {}", e))?;

            println!("Removing node...");
            remove_node(&section, id, user_id, now, sql_gen, &mut tx).await?;

            println!("Deleting path...");
            delete_descendant_path(&section, id, &mut tx).await?;

            tx.commit()
                .await
                .map_err(|e| format!("Failed to commit transaction: {}", e))?;

            send_public_message(
                sender.clone(),
                MsgType::UnreferencedNodeRemove,
                json!(value),
            )
            .await?;
        } else {
            send_private_message(self.ws_writer.clone(), msg.msg_type.clone(), json!(value))
                .await?;
        }

        println!(
            "[{}] End handle_support_check_before_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_leaf_check_before_remove(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_leaf_check_before_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: LeafCheckBeforeRemove = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse LeafCheckBeforeRemove request: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let id = value.id;
        let now = Utc::now();

        let sql_gen = self
            .sql_factory
            .get(section)
            .ok_or_else(|| format!("No SqlGen found for section: {}", section))?;

        value.leaf_reference = has_leaf_reference(section, id, sql_gen, pool).await?;
        value.external_reference = has_external_reference(id, sql_gen, pool).await?;

        if !value.leaf_reference && !value.external_reference {
            let mut tx = pool
                .begin()
                .await
                .map_err(|e| format!("Failed to begin transaction: {}", e))?;

            println!("Removing node...");
            remove_node(&section, id, user_id, now, sql_gen, &mut tx).await?;

            println!("Deleting path...");
            delete_descendant_path(&section, id, &mut tx).await?;

            tx.commit()
                .await
                .map_err(|e| format!("Failed to commit transaction: {}", e))?;

            send_public_message(
                sender.clone(),
                MsgType::UnreferencedNodeRemove,
                json!(value),
            )
            .await?;
        } else {
            send_private_message(self.ws_writer.clone(), msg.msg_type.clone(), json!(value))
                .await?;
        }

        println!(
            "[{}] End handle_leaf_check_before_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_branch_remove(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_branch_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: BranchRemove = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse RemoveBranchNode: {}", e))?;

        value.session_id = self.session_id.to_string();

        let section = &value.section;
        validate_section(section)?;

        let id = value.id;

        let sql_gen = self
            .sql_factory
            .get(section)
            .ok_or_else(|| format!("No SqlGen found for section: {}", section))?;

        let now = Utc::now();

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        remove_node(&section, id, user_id, now, sql_gen, &mut tx).await?;

        reconnect_path_before_remove(&section, id, &mut tx).await?;
        delete_descendant_path(&section, id, &mut tx).await?;
        delete_ancestor_path(&section, id, &mut tx).await?;

        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_branch_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_support_remove(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_support_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: SupportRemove = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse SupportRemove: {}", e))?;

        value.session_id = self.session_id.to_string();

        let section = &value.section;
        validate_section(section)?;

        let id = value.id;

        let sql_gen = self
            .sql_factory
            .get(section)
            .ok_or_else(|| format!("No SqlGen found for section: {}", section))?;

        let now = Utc::now();

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        println!("Removing support node...");
        remove_node(&section, id, user_id, now, sql_gen, &mut tx).await?;

        println!("Removing support entry...");
        remove_support_reference(&section, id, user_id, now, sql_gen, &mut tx).await?;

        println!("Deleting path...");
        delete_descendant_path(&section, id, &mut tx).await?;

        println!("Committing transaction...");
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        println!("Sending public message...");
        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_support_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_support_replace(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_support_replace",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: SupportReplace = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse SupportReplace: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let old_id = value.old_id;
        let new_id = value.new_id;

        println!("Section: {}", section);

        let now = Utc::now();
        value.session_id = self.session_id.to_string();

        let sql_gen = self
            .sql_factory
            .get(section)
            .ok_or_else(|| format!("No SqlGen found for section: {}", section))?;

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        println!("Removing support node...");
        remove_node(&section, old_id, user_id, now, sql_gen, &mut tx).await?;

        println!("Replacing support entry...");
        replace_support_reference(&section, old_id, new_id, user_id, now, &mut tx).await?;

        println!("Deleting path...");
        delete_descendant_path(&section, old_id, &mut tx).await?;

        println!("Committing transaction...");
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_support_replace",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_leaf_replace(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_leaf_replace",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: LeafReplace = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse LeafReplace: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        if section == SALE || section == PURCHASE {
            return Err(format!(
                "Replace operation is not supported for section '{}'",
                section
            ));
        }

        let old_id = value.old_id;
        let new_id = value.new_id;
        value.session_id = self.session_id.to_string();

        let sql_gen = self
            .sql_factory
            .get(section)
            .ok_or_else(|| format!("No SqlGen implementation found for section: '{}'", section))?;

        if let Some(sql) = sql_gen.has_replace_conflict(section) {
            let conflict_exists: bool = sqlx::query_scalar(&sql)
                .bind(old_id)
                .bind(new_id)
                .fetch_one(pool)
                .await
                .map_err(|e| format!("Failed to check replace conflict: {}", e))?;

            if conflict_exists {
                value.status = false;
                send_private_message(self.ws_writer.clone(), msg.msg_type.clone(), json!(value))
                    .await?;

                return Err(format!("Replace conflict detected in section {}", section));
            }
        }

        let now = Utc::now();

        println!("Merge leaf total...");
        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        if let Some(merge_sql) = sql_gen.merge_node_total(section) {
            sqlx::query(&merge_sql)
                .bind(user_id)
                .bind(now)
                .bind(old_id)
                .bind(new_id)
                .execute(tx.as_mut())
                .await
                .map_err(|e| format!("Failed to merge node totals: {}", e))?;
        }

        println!("Removing leaf node...");
        remove_node(&section, old_id, user_id, now, sql_gen, &mut tx).await?;

        println!("Replacing leaf entry...");
        replace_leaf_reference(&section, old_id, new_id, user_id, now, sql_gen, &mut tx).await?;

        if section == ITEM && value.external_reference {
            for entry_table in ["stakeholder_entry", "sale_entry", "purchase_entry"] {
                let sql = format!(
                    r#"
                    UPDATE {entry_table}
                    SET rhs_node = CASE WHEN rhs_node = $3 THEN $4 ELSE rhs_node END,
                        external_item = CASE WHEN external_item = $3 THEN $4 ELSE external_item END,
                        updated_by = $1,
                        updated_time = $2
                    WHERE (rhs_node = $3 OR external_item = $3) AND is_valid = TRUE;
                    "#
                );

                sqlx::query(&sql)
                    .bind(user_id) // $1
                    .bind(now) // $2
                    .bind(old_id) // $3
                    .bind(new_id) // $4
                    .execute(tx.as_mut())
                    .await
                    .map_err(|e| {
                        format!(
                            "Failed to replace item external entry in table {}: {}",
                            entry_table, e
                        )
                    })?;
            }
        }

        println!("Deleting path...");
        delete_descendant_path(&section, old_id, tx.as_mut()).await?;

        println!("Committing transaction...");
        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        value.status = true;
        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_leaf_replace",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }
}

impl Session {
    async fn handle_fetch_table(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_fetch_table",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let pool = self.pgpool.as_ref().ok_or("pgpool not initialized")?;

        let mut value: FetchTable = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse FetchTable: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let node_id = value.node_id;
        let kind = value.kind;

        let sql_gen = self
            .sql_factory
            .get(section)
            .ok_or_else(|| format!("No SqlGen found for section: {}", section))?;

        let sql = match kind {
            LEAF_NODE => Some(sql_gen.leaf_entry(section)),
            SUPPORT_NODE => sql_gen.support_entry(section),
            _ => None,
        }
        .ok_or_else(|| format!("No SQL statement for kind: {}", kind))?;

        let rows = sqlx::query(&sql)
            .bind(node_id)
            .fetch_all(pool)
            .await
            .map_err(|e| format!("Query failed: {}", e))?;

        if rows.is_empty() {
            println!(
                "No entry rows found for section '{}' and id {}",
                section, node_id
            );
            return Ok(());
        }

        value.entry = pg_to_json_rows(&rows)?;
        send_private_message(self.ws_writer.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_fetch_table",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        Ok(())
    }

    async fn handle_entry_insert(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_entry_insert",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: EntryInsert = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse EntryInsert: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let entry = &mut value.entry;

        let now = Utc::now().to_rfc3339();
        value.session_id = self.session_id.to_string();

        let entry_table = format!("{}_entry", section);

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        entry.insert(USER_ID.to_string(), Value::String(user_id.to_string()));
        entry.insert(CREATED_BY.to_string(), Value::String(user_id.to_string()));
        entry.insert(CREATED_TIME.to_string(), Value::String(now.clone()));

        if let Some(lhs) = &mut value.lhs_delta {
            lhs.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
            lhs.insert(UPDATED_TIME.to_string(), Value::String(now.clone()));
            update_node_total(section, lhs, tx.as_mut()).await?;
        }

        if let Some(rhs) = &mut value.rhs_delta {
            rhs.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
            rhs.insert(UPDATED_TIME.to_string(), Value::String(now));
            update_node_total(section, rhs, tx.as_mut()).await?;
        }

        insert_row(entry_table.as_str(), &entry, tx.as_mut()).await?;

        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_entry_insert",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        Ok(())
    }

    async fn handle_entry_update(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_entry_update",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: Update = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse EntryUpdate: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let cache = &mut value.cache;
        let id = value.id;

        let now = Utc::now().to_rfc3339();
        value.session_id = self.session_id.to_string();

        let entry_table = format!("{}_entry", section);

        cache.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
        cache.insert(UPDATED_TIME.to_string(), Value::String(now.clone()));

        update_row(entry_table.as_str(), id, &cache, pool).await?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_entry_update",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_update_entry_value(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_update_entry_value",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: UpdateEntryValue = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse UpdateEntryValue: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let cache = &mut value.cache;
        let entry_id = value.entry_id;

        let now = Utc::now();
        let now_str = now.to_rfc3339();
        value.session_id = self.session_id.to_string();

        let entry_table = format!("{}_entry", section);

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        cache.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
        cache.insert(UPDATED_TIME.to_string(), Value::String(now_str.clone()));

        if let Some(lhs) = &mut value.lhs_delta {
            lhs.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
            lhs.insert(UPDATED_TIME.to_string(), Value::String(now_str.clone()));
            update_node_total(section, lhs, tx.as_mut()).await?;
        }

        if let Some(rhs) = &mut value.rhs_delta {
            rhs.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
            rhs.insert(UPDATED_TIME.to_string(), Value::String(now_str.clone()));
            update_node_total(section, rhs, tx.as_mut()).await?;
        }

        update_row(&entry_table, entry_id, cache, tx.as_mut()).await?;

        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_update_entry_value",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_update_entry_rhs_node(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_update_rhs_node",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: UpdateEntryRhsNode = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse UpdateRhsNode: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let field = &value.field;
        validate_field(field)?;

        let new_rhs_id: &Uuid = &value.new_rhs_id;

        let entry = &mut value.entry;
        let entry_id = value.entry_id;

        let now = Utc::now();
        let now_str = now.to_rfc3339();
        value.session_id = self.session_id.to_string();

        let entry_table = format!("{}_entry", section);

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        entry.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
        entry.insert(UPDATED_TIME.to_string(), Value::String(now_str.clone()));

        if let Some(old) = &mut value.old_rhs_delta {
            old.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
            old.insert(UPDATED_TIME.to_string(), Value::String(now_str.clone()));
            update_node_total(section, old, tx.as_mut()).await?;
        }

        if let Some(new) = &mut value.new_rhs_delta {
            new.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
            new.insert(UPDATED_TIME.to_string(), Value::String(now_str.clone()));
            update_node_total(section, new, tx.as_mut()).await?;
        }

        let sql = format!(
            "UPDATE {} SET updated_by = $1, updated_time = $2, {} = $3 WHERE id = $4",
            entry_table, field
        );

        sqlx::query(&sql)
            .bind(user_id) // updated_by → $1
            .bind(now) // updated_time → $2
            .bind(new_rhs_id) //  → $3
            .bind(entry_id) // id → $4
            .execute(tx.as_mut())
            .await
            .map_err(|e| format!("Failed to update entry rhs node: {}", e))?;

        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_update_rhs_node",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_update_entry_support_node(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_update_support_node",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: UpdateEntrySupportNode = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse UpdateSupportNode: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let new_support_id = value.new_support_id;
        let entry_id = value.entry_id;

        if entry_id == Uuid::nil() {
            return Err("entry_id cannot be nil".to_string());
        }

        let now = Utc::now();

        value.session_id = self.session_id.to_string();
        let meta = &mut value.meta;

        meta.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
        meta.insert(UPDATED_TIME.to_string(), Value::String(now.to_rfc3339()));

        let entry_table = format!("{}_entry", section);

        let sql = format!(
            "UPDATE {} SET updated_by = $1, updated_time = $2, support_node = $3 WHERE id = $4",
            entry_table
        );

        sqlx::query(&sql)
            .bind(user_id) // updated_by → $1
            .bind(now) // updated_time → $2
            .bind(new_support_id) //  → $3
            .bind(entry_id) // id → $4
            .execute(pool)
            .await
            .map_err(|e| format!("Failed to update entry support node: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_update_support_node",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_entry_remove(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_entry_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: EntryRemove = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse EntryRemove: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let entry_id = value.entry_id;

        let now = Utc::now();
        let now_str = now.to_rfc3339();
        value.session_id = self.session_id.to_string();

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| format!("Failed to begin transaction: {}", e))?;

        let sql = format!(
            "UPDATE {0}_entry SET is_valid = FALSE, updated_by = $1, updated_time = $2 WHERE id = $3",
            section
        );

        sqlx::query(&sql)
            .bind(&user_id)
            .bind(now)
            .bind(entry_id)
            .execute(tx.as_mut())
            .await
            .map_err(|e| format!("Failed to mark entry as deleted: {e}"))?;

        if let Some(lhs) = &mut value.lhs_node {
            lhs.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
            lhs.insert(UPDATED_TIME.to_string(), Value::String(now_str.clone()));
            update_node_total(section, lhs, tx.as_mut()).await?;
        }

        if let Some(rhs) = &mut value.rhs_node {
            rhs.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
            rhs.insert(UPDATED_TIME.to_string(), Value::String(now_str));
            update_node_total(section, rhs, tx.as_mut()).await?;
        }

        tx.commit()
            .await
            .map_err(|e| format!("Failed to commit transaction: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_entry_remove",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }

    async fn handle_check_action(&self, msg: &Msg) -> Result<(), String> {
        println!(
            "[{}] Start handle_check_action",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: CheckAction = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse CheckAction: {}", e))?;

        let section = &value.section;
        validate_section(section)?;

        let meta = &mut value.meta;

        let now = Utc::now();
        value.session_id = self.session_id.to_string();

        let sql_gen = self
            .sql_factory
            .get(&section)
            .ok_or_else(|| format!("No SqlGen for section: {}", section))?;

        let sql = sql_gen.check_action(&section);

        meta.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
        meta.insert(UPDATED_TIME.to_string(), Value::String(now.to_rfc3339()));

        sqlx::query(&sql)
            .bind(user_id) // $1: updated_by
            .bind(now) // $2: updated_time
            .bind(value.check) // $3: check (i32)
            .bind(value.node_id) // $4: node_id
            .execute(pool)
            .await
            .map_err(|e| format!("Failed to execute check_action: {}", e))?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] End handle_check_action",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(())
    }
}

impl Session {
    async fn handle_update_settlement(&self, msg: &Msg) -> Result<(), String> {
        let (user_id, pool, sender) = self.resolve_context()?;

        let mut value: Update = from_value(msg.value.clone())
            .map_err(|e| format!("Failed to parse update data request: {}", e))?;

        value.session_id = self.session_id.to_string();
        let table_name = format!("{}_settlement", value.section);

        let now = Utc::now().to_rfc3339();
        let cache = &mut value.cache;

        cache.insert(UPDATED_BY.to_string(), Value::String(user_id.to_string()));
        cache.insert(UPDATED_TIME.to_string(), Value::String(now));

        update_row(&table_name, value.id, cache, pool).await?;

        send_public_message(sender.clone(), msg.msg_type.clone(), json!(value)).await?;

        println!(
            "[{}] Update: successful.",
            Local::now().format("%Y-%m-%d %H:%M:%S")
        );
        Ok(())
    }
}

fn build_pg_url(user: &str, password: &str, database: &str) -> String {
    format!(
        "postgresql://{}:{}@localhost:5432/{}?connect_timeout=5",
        encode(user),
        encode(password),
        encode(database)
    )
}

fn pg_to_json_rows(rows: &[PgRow]) -> Result<Value, String> {
    let mut result = Vec::new();

    for row in rows {
        let mut obj = Map::new();

        for column in row.columns() {
            let name = column.name().to_string();
            let type_name = column.type_info().name();

            let value = match type_name {
                // String types
                "TEXT" => row
                    .try_get::<Option<&str>, _>(name.as_str())
                    .map(|opt| opt.map(|s| Value::String(s.to_string()))),

                // UUID
                "UUID" => row
                    .try_get::<Option<Uuid>, _>(name.as_str())
                    .map(|opt| opt.map(|u| Value::String(u.to_string()))),

                // Integer types
                "INT4" => row
                    .try_get::<Option<i32>, _>(name.as_str())
                    .map(|opt| opt.map(|v| Value::Number(v.into()))),

                "INT8" => row
                    .try_get::<Option<i64>, _>(name.as_str())
                    .map(|opt| opt.map(|v| Value::Number(Number::from(v)))),

                // NUMERIC types
                "NUMERIC" => row
                    .try_get::<Option<Decimal>, _>(name.as_str())
                    .map(|opt| opt.map(|d| Value::String(d.to_string()))),

                // Boolean
                "BOOL" => row
                    .try_get::<Option<bool>, _>(name.as_str())
                    .map(|opt| opt.map(Value::Bool)),

                // Timestamps
                "TIMESTAMPTZ" => row
                    .try_get::<Option<DateTime<Utc>>, _>(name.as_str())
                    .map(|opt| opt.map(|dt| Value::String(dt.to_rfc3339()))),

                // Fallback: just null
                _ => Ok(Some(Value::Null)),
            }
            .unwrap_or(Some(Value::Null));

            obj.insert(name, value.unwrap_or(Value::Null));
        }

        result.push(Value::Object(obj));
    }

    Ok(Value::Array(result))
}

fn json_to_pg_bind<'q>(
    query: Query<'q, Postgres, PgArguments>,
    field: &str,
    value: &'q Value,
) -> Query<'q, Postgres, PgArguments> {
    match FIELD_TYPE_MAP.get(field).map(|s| *s) {
        Some("UUID") => {
            let uuid = value
                .as_str()
                .and_then(|s| Uuid::parse_str(s).ok())
                .unwrap_or(Uuid::nil());
            query.bind(uuid)
        }
        Some("TIMESTAMPTZ") => {
            let dt = value
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);
            query.bind(dt)
        }
        Some("INT4") => {
            let i = value.as_i64().unwrap_or_default() as i32;
            query.bind(i)
        }
        Some("INT8") => {
            let i = value.as_i64().unwrap_or_default();
            query.bind(i)
        }
        Some("NUMERIC") => {
            let s = value.as_str().unwrap_or_default();
            match Decimal::from_str(s) {
                Ok(d) => query.bind(d),
                Err(e) => {
                    eprintln!("Invalid NUMERIC string '{}': {}", s, e);
                    query.bind(Decimal::ZERO)
                }
            }
        }
        Some("BOOL") => {
            let b = value.as_bool().unwrap_or(false);
            query.bind(b)
        }
        Some("TEXT") => {
            let s = value.as_str().unwrap_or_default();
            query.bind(s)
        }
        _ => {
            if let Some(s) = value.as_str() {
                query.bind(s)
            } else {
                query.bind(Json(value))
            }
        }
    }
}

async fn reconnect_path_before_remove(
    section: &str,
    id: Uuid,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let path_table = format!("{}_path", section);

    let sql = format!(
        r#"
        WITH parent AS (
            SELECT ancestor AS parent_id
            FROM {0}
            WHERE descendant = $1
            LIMIT 1
        ),
        children AS (
            SELECT descendant AS child_id
            FROM {0}
            WHERE ancestor = $1
        )
        INSERT INTO {0} (ancestor, descendant, distance)
        SELECT
            parent.parent_id,
            children.child_id,
            1
        FROM parent, children
        ON CONFLICT (ancestor, descendant)
        DO NOTHING
        "#,
        path_table
    );

    sqlx::query(&sql)
        .bind(id) // $1
        .execute(conn)
        .await
        .map_err(|e| {
            format!(
                "reconnect_path_before_remove failed, {}_path : {}",
                section, e
            )
        })?;

    Ok(())
}

async fn insert_row(
    table: &str,
    data: &HashMap<String, Value>,
    conn: &mut PgConnection,
) -> Result<(), String> {
    if data.is_empty() {
        return Ok(());
    }

    let mut fields = Vec::new();
    let mut values = Vec::new();

    for (field, value) in data.iter() {
        fields.push(field.clone());
        values.push((field.as_str(), value.clone()));
    }

    let placeholders = (1..=fields.len())
        .map(|i| format!("${}", i))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table,
        fields.join(", "),
        placeholders
    );

    let mut query = sqlx::query(&sql);

    for (field, value) in &values {
        query = json_to_pg_bind(query, field, value);
    }

    query
        .execute(&mut *conn)
        .await
        .map_err(|e| format!("insert_row failed for table {} : {}", table, e))?;

    Ok(())
}

async fn insert_path(
    section: &str,
    ancestor: Uuid,
    descendant: Uuid,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let sql = format!(
        "INSERT INTO {}_path (ancestor, descendant, distance) VALUES ($1, $2, 1) ON CONFLICT DO NOTHING",
        section
    );

    sqlx::query(&sql)
        .bind(ancestor)
        .bind(descendant)
        .execute(conn)
        .await
        .map_err(|e| format!("insert_path failed for {}_path: {}", section, e))?;

    Ok(())
}

async fn delete_descendant_path(
    section: &str,
    node_id: Uuid,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let sql = format!("DELETE FROM {}_path WHERE descendant = $1", section);

    sqlx::query(&sql)
        .bind(node_id)
        .execute(conn)
        .await
        .map_err(|e| format!("delete_descendant_path failed, {}-{}", section, e))?;

    Ok(())
}

async fn delete_ancestor_path(
    section: &str,
    node_id: Uuid,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let sql = format!("DELETE FROM {}_path WHERE ancestor = $1", section);

    sqlx::query(&sql)
        .bind(node_id)
        .execute(conn)
        .await
        .map_err(|e| format!("delete_ancestor_path failed, {}-{}", section, e))?;

    Ok(())
}

async fn update_row<'a, E>(
    table: &str,
    id: Uuid,
    data: &HashMap<String, Value>,
    executor: E,
) -> Result<(), String>
where
    E: Executor<'a, Database = Postgres>,
{
    if data.is_empty() {
        return Ok(());
    }

    let mut assignments = Vec::new();
    let mut values = Vec::new();

    for (i, (field, value)) in data.iter().enumerate() {
        assignments.push(format!("{} = ${}", field, i + 1));
        values.push((field.as_str(), value.clone()));
    }

    let id_param_index = values.len() + 1;
    let sql = format!(
        "UPDATE {} SET {} WHERE id = ${}",
        table,
        assignments.join(", "),
        id_param_index
    );

    let mut query = sqlx::query(&sql);

    for (field, value) in &values {
        query = json_to_pg_bind(query, field, value);
    }

    query = query.bind(id);
    query
        .execute(executor)
        .await
        .map_err(|e| format!("Failed to update row in table '{}': {}", table, e))?;

    Ok(())
}

async fn update_node_total(
    section: &str,
    node: &HashMap<String, Value>,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let id: Uuid = node
        .get("id")
        .and_then(|v| v.as_str())
        .ok_or("Missing or invalid 'id'")?
        .parse()
        .map_err(|e| format!("Invalid 'id' UUID: {}", e))?;

    let updated_by: Uuid = node
        .get("updated_by")
        .and_then(|v| v.as_str())
        .ok_or("Missing or invalid 'updated_by'")?
        .parse()
        .map_err(|e| format!("Invalid 'updated_by' UUID: {}", e))?;

    let updated_time = node
        .get("updated_time")
        .and_then(|v| v.as_str())
        .and_then(|s| {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        })
        .ok_or("Missing or invalid 'updated_time'")?;

    let initial_delta = match node.get("initial_delta").and_then(|v| v.as_str()) {
        Some(s) => Decimal::from_str(s).unwrap_or_else(|e| {
            eprintln!("Invalid NUMERIC string 'initial_delta': '{}': {}", s, e);
            Decimal::ZERO
        }),
        None => {
            eprintln!("Expected string for 'initial_delta'");
            Decimal::ZERO
        }
    };

    let final_delta = match node.get("final_delta").and_then(|v| v.as_str()) {
        Some(s) => Decimal::from_str(s).unwrap_or_else(|e| {
            eprintln!("Invalid NUMERIC string 'final_delta': '{}': {}", s, e);
            Decimal::ZERO
        }),
        None => {
            eprintln!("Expected string for 'final_delta'");
            Decimal::ZERO
        }
    };

    let sql = format!(
        "UPDATE {section}_node SET
                updated_by    = $1,
                updated_time  = $2,
                initial_total = initial_total + CASE WHEN direction_rule THEN $3 ELSE -$3 END,
                final_total   = final_total   + CASE WHEN direction_rule THEN $4 ELSE -$4 END
             WHERE id = $5"
    );

    sqlx::query(&sql)
        .bind(updated_by)
        .bind(updated_time)
        .bind(initial_delta)
        .bind(final_delta)
        .bind(id)
        .execute(conn)
        .await
        .map_err(|e| format!("Failed to update node total {id}: {e}"))?;

    Ok(())
}

async fn remove_node(
    section: &str,
    id: Uuid,
    updated_by: Uuid,
    updated_time: DateTime<Utc>,
    sql_gen: &dyn SqlGen,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let sql = sql_gen.remove_node(section);

    sqlx::query(&sql)
        .bind(updated_by)
        .bind(updated_time)
        .bind(id)
        .execute(conn)
        .await
        .map_err(|e| {
            format!(
                "Failed to remove node (id: {}) in '{}_node': {}",
                id, section, e
            )
        })?;

    Ok(())
}

async fn has_leaf_reference(
    section: &str,
    id: Uuid,
    sql_gen: &dyn SqlGen,
    pool: &PgPool,
) -> Result<bool, String> {
    let sql = sql_gen.has_leaf_reference(section);

    let exists: bool = sqlx::query_scalar(&sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Check leaf reference failed: {}", e))?;

    Ok(exists)
}

async fn has_support_reference(
    section: &str,
    id: Uuid,
    sql_gen: &dyn SqlGen,
    pool: &PgPool,
) -> Result<bool, String> {
    let Some(sql) = sql_gen.has_support_reference(section) else {
        return Ok(false);
    };

    let exists: bool = sqlx::query_scalar(&sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Check support reference failed: {}", e))?;

    Ok(exists)
}

async fn has_external_reference(
    id: Uuid,
    sql_gen: &dyn SqlGen,
    pool: &PgPool,
) -> Result<bool, String> {
    let Some(sql) = sql_gen.has_external_reference() else {
        return Ok(false);
    };

    let exists: bool = sqlx::query_scalar(&sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .map_err(|e| format!("Check external reference failed: {}", e))?;

    Ok(exists)
}

async fn remove_leaf_reference(
    section: &str,
    id: Uuid,
    updated_by: Uuid,
    updated_time: DateTime<Utc>,
    sql_gen: &dyn SqlGen,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let sql = sql_gen.remove_leaf_entry(section);

    sqlx::query(&sql)
        .bind(updated_by) // $1
        .bind(updated_time) // $2
        .bind(id) // $3
        .execute(conn)
        .await
        .map_err(|e| format!("Check support reference failed: {}", e))?;

    Ok(())
}

async fn remove_support_reference(
    section: &str,
    id: Uuid,
    updated_by: Uuid,
    updated_time: DateTime<Utc>,
    sql_gen: &dyn SqlGen,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let Some(sql) = sql_gen.remove_support_reference(section) else {
        return Ok(());
    };

    sqlx::query(&sql)
        .bind(updated_by)
        .bind(updated_time)
        .bind(id)
        .execute(conn)
        .await
        .map_err(|e| format!("Check support reference failed: {}", e))?;

    Ok(())
}

async fn replace_support_reference(
    section: &str,
    old_id: Uuid,
    new_id: Uuid,
    updated_by: Uuid,
    updated_time: DateTime<Utc>,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let sql = format!(
        r#"
        UPDATE {}_entry
        SET
            support_id = $1,
            updated_by = $2,
            updated_at = $3
        WHERE support_id = $4 AND is_valid = TRUE
        "#,
        section
    );

    sqlx::query(&sql)
        .bind(new_id)
        .bind(updated_by)
        .bind(updated_time)
        .bind(old_id)
        .execute(conn)
        .await
        .map_err(|e| format!("replace_support_reference failed for {}: {}", section, e))?;

    Ok(())
}

async fn replace_leaf_reference(
    section: &str,
    old_id: Uuid,
    new_id: Uuid,
    updated_by: Uuid,
    updated_time: DateTime<Utc>,
    sql_gen: &dyn SqlGen,
    conn: &mut PgConnection,
) -> Result<(), String> {
    let Some(sql) = sql_gen.replace_leaf_entry(section) else {
        return Ok(());
    };

    sqlx::query(&sql)
        .bind(updated_by)
        .bind(updated_time)
        .bind(old_id)
        .bind(new_id)
        .execute(conn)
        .await
        .map_err(|e| format!("replace_leaf_reference failed for {}: {}", section, e))?;

    Ok(())
}

fn validate_section(section: &str) -> Result<(), String> {
    if ALLOWED_SECTIONS.contains(section) {
        Ok(())
    } else {
        Err(format!("Illegal section name: '{}'", section))
    }
}

pub fn validate_field(field: &str) -> Result<(), String> {
    if ALLOWED_FIELDS.contains(field) {
        Ok(())
    } else {
        Err(format!("Illegal field name: '{}'", field))
    }
}

async fn is_database_empty(conn: &mut PgConnection) -> Result<bool, String> {
    let (is_empty,): (bool,) = sqlx::query_as(
        r#"
        SELECT NOT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_tables
            WHERE schemaname = 'public'
        )
        "#,
    )
    .fetch_one(conn)
    .await
    .map_err(|e| format!("Failed to check if database is empty: {}", e))?;

    Ok(is_empty)
}
