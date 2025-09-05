use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

// 简化派生属性的宏
macro_rules! message_struct {
    ($(#[$attr:meta])* pub struct $name:ident { $($field:tt)* }) => {
        $(#[$attr])*
        #[derive(Debug, Serialize, Deserialize)]
        pub struct $name { $($field)* }
    };
}

// 消息类型枚举
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MsgType {
    Register,
    RegisterResult,
    WorkspaceAccessPending,
    OneNode,
    Login,
    LoginFailed,
    LoginSuccess,
    NodeInsert,
    NodeDrag,
    EntryInsert,
    EntrySearch,
    NodeUpdate,
    EntryUpdate,
    EntryRhsNode,
    EntrySupportNode,
    LeafRemove,
    BranchRemove,
    DirectionRule,
    LeafReplace,
    EntryRemove,
    TreeApplied,
    SessionId,
    GlobalConfig,
    TreeAcked,
    LeafAcked,
    SupportAcked,
    CheckAction,
    DocumentDir,
    DefaultUnit,
    Name,
    EntryRate,
    EntryNumeric, // debit or credit
    UpdateDefaultUnitFailed,
    LeafReference,
    UnreferencedNodeRemove,
    Other,
}

message_struct! {
    pub struct Msg {
        pub msg_type: MsgType,
        pub value: Value,
    }
}

message_struct! {
    pub struct LeafAcked {
        pub section: String,
        pub node_id : Uuid,
        pub entry_array : Value,
    }
}

message_struct! {
    pub struct SupportAcked {
        pub section: String,
        pub node_id : Uuid,
        pub entry_array : Value,
    }
}

message_struct! {
    pub struct CheckAction {
        pub section: String,
        pub session_id : String,
        pub node_id : Uuid,
        pub check: i32,
        pub meta: HashMap<String, Value>,
    }
}

// 登录信息
message_struct! {
    pub struct Login {
        pub email: String,
        pub password: String,
        pub workspace: String,
    }
}

message_struct! {
    pub struct RegisterInfo {
        pub email: String,
        pub password: String,
    }
}

message_struct! {
    pub struct NodeInsert {
        pub section: String,
        pub session_id : String,
        pub node: HashMap<String, Value>,
        pub path: HashMap<String, Value>,
    }
}

message_struct! {
    pub struct EntryInsert {
        pub section: String,
        pub session_id : String,
        pub entry: HashMap<String, Value>,
        pub entry_id :Uuid,
        pub lhs_delta: Option<HashMap<String, Value>>,
        pub rhs_delta: Option<HashMap<String, Value>>,
    }
}

message_struct! {
    pub struct SearchEntry {
        pub section: String,
        pub keyword : String,
        pub entry_array: Value,
    }
}

message_struct! {
    pub struct EntryRhsNode {
        pub section: String,
        pub session_id : String,
        pub entry: HashMap<String, Value>,
        pub entry_id : Uuid,
        pub field : String,
        pub old_rhs_id : Uuid,
        pub new_rhs_id : Uuid,
        pub old_rhs_delta: Option<HashMap<String, Value>>,
        pub new_rhs_delta: Option<HashMap<String, Value>>,
    }
}

message_struct! {
    pub struct EntryValue {
        pub section: String,
        pub session_id : String,
        pub cache: HashMap<String, Value>,
        pub entry_id : Uuid,
        pub is_parallel: bool,
        pub lhs_delta: Option<HashMap<String, Value>>,
        pub rhs_delta: Option<HashMap<String, Value>>,
    }
}

message_struct! {
    pub struct EntrySupportNode {
        pub section: String,
        pub session_id : String,
        pub entry_id : Uuid,
        pub old_support_id : Uuid,
        pub new_support_id : Uuid,
        pub meta: HashMap<String, Value>,
    }
}

message_struct! {
    pub struct EntryRemove {
        pub section: String,
        pub session_id : String,
        pub entry_id :Uuid,
        pub lhs_delta: Option<HashMap<String, Value>>,
        pub rhs_delta: Option<HashMap<String, Value>>,
    }
}

message_struct! {
    pub struct TreeAcked {
        pub section: String,
        pub start: DateTime<Utc>,
        pub end: DateTime<Utc>,
    }
}

message_struct! {
    pub struct OneNode {
        pub section: String,
        pub node_id: Uuid,
        pub node: Value,
        pub ancestor: Uuid,
    }
}

message_struct! {
    pub struct LeafReference {
        pub section: String,
        pub id : Uuid,
        pub internal_reference: bool,
        pub external_reference: bool,
    }
}

message_struct! {
    pub struct Update {
        pub section: String,
        pub session_id : String,
        pub id: Uuid,
        pub cache: HashMap<String, Value>,
    }
}

message_struct! {
    pub struct DirectionRule {
        pub section: String,
        pub session_id : String,
        pub id: Uuid,
        pub direction_rule: bool,
        pub meta: HashMap<String, Value>,
    }
}

message_struct! {
    pub struct DocumentDir {
        pub section: String,
        pub session_id : String,
        pub document_dir: String,
    }
}

message_struct! {
    pub struct DefaultUnit {
        pub section: String,
        pub default_unit: i32,
    }
}

message_struct! {
    pub struct NodeDrag {
        pub section: String,
        pub session_id : String,
        pub path: HashMap<String, Value>,
        pub node: HashMap<String, Value>,
    }
}

message_struct! {
    pub struct LeafRemove {
        pub section: String,
        pub session_id : String,
        pub id: Uuid,
        pub leaf_entry: HashMap<Uuid, Vec<Uuid>>,
        pub support_entry: HashMap<Uuid, Vec<Uuid>>,
        pub node_delta: Vec<NodeDelta>,
    }
}

message_struct! {
   pub struct NodeDelta {
       pub id: Uuid,
       pub initial_delta: Decimal,
       pub final_delta: Decimal,
    }
}

message_struct! {
    pub struct BranchRemove {
        pub section: String,
        pub session_id : String,
        pub id: Uuid,
    }
}

message_struct! {
    pub struct LeafReplace {
        pub section: String,
        pub session_id : String,
        pub status: bool,
        pub external_reference: bool,
        pub old_id: Uuid,
        pub new_id: Uuid,
    }
}
