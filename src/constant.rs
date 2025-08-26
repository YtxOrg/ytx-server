use once_cell::sync::Lazy;
use std::collections::HashSet;

pub const FINANCE: &str = "finance";
pub const STAKEHOLDER: &str = "stakeholder";
pub const ITEM: &str = "item";
pub const TASK: &str = "task";
pub const SALE: &str = "sale";
pub const PURCHASE: &str = "purchase";

pub const SECTIONS: &[&str] = &[FINANCE, STAKEHOLDER, ITEM, TASK, SALE, PURCHASE];

pub static ALLOWED_SECTIONS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| SECTIONS.iter().copied().collect());

pub const MSG_TYPE: &str = "msg_type";
pub const VALUE: &str = "value";

pub const _BRANCH_NODE: i32 = 0;
pub const LEAF_NODE: i32 = 1;
pub const SUPPORT_NODE: i32 = 2;

pub const USER_ID: &str = "user_id";
pub const CREATED_BY: &str = "created_by";
pub const CREATED_TIME: &str = "created_time";
pub const SESSION_ID: &str = "session_id";
pub const UPDATED_BY: &str = "updated_by";
pub const UPDATED_TIME: &str = "updated_time";
pub const NODE: &str = "node";
pub const PATH: &str = "path";
pub const SECTION: &str = "section";

pub const ANCESTOR: &str = "ancestor";
pub const DESCENDANT: &str = "descendant";

pub const DEBIT: &str = "debit";
pub const CREDIT: &str = "credit";
pub const RATE: &str = "rate";

pub const STATUS: &str = "status";

pub const NODE_ID: &str = "node_id";
pub const ENTRY_ID: &str = "entry_id";
pub const SUPPORT_ID: &str = "support_id";

pub const YTX_SECRET_PATH: &str = "secret/data/postgres/ytx";
pub const AUTH_READWRITE_ROLE: &str = "ytx_auth_readwrite";
pub const MAIN_ITEM_READONLY_ROLE: &str = "ytx_main_item_readonly";
