use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};

pub static FIELD_TYPE_MAP: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut m = HashMap::new();

    m.insert("id", "UUID");
    m.insert("user_id", "UUID");
    m.insert("created_by", "UUID");
    m.insert("updated_by", "UUID");
    m.insert("lhs_node", "UUID");
    m.insert("rhs_node", "UUID");
    m.insert("support_node", "UUID");
    m.insert("employee", "UUID");
    m.insert("party", "UUID");
    m.insert("settlement_node", "UUID");
    m.insert("ancestor", "UUID");
    m.insert("descendant", "UUID");
    m.insert("external_item", "UUID");

    m.insert("name", "TEXT");
    m.insert("code", "TEXT");
    m.insert("description", "TEXT");
    m.insert("note", "TEXT");
    m.insert("document", "TEXT");
    m.insert("username", "TEXT");
    m.insert("key", "TEXT");
    m.insert("symbol", "TEXT");

    m.insert("direction_rule", "BOOL");
    m.insert("is_valid", "BOOL");
    m.insert("is_checked", "BOOL");
    m.insert("is_finished", "BOOL");
    m.insert("value", "BOOL");

    m.insert("kind", "INT4");
    m.insert("unit", "INT4");
    m.insert("payment_term", "INT4");
    m.insert("distance", "INT4");

    m.insert("version", "INT8");

    m.insert("initial_total", "NUMERIC");
    m.insert("final_total", "NUMERIC");
    m.insert("unit_price", "NUMERIC");
    m.insert("commission", "NUMERIC");
    m.insert("unit_cost", "NUMERIC");
    m.insert("first", "NUMERIC");
    m.insert("second", "NUMERIC");
    m.insert("discount_total", "NUMERIC");
    m.insert("discount", "NUMERIC");
    m.insert("discount_price", "NUMERIC");
    m.insert("lhs_rate", "NUMERIC");
    m.insert("lhs_debit", "NUMERIC");
    m.insert("lhs_credit", "NUMERIC");
    m.insert("rhs_rate", "NUMERIC");
    m.insert("rhs_debit", "NUMERIC");
    m.insert("rhs_credit", "NUMERIC");

    m.insert("created_time", "TIMESTAMPTZ");
    m.insert("updated_time", "TIMESTAMPTZ");
    m.insert("issued_time", "TIMESTAMPTZ");

    m
});

pub static ALLOWED_FIELDS: Lazy<HashSet<&'static str>> =
    Lazy::new(|| FIELD_TYPE_MAP.keys().copied().collect());
