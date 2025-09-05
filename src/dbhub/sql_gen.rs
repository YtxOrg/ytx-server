//! # SqlGen Parameter Convention
//!
//! For all SQL templates that involve node mutation (removal, replacement, merge, etc.), the following
//! positional parameter bindings are used consistently across all implementations:
//!
//! - `$1`: `updated_by` – the user ID who performed the operation
//! - `$2`: `updated_time` – the UTC timestamp of the operation
//! - `$3`: `old_id` – the node ID to be removed or replaced
//! - `$4`: `new_id` – the node ID to be used as a replacement
//!
//! This convention applies to methods such as:
//! - `remove_node`
//! - `remove_leaf_entry`
//! - `replace_leaf_entry`
//! - `remove_support_reference`
//! - `merge_node_total`
//! - and others where applicable
//!
//! > Note: Some methods may not require all four parameters; for example, `remove_node` only uses `$1` to `$3`.

use crate::{constant::*, dbhub::section::*};
use chrono::{Datelike, Duration, TimeZone, Utc};

pub trait SqlGen: Send + Sync {
    fn fetch_tree_applied(&self, section: &str) -> String {
        format!("SELECT * FROM {}_node WHERE is_valid = TRUE", section)
    }

    fn fetch_tree_acked(&self, _section: &str) -> Option<String> {
        None
    }

    fn remove_node(&self, section: &str) -> String {
        format!(
            "UPDATE {}_node SET is_valid = FALSE, updated_by = $1, updated_time = $2 WHERE id = $3",
            section
        )
    }

    fn has_leaf_reference(&self, section: &str) -> String {
        format!(
            r#"
            SELECT (
                EXISTS(SELECT 1 FROM {0}_entry WHERE lhs_node = $1 AND is_valid = TRUE) OR
                EXISTS(SELECT 1 FROM {0}_entry WHERE rhs_node = $1 AND is_valid = TRUE)
            )
            "#,
            section
        )
    }

    fn fetch_leaf_entry(&self, section: &str) -> String {
        format!(
            r#"
            SELECT * FROM {section}_entry
            WHERE (lhs_node = $1 OR rhs_node = $1) AND is_valid = TRUE
            "#,
        )
    }

    fn remove_leaf_entry(&self, section: &str) -> String {
        format!(
            r#"
            UPDATE {}_entry
            SET
                is_valid = FALSE,
                updated_time = $2,
                updated_by = $1
            WHERE
                (lhs_node = $3 OR rhs_node = $3) AND is_valid = TRUE
            "#,
            section
        )
    }

    fn update_is_checked(&self, section: &str) -> String {
        format!(
            r#"
            UPDATE {}_entry
            SET is_checked = CASE
                WHEN $3 = 0 THEN FALSE      -- Check::Off
                WHEN $3 = 1 THEN TRUE       -- Check::On
                WHEN $3 = 2 THEN NOT is_checked -- Check::Flip
            END,
            updated_by   = $1,
            updated_time = $2
            WHERE (lhs_node = $4 OR rhs_node = $4) AND is_valid = TRUE
            "#,
            section
        )
    }

    fn update_direction_rule(&self, section: &str) -> String {
        format!(
            r#"
            UPDATE {section}_node
            SET
                direction_rule = $4,
                initial_total = -initial_total,
                final_total = -final_total,
                updated_by = $1,
                updated_time = $2
            WHERE id = $3 AND is_valid = TRUE
            "#,
        )
    }

    /// Provides the SQL statement for replacing references to a leaf node (i.e., replacing old_id with new_id)
    ///
    /// This method is only implemented for sections like `finance`, `item`, `stakeholder`, and `task`,
    /// where entries allow replacing leaf node references.
    ///
    /// Sections like `sale` and `purchase` do **not** implement this method because
    /// their order tables are structurally not designed to support node replacement.
    ///
    /// Parameters:
    /// - $1: new_node_id
    /// - $4: old_node_id
    fn replace_leaf_entry(&self, section: &str) -> Option<String> {
        Some(format!(
            r#"
            UPDATE {section}_entry
            SET
                lhs_node = CASE WHEN lhs_node = $3 THEN $4 ELSE lhs_node END,
                rhs_node = CASE WHEN rhs_node = $3 THEN $4 ELSE rhs_node END,
                updated_by = $1,
                updated_time = $2
            WHERE (lhs_node = $3 OR rhs_node = $3) AND is_valid = TRUE
            "#
        ))
    }

    /// Returns the SQL used to collect reference information before removing a leaf node.
    ///
    /// This query is used to gather affected node IDs and related entry data
    /// (such as debit, credit, rate, and support_node) before deleting a node.
    ///
    /// Only applicable to `finance`, `item`, and `task` sections, where the entry
    /// table supports **double-entry** structure with both `lhs_node`
    /// and `rhs_node` columns referencing nodes.
    ///
    /// Other sections like `sale`, `purchase`, and `stakeholder` do not require or support
    /// this type of reference collection due to their simpler structure.
    ///
    /// Parameters:
    /// - `$1`: The leaf node ID to be removed.
    fn fetch_leaf_entry_refs(&self, _section: &str) -> Option<String> {
        None
    }

    /// Returns the SQL used to check whether a node is referenced as a support node
    /// (`support_node`) in any valid entry.
    ///
    /// Only `stakeholder` nodes can be referenced as support nodes by `task` entries.
    /// Other sections (e.g., `sale`, `purchase`, `item`) never appear as support nodes
    /// and therefore do not need this check.
    fn has_support_reference(&self, _section: &str) -> Option<String> {
        None
    }

    /// Returns the SQL used to fetch entries where the given node is referenced
    /// as a support node.
    ///
    /// This only applies to `stakeholder` nodes being referenced by `task` entries.
    fn fetch_support_entry(&self, _section: &str) -> Option<String> {
        None
    }

    /// Returns SQL to clear `support_node` references from entries involving a given node.
    ///
    /// This only applies to the `task` section, whose entry table includes a `support_node`
    /// column that references `stakeholder` nodes. Other sections do not need to implement
    /// this behavior.
    ///
    /// Parameters:
    ///   $1 - updated_time (e.g., NOW())
    ///   $2 - updated_by (user_id)
    ///   $3 - support_node (the stakeholder node to disassociate)
    fn remove_support_reference(&self, _section: &str) -> Option<String> {
        None
    }

    /// Returns a SQL query to check whether the given node is externally referenced
    /// by other tables (e.g., sale, purchase). This is used to prevent
    /// deletion or replacement of nodes that are in active use.
    ///
    /// Only `item` and `stakeholder` sections require this check. Other sections
    /// (e.g., finance, task) either don’t have external dependencies or manage them internally.
    ///
    /// The returned query must be a `SELECT EXISTS (...)` statement where `$1` is the node id.
    fn has_external_reference(&self) -> Option<String> {
        None
    }

    /// Returns a SQL query to check whether replacing a node would result in a conflict.
    /// A conflict occurs when an existing entry already references the new node,
    /// and we attempt to replace the old node with it in the same entry record.
    /// This would cause both sides of the entry to point to the same node,
    /// which is logically invalid in double-entry systems.
    ///
    /// This check is only needed for sections with double-entry entries:
    /// namely `finance`, `item`, and `task`.
    ///
    /// The query should be a `SELECT EXISTS (...)` form using `$1` and `$2`
    /// for the old node ID and the new node ID respectively.
    fn has_replace_conflict(&self, section: &str) -> Option<String> {
        match section {
            FINANCE | ITEM | TASK => Some(format!(
                r#"
            SELECT EXISTS (
                SELECT 1 FROM {section}_entry
                WHERE is_valid = TRUE AND (
                    (lhs_node = $1 AND rhs_node = $2) OR
                    (rhs_node = $1 AND lhs_node = $2)
                )
            )
            "#
            )),
            _ => None,
        }
    }

    /// Returns a SQL statement that merges the totals (initial_total and final_total)
    /// from an old node into a new node within the same section.
    ///
    /// The merging respects the `direction_rule` of each node:
    /// - If the direction rules match, the values are added.
    /// - If they differ, the old values are subtracted.
    ///
    /// This SQL is typically used when replacing or merging two nodes,
    /// and should only be called for sections that support such operations,
    /// such as `finance`, `item`, or `task`.
    fn merge_node_total(&self, section: &str) -> Option<String> {
        Some(format!(
            r#"
            UPDATE {section}_node AS new
            SET
                initial_total = new.initial_total +
                    CASE
                        WHEN new.direction_rule = old.direction_rule THEN old.initial_total ELSE -old.initial_total
                    END,
                final_total = new.final_total +
                    CASE
                        WHEN new.direction_rule = old.direction_rule THEN old.final_total ELSE -old.final_total
                    END,
                updated_by = $1,
                updated_time = $2
            FROM {section}_node AS old
            WHERE old.id = $3 AND new.id = $4
            "#
        ))
    }
}

/// Notes on `stakeholder_entry` schema:
///
/// - `rhs_node` is used to reference `item` nodes,
///   aligned with `sale_entry` and `purchase_entry` schemas.
///
/// - `support_node` references the same stakeholder's own leaf node,
///   not a regular `SupportNode` as in modules like `finance`, `item`, or `task`.
impl SqlGen for Stakeholder {
    fn has_leaf_reference(&self, _section: &str) -> String {
        "SELECT EXISTS(SELECT 1 FROM stakeholder_entry WHERE lhs_node = $1 AND is_valid = TRUE)"
            .to_string()
    }

    fn fetch_leaf_entry(&self, _section: &str) -> String {
        r#"
            SELECT * FROM stakeholder_entry
            WHERE
                lhs_node = $1 AND is_valid = TRUE
            "#
        .to_string()
    }

    fn remove_leaf_entry(&self, _section: &str) -> String {
        r#"
            UPDATE stakeholder_entry
            SET
                is_valid = FALSE,
                updated_by = $1,
                updated_time = $2
            WHERE lhs_node = $3 AND is_valid = TRUE
            "#
        .to_string()
    }

    fn replace_leaf_entry(&self, _section: &str) -> Option<String> {
        Some(
            r#"
            UPDATE stakeholder_entry
            SET
                lhs_node     = $4,
                updated_by   = $1,
                updated_time = $2
            WHERE lhs_node = $3 AND is_valid = TRUE
            "#
            .to_string(),
        )
    }

    fn update_is_checked(&self, section: &str) -> String {
        format!(
            r#"
            UPDATE {}_entry
            SET is_checked = CASE
                WHEN $3 = 0 THEN FALSE      -- Check::Off
                WHEN $3 = 1 THEN TRUE       -- Check::On
                WHEN $3 = 2 THEN NOT is_checked -- Check::Flip
            END,
            updated_by   = $1,
            updated_time = $2
            WHERE lhs_node = $4 AND is_valid = TRUE
            "#,
            section
        )
    }

    fn has_external_reference(&self) -> Option<String> {
        Some(
            r#"
            SELECT (
                EXISTS(SELECT 1 FROM sale_node     WHERE (party = $1 OR employee = $1) AND is_valid = TRUE) OR
                EXISTS(SELECT 1 FROM purchase_node WHERE (party = $1 OR employee = $1) AND is_valid = TRUE)
            )
            "#
            .to_string(),
        )
    }

    fn has_support_reference(&self, _section: &str) -> Option<String> {
        Some(format!(
            "SELECT EXISTS(SELECT 1 FROM task_entry WHERE support_node = $1 AND is_valid = TRUE)"
        ))
    }

    fn fetch_support_entry(&self, _section: &str) -> Option<String> {
        Some(format!(
            r#"
            SELECT * FROM task_entry
            WHERE support_node = $1 AND is_valid = TRUE
            "#,
        ))
    }

    fn remove_support_reference(&self, _section: &str) -> Option<String> {
        Some(format!(
            r#"
            UPDATE task_entry
            SET
                support_node = NULL,
                updated_time = $2,
                updated_by = $1
            WHERE
                support_node = $3
                AND is_valid = TRUE
            "#
        ))
    }

    fn merge_node_total(&self, _section: &str) -> Option<String> {
        None
    }
}

impl SqlGen for Sale {
    /// Reads all relevant nodes from the database table.
    ///
    /// This SQL query retrieves records from the `{section}_node` table. It includes:
    /// - Branch nodes (`kind = 0`)
    /// - Unfinished nodes (`is_finished = FALSE`)
    /// - Pending nodes (`unit = 2`)
    /// - Nodes issued within today's UTC date (`issued_time >= UTC 00:00`)
    /// - Only valid nodes (`is_valid = TRUE`)
    ///
    /// The table name is dynamically generated from the `section` parameter.
    ///
    /// Returns a SQL query string used for node selection.
    fn fetch_tree_applied(&self, section: &str) -> String {
        let now = Utc::now();
        let today_start = now
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("Invalid today start time")
            .and_utc();
        let tomorrow_start = (now + Duration::days(1))
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("Invalid tomorrow start time")
            .and_utc();

        format!(
            r#"
            SELECT * FROM {section}_node
            WHERE (
                (issued_time >= TIMESTAMPTZ '{today_start}' AND issued_time < TIMESTAMPTZ '{tomorrow_start}')
                OR kind = 0
                OR is_finished = FALSE
                OR unit = 2
            )
            AND is_valid = TRUE
            "#,
        )
    }

    fn has_leaf_reference(&self, _section: &str) -> String {
        "SELECT EXISTS(SELECT 1 FROM sale_entry WHERE lhs_node = $1 AND is_valid = TRUE)"
            .to_string()
    }

    fn fetch_leaf_entry(&self, _section: &str) -> String {
        format!(
            r#"
            SELECT * FROM sale_entry
            WHERE lhs_node = $1 AND is_valid = TRUE
            "#,
        )
    }

    fn remove_leaf_entry(&self, _section: &str) -> String {
        format!(
            r#"
            UPDATE sale_entry
            SET
                is_valid = FALSE,
                updated_time = $2,
                updated_by = $1
            WHERE
                lhs_node = $3 AND is_valid = TRUE
            "#
        )
    }

    fn has_external_reference(&self) -> Option<String> {
        Some(format!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM sale_node
                WHERE id = $1 AND settlement_id != '00000000-0000-0000-0000-000000000000' AND is_valid = TRUE
            )
            "#
        ))
    }

    fn fetch_tree_acked(&self, section: &str) -> Option<String> {
        Some(format!(
            r#"
            SELECT * FROM {section}_node
            WHERE (
                (issued_time >= $1 AND issued_time < $2
                OR kind = 0
                OR is_finished = FALSE
                OR unit = 2
            )
            AND is_valid = TRUE
            "#,
        ))
    }

    fn update_direction_rule(&self, section: &str) -> String {
        format!(
            r#"
            UPDATE {section}_node
            SET
                direction_rule = $4,
                initial_total = -initial_total,
                final_total = -final_total,
                first_total = - first_total,
                second_total = - second_total,
                discount_total = - discount_total,
                updated_by = $1,
                updated_time = $2
            WHERE id = $3 AND is_valid = TRUE
            "#,
        )
    }

    fn replace_leaf_entry(&self, _section: &str) -> Option<String> {
        None
    }

    fn merge_node_total(&self, _section: &str) -> Option<String> {
        None
    }
}

impl SqlGen for Purchase {
    /// Reads all relevant nodes from the database table.
    ///
    /// This SQL query retrieves records from the `{section}_node` table. It includes:
    /// - Branch nodes (`kind = 0`)
    /// - Unfinished nodes (`is_finished = FALSE`)
    /// - Pending nodes (`unit = 2`)
    /// - Nodes issued within today's UTC date (`issued_time >= UTC 00:00`)
    /// - Only valid nodes (`is_valid = TRUE`)
    ///
    /// The table name is dynamically generated from the `section` parameter.
    ///
    /// Returns a SQL query string used for node selection.
    fn fetch_tree_applied(&self, section: &str) -> String {
        let now = Utc::now();
        let today_start = now
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("Invalid today start time")
            .and_utc();
        let tomorrow_start = (now + Duration::days(1))
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("Invalid tomorrow start time")
            .and_utc();

        format!(
            r#"
            SELECT * FROM {section}_node
            WHERE (
                (issued_time >= TIMESTAMPTZ '{today_start}' AND issued_time < TIMESTAMPTZ '{tomorrow_start}')
                OR kind = 0
                OR is_finished = FALSE
                OR unit = 2
            )
            AND is_valid = TRUE
            "#,
        )
    }

    fn has_leaf_reference(&self, _section: &str) -> String {
        "SELECT EXISTS(SELECT 1 FROM purchase_entry WHERE lhs_node = $1 AND is_valid = TRUE)"
            .to_string()
    }

    fn fetch_leaf_entry(&self, _section: &str) -> String {
        format!(
            r#"
            SELECT * FROM purchase_entry
            WHERE lhs_node = $1 AND is_valid = TRUE
            "#,
        )
    }

    fn remove_leaf_entry(&self, _section: &str) -> String {
        format!(
            r#"
            UPDATE purchase_entry
            SET
                is_valid = FALSE,
                updated_time = $2,
                updated_by = $1
            WHERE
                lhs_node = $3 AND is_valid = TRUE
            "#
        )
    }

    fn has_external_reference(&self) -> Option<String> {
        Some(format!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM purchase_node
                WHERE id = $1 AND settlement_id != '00000000-0000-0000-0000-000000000000' AND is_valid = TRUE
            )
            "#
        ))
    }

    fn fetch_tree_acked(&self, section: &str) -> Option<String> {
        Some(format!(
            r#"
            SELECT * FROM {section}_node
            WHERE (
                (issued_time >= $1 AND issued_time < $2
                OR kind = 0
                OR is_finished = FALSE
                OR unit = 2
            )
            AND is_valid = TRUE
            "#,
        ))
    }

    fn update_direction_rule(&self, section: &str) -> String {
        format!(
            r#"
            UPDATE {section}_node
            SET
                direction_rule = $4,
                initial_total = -initial_total,
                final_total = -final_total,
                first_total = - first_total,
                second_total = - second_total,
                discount_total = - discount_total,
                updated_by = $1,
                updated_time = $2
            WHERE id = $3 AND is_valid = TRUE
            "#,
        )
    }

    fn replace_leaf_entry(&self, _section: &str) -> Option<String> {
        None
    }

    fn merge_node_total(&self, _section: &str) -> Option<String> {
        None
    }
}

impl SqlGen for Item {
    fn has_external_reference(&self) -> Option<String> {
        Some(
            r#"
            SELECT (
                EXISTS(SELECT 1 FROM stakeholder_entry WHERE (rhs_node = $1 OR external_item = $1) AND is_valid = TRUE) OR
                EXISTS(SELECT 1 FROM sale_entry        WHERE (rhs_node = $1 OR external_item = $1) AND is_valid = TRUE) OR
                EXISTS(SELECT 1 FROM purchase_entry    WHERE (rhs_node = $1 OR external_item = $1) AND is_valid = TRUE)
            )
            "#
            .to_string(),
        )
    }

    fn fetch_leaf_entry_refs(&self, section: &str) -> Option<String> {
        Some(format!(
            r#"
        SELECT rhs_node AS node_id, id AS entry_id, rhs_debit as debit, rhs_credit as credit, unit_cost AS rate, NULL AS support_id
        FROM {0}_entry
        WHERE lhs_node = $1 AND is_valid = TRUE

        UNION ALL

        SELECT lhs_node AS node_id, id AS entry_id, lhs_debit as debit, lhs_credit as credit, unit_cost AS rate, NULL AS support_id
        FROM {0}_entry
        WHERE rhs_node = $1 AND is_valid = TRUE
        "#,
            section
        ))
    }
}

impl SqlGen for Finance {
    fn fetch_leaf_entry_refs(&self, section: &str) -> Option<String> {
        Some(format!(
            r#"
            SELECT rhs_node AS node_id, id AS entry_id, rhs_debit as debit, rhs_credit as credit, lhs_rate AS rate, support_node AS support_id
            FROM {0}_entry
            WHERE lhs_node = $1 AND is_valid = TRUE

            UNION ALL

            SELECT lhs_node AS node_id, id AS entry_id, lhs_debit as debit, lhs_credit as credit, rhs_rate AS rate, support_node AS support_id
            FROM {0}_entry
            WHERE rhs_node = $1 AND is_valid = TRUE
            "#,
            section
        ))
    }
}

impl SqlGen for Task {
    fn fetch_tree_applied(&self, section: &str) -> String {
        let now = Utc::now();
        let start = Utc
            .with_ymd_and_hms(now.year() - 1, 1, 1, 0, 0, 0)
            .single()
            .expect("Invalid start date");
        let end = (now + Duration::days(1))
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .expect("Invalid end time")
            .and_utc();

        format!(
            r#"
            SELECT * FROM {section}_node
            WHERE is_valid = TRUE
            AND (
                    (issued_time >= TIMESTAMPTZ '{start}' AND issued_time < TIMESTAMPTZ '{end}')
                    OR kind = 0
                )
            "#
        )
    }

    fn fetch_tree_acked(&self, section: &str) -> Option<String> {
        Some(format!(
            r#"
            SELECT * FROM {section}_node
            WHERE issued_time >= $1 AND issued_time < $2 AND is_valid = TRUE AND kind != 0
            "#,
        ))
    }

    fn fetch_leaf_entry_refs(&self, section: &str) -> Option<String> {
        Some(format!(
            r#"
        SELECT rhs_node AS node_id, id AS entry_id, rhs_debit as debit, rhs_credit as credit, unit_cost AS rate, support_node AS support_id
        FROM {0}_entry
        WHERE lhs_node = $1 AND is_valid = TRUE

        UNION ALL

        SELECT lhs_node AS node_id, id AS entry_id, lhs_debit as debit, lhs_credit as credit, unit_cost AS rate, support_node AS support_id
        FROM {0}_entry
        WHERE rhs_node = $1 AND is_valid = TRUE
        "#,
            section
        ))
    }
}
