use std::collections::HashSet;

use crate::sql::ast::{
    BinaryOp, ColumnRef, Expr, FromClause, Literal, SelectColumn, SelectStmt,
};

use super::rules::extract_indexable_predicate;

/// Physical scan choice for a single table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanPlan {
    SeqScan,
    IndexScan {
        column: String,
        op: BinaryOp,
        value: Literal,
    },
}

/// Physical join strategy (nested loop with optional indexed inner).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinPlan {
    pub outer_table: String,
    pub inner_table: String,
    pub outer_column: ColumnRef,
    pub inner_column: ColumnRef,
    pub inner_has_index: bool,
}

/// FROM clause plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromClausePlan {
    Single {
        table: String,
        scan: ScanPlan,
    },
    Join(JoinPlan),
}

/// Top-level select plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub from: FromClausePlan,
    pub columns: SelectColumn,
    pub filter: Option<Expr>,
}

/// Very small cost-based planner that selects between a seq scan and an index
/// (when available) and prefers to put an indexed table on the inner side of a join.
pub struct Planner {
    indexed_columns: HashSet<(String, String)>,
}

impl Planner {
    pub fn new(indexed_columns: HashSet<(String, String)>) -> Self {
        Self { indexed_columns }
    }

    pub fn plan_select(&self, stmt: &SelectStmt) -> Plan {
        let filter = stmt.where_clause.clone();
        let columns = stmt.columns.clone();

        let from = match &stmt.from {
            FromClause::Table(table) => {
                let scan = self.plan_scan(table, stmt.where_clause.as_ref());
                FromClausePlan::Single {
                    table: table.clone(),
                    scan,
                }
            }
            FromClause::Join {
                left_table,
                right_table,
                left_column,
                right_column,
            } => {
                let right_indexed = self
                    .indexed_columns
                    .contains(&(right_table.clone(), right_column.column.clone()));
                let left_indexed = self
                    .indexed_columns
                    .contains(&(left_table.clone(), left_column.column.clone()));

                let (outer_table, inner_table, outer_col, inner_col, inner_has_index) =
                    if right_indexed {
                        (
                            left_table.clone(),
                            right_table.clone(),
                            left_column.clone(),
                            right_column.clone(),
                            true,
                        )
                    } else if left_indexed {
                        (
                            right_table.clone(),
                            left_table.clone(),
                            right_column.clone(),
                            left_column.clone(),
                            true,
                        )
                    } else {
                        (
                            left_table.clone(),
                            right_table.clone(),
                            left_column.clone(),
                            right_column.clone(),
                            false,
                        )
                    };

                FromClausePlan::Join(JoinPlan {
                    outer_table,
                    inner_table,
                    outer_column: outer_col,
                    inner_column: inner_col,
                    inner_has_index,
                })
            }
        };

        Plan {
            from,
            columns,
            filter,
        }
    }

    fn plan_scan(&self, table: &str, filter: Option<&Expr>) -> ScanPlan {
        if let Some(expr) = filter {
            if let Some((col, op, lit)) = extract_indexable_predicate(expr) {
                if col.table.as_deref().map_or(true, |t| t == table)
                    && self
                        .indexed_columns
                        .contains(&(table.to_string(), col.column.clone()))
                {
                    return ScanPlan::IndexScan {
                        column: col.column,
                        op,
                        value: lit,
                    };
                }
            }
        }

        ScanPlan::SeqScan
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{FromClause, SelectStmt};

    #[test]
    fn plans_index_scan_when_available() {
        let mut idx = HashSet::new();
        idx.insert(("users".to_string(), "id".to_string()));
        let planner = Planner::new(idx);

        let stmt = SelectStmt {
            columns: SelectColumn::All,
            from: FromClause::Table("users".to_string()),
            where_clause: Some(Expr::binary_op(
                Expr::Column(ColumnRef::new(None, "id")),
                BinaryOp::Eq,
                Expr::Literal(Literal::Integer(1)),
            )),
        };

        let plan = planner.plan_select(&stmt);
        match plan.from {
            FromClausePlan::Single { table, scan } => {
                assert_eq!(table, "users");
                match scan {
                    ScanPlan::IndexScan { column, .. } => assert_eq!(column, "id"),
                    _ => panic!("Expected index scan"),
                }
            }
            _ => panic!("Expected single-table plan"),
        }
    }

    #[test]
    fn plans_seq_scan_when_no_index() {
        let planner = Planner::new(HashSet::new());
        let stmt = SelectStmt {
            columns: SelectColumn::All,
            from: FromClause::Table("users".to_string()),
            where_clause: Some(Expr::binary_op(
                Expr::Column(ColumnRef::new(None, "id")),
                BinaryOp::Eq,
                Expr::Literal(Literal::Integer(1)),
            )),
        };

        let plan = planner.plan_select(&stmt);
        match plan.from {
            FromClausePlan::Single { scan, .. } => match scan {
                ScanPlan::SeqScan => {}
                _ => panic!("Expected seq scan"),
            },
            _ => panic!("Expected single-table plan"),
        }
    }

    #[test]
    fn prefers_indexed_inner_on_join() {
        let mut idx = HashSet::new();
        idx.insert(("orders".to_string(), "user_id".to_string()));
        let planner = Planner::new(idx);

        let stmt = SelectStmt {
            columns: SelectColumn::All,
            from: FromClause::Join {
                left_table: "users".to_string(),
                right_table: "orders".to_string(),
                left_column: ColumnRef::new(Some("users".to_string()), "id"),
                right_column: ColumnRef::new(Some("orders".to_string()), "user_id"),
            },
            where_clause: None,
        };

        let plan = planner.plan_select(&stmt);
        match plan.from {
            FromClausePlan::Join(join_plan) => {
                assert_eq!(join_plan.outer_table, "users");
                assert_eq!(join_plan.inner_table, "orders");
                assert!(join_plan.inner_has_index);
            }
            _ => panic!("Expected join plan"),
        }
    }

    #[test]
    fn swaps_join_order_when_left_indexed() {
        let mut idx = HashSet::new();
        idx.insert(("users".to_string(), "id".to_string()));
        let planner = Planner::new(idx);

        let stmt = SelectStmt {
            columns: SelectColumn::All,
            from: FromClause::Join {
                left_table: "users".to_string(),
                right_table: "orders".to_string(),
                left_column: ColumnRef::new(Some("users".to_string()), "id"),
                right_column: ColumnRef::new(Some("orders".to_string()), "user_id"),
            },
            where_clause: None,
        };

        let plan = planner.plan_select(&stmt);
        match plan.from {
            FromClausePlan::Join(join_plan) => {
                assert_eq!(join_plan.outer_table, "orders");
                assert_eq!(join_plan.inner_table, "users");
                assert!(join_plan.inner_has_index);
            }
            _ => panic!("Expected join plan"),
        }
    }
}
