use crate::sql::ast::{
    BinaryOp, ColumnRef, Expr, FromClause, Literal, SelectColumn, SelectStmt,
};

use super::rules::extract_indexable_predicates;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMetadata {
    pub table: String,
    pub columns: Vec<String>,
}

/// Physical scan choice for a single table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanPlan {
    SeqScan,
    IndexScan {
        index_columns: Vec<String>,
        predicates: Vec<(String, BinaryOp, Literal)>,
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
    indexed_columns: Vec<IndexMetadata>,
}

impl Planner {
    pub fn new(indexed_columns: Vec<IndexMetadata>) -> Self {
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
                    .iter()
                    .any(|idx| idx.table == *right_table && idx.columns.first() == Some(&right_column.column));
                let left_indexed = self
                    .indexed_columns
                    .iter()
                    .any(|idx| idx.table == *left_table && idx.columns.first() == Some(&left_column.column));

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
        let predicates = filter
            .map(|expr| extract_indexable_predicates(expr))
            .unwrap_or_default();

        let table_indexes: Vec<&IndexMetadata> = self
            .indexed_columns
            .iter()
            .filter(|idx| idx.table == table)
            .collect();

        let table_preds: Vec<(String, BinaryOp, Literal)> = predicates
            .into_iter()
            .filter(|(col, _, _)| col.table.as_deref().map_or(true, |t| t == table))
            .map(|(col, op, lit)| (col.column, op, lit))
            .collect();

        let mut best: Option<(&IndexMetadata, Vec<(String, BinaryOp, Literal)>)> = None;

        for idx in table_indexes {
            let mut used = Vec::new();
            for col_name in &idx.columns {
                match table_preds.iter().find(|(c, _, _)| c == col_name) {
                    Some(pred) => used.push(pred.clone()),
                    None => break,
                }
            }

            if used.is_empty() {
                continue;
            }

            if best.as_ref().map_or(true, |(_, b_used)| used.len() > b_used.len()) {
                best = Some((idx, used));
            }
        }

        if let Some((idx, used)) = best {
            ScanPlan::IndexScan {
                index_columns: idx.columns.clone(),
                predicates: used,
            }
        } else {
            ScanPlan::SeqScan
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::ast::{FromClause, SelectStmt};

    #[test]
    fn plans_index_scan_when_available() {
        let planner = Planner::new(vec![IndexMetadata {
            table: "users".to_string(),
            columns: vec!["id".to_string()],
        }]);

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
                    ScanPlan::IndexScan { index_columns, .. } => {
                        assert_eq!(index_columns, vec!["id".to_string()])
                    }
                    _ => panic!("Expected index scan"),
                }
            }
            _ => panic!("Expected single-table plan"),
        }
    }

    #[test]
    fn plans_seq_scan_when_no_index() {
        let planner = Planner::new(Vec::new());
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
        let planner = Planner::new(vec![IndexMetadata {
            table: "orders".to_string(),
            columns: vec!["user_id".to_string()],
        }]);

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
        let planner = Planner::new(vec![IndexMetadata {
            table: "users".to_string(),
            columns: vec!["id".to_string()],
        }]);

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

    #[test]
    fn chooses_longer_composite_index_prefix() {
        let planner = Planner::new(vec![IndexMetadata {
            table: "items".to_string(),
            columns: vec!["a".to_string(), "b".to_string()],
        }]);

        let stmt = SelectStmt {
            columns: SelectColumn::All,
            from: FromClause::Table("items".to_string()),
            where_clause: Some(Expr::binary_op(
                Expr::binary_op(
                    Expr::Column(ColumnRef::new(None, "a")),
                    BinaryOp::Eq,
                    Expr::Literal(Literal::Integer(1)),
                ),
                BinaryOp::And,
                Expr::binary_op(
                    Expr::Column(ColumnRef::new(None, "b")),
                    BinaryOp::Lt,
                    Expr::Literal(Literal::Integer(5)),
                ),
            )),
        };

        let plan = planner.plan_select(&stmt);
        match plan.from {
            FromClausePlan::Single { scan, .. } => match scan {
                ScanPlan::IndexScan { predicates, .. } => {
                    assert_eq!(predicates.len(), 2);
                }
                _ => panic!("Expected index scan"),
            },
            _ => panic!("Expected single-table plan"),
        }
    }
}
