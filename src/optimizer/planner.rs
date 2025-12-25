use crate::sql::ast::{
    BinaryOp, ColumnRef, Expr, FromClause, IndexType, Literal, SelectColumn, SelectStmt,
};

use super::rules::extract_indexable_predicates;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMetadata {
    pub table: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
}

/// Physical scan choice for a single table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanPlan {
    SeqScan,
    IndexScan {
        index_columns: Vec<String>,
        index_type: IndexType,
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
    pub strategy: JoinStrategy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinStrategy {
    NestedLoop { inner_has_index: bool },
    MergeJoin,
}

/// FROM clause plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromClausePlan {
    Single { table: String, scan: ScanPlan },
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
                let right_indexed = self.indexed_columns.iter().any(|idx| {
                    idx.table == *right_table && idx.columns.first() == Some(&right_column.column)
                });
                let left_indexed = self.indexed_columns.iter().any(|idx| {
                    idx.table == *left_table && idx.columns.first() == Some(&left_column.column)
                });

                let (outer_table, inner_table, outer_col, inner_col) = if right_indexed {
                    (
                        left_table.clone(),
                        right_table.clone(),
                        left_column.clone(),
                        right_column.clone(),
                    )
                } else if left_indexed {
                    (
                        right_table.clone(),
                        left_table.clone(),
                        right_column.clone(),
                        left_column.clone(),
                    )
                } else {
                    (
                        left_table.clone(),
                        right_table.clone(),
                        left_column.clone(),
                        right_column.clone(),
                    )
                };

                let strategy = if right_indexed || left_indexed {
                    JoinStrategy::NestedLoop {
                        inner_has_index: true,
                    }
                } else {
                    JoinStrategy::MergeJoin
                };

                FromClausePlan::Join(JoinPlan {
                    outer_table,
                    inner_table,
                    outer_column: outer_col,
                    inner_column: inner_col,
                    strategy,
                })
            }
        };

        Plan {
            from,
            columns,
            filter,
        }
    }

    /// Choose a scan strategy for a single table based on available indexes and predicates.
    pub fn plan_scan(&self, table: &str, filter: Option<&Expr>) -> ScanPlan {
        let predicates = filter.map(extract_indexable_predicates).unwrap_or_default();

        let table_indexes: Vec<&IndexMetadata> = self
            .indexed_columns
            .iter()
            .filter(|idx| idx.table == table)
            .collect();

        let table_preds: Vec<(String, BinaryOp, Literal)> = predicates
            .into_iter()
            .filter(|(col, _, _)| col.table.as_deref().is_none_or(|t| t == table))
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

            if idx.index_type == IndexType::Hash {
                if used.len() != idx.columns.len() {
                    continue;
                }
                if !used.iter().all(|(_, op, _)| *op == BinaryOp::Eq) {
                    continue;
                }
            }

            if best
                .as_ref()
                .is_none_or(|(_, b_used)| used.len() > b_used.len())
            {
                best = Some((idx, used));
            }
        }

        if let Some((idx, used)) = best {
            ScanPlan::IndexScan {
                index_columns: idx.columns.clone(),
                index_type: idx.index_type,
                predicates: used,
            }
        } else {
            ScanPlan::SeqScan
        }
    }
}
