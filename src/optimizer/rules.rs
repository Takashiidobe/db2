use crate::sql::ast::{BinaryOp, ColumnRef, Expr, Literal};

/// Extract a simple column-literal predicate if present.
///
/// Returns (column, operator, literal) with operator adjusted for operand order.
pub fn extract_indexable_predicate(expr: &Expr) -> Option<(ColumnRef, BinaryOp, Literal)> {
    match expr {
        Expr::BinaryOp { left, op, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(col), Expr::Literal(lit)) => Some((col.clone(), *op, lit.clone())),
            (Expr::Literal(lit), Expr::Column(col)) => {
                let swapped_op = match op {
                    BinaryOp::Lt => BinaryOp::Gt,
                    BinaryOp::LtEq => BinaryOp::GtEq,
                    BinaryOp::Gt => BinaryOp::Lt,
                    BinaryOp::GtEq => BinaryOp::LtEq,
                    other => *other,
                };
                Some((col.clone(), swapped_op, lit.clone()))
            }
            _ => None,
        },
        _ => None,
    }
}
