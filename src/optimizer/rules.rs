use crate::sql::ast::{BinaryOp, ColumnRef, Expr, Literal};

/// Extract a simple column-literal predicate if present.
///
/// Returns (column, operator, literal) with operator adjusted for operand order.
pub fn extract_indexable_predicates(expr: &Expr) -> Vec<(ColumnRef, BinaryOp, Literal)> {
    let mut preds = Vec::new();
    collect_predicates(expr, &mut preds);
    preds
}

fn collect_predicates(expr: &Expr, out: &mut Vec<(ColumnRef, BinaryOp, Literal)>) {
    match expr {
        Expr::BinaryOp { left, op, right } if *op == BinaryOp::And => {
            collect_predicates(left, out);
            collect_predicates(right, out);
        }
        Expr::BinaryOp { left, op, right } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(col), Expr::Literal(lit)) => out.push((col.clone(), *op, lit.clone())),
            (Expr::Literal(lit), Expr::Column(col)) => {
                let swapped_op = match op {
                    BinaryOp::Lt => BinaryOp::Gt,
                    BinaryOp::LtEq => BinaryOp::GtEq,
                    BinaryOp::Gt => BinaryOp::Lt,
                    BinaryOp::GtEq => BinaryOp::LtEq,
                    other => *other,
                };
                out.push((col.clone(), swapped_op, lit.clone()));
            }
            _ => {}
        },
        _ => {}
    }
}
