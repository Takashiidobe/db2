mod tests {
    use crate::optimizer::planner::*;
    use crate::sql::ast::*;

    #[test]
    fn plans_index_scan_when_available() {
        let planner = Planner::new(vec![IndexMetadata {
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
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
            index_type: IndexType::BTree,
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
                assert!(matches!(
                    join_plan.strategy,
                    JoinStrategy::NestedLoop {
                        inner_has_index: true
                    }
                ));
            }
            _ => panic!("Expected join plan"),
        }
    }

    #[test]
    fn swaps_join_order_when_left_indexed() {
        let planner = Planner::new(vec![IndexMetadata {
            table: "users".to_string(),
            columns: vec!["id".to_string()],
            index_type: IndexType::BTree,
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
                assert!(matches!(
                    join_plan.strategy,
                    JoinStrategy::NestedLoop {
                        inner_has_index: true
                    }
                ));
            }
            _ => panic!("Expected join plan"),
        }
    }

    #[test]
    fn chooses_longer_composite_index_prefix() {
        let planner = Planner::new(vec![IndexMetadata {
            table: "items".to_string(),
            columns: vec!["a".to_string(), "b".to_string()],
            index_type: IndexType::BTree,
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
