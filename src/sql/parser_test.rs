mod tests {
    use crate::sql::ast::{BinaryOp, Expr, FromClause, IndexType, Literal, SelectColumn};
    use crate::sql::{parse_sql, parse_sql_statements};
    use crate::sql::parser::{Token, Tokenizer};
    use crate::sql::{DataType, Statement, TransactionCommand};

    #[test]
    fn test_tokenize_create_table() {
        let mut tokenizer = Tokenizer::new("CREATE TABLE users (id INTEGER, name VARCHAR)");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Create);
        assert_eq!(tokens[1], Token::Table);
        assert_eq!(tokens[2], Token::Identifier("users".to_string()));
    }

    #[test]
    fn test_parse_create_table_simple() {
        let sql = "CREATE TABLE users (id INTEGER, active BOOLEAN, name VARCHAR)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table_name, "users");
                assert_eq!(create.columns.len(), 3);
                assert_eq!(create.columns[0].name, "id");
                assert_eq!(create.columns[0].data_type, DataType::Integer);
                assert_eq!(create.columns[1].name, "active");
                assert_eq!(create.columns[1].data_type, DataType::Boolean);
                assert_eq!(create.columns[2].name, "name");
                assert_eq!(create.columns[2].data_type, DataType::Varchar);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_create_table_unsigned() {
        let sql = "CREATE TABLE metrics (id UNSIGNED, name VARCHAR)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.columns[0].data_type, DataType::Unsigned);
                assert_eq!(create.columns[1].data_type, DataType::Varchar);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_create_table_float() {
        let sql = "CREATE TABLE metrics (value FLOAT, note VARCHAR)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.columns[0].data_type, DataType::Float);
                assert_eq!(create.columns[1].data_type, DataType::Varchar);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_create_table_case_insensitive() {
        let sql = "create table Users (ID integer, Name varchar)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table_name, "Users");
                assert_eq!(create.columns.len(), 2);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO users VALUES (1, true, 'Alice')";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.table_name, "users");
                assert_eq!(insert.values.len(), 1);
                assert_eq!(insert.values[0].len(), 3);
                assert_eq!(insert.values[0][0], Literal::Integer(1));
                assert_eq!(insert.values[0][1], Literal::Boolean(true));
                assert_eq!(insert.values[0][2], Literal::String("Alice".to_string()));
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_multiple_values() {
        let sql = "INSERT INTO test VALUES (42, 'hello', -100, 'world')";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.values.len(), 1);
                assert_eq!(insert.values[0][0], Literal::Integer(42));
                assert_eq!(insert.values[0][1], Literal::String("hello".to_string()));
                assert_eq!(insert.values[0][2], Literal::Integer(-100));
                assert_eq!(insert.values[0][3], Literal::String("world".to_string()));
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_large_unsigned_literal() {
        let sql = "INSERT INTO test VALUES (18446744073709551615)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(
                    insert.values[0][0],
                    Literal::Integer(18446744073709551615i128)
                );
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_float_literal() {
        let sql = "INSERT INTO test VALUES (1.5, -2.0, 3e2)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.values[0][0], Literal::Float(1.5));
                assert_eq!(insert.values[0][1], Literal::Float(-2.0));
                assert_eq!(insert.values[0][2], Literal::Float(300.0));
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_string_with_escaped_quote() {
        let sql = "INSERT INTO test VALUES ('it''s working')";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(
                    insert.values[0][0],
                    Literal::String("it's working".to_string())
                );
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_boolean_literal_in_where() {
        let sql = "SELECT * FROM flags WHERE active = false";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Select(select) => {
                let where_expr = select.where_clause.expect("where clause");
                match where_expr {
                    Expr::BinaryOp { left, op, right } => {
                        assert_eq!(op, BinaryOp::Eq);
                        assert!(matches!(*left, Expr::Column(_)));
                        assert!(matches!(*right, Expr::Literal(Literal::Boolean(false))));
                    }
                    _ => panic!("Expected binary op"),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_create_table_single_column() {
        let sql = "CREATE TABLE test (id INTEGER)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.columns.len(), 1);
                assert_eq!(create.columns[0].name, "id");
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_error_missing_paren() {
        let sql = "CREATE TABLE test (id INTEGER";
        let result = parse_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_invalid_type() {
        let sql = "CREATE TABLE test (id INVALID)";
        let result = parse_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_whitespace_handling() {
        let sql = "  CREATE   TABLE   users  (  id   INTEGER  ,  name  VARCHAR  )  ";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table_name, "users");
                assert_eq!(create.columns.len(), 2);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_select_join() {
        let sql = "SELECT users.id, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE orders.amount > 10";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Select(select) => {
                if let SelectColumn::Columns(cols) = select.columns {
                    assert_eq!(cols.len(), 2);
                    assert_eq!(cols[0].table.as_deref(), Some("users"));
                    assert_eq!(cols[0].column, "id");
                    assert_eq!(cols[1].table.as_deref(), Some("orders"));
                    assert_eq!(cols[1].column, "amount");
                } else {
                    panic!("Expected column list");
                }

                match select.from {
                    FromClause::Join {
                        left_table,
                        right_table,
                        left_column,
                        right_column,
                    } => {
                        assert_eq!(left_table, "users");
                        assert_eq!(right_table, "orders");
                        assert_eq!(left_column.table.as_deref(), Some("users"));
                        assert_eq!(left_column.column, "id");
                        assert_eq!(right_column.table.as_deref(), Some("orders"));
                        assert_eq!(right_column.column, "user_id");
                    }
                    _ => panic!("Expected JOIN in FROM"),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_delete_without_where() {
        let stmt = parse_sql("DELETE FROM users").unwrap();

        match stmt {
            Statement::Delete(delete) => {
                assert_eq!(delete.table_name, "users");
                assert!(delete.where_clause.is_none());
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_parse_delete_with_where() {
        let stmt = parse_sql("DELETE FROM users WHERE id = 5").unwrap();

        match stmt {
            Statement::Delete(delete) => {
                assert_eq!(delete.table_name, "users");
                let where_clause = delete.where_clause.expect("where clause");
                match where_clause {
                    Expr::BinaryOp { op, .. } => assert_eq!(op, BinaryOp::Eq),
                    other => panic!("Unexpected where clause: {:?}", other),
                }
            }
            _ => panic!("Expected Delete statement"),
        }
    }

    #[test]
    fn test_parse_create_index_default_type() {
        let stmt = parse_sql("CREATE INDEX idx_test ON items(id)").unwrap();
        match stmt {
            Statement::CreateIndex(create) => {
                assert_eq!(create.index_name, "idx_test");
                assert_eq!(create.table_name, "items");
                assert_eq!(create.columns, vec!["id"]);
                assert_eq!(create.index_type, IndexType::BTree);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    #[test]
    fn test_parse_create_hash_index() {
        let stmt = parse_sql("CREATE INDEX idx_hash ON items USING HASH (id)").unwrap();
        match stmt {
            Statement::CreateIndex(create) => {
                assert_eq!(create.index_name, "idx_hash");
                assert_eq!(create.index_type, IndexType::Hash);
                assert_eq!(create.columns, vec!["id"]);
            }
            _ => panic!("Expected CreateIndex statement"),
        }
    }

    #[test]
    fn test_parse_statement_with_trailing_semicolon() {
        let stmt = parse_sql("SELECT * FROM users;").unwrap();
        match stmt {
            Statement::Select(_) => {}
            _ => panic!("Expected Select statement"),
        }
    }

    #[test]
    fn test_parse_statement_with_multiple_trailing_semicolons() {
        let stmt = parse_sql("BEGIN;;").unwrap();
        match stmt {
            Statement::Transaction(txn) => {
                assert_eq!(txn.command, TransactionCommand::Begin);
            }
            _ => panic!("Expected Transaction statement"),
        }
    }

    #[test]
    fn test_parse_multiple_statements() {
        let stmts = parse_sql_statements("CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (1);")
            .unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(matches!(stmts[0], Statement::CreateTable(_)));
        assert!(matches!(stmts[1], Statement::Insert(_)));
    }

    #[test]
    fn test_parse_multiple_statements_with_extra_semicolons() {
        let stmts = parse_sql_statements("BEGIN;;COMMIT;").unwrap();
        assert_eq!(stmts.len(), 2);
        match &stmts[0] {
            Statement::Transaction(txn) => assert_eq!(txn.command, TransactionCommand::Begin),
            _ => panic!("Expected Transaction statement"),
        }
        match &stmts[1] {
            Statement::Transaction(txn) => assert_eq!(txn.command, TransactionCommand::Commit),
            _ => panic!("Expected Transaction statement"),
        }
    }

    #[test]
    fn test_parse_begin_transaction() {
        let stmt = parse_sql("BEGIN TRANSACTION").unwrap();
        match stmt {
            Statement::Transaction(txn) => {
                assert_eq!(txn.command, TransactionCommand::Begin);
            }
            _ => panic!("Expected Transaction statement"),
        }
    }

    #[test]
    fn test_parse_commit() {
        let stmt = parse_sql("COMMIT").unwrap();
        match stmt {
            Statement::Transaction(txn) => {
                assert_eq!(txn.command, TransactionCommand::Commit);
            }
            _ => panic!("Expected Transaction statement"),
        }
    }

    #[test]
    fn test_parse_rollback() {
        let stmt = parse_sql("ROLLBACK").unwrap();
        match stmt {
            Statement::Transaction(txn) => {
                assert_eq!(txn.command, TransactionCommand::Rollback);
            }
            _ => panic!("Expected Transaction statement"),
        }
    }
}
