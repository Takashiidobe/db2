mod tests {
    use crate::sql::{
        CreateTableStmt, DataType, InsertStmt, Statement,
        ast::{ColumnDef, Literal},
    };

    #[test]
    fn test_data_type_display() {
        assert_eq!(format!("{}", DataType::Integer), "INTEGER");
        assert_eq!(format!("{}", DataType::Unsigned), "UNSIGNED");
        assert_eq!(format!("{}", DataType::Float), "FLOAT");
        assert_eq!(format!("{}", DataType::Boolean), "BOOLEAN");
        assert_eq!(format!("{}", DataType::Varchar), "VARCHAR");
    }

    #[test]
    fn test_column_def() {
        let col = ColumnDef::new("id", DataType::Integer);
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, DataType::Integer);
    }

    #[test]
    fn test_create_table_stmt() {
        let stmt = CreateTableStmt::new(
            "users",
            vec![
                ColumnDef::new("id", DataType::Integer),
                ColumnDef::new("name", DataType::Varchar),
            ],
        );
        assert_eq!(stmt.table_name, "users");
        assert_eq!(stmt.columns.len(), 2);
    }

    #[test]
    fn test_literal_display() {
        assert_eq!(format!("{}", Literal::Integer(42)), "42");
        assert_eq!(format!("{}", Literal::Float(1.5)), "1.5");
        assert_eq!(format!("{}", Literal::Boolean(true)), "true");
        assert_eq!(
            format!("{}", Literal::String("hello".to_string())),
            "'hello'"
        );
    }

    #[test]
    fn test_insert_stmt() {
        let stmt = InsertStmt::new(
            "users",
            vec![vec![
                Literal::Integer(1),
                Literal::String("Alice".to_string()),
            ]],
        );
        assert_eq!(stmt.table_name, "users");
        assert_eq!(stmt.values.len(), 1);
    }

    #[test]
    fn test_statement_enum() {
        let create = Statement::CreateTable(CreateTableStmt::new(
            "test",
            vec![ColumnDef::new("id", DataType::Integer)],
        ));
        assert!(matches!(create, Statement::CreateTable(_)));

        let insert = Statement::Insert(InsertStmt::new("test", vec![vec![Literal::Integer(1)]]));
        assert!(matches!(insert, Statement::Insert(_)));
    }
}
