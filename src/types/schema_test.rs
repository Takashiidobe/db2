#[cfg(test)]
mod tests {
    use crate::types::{Column, DataType, Schema, SchemaError, Value};

    #[test]
    fn test_data_type_matches() {
        assert!(DataType::Integer.matches(&Value::Integer(42)));
        assert!(DataType::Integer.matches(&Value::Unsigned(42)));
        assert!(DataType::Unsigned.matches(&Value::Unsigned(5)));
        assert!(DataType::Unsigned.matches(&Value::Integer(0)));
        assert!(!DataType::Unsigned.matches(&Value::Integer(-1)));
        assert!(DataType::Boolean.matches(&Value::Boolean(true)));
        assert!(!DataType::Integer.matches(&Value::String("hello".to_string())));
        assert!(!DataType::Boolean.matches(&Value::Integer(0)));
        assert!(DataType::String.matches(&Value::String("hello".to_string())));
        assert!(!DataType::String.matches(&Value::Integer(42)));
    }

    #[test]
    fn test_column_creation() {
        let col = Column::new("id", DataType::Integer);
        assert_eq!(col.name(), "id");
        assert_eq!(col.data_type(), DataType::Integer);
    }

    #[test]
    fn test_column_validation() {
        let col = Column::new("age", DataType::Integer);
        assert!(col.validate(&Value::Integer(25)).is_ok());
        assert!(col.validate(&Value::String("25".to_string())).is_err());
    }

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        assert_eq!(schema.column_count(), 2);
        assert_eq!(schema.column(0).unwrap().name(), "id");
        assert_eq!(schema.column(1).unwrap().name(), "name");
        assert!(schema.column(2).is_none());
    }

    #[test]
    fn test_find_column() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        let (idx, col) = schema.find_column("name").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.name(), "name");

        assert!(schema.find_column("nonexistent").is_none());
    }

    #[test]
    fn test_validate_row_success() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        let row = vec![Value::Integer(1), Value::String("Alice".to_string())];
        assert!(schema.validate_row(&row).is_ok());

        let unsigned_schema = Schema::new(vec![
            Column::new("id", DataType::Unsigned),
            Column::new("name", DataType::String),
        ]);
        let row = vec![Value::Unsigned(1), Value::String("Bob".to_string())];
        assert!(unsigned_schema.validate_row(&row).is_ok());
    }

    #[test]
    fn test_validate_row_count_mismatch() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        let row = vec![Value::Integer(1)]; // Only 1 value, expected 2
        let result = schema.validate_row(&row);
        assert!(matches!(
            result,
            Err(SchemaError::ColumnCountMismatch {
                expected: 2,
                found: 1
            })
        ));
    }

    #[test]
    fn test_validate_row_type_mismatch() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        let row = vec![
            Value::String("wrong".to_string()),
            Value::String("Alice".to_string()),
        ];
        let result = schema.validate_row(&row);
        assert!(matches!(result, Err(SchemaError::TypeMismatch { .. })));
    }

    #[test]
    fn test_validate_multiple_types() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
            Column::new("age", DataType::Integer),
            Column::new("email", DataType::String),
        ]);

        let row = vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Integer(30),
            Value::String("alice@example.com".to_string()),
        ];
        assert!(schema.validate_row(&row).is_ok());
    }
}
