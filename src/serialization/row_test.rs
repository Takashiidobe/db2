mod tests {
    use crate::serialization::{RowSerializationError, RowSerializer, codec};
    use crate::types::{Column, DataType, Schema, Value};

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
            Column::new("active", DataType::Boolean),
        ])
    }

    #[test]
    fn test_serialize_row() {
        let row = vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Boolean(true),
        ];

        let bytes = RowSerializer::serialize(&row, None).unwrap();

        // Check format:
        // 2 bytes (count=3) + 8 bytes (int) + (4 + 5) bytes (string "Alice") + 1 byte (bool)
        // = 2 + 8 + 9 + 1 = 20 bytes
        assert_eq!(bytes.len(), 2 + 8 + (4 + 5) + 1);
    }

    #[test]
    fn test_round_trip_with_schema() {
        let schema = create_test_schema();
        let original = vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Boolean(true),
        ];

        let bytes = RowSerializer::serialize(&original, Some(&schema)).unwrap();
        let deserialized = RowSerializer::deserialize(&bytes, &schema).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_round_trip_without_validation() {
        let schema = create_test_schema();
        let original = vec![
            Value::Integer(42),
            Value::String("Bob".to_string()),
            Value::Boolean(false),
        ];

        // Serialize without schema validation
        let bytes = RowSerializer::serialize(&original, None).unwrap();
        // Deserialize with schema (needed for type information)
        let deserialized = RowSerializer::deserialize(&bytes, &schema).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_mixed_types() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
            Column::new("email", DataType::String),
            Column::new("age", DataType::Integer),
        ]);

        let row = vec![
            Value::Integer(100),
            Value::String("Charlie".to_string()),
            Value::String("charlie@example.com".to_string()),
            Value::Integer(35),
        ];

        let bytes = RowSerializer::serialize(&row, Some(&schema)).unwrap();
        let deserialized = RowSerializer::deserialize(&bytes, &schema).unwrap();

        assert_eq!(row, deserialized);
    }

    #[test]
    fn test_empty_strings() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        let row = vec![Value::Integer(1), Value::String("".to_string())];

        let bytes = RowSerializer::serialize(&row, Some(&schema)).unwrap();
        let deserialized = RowSerializer::deserialize(&bytes, &schema).unwrap();

        assert_eq!(row, deserialized);
    }

    #[test]
    fn test_utf8_strings() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("message", DataType::String),
        ]);

        let row = vec![
            Value::Integer(1),
            Value::String("Hello 世界 🌍".to_string()),
        ];

        let bytes = RowSerializer::serialize(&row, Some(&schema)).unwrap();
        let deserialized = RowSerializer::deserialize(&bytes, &schema).unwrap();

        assert_eq!(row, deserialized);
    }

    #[test]
    fn test_extreme_values() {
        let schema = Schema::new(vec![
            Column::new("min", DataType::Integer),
            Column::new("max", DataType::Integer),
        ]);

        let row = vec![Value::Integer(i64::MIN), Value::Integer(i64::MAX)];

        let bytes = RowSerializer::serialize(&row, Some(&schema)).unwrap();
        let deserialized = RowSerializer::deserialize(&bytes, &schema).unwrap();

        assert_eq!(row, deserialized);
    }

    #[test]
    fn test_unsigned_round_trip() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Unsigned),
            Column::new("note", DataType::String),
        ]);

        let row = vec![
            Value::Unsigned(u64::MAX),
            Value::String("max value".to_string()),
        ];

        let bytes = RowSerializer::serialize(&row, Some(&schema)).unwrap();
        let deserialized = RowSerializer::deserialize(&bytes, &schema).unwrap();

        assert_eq!(row, deserialized);
    }

    #[test]
    fn test_schema_validation_on_serialize() {
        let schema = create_test_schema();

        // Wrong number of columns
        let row = vec![Value::Integer(1), Value::String("Alice".to_string())];
        let result = RowSerializer::serialize(&row, Some(&schema));
        assert!(matches!(
            result,
            Err(RowSerializationError::ColumnCountMismatch { .. })
        ));
    }

    #[test]
    fn test_column_count_mismatch_on_deserialize() {
        let schema = create_test_schema();

        // Create bytes with wrong column count
        let mut bytes = Vec::new();
        codec::write_u16(&mut bytes, 2).unwrap(); // Wrong count
        codec::write_i64(&mut bytes, 1).unwrap();
        codec::write_string(&mut bytes, "Alice").unwrap();

        let result = RowSerializer::deserialize(&bytes, &schema);
        assert!(matches!(
            result,
            Err(RowSerializationError::ColumnCountMismatch {
                expected: 3,
                found: 2
            })
        ));
    }

    #[test]
    fn test_truncated_data() {
        let schema = create_test_schema();
        let row = vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Boolean(true),
        ];

        let mut bytes = RowSerializer::serialize(&row, Some(&schema)).unwrap();

        // Truncate the data
        bytes.truncate(bytes.len() - 1);

        let result = RowSerializer::deserialize(&bytes, &schema);
        assert!(matches!(result, Err(RowSerializationError::IoError(_))));
    }

    #[test]
    fn test_single_column() {
        let schema = Schema::new(vec![Column::new("id", DataType::Integer)]);
        let row = vec![Value::Integer(42)];

        let bytes = RowSerializer::serialize(&row, Some(&schema)).unwrap();
        let deserialized = RowSerializer::deserialize(&bytes, &schema).unwrap();

        assert_eq!(row, deserialized);
    }

    #[test]
    fn test_many_columns() {
        let mut columns = Vec::new();
        let mut row = Vec::new();

        for i in 0..20 {
            if i % 2 == 0 {
                columns.push(Column::new(format!("col{}", i), DataType::Integer));
                row.push(Value::Integer(i as i64));
            } else {
                columns.push(Column::new(format!("col{}", i), DataType::String));
                row.push(Value::String(format!("value{}", i)));
            }
        }

        let schema = Schema::new(columns);
        let bytes = RowSerializer::serialize(&row, Some(&schema)).unwrap();
        let deserialized = RowSerializer::deserialize(&bytes, &schema).unwrap();

        assert_eq!(row, deserialized);
    }
}
