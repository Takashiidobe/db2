#[cfg(test)]
mod tests {
    use crate::serialization::*;
    use crate::types::*;

    #[test]
    fn test_serialize_integers() {
        let values = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        let bytes = ColumnSerializer::serialize(&values).unwrap();

        // Check format:
        // 4 bytes (count=3) + 1 byte (type=0) + 3 * 8 bytes (integers) = 29 bytes
        assert_eq!(bytes.len(), 4 + 1 + 3 * 8);
    }

    #[test]
    fn test_serialize_strings() {
        let values = vec![
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ];
        let bytes = ColumnSerializer::serialize(&values).unwrap();

        // 4 bytes (count) + 1 byte (type) +
        // (4 + 5) for "hello" + (4 + 5) for "world" = 23 bytes
        assert_eq!(bytes.len(), 4 + 1 + (4 + 5) + (4 + 5));
    }

    #[test]
    fn test_serialize_booleans() {
        let values = vec![Value::Boolean(true), Value::Boolean(false)];
        let bytes = ColumnSerializer::serialize(&values).unwrap();

        // 4 bytes (count) + 1 byte (type) + 2 bytes for booleans
        assert_eq!(bytes.len(), 4 + 1 + 2);
    }

    #[test]
    fn test_round_trip_integers() {
        let original = vec![
            Value::Integer(42),
            Value::Integer(-100),
            Value::Integer(0),
            Value::Integer(i64::MAX),
            Value::Integer(i64::MIN),
        ];

        let bytes = ColumnSerializer::serialize(&original).unwrap();
        let deserialized = ColumnSerializer::deserialize(&bytes).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_round_trip_strings() {
        let original = vec![
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
            Value::String("".to_string()),
            Value::String("UTF-8: 世界 🌍".to_string()),
        ];

        let bytes = ColumnSerializer::serialize(&original).unwrap();
        let deserialized = ColumnSerializer::deserialize(&bytes).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_round_trip_booleans() {
        let original = vec![
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(true),
        ];

        let bytes = ColumnSerializer::serialize(&original).unwrap();
        let deserialized = ColumnSerializer::deserialize(&bytes).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_empty_column_error() {
        let values: Vec<Value> = vec![];
        let result = ColumnSerializer::serialize(&values);

        assert!(matches!(result, Err(SerializationError::EmptyColumn)));
    }

    #[test]
    fn test_mixed_types_error() {
        let values = vec![Value::Integer(42), Value::String("hello".to_string())];
        let result = ColumnSerializer::serialize(&values);

        assert!(matches!(
            result,
            Err(SerializationError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn test_invalid_type_tag() {
        // Manually create bytes with invalid type tag
        let mut bytes = Vec::new();
        codec::write_u32(&mut bytes, 1).unwrap(); // 1 value
        codec::write_u8(&mut bytes, 99).unwrap(); // Invalid type tag
        codec::write_i64(&mut bytes, 42).unwrap();

        let result = ColumnSerializer::deserialize(&bytes);
        assert!(matches!(
            result,
            Err(SerializationError::InvalidTypeTag(99))
        ));
    }

    #[test]
    fn test_truncated_data() {
        let values = vec![Value::Integer(1), Value::Integer(2)];
        let mut bytes = ColumnSerializer::serialize(&values).unwrap();

        // Truncate the data
        bytes.truncate(bytes.len() - 5);

        let result = ColumnSerializer::deserialize(&bytes);
        assert!(matches!(result, Err(SerializationError::IoError(_))));
    }

    #[test]
    fn test_single_value() {
        let original = vec![Value::Integer(42)];
        let bytes = ColumnSerializer::serialize(&original).unwrap();
        let deserialized = ColumnSerializer::deserialize(&bytes).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_large_column() {
        // Test with 1000 values
        let original: Vec<Value> = (0..1000).map(Value::Integer).collect();
        let bytes = ColumnSerializer::serialize(&original).unwrap();
        let deserialized = ColumnSerializer::deserialize(&bytes).unwrap();
        assert_eq!(original, deserialized);
    }
}
