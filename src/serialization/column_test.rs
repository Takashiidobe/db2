#[cfg(test)]
mod tests {
    use crate::serialization::*;
    use crate::types::*;

    #[test]
    fn test_serialize_integers() {
        let values = vec![Value::Integer(1), Value::Integer(2), Value::Integer(3)];
        let bytes = ColumnSerializer::serialize(&values).unwrap();

        // Check format:
        // 4 bytes (count=3) + 1 byte (type=0) + 3 * (1 + 8 bytes) = 32 bytes
        assert_eq!(bytes.len(), 4 + 1 + 3 * (1 + 8));
    }

    #[test]
    fn test_serialize_unsigned() {
        let values = vec![Value::Unsigned(1), Value::Unsigned(2)];
        let bytes = ColumnSerializer::serialize(&values).unwrap();

        // 4 bytes (count=2) + 1 byte (type) + 2 * (1 + 8 bytes)
        assert_eq!(bytes.len(), 4 + 1 + 2 * (1 + 8));
    }

    #[test]
    fn test_serialize_floats() {
        let values = vec![Value::Float(1.5), Value::Float(-2.25)];
        let bytes = ColumnSerializer::serialize(&values).unwrap();

        // 4 bytes (count=2) + 1 byte (type) + 2 * (1 + 8 bytes)
        assert_eq!(bytes.len(), 4 + 1 + 2 * (1 + 8));
    }

    #[test]
    fn test_serialize_strings() {
        let values = vec![
            Value::String("hello".to_string()),
            Value::String("world".to_string()),
        ];
        let bytes = ColumnSerializer::serialize(&values).unwrap();

        // 4 bytes (count) + 1 byte (type) +
        // 2 * (1 + (4 + 5)) for "hello" and "world" = 25 bytes
        assert_eq!(bytes.len(), 4 + 1 + 2 * (1 + (4 + 5)));
    }

    #[test]
    fn test_serialize_booleans() {
        let values = vec![Value::Boolean(true), Value::Boolean(false)];
        let bytes = ColumnSerializer::serialize(&values).unwrap();

        // 4 bytes (count) + 1 byte (type) + 2 * (1 + 1 bytes) for booleans
        assert_eq!(bytes.len(), 4 + 1 + 2 * (1 + 1));
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
    fn test_round_trip_unsigned() {
        let original = vec![
            Value::Unsigned(0),
            Value::Unsigned(42),
            Value::Unsigned(u64::MAX),
        ];

        let bytes = ColumnSerializer::serialize(&original).unwrap();
        let deserialized = ColumnSerializer::deserialize(&bytes).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_round_trip_floats() {
        let original = vec![
            Value::Float(0.0),
            Value::Float(-1.5),
            Value::Float(1234.5678),
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
    fn test_round_trip_dates() {
        let original = vec![
            Value::Date(Date::parse("2025-01-02").expect("valid date")),
            Value::Date(Date::parse("2024-12-31").expect("valid date")),
        ];

        let bytes = ColumnSerializer::serialize(&original).unwrap();
        let deserialized = ColumnSerializer::deserialize(&bytes).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_round_trip_timestamps() {
        let original = vec![
            Value::Timestamp(Timestamp::parse("2025-01-02 03:04:05").expect("valid ts")),
            Value::Timestamp(Timestamp::parse("2025-12-31 23:59:59").expect("valid ts")),
        ];

        let bytes = ColumnSerializer::serialize(&original).unwrap();
        let deserialized = ColumnSerializer::deserialize(&bytes).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_round_trip_decimals() {
        let original = vec![
            Value::Decimal(Decimal::parse("12.340").expect("valid decimal")),
            Value::Decimal(Decimal::parse("-0.001").expect("valid decimal")),
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
