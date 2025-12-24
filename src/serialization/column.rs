use crate::serialization::codec;
use crate::types::Value;
use std::io::{self, Cursor};

/// Type tags for binary serialization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum TypeTag {
    Integer = 0,
    String = 1,
}

impl TypeTag {
    fn from_u8(value: u8) -> Result<Self, SerializationError> {
        match value {
            0 => Ok(TypeTag::Integer),
            1 => Ok(TypeTag::String),
            _ => Err(SerializationError::InvalidTypeTag(value)),
        }
    }

    fn from_value(value: &Value) -> Self {
        match value {
            Value::Integer(_) => TypeTag::Integer,
            Value::String(_) => TypeTag::String,
        }
    }
}

/// Errors that can occur during serialization/deserialization
#[derive(Debug)]
pub enum SerializationError {
    IoError(io::Error),
    InvalidTypeTag(u8),
    TypeMismatch { expected: String, found: String },
    EmptyColumn,
}

impl From<io::Error> for SerializationError {
    fn from(err: io::Error) -> Self {
        SerializationError::IoError(err)
    }
}

impl std::fmt::Display for SerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerializationError::IoError(e) => write!(f, "I/O error: {}", e),
            SerializationError::InvalidTypeTag(tag) => write!(f, "Invalid type tag: {}", tag),
            SerializationError::TypeMismatch { expected, found } => {
                write!(f, "Type mismatch: expected {}, found {}", expected, found)
            }
            SerializationError::EmptyColumn => write!(f, "Cannot serialize empty column"),
        }
    }
}

impl std::error::Error for SerializationError {}

/// Column serialization format:
/// ```text
/// [4 bytes: value_count (u32)]
/// [1 byte: type_tag (0=Integer, 1=String)]
/// [values...]
///
/// For Integer: [8 bytes: i64] (repeated value_count times)
/// For String:  [4 bytes: length (u32)][length bytes: UTF-8 data] (repeated value_count times)
/// ```
pub struct ColumnSerializer;

impl ColumnSerializer {
    /// Serialize a column of values into bytes.
    ///
    /// # Errors
    /// - Returns `SerializationError::EmptyColumn` if the values slice is empty
    /// - Returns `SerializationError::TypeMismatch` if values have different types
    pub fn serialize(values: &[Value]) -> Result<Vec<u8>, SerializationError> {
        if values.is_empty() {
            return Err(SerializationError::EmptyColumn);
        }

        let mut buf = Vec::new();

        // Write value count
        codec::write_u32(&mut buf, values.len() as u32)?;

        // Determine and write type tag
        let type_tag = TypeTag::from_value(&values[0]);
        codec::write_u8(&mut buf, type_tag as u8)?;

        // Verify all values have the same type and serialize them
        for value in values {
            let value_type = TypeTag::from_value(value);
            if value_type != type_tag {
                return Err(SerializationError::TypeMismatch {
                    expected: format!("{:?}", type_tag),
                    found: format!("{:?}", value_type),
                });
            }

            match value {
                Value::Integer(i) => codec::write_i64(&mut buf, *i)?,
                Value::String(s) => codec::write_string(&mut buf, s)?,
            }
        }

        Ok(buf)
    }

    /// Deserialize a column of values from bytes.
    ///
    /// # Errors
    /// - Returns `SerializationError::IoError` if data is truncated or malformed
    /// - Returns `SerializationError::InvalidTypeTag` if type tag is unknown
    pub fn deserialize(bytes: &[u8]) -> Result<Vec<Value>, SerializationError> {
        let mut cursor = Cursor::new(bytes);

        // Read value count
        let value_count = codec::read_u32(&mut cursor)? as usize;

        // Read type tag
        let type_tag_byte = codec::read_u8(&mut cursor)?;
        let type_tag = TypeTag::from_u8(type_tag_byte)?;

        // Deserialize values based on type
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            let value = match type_tag {
                TypeTag::Integer => {
                    let i = codec::read_i64(&mut cursor)?;
                    Value::Integer(i)
                }
                TypeTag::String => {
                    let s = codec::read_string(&mut cursor)?;
                    Value::String(s)
                }
            };
            values.push(value);
        }

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_empty_column_error() {
        let values: Vec<Value> = vec![];
        let result = ColumnSerializer::serialize(&values);

        assert!(matches!(result, Err(SerializationError::EmptyColumn)));
    }

    #[test]
    fn test_mixed_types_error() {
        let values = vec![Value::Integer(42), Value::String("hello".to_string())];
        let result = ColumnSerializer::serialize(&values);

        assert!(matches!(result, Err(SerializationError::TypeMismatch { .. })));
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
