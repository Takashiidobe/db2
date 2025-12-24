use crate::serialization::codec;
use crate::types::{Schema, Value};
use std::io::{self, Cursor};

/// Errors that can occur during row serialization/deserialization
#[derive(Debug)]
pub enum RowSerializationError {
    IoError(io::Error),
    ColumnCountMismatch { expected: usize, found: usize },
}

impl From<io::Error> for RowSerializationError {
    fn from(err: io::Error) -> Self {
        RowSerializationError::IoError(err)
    }
}

impl std::fmt::Display for RowSerializationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowSerializationError::IoError(e) => write!(f, "I/O error: {}", e),
            RowSerializationError::ColumnCountMismatch { expected, found } => {
                write!(
                    f,
                    "Column count mismatch: expected {}, found {}",
                    expected, found
                )
            }
        }
    }
}

impl std::error::Error for RowSerializationError {}

/// Row serialization format:
/// ```text
/// [2 bytes: column_count (u16)]
/// [for each column: serialized Value]
///
/// Value serialization:
///   Integer: [8 bytes: i64]
///   Boolean: [1 byte: 0 or 1]
///   String:  [4 bytes: length (u32)][length bytes: UTF-8 data]
/// ```
pub struct RowSerializer;

impl RowSerializer {
    /// Serialize a row of values into bytes.
    ///
    /// The row is serialized without type tags, as the schema provides type information.
    /// This is more space-efficient than column serialization.
    ///
    /// # Arguments
    /// * `row` - Slice of values representing the row
    /// * `schema` - Optional schema for validation (if provided, validates before serializing)
    ///
    /// # Errors
    /// Returns error if I/O fails or if schema validation fails
    pub fn serialize(row: &[Value], schema: Option<&Schema>) -> Result<Vec<u8>, RowSerializationError> {
        // Validate against schema if provided
        if let Some(schema) = schema {
            schema.validate_row(row).map_err(|_| {
                RowSerializationError::ColumnCountMismatch {
                    expected: schema.column_count(),
                    found: row.len(),
                }
            })?;
        }

        let mut buf = Vec::new();

        // Write column count
        codec::write_u16(&mut buf, row.len() as u16)?;

        // Write each value
        for value in row {
            match value {
                Value::Integer(i) => codec::write_i64(&mut buf, *i)?,
                Value::Boolean(b) => codec::write_u8(&mut buf, *b as u8)?,
                Value::String(s) => codec::write_string(&mut buf, s)?,
            }
        }

        Ok(buf)
    }

    /// Deserialize a row of values from bytes using a schema.
    ///
    /// The schema is required to know the types of each column.
    ///
    /// # Arguments
    /// * `bytes` - Serialized row data
    /// * `schema` - Schema defining the column types
    ///
    /// # Errors
    /// Returns error if:
    /// - Data is truncated or malformed
    /// - Column count doesn't match schema
    pub fn deserialize(bytes: &[u8], schema: &Schema) -> Result<Vec<Value>, RowSerializationError> {
        let mut cursor = Cursor::new(bytes);

        // Read column count
        let column_count = codec::read_u16(&mut cursor)? as usize;

        // Verify column count matches schema
        if column_count != schema.column_count() {
            return Err(RowSerializationError::ColumnCountMismatch {
                expected: schema.column_count(),
                found: column_count,
            });
        }

        // Deserialize each value according to schema
        let mut values = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let column = schema.column(i).expect("column index validated");
            let value = match column.data_type() {
                crate::types::DataType::Integer => {
                    let i = codec::read_i64(&mut cursor)?;
                    Value::Integer(i)
                }
                crate::types::DataType::Boolean => {
                    let b = codec::read_u8(&mut cursor)?;
                    Value::Boolean(b != 0)
                }
                crate::types::DataType::String => {
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
    use crate::types::{Column, DataType};

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
