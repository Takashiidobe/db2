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
/// [8 bytes: xmin (u64)]
/// [8 bytes: xmax (u64)]
/// [2 bytes: column_count (u16)]
/// [for each column: serialized Value]
///
/// Value serialization:
///   Integer: [8 bytes: i64]
///   Unsigned: [8 bytes: u64]
///   Float:    [8 bytes: f64]
///   Boolean:  [1 byte: 0 or 1]
///   String:  [4 bytes: length (u32)][length bytes: UTF-8 data]
/// ```
pub struct RowSerializer;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowMetadata {
    pub xmin: u64,
    pub xmax: u64,
}

impl Default for RowMetadata {
    fn default() -> Self {
        Self { xmin: 0, xmax: 0 }
    }
}

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
    pub fn serialize(
        row: &[Value],
        schema: Option<&Schema>,
    ) -> Result<Vec<u8>, RowSerializationError> {
        Self::serialize_with_metadata(row, schema, RowMetadata::default())
    }

    pub fn serialize_with_metadata(
        row: &[Value],
        schema: Option<&Schema>,
        metadata: RowMetadata,
    ) -> Result<Vec<u8>, RowSerializationError> {
        // Validate against schema if provided
        if let Some(schema) = schema {
            schema
                .validate_row(row)
                .map_err(|_| RowSerializationError::ColumnCountMismatch {
                    expected: schema.column_count(),
                    found: row.len(),
                })?;
        }

        let mut buf = Vec::new();

        // Write MVCC metadata
        codec::write_u64(&mut buf, metadata.xmin)?;
        codec::write_u64(&mut buf, metadata.xmax)?;

        // Write column count
        codec::write_u16(&mut buf, row.len() as u16)?;

        // Write each value
        for value in row {
            match value {
                Value::Null => {
                    codec::write_u8(&mut buf, 1)?;
                }
                Value::Integer(i) => {
                    codec::write_u8(&mut buf, 0)?;
                    codec::write_i64(&mut buf, *i)?;
                }
                Value::Unsigned(u) => {
                    codec::write_u8(&mut buf, 0)?;
                    codec::write_u64(&mut buf, *u)?;
                }
                Value::Float(fv) => {
                    codec::write_u8(&mut buf, 0)?;
                    codec::write_f64(&mut buf, *fv)?;
                }
                Value::Boolean(b) => {
                    codec::write_u8(&mut buf, 0)?;
                    codec::write_u8(&mut buf, *b as u8)?;
                }
                Value::String(s) => {
                    codec::write_u8(&mut buf, 0)?;
                    codec::write_string(&mut buf, s)?;
                }
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
        let (_, values) = Self::deserialize_with_metadata(bytes, schema)?;
        Ok(values)
    }

    pub fn deserialize_with_metadata(
        bytes: &[u8],
        schema: &Schema,
    ) -> Result<(RowMetadata, Vec<Value>), RowSerializationError> {
        let mut cursor = Cursor::new(bytes);

        let xmin = codec::read_u64(&mut cursor)?;
        let xmax = codec::read_u64(&mut cursor)?;

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
            let is_null = codec::read_u8(&mut cursor)? != 0;
            if is_null {
                values.push(Value::Null);
                continue;
            }
            let column = schema.column(i).expect("column index validated");
            let value = match column.data_type() {
                crate::types::DataType::Integer => {
                    let i = codec::read_i64(&mut cursor)?;
                    Value::Integer(i)
                }
                crate::types::DataType::Unsigned => {
                    let u = codec::read_u64(&mut cursor)?;
                    Value::Unsigned(u)
                }
                crate::types::DataType::Float => {
                    let f = codec::read_f64(&mut cursor)?;
                    Value::Float(f)
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

        Ok((RowMetadata { xmin, xmax }, values))
    }
}
