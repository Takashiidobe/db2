use crate::serialization::codec;
use crate::types::Value;
use std::io::{self, Cursor};

/// Type tags for binary serialization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum TypeTag {
    Integer = 0,
    String = 1,
    Boolean = 2,
    Unsigned = 3,
    Float = 4,
}

impl TypeTag {
    fn from_u8(value: u8) -> Result<Self, SerializationError> {
        match value {
            0 => Ok(TypeTag::Integer),
            1 => Ok(TypeTag::String),
            2 => Ok(TypeTag::Boolean),
            3 => Ok(TypeTag::Unsigned),
            4 => Ok(TypeTag::Float),
            _ => Err(SerializationError::InvalidTypeTag(value)),
        }
    }

    fn from_value(value: &Value) -> Self {
        match value {
            Value::Integer(_) => TypeTag::Integer,
            Value::Boolean(_) => TypeTag::Boolean,
            Value::String(_) => TypeTag::String,
            Value::Unsigned(_) => TypeTag::Unsigned,
            Value::Float(_) => TypeTag::Float,
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
/// [1 byte: type_tag (0=Integer, 1=String, 2=Boolean)]
/// [values...]
///
/// For Integer:  [8 bytes: i64] (repeated value_count times)
/// For Unsigned: [8 bytes: u64]
/// For Float:    [8 bytes: f64]
/// For Boolean:  [1 byte: 0 or 1]
/// For String:   [4 bytes: length (u32)][length bytes: UTF-8 data] (repeated value_count times)
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
                Value::Unsigned(u) => codec::write_u64(&mut buf, *u)?,
                Value::Float(fv) => codec::write_f64(&mut buf, *fv)?,
                Value::Boolean(b) => codec::write_u8(&mut buf, *b as u8)?,
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
                TypeTag::Unsigned => {
                    let u = codec::read_u64(&mut cursor)?;
                    Value::Unsigned(u)
                }
                TypeTag::Float => {
                    let f = codec::read_f64(&mut cursor)?;
                    Value::Float(f)
                }
                TypeTag::String => {
                    let s = codec::read_string(&mut cursor)?;
                    Value::String(s)
                }
                TypeTag::Boolean => {
                    let b = codec::read_u8(&mut cursor)?;
                    Value::Boolean(b != 0)
                }
            };
            values.push(value);
        }

        Ok(values)
    }
}
