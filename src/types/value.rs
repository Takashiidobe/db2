use std::cmp::Ordering;
use std::fmt;

/// Core data type for the database.
/// Supports Integer (i64), Unsigned (u64), Float (f64), Boolean, and String (VARCHAR) types.
#[derive(Debug, Clone)]
pub enum Value {
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    Boolean(bool),
    String(String),
}

impl Value {
    /// Returns true if this value is an Integer
    pub fn is_integer(&self) -> bool {
        matches!(self, Value::Integer(_))
    }

    /// Returns true if this value is a Float
    pub fn is_float(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    /// Returns true if this value is an Unsigned Integer
    pub fn is_unsigned(&self) -> bool {
        matches!(self, Value::Unsigned(_))
    }

    /// Returns true if this value is a Boolean
    pub fn is_boolean(&self) -> bool {
        matches!(self, Value::Boolean(_))
    }

    /// Returns true if this value is a String
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    /// Returns the Integer value if this is an Integer, None otherwise
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Returns the Unsigned value if this is an Unsigned Integer, None otherwise
    pub fn as_unsigned(&self) -> Option<u64> {
        match self {
            Value::Unsigned(u) => Some(*u),
            _ => None,
        }
    }

    /// Returns the Float value if this is a Float, None otherwise
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Returns the Boolean value if this is a Boolean, None otherwise
    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Returns a reference to the String value if this is a String, None otherwise
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    fn kind(&self) -> ValueKind {
        match self {
            Value::Integer(_) | Value::Unsigned(_) | Value::Float(_) => ValueKind::Numeric,
            Value::Boolean(_) => ValueKind::Boolean,
            Value::String(_) => ValueKind::String,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Unsigned(u) => write!(f, "{}", u),
            Value::Float(fl) => write!(f, "{}", fl),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::String(s) => write!(f, "{}", s),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Unsigned(a), Value::Unsigned(b)) => a == b,
            (Value::Integer(a), Value::Unsigned(b)) | (Value::Unsigned(b), Value::Integer(a)) => {
                *a >= 0 && (*a as u64) == *b
            }
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::Float(a), Value::Integer(b)) | (Value::Integer(b), Value::Float(a)) => {
                (*a as f64) == *b as f64
            }
            (Value::Float(a), Value::Unsigned(b)) | (Value::Unsigned(b), Value::Float(a)) => {
                (*a as f64) == *b as f64
            }
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Unsigned(a), Value::Unsigned(b)) => a.cmp(b),
            (Value::Integer(a), Value::Unsigned(b)) => {
                if *a < 0 {
                    Ordering::Less
                } else {
                    (*a as u64).cmp(b)
                }
            }
            (Value::Unsigned(a), Value::Integer(b)) => {
                if *b < 0 {
                    Ordering::Greater
                } else {
                    a.cmp(&(*b as u64))
                }
            }
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.total_cmp(&(*b as f64)),
            (Value::Float(a), Value::Unsigned(b)) => a.total_cmp(&(*b as f64)),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).total_cmp(b),
            (Value::Unsigned(a), Value::Float(b)) => (*a as f64).total_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            _ => match (self.kind(), other.kind()) {
                (ValueKind::Numeric, ValueKind::Boolean) => Ordering::Less,
                (ValueKind::Numeric, ValueKind::String) => Ordering::Less,
                (ValueKind::Boolean, ValueKind::Numeric) => Ordering::Greater,
                (ValueKind::Boolean, ValueKind::String) => Ordering::Less,
                (ValueKind::String, ValueKind::Numeric) => Ordering::Greater,
                (ValueKind::String, ValueKind::Boolean) => Ordering::Greater,
                _ => Ordering::Equal,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueKind {
    Numeric,
    Boolean,
    String,
}
