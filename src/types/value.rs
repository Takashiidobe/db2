use std::fmt;

/// Core data type for the database.
/// Supports Integer (i64), Boolean, and String (VARCHAR) types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Integer(i64),
    Boolean(bool),
    String(String),
}

impl Value {
    /// Returns true if this value is an Integer
    pub fn is_integer(&self) -> bool {
        matches!(self, Value::Integer(_))
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
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Integer(i) => write!(f, "{}", i),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::String(s) => write!(f, "{}", s),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            // Define ordering between different types: Integer < Boolean < String
            (Value::Integer(_), Value::Boolean(_)) => std::cmp::Ordering::Less,
            (Value::Boolean(_), Value::Integer(_)) => std::cmp::Ordering::Greater,
            (Value::Integer(_), Value::String(_)) => std::cmp::Ordering::Less,
            (Value::String(_), Value::Integer(_)) => std::cmp::Ordering::Greater,
            (Value::Boolean(_), Value::String(_)) => std::cmp::Ordering::Less,
            (Value::String(_), Value::Boolean(_)) => std::cmp::Ordering::Greater,
        }
    }
}
