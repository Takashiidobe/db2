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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_creation() {
        let val = Value::Integer(42);
        assert!(val.is_integer());
        assert!(!val.is_boolean());
        assert!(!val.is_string());
        assert_eq!(val.as_integer(), Some(42));
        assert_eq!(val.as_boolean(), None);
        assert_eq!(val.as_string(), None);
    }

    #[test]
    fn test_boolean_creation() {
        let val = Value::Boolean(true);
        assert!(val.is_boolean());
        assert!(!val.is_integer());
        assert_eq!(val.as_boolean(), Some(true));
        assert_eq!(val.as_integer(), None);
    }

    #[test]
    fn test_string_creation() {
        let val = Value::String("hello".to_string());
        assert!(val.is_string());
        assert!(!val.is_boolean());
        assert!(!val.is_integer());
        assert_eq!(val.as_string(), Some("hello"));
        assert_eq!(val.as_integer(), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Value::Integer(42)), "42");
        assert_eq!(format!("{}", Value::Boolean(false)), "false");
        assert_eq!(format!("{}", Value::String("hello".to_string())), "hello");
    }

    #[test]
    fn test_equality() {
        assert_eq!(Value::Integer(42), Value::Integer(42));
        assert_ne!(Value::Integer(42), Value::Integer(43));
        assert_eq!(
            Value::String("hello".to_string()),
            Value::String("hello".to_string())
        );
        assert_ne!(
            Value::String("hello".to_string()),
            Value::String("world".to_string())
        );
        // Different types are not equal
        assert_ne!(Value::Integer(42), Value::String("42".to_string()));
        assert_ne!(Value::Boolean(true), Value::Integer(1));
    }

    #[test]
    fn test_integer_comparison() {
        let a = Value::Integer(10);
        let b = Value::Integer(20);
        let c = Value::Integer(10);

        assert!(a < b);
        assert!(b > a);
        assert!(a <= c);
        assert!(a >= c);
    }

    #[test]
    fn test_string_comparison() {
        let a = Value::String("apple".to_string());
        let b = Value::String("banana".to_string());
        let c = Value::String("apple".to_string());

        assert!(a < b);
        assert!(b > a);
        assert!(a <= c);
        assert!(a >= c);
    }

    #[test]
    fn test_boolean_comparison() {
        let t = Value::Boolean(true);
        let f = Value::Boolean(false);
        assert!(f < t);
        assert!(t > f);
    }

    #[test]
    fn test_mixed_type_comparison() {
        let int_val = Value::Integer(42);
        let bool_val = Value::Boolean(true);
        let str_val = Value::String("42".to_string());

        // Different types are ordered: Integer < Boolean < String
        assert!(int_val < bool_val);
        assert!(bool_val < str_val);
        assert!(int_val < str_val);
    }

    #[test]
    fn test_clone() {
        let original = Value::String("test".to_string());
        let cloned = original.clone();
        assert_eq!(original, cloned);
    }

    #[test]
    fn test_debug() {
        let val = Value::Integer(42);
        assert_eq!(format!("{:?}", val), "Integer(42)");

        let val = Value::String("test".to_string());
        assert_eq!(format!("{:?}", val), "String(\"test\")");
    }
}
