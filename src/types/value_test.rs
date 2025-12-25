#[cfg(test)]
mod tests {
    use crate::types::{Date, Decimal, Timestamp, Value};

    #[test]
    fn test_integer_creation() {
        let val = Value::Integer(42);
        assert!(val.is_integer());
        assert!(!val.is_unsigned());
        assert!(!val.is_float());
        assert!(!val.is_boolean());
        assert!(!val.is_string());
        assert_eq!(val.as_integer(), Some(42));
        assert_eq!(val.as_boolean(), None);
        assert_eq!(val.as_string(), None);
    }

    #[test]
    fn test_unsigned_creation() {
        let val = Value::Unsigned(42);
        assert!(val.is_unsigned());
        assert!(!val.is_integer());
        assert!(!val.is_float());
        assert_eq!(val.as_unsigned(), Some(42));
        assert_eq!(val.as_integer(), None);
    }

    #[test]
    fn test_float_creation() {
        let val = Value::Float(3.14);
        assert!(val.is_float());
        assert!(!val.is_integer());
        assert!(!val.is_unsigned());
        assert_eq!(val.as_float(), Some(3.14));
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
    fn test_date_creation() {
        let date = Date::parse("2025-01-02").expect("valid date");
        let val = Value::Date(date);
        assert!(val.is_date());
        assert!(!val.is_timestamp());
        assert!(!val.is_decimal());
        assert_eq!(val.as_date(), Some(date));
    }

    #[test]
    fn test_timestamp_creation() {
        let ts = Timestamp::parse("2025-01-02 03:04:05").expect("valid timestamp");
        let val = Value::Timestamp(ts);
        assert!(val.is_timestamp());
        assert!(!val.is_date());
        assert_eq!(val.as_timestamp(), Some(ts));
    }

    #[test]
    fn test_decimal_creation() {
        let dec = Decimal::parse("12.34").expect("valid decimal");
        let val = Value::Decimal(dec);
        assert!(val.is_decimal());
        assert_eq!(val.as_decimal(), Some(dec));
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Value::Integer(42)), "42");
        assert_eq!(format!("{}", Value::Unsigned(42)), "42");
        assert_eq!(format!("{}", Value::Float(1.5)), "1.5");
        assert_eq!(format!("{}", Value::Boolean(false)), "false");
        assert_eq!(format!("{}", Value::String("hello".to_string())), "hello");
        assert_eq!(
            format!(
                "{}",
                Value::Date(Date::parse("2025-01-02").expect("valid date"))
            ),
            "2025-01-02"
        );
        assert_eq!(
            format!(
                "{}",
                Value::Timestamp(Timestamp::parse("2025-01-02 03:04:05").expect("valid ts"))
            ),
            "2025-01-02 03:04:05"
        );
        assert_eq!(
            format!("{}", Value::Decimal(Decimal::parse("12.34").expect("valid decimal"))),
            "12.34"
        );
        assert_eq!(format!("{}", Value::Null), "NULL");
    }

    #[test]
    fn test_equality() {
        assert_eq!(Value::Integer(42), Value::Integer(42));
        assert_ne!(Value::Integer(42), Value::Integer(43));
        assert_eq!(Value::Integer(5), Value::Unsigned(5));
        assert_ne!(Value::Integer(-1), Value::Unsigned(1));
        assert_eq!(Value::Float(5.0), Value::Integer(5));
        assert_eq!(Value::Float(5.0), Value::Unsigned(5));
        assert_ne!(Value::Float(5.1), Value::Integer(5));
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
        let u = Value::Unsigned(15);
        let f = Value::Float(10.5);

        assert!(a < b);
        assert!(b > a);
        assert!(a <= c);
        assert!(a >= c);
        assert!(u > a);
        assert!(Value::Unsigned(0) > Value::Integer(-1));
        assert!(f > a);
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
        let date_val = Value::Date(Date::parse("2025-01-02").expect("valid date"));

        // Different types are ordered: Numeric < Date < Boolean < String
        assert!(int_val < date_val);
        assert!(date_val < bool_val);
        assert!(bool_val < str_val);
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

        let val = Value::Unsigned(7);
        assert_eq!(format!("{:?}", val), "Unsigned(7)");

        let val = Value::Float(1.5);
        assert_eq!(format!("{:?}", val), "Float(1.5)");

        let val = Value::String("test".to_string());
        assert_eq!(format!("{:?}", val), "String(\"test\")");
    }
}
