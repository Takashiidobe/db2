use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Date {
    pub year: i32,
    pub month: u8,
    pub day: u8,
}

impl Date {
    pub fn parse(input: &str) -> Result<Self, String> {
        if input.len() != 10 {
            return Err("Date must be in YYYY-MM-DD format".to_string());
        }
        let year: i32 = input[0..4]
            .parse()
            .map_err(|_| "Invalid year in date".to_string())?;
        let month: u8 = input[5..7]
            .parse()
            .map_err(|_| "Invalid month in date".to_string())?;
        let day: u8 = input[8..10]
            .parse()
            .map_err(|_| "Invalid day in date".to_string())?;

        if &input[4..5] != "-" || &input[7..8] != "-" {
            return Err("Date must be in YYYY-MM-DD format".to_string());
        }

        validate_date_components(year, month, day)?;

        Ok(Self { year, month, day })
    }

    fn key(&self) -> i32 {
        self.year * 10_000 + self.month as i32 * 100 + self.day as i32
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamp {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
}

impl Timestamp {
    pub fn parse(input: &str) -> Result<Self, String> {
        if input.len() != 19 {
            return Err("Timestamp must be in YYYY-MM-DD HH:MM:SS format".to_string());
        }
        if &input[4..5] != "-" || &input[7..8] != "-" || &input[10..11] != " " {
            return Err("Timestamp must be in YYYY-MM-DD HH:MM:SS format".to_string());
        }
        if &input[13..14] != ":" || &input[16..17] != ":" {
            return Err("Timestamp must be in YYYY-MM-DD HH:MM:SS format".to_string());
        }

        let year: i32 = input[0..4]
            .parse()
            .map_err(|_| "Invalid year in timestamp".to_string())?;
        let month: u8 = input[5..7]
            .parse()
            .map_err(|_| "Invalid month in timestamp".to_string())?;
        let day: u8 = input[8..10]
            .parse()
            .map_err(|_| "Invalid day in timestamp".to_string())?;
        let hour: u8 = input[11..13]
            .parse()
            .map_err(|_| "Invalid hour in timestamp".to_string())?;
        let minute: u8 = input[14..16]
            .parse()
            .map_err(|_| "Invalid minute in timestamp".to_string())?;
        let second: u8 = input[17..19]
            .parse()
            .map_err(|_| "Invalid second in timestamp".to_string())?;

        validate_date_components(year, month, day)?;
        if hour > 23 {
            return Err("Invalid hour in timestamp".to_string());
        }
        if minute > 59 {
            return Err("Invalid minute in timestamp".to_string());
        }
        if second > 59 {
            return Err("Invalid second in timestamp".to_string());
        }

        Ok(Self {
            year,
            month,
            day,
            hour,
            minute,
            second,
        })
    }

    fn key(&self) -> i64 {
        (self.year as i64) * 10_000_000_000
            + (self.month as i64) * 100_000_000
            + (self.day as i64) * 1_000_000
            + (self.hour as i64) * 10_000
            + (self.minute as i64) * 100
            + (self.second as i64)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal {
    pub value: i128,
    pub scale: u32,
}

impl Decimal {
    pub fn parse(input: &str) -> Result<Self, String> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err("Decimal literal cannot be empty".to_string());
        }

        let mut chars = trimmed.chars().peekable();
        let mut sign = 1i128;
        if let Some(&ch) = chars.peek() {
            if ch == '-' {
                sign = -1;
                chars.next();
            } else if ch == '+' {
                chars.next();
            }
        }

        let remaining: String = chars.collect();
        let parts: Vec<&str> = remaining.split('.').collect();
        if parts.len() > 2 {
            return Err("Invalid decimal literal".to_string());
        }

        let int_part = parts[0];
        let frac_part = if parts.len() == 2 { parts[1] } else { "" };
        if int_part.is_empty() && frac_part.is_empty() {
            return Err("Invalid decimal literal".to_string());
        }
        if !int_part.chars().all(|c| c.is_ascii_digit())
            || !frac_part.chars().all(|c| c.is_ascii_digit())
        {
            return Err("Invalid decimal literal".to_string());
        }

        let scale = frac_part.len() as u32;
        let int_value: i128 = if int_part.is_empty() {
            0
        } else {
            int_part
                .parse()
                .map_err(|_| "Invalid decimal literal".to_string())?
        };
        let frac_value: i128 = if frac_part.is_empty() {
            0
        } else {
            frac_part
                .parse()
                .map_err(|_| "Invalid decimal literal".to_string())?
        };

        let multiplier = pow10_i128(scale).ok_or_else(|| "Decimal scale too large".to_string())?;
        let value = int_value
            .checked_mul(multiplier)
            .and_then(|v| v.checked_add(frac_value))
            .ok_or_else(|| "Decimal literal overflow".to_string())?;

        Ok(Self {
            value: value * sign,
            scale,
        })
    }

    pub fn from_i128(value: i128) -> Self {
        Self { value, scale: 0 }
    }

    pub fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        let s = value.to_string();
        Self::parse(&s).ok()
    }

    pub fn to_f64(&self) -> Option<f64> {
        self.to_string().parse::<f64>().ok()
    }

    fn rescale(&self, target_scale: u32) -> i128 {
        if self.scale == target_scale {
            self.value
        } else {
            let factor = pow10_i128(target_scale.saturating_sub(self.scale))
                .unwrap_or_else(|| if self.value.is_negative() { i128::MIN } else { i128::MAX });
            self.value
                .checked_mul(factor)
                .unwrap_or_else(|| if self.value.is_negative() { i128::MIN } else { i128::MAX })
        }
    }

    fn cmp_scaled(&self, other: &Self) -> Ordering {
        let scale = self.scale.max(other.scale);
        let left = self.rescale(scale);
        let right = other.rescale(scale);
        left.cmp(&right)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.value);
        }

        let sign = if self.value < 0 { "-" } else { "" };
        let abs_value = self.value.checked_abs().unwrap_or(i128::MAX);
        let digits = abs_value.to_string();
        let scale = self.scale as usize;

        if digits.len() <= scale {
            let zeros = "0".repeat(scale - digits.len());
            write!(f, "{}0.{}{}", sign, zeros, digits)
        } else {
            let split = digits.len() - scale;
            write!(f, "{}{}.{}", sign, &digits[..split], &digits[split..])
        }
    }
}

/// Core data type for the database.
/// Supports Integer (i64), Unsigned (u64), Float (f64), Boolean, and String (VARCHAR) types.
#[derive(Debug, Clone)]
pub enum Value {
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    Boolean(bool),
    String(String),
    Date(Date),
    Timestamp(Timestamp),
    Decimal(Decimal),
    Null,
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

    pub fn is_date(&self) -> bool {
        matches!(self, Value::Date(_))
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(self, Value::Timestamp(_))
    }

    pub fn is_decimal(&self) -> bool {
        matches!(self, Value::Decimal(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
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

    pub fn as_date(&self) -> Option<Date> {
        match self {
            Value::Date(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_timestamp(&self) -> Option<Timestamp> {
        match self {
            Value::Timestamp(t) => Some(*t),
            _ => None,
        }
    }

    pub fn as_decimal(&self) -> Option<Decimal> {
        match self {
            Value::Decimal(d) => Some(*d),
            _ => None,
        }
    }

    fn kind(&self) -> ValueKind {
        match self {
            Value::Integer(_) | Value::Unsigned(_) | Value::Float(_) | Value::Decimal(_) => {
                ValueKind::Numeric
            }
            Value::Boolean(_) => ValueKind::Boolean,
            Value::String(_) => ValueKind::String,
            Value::Date(_) => ValueKind::Date,
            Value::Timestamp(_) => ValueKind::Timestamp,
            Value::Null => ValueKind::Null,
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
            Value::Date(d) => write!(f, "{}", d),
            Value::Timestamp(t) => write!(f, "{}", t),
            Value::Decimal(d) => write!(f, "{}", d),
            Value::Null => write!(f, "NULL"),
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
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Decimal(a), Value::Decimal(b)) => a.cmp_scaled(b) == Ordering::Equal,
            (Value::Decimal(a), Value::Integer(b)) | (Value::Integer(b), Value::Decimal(a)) => {
                a.cmp_scaled(&Decimal::from_i128(*b as i128)) == Ordering::Equal
            }
            (Value::Decimal(a), Value::Unsigned(b)) | (Value::Unsigned(b), Value::Decimal(a)) => {
                a.cmp_scaled(&Decimal::from_i128(*b as i128)) == Ordering::Equal
            }
            (Value::Decimal(a), Value::Float(b)) | (Value::Float(b), Value::Decimal(a)) => {
                Decimal::from_f64(*b)
                    .map(|dec| a.cmp_scaled(&dec) == Ordering::Equal)
                    .unwrap_or(false)
            }
            (Value::Null, Value::Null) => true,
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
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
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
            (Value::Date(a), Value::Date(b)) => a.key().cmp(&b.key()),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.key().cmp(&b.key()),
            (Value::Decimal(a), Value::Decimal(b)) => a.cmp_scaled(b),
            (Value::Decimal(a), Value::Integer(b)) => {
                a.cmp_scaled(&Decimal::from_i128(*b as i128))
            }
            (Value::Integer(a), Value::Decimal(b)) => {
                Decimal::from_i128(*a as i128).cmp_scaled(b)
            }
            (Value::Decimal(a), Value::Unsigned(b)) => {
                a.cmp_scaled(&Decimal::from_i128(*b as i128))
            }
            (Value::Unsigned(a), Value::Decimal(b)) => {
                Decimal::from_i128(*a as i128).cmp_scaled(b)
            }
            (Value::Decimal(a), Value::Float(b)) => {
                Decimal::from_f64(*b)
                    .map(|dec| a.cmp_scaled(&dec))
                    .unwrap_or_else(|| {
                        a.to_f64()
                            .unwrap_or(0.0)
                            .total_cmp(b)
                    })
            }
            (Value::Float(a), Value::Decimal(b)) => {
                Decimal::from_f64(*a)
                    .map(|dec| dec.cmp_scaled(b))
                    .unwrap_or_else(|| {
                        a.total_cmp(&b.to_f64().unwrap_or(0.0))
                    })
            }
            _ => match (self.kind(), other.kind()) {
                (ValueKind::Numeric, ValueKind::Date) => Ordering::Less,
                (ValueKind::Numeric, ValueKind::Timestamp) => Ordering::Less,
                (ValueKind::Numeric, ValueKind::Boolean) => Ordering::Less,
                (ValueKind::Numeric, ValueKind::String) => Ordering::Less,
                (ValueKind::Date, ValueKind::Numeric) => Ordering::Greater,
                (ValueKind::Date, ValueKind::Timestamp) => Ordering::Less,
                (ValueKind::Date, ValueKind::Boolean) => Ordering::Less,
                (ValueKind::Date, ValueKind::String) => Ordering::Less,
                (ValueKind::Timestamp, ValueKind::Numeric) => Ordering::Greater,
                (ValueKind::Timestamp, ValueKind::Date) => Ordering::Greater,
                (ValueKind::Timestamp, ValueKind::Boolean) => Ordering::Less,
                (ValueKind::Timestamp, ValueKind::String) => Ordering::Less,
                (ValueKind::Boolean, ValueKind::Numeric) => Ordering::Greater,
                (ValueKind::Boolean, ValueKind::Date) => Ordering::Greater,
                (ValueKind::Boolean, ValueKind::Timestamp) => Ordering::Greater,
                (ValueKind::Boolean, ValueKind::String) => Ordering::Less,
                (ValueKind::String, ValueKind::Numeric) => Ordering::Greater,
                (ValueKind::String, ValueKind::Date) => Ordering::Greater,
                (ValueKind::String, ValueKind::Timestamp) => Ordering::Greater,
                (ValueKind::String, ValueKind::Boolean) => Ordering::Greater,
                _ => Ordering::Equal,
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueKind {
    Numeric,
    Date,
    Timestamp,
    Boolean,
    String,
    Null,
}

fn validate_date_components(year: i32, month: u8, day: u8) -> Result<(), String> {
    if !(1..=12).contains(&month) {
        return Err("Invalid month in date".to_string());
    }
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => unreachable!("month validated above"),
    };
    if day == 0 || day > max_day {
        return Err("Invalid day in date".to_string());
    }
    Ok(())
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

fn pow10_i128(exp: u32) -> Option<i128> {
    let mut value: i128 = 1;
    for _ in 0..exp {
        value = value.checked_mul(10)?;
    }
    Some(value)
}
