use super::Value;

/// Data types supported by the database
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Integer,
    Unsigned,
    Boolean,
    String,
}

impl DataType {
    /// Check if a value matches this data type
    pub fn matches(&self, value: &Value) -> bool {
        match (self, value) {
            (DataType::Integer, Value::Integer(_)) => true,
            (DataType::Integer, Value::Unsigned(u)) => *u <= i64::MAX as u64,
            (DataType::Unsigned, Value::Unsigned(_)) => true,
            (DataType::Unsigned, Value::Integer(i)) => *i >= 0,
            (DataType::Boolean, Value::Boolean(_)) => true,
            (DataType::String, Value::String(_)) => true,
            _ => false,
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Unsigned => write!(f, "UNSIGNED"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::String => write!(f, "VARCHAR"),
        }
    }
}

/// A column definition with name and type
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    name: String,
    data_type: DataType,
}

impl Column {
    /// Create a new column
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the column data type
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    /// Validate that a value matches this column's type
    pub fn validate(&self, value: &Value) -> Result<(), SchemaError> {
        if self.data_type.matches(value) {
            Ok(())
        } else {
            Err(SchemaError::TypeMismatch {
                column: self.name.clone(),
                expected: self.data_type,
                found: format!("{:?}", value),
            })
        }
    }
}

/// A schema defines the structure of a table (column names and types)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    /// Create a new schema from a list of columns
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get a column by index
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Get all columns
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Find a column by name
    pub fn find_column(&self, name: &str) -> Option<(usize, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, col)| col.name() == name)
    }

    /// Validate that a row matches this schema
    pub fn validate_row(&self, row: &[Value]) -> Result<(), SchemaError> {
        if row.len() != self.columns.len() {
            return Err(SchemaError::ColumnCountMismatch {
                expected: self.columns.len(),
                found: row.len(),
            });
        }

        for (value, column) in row.iter().zip(self.columns.iter()) {
            column
                .validate(value)
                .map_err(|_| SchemaError::TypeMismatch {
                    column: column.name().to_string(),
                    expected: column.data_type(),
                    found: format!("{:?}", value),
                })?;
        }

        Ok(())
    }
}

/// Errors that can occur during schema validation
#[derive(Debug)]
pub enum SchemaError {
    ColumnCountMismatch {
        expected: usize,
        found: usize,
    },
    TypeMismatch {
        column: String,
        expected: DataType,
        found: String,
    },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::ColumnCountMismatch { expected, found } => {
                write!(
                    f,
                    "Column count mismatch: expected {} columns, found {}",
                    expected, found
                )
            }
            SchemaError::TypeMismatch {
                column,
                expected,
                found,
            } => {
                write!(
                    f,
                    "Type mismatch in column '{}': expected {}, found {}",
                    column, expected, found
                )
            }
        }
    }
}

impl std::error::Error for SchemaError {}
