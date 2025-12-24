use super::Value;

/// Data types supported by the database
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Integer,
    Boolean,
    String,
}

impl DataType {
    /// Check if a value matches this data type
    pub fn matches(&self, value: &Value) -> bool {
        matches!(
            (self, value),
            (DataType::Integer, Value::Integer(_))
                | (DataType::Boolean, Value::Boolean(_))
                | (DataType::String, Value::String(_))
        )
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_matches() {
        assert!(DataType::Integer.matches(&Value::Integer(42)));
        assert!(DataType::Boolean.matches(&Value::Boolean(true)));
        assert!(!DataType::Integer.matches(&Value::String("hello".to_string())));
        assert!(!DataType::Boolean.matches(&Value::Integer(0)));
        assert!(DataType::String.matches(&Value::String("hello".to_string())));
        assert!(!DataType::String.matches(&Value::Integer(42)));
    }

    #[test]
    fn test_column_creation() {
        let col = Column::new("id", DataType::Integer);
        assert_eq!(col.name(), "id");
        assert_eq!(col.data_type(), DataType::Integer);
    }

    #[test]
    fn test_column_validation() {
        let col = Column::new("age", DataType::Integer);
        assert!(col.validate(&Value::Integer(25)).is_ok());
        assert!(col.validate(&Value::String("25".to_string())).is_err());
    }

    #[test]
    fn test_schema_creation() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        assert_eq!(schema.column_count(), 2);
        assert_eq!(schema.column(0).unwrap().name(), "id");
        assert_eq!(schema.column(1).unwrap().name(), "name");
        assert!(schema.column(2).is_none());
    }

    #[test]
    fn test_find_column() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        let (idx, col) = schema.find_column("name").unwrap();
        assert_eq!(idx, 1);
        assert_eq!(col.name(), "name");

        assert!(schema.find_column("nonexistent").is_none());
    }

    #[test]
    fn test_validate_row_success() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        let row = vec![Value::Integer(1), Value::String("Alice".to_string())];
        assert!(schema.validate_row(&row).is_ok());
    }

    #[test]
    fn test_validate_row_count_mismatch() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        let row = vec![Value::Integer(1)]; // Only 1 value, expected 2
        let result = schema.validate_row(&row);
        assert!(matches!(
            result,
            Err(SchemaError::ColumnCountMismatch {
                expected: 2,
                found: 1
            })
        ));
    }

    #[test]
    fn test_validate_row_type_mismatch() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
        ]);

        let row = vec![
            Value::String("wrong".to_string()),
            Value::String("Alice".to_string()),
        ];
        let result = schema.validate_row(&row);
        assert!(matches!(result, Err(SchemaError::TypeMismatch { .. })));
    }

    #[test]
    fn test_validate_multiple_types() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Integer),
            Column::new("name", DataType::String),
            Column::new("age", DataType::Integer),
            Column::new("email", DataType::String),
        ]);

        let row = vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Integer(30),
            Value::String("alice@example.com".to_string()),
        ];
        assert!(schema.validate_row(&row).is_ok());
    }
}
