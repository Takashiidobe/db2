/// SQL data types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Integer,
    Varchar,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Varchar => write!(f, "VARCHAR"),
        }
    }
}

/// Column definition in CREATE TABLE
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

impl CreateTableStmt {
    pub fn new(table_name: impl Into<String>, columns: Vec<ColumnDef>) -> Self {
        Self {
            table_name: table_name.into(),
            columns,
        }
    }
}

/// Literal value in SQL
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Literal {
    Integer(i64),
    String(String),
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::String(s) => write!(f, "'{}'", s),
        }
    }
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStmt {
    pub table_name: String,
    pub values: Vec<Literal>,
}

impl InsertStmt {
    pub fn new(table_name: impl Into<String>, values: Vec<Literal>) -> Self {
        Self {
            table_name: table_name.into(),
            values,
        }
    }
}

/// Binary operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Eq,    // =
    NotEq, // !=
    Lt,    // <
    LtEq,  // <=
    Gt,    // >
    GtEq,  // >=
}

/// Expression in SQL
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// Column reference (e.g., "id", "name")
    Column(String),
    /// Literal value
    Literal(Literal),
    /// Binary operation (e.g., col = 5)
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
}

impl Expr {
    /// Create a binary operation expression
    pub fn binary_op(left: Expr, op: BinaryOp, right: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }
}

/// Column selection in SELECT
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectColumn {
    /// All columns (*)
    All,
    /// Specific columns
    Columns(Vec<String>),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStmt {
    pub columns: SelectColumn,
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

impl SelectStmt {
    pub fn new(
        columns: SelectColumn,
        table_name: impl Into<String>,
        where_clause: Option<Expr>,
    ) -> Self {
        Self {
            columns,
            table_name: table_name.into(),
            where_clause,
        }
    }
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table_name: String,
    pub column_name: String,
}

impl CreateIndexStmt {
    pub fn new(
        index_name: impl Into<String>,
        table_name: impl Into<String>,
        column_name: impl Into<String>,
    ) -> Self {
        Self {
            index_name: index_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}

/// SQL statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
    CreateIndex(CreateIndexStmt),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_display() {
        assert_eq!(format!("{}", DataType::Integer), "INTEGER");
        assert_eq!(format!("{}", DataType::Varchar), "VARCHAR");
    }

    #[test]
    fn test_column_def() {
        let col = ColumnDef::new("id", DataType::Integer);
        assert_eq!(col.name, "id");
        assert_eq!(col.data_type, DataType::Integer);
    }

    #[test]
    fn test_create_table_stmt() {
        let stmt = CreateTableStmt::new(
            "users",
            vec![
                ColumnDef::new("id", DataType::Integer),
                ColumnDef::new("name", DataType::Varchar),
            ],
        );
        assert_eq!(stmt.table_name, "users");
        assert_eq!(stmt.columns.len(), 2);
    }

    #[test]
    fn test_literal_display() {
        assert_eq!(format!("{}", Literal::Integer(42)), "42");
        assert_eq!(
            format!("{}", Literal::String("hello".to_string())),
            "'hello'"
        );
    }

    #[test]
    fn test_insert_stmt() {
        let stmt = InsertStmt::new(
            "users",
            vec![Literal::Integer(1), Literal::String("Alice".to_string())],
        );
        assert_eq!(stmt.table_name, "users");
        assert_eq!(stmt.values.len(), 2);
    }

    #[test]
    fn test_statement_enum() {
        let create = Statement::CreateTable(CreateTableStmt::new(
            "test",
            vec![ColumnDef::new("id", DataType::Integer)],
        ));
        assert!(matches!(create, Statement::CreateTable(_)));

        let insert = Statement::Insert(InsertStmt::new("test", vec![Literal::Integer(1)]));
        assert!(matches!(insert, Statement::Insert(_)));
    }
}
