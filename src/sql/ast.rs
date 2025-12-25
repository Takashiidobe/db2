/// SQL data types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Integer,
    Unsigned,
    Float,
    Boolean,
    Varchar,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Unsigned => write!(f, "UNSIGNED"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Boolean => write!(f, "BOOLEAN"),
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
#[derive(Debug, Clone)]
pub enum Literal {
    Integer(i128),
    Float(f64),
    Boolean(bool),
    String(String),
}

impl PartialEq for Literal {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Literal::Integer(a), Literal::Integer(b)) => a == b,
            (Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
            (Literal::Boolean(a), Literal::Boolean(b)) => a == b,
            (Literal::String(a), Literal::String(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Literal {}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::Float(fl) => write!(f, "{}", fl),
            Literal::Boolean(b) => write!(f, "{}", b),
            Literal::String(s) => write!(f, "'{}'", s),
        }
    }
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertStmt {
    pub table_name: String,
    pub values: Vec<Vec<Literal>>,
}

impl InsertStmt {
    pub fn new(table_name: impl Into<String>, values: Vec<Vec<Literal>>) -> Self {
        Self {
            table_name: table_name.into(),
            values,
        }
    }
}

/// Column reference, optionally qualified with table name
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

impl ColumnRef {
    pub fn new(table: Option<String>, column: impl Into<String>) -> Self {
        Self {
            table,
            column: column.into(),
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
    And,   // AND
}

/// Expression in SQL
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// Column reference (e.g., "id", "name")
    Column(ColumnRef),
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
    Columns(Vec<ColumnRef>),
}

/// FROM clause source
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromClause {
    Table(String),
    Join {
        left_table: String,
        right_table: String,
        left_column: ColumnRef,
        right_column: ColumnRef,
    },
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectStmt {
    pub columns: SelectColumn,
    pub from: FromClause,
    pub where_clause: Option<Expr>,
}

impl SelectStmt {
    pub fn new(columns: SelectColumn, from: FromClause, where_clause: Option<Expr>) -> Self {
        Self {
            columns,
            from,
            where_clause,
        }
    }
}

/// Supported index types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    BTree,
    Hash,
}

impl Default for IndexType {
    fn default() -> Self {
        IndexType::BTree
    }
}

impl std::fmt::Display for IndexType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexType::BTree => write!(f, "BTREE"),
            IndexType::Hash => write!(f, "HASH"),
        }
    }
}

impl IndexType {
    pub fn from_str(value: &str) -> Option<Self> {
        match value.to_uppercase().as_str() {
            "BTREE" => Some(IndexType::BTree),
            "HASH" => Some(IndexType::Hash),
            _ => None,
        }
    }
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
}

impl CreateIndexStmt {
    pub fn new(
        index_name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            index_name: index_name.into(),
            table_name: table_name.into(),
            columns,
            index_type: IndexType::default(),
        }
    }

    pub fn with_type(
        index_name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
        index_type: IndexType,
    ) -> Self {
        Self {
            index_name: index_name.into(),
            table_name: table_name.into(),
            columns,
            index_type,
        }
    }
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStmt {
    pub table_name: String,
}

impl DropTableStmt {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
        }
    }
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropIndexStmt {
    pub index_name: String,
}

impl DropIndexStmt {
    pub fn new(index_name: impl Into<String>) -> Self {
        Self {
            index_name: index_name.into(),
        }
    }
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteStmt {
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

impl DeleteStmt {
    pub fn new(table_name: impl Into<String>, where_clause: Option<Expr>) -> Self {
        Self {
            table_name: table_name.into(),
            where_clause,
        }
    }
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateStmt {
    pub table_name: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

impl UpdateStmt {
    pub fn new(
        table_name: impl Into<String>,
        assignments: Vec<(String, Expr)>,
        where_clause: Option<Expr>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            assignments,
            where_clause,
        }
    }
}

/// Transaction control commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionCommand {
    Begin,
    Commit,
    Rollback,
}

/// Transaction statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionStmt {
    pub command: TransactionCommand,
}

impl TransactionStmt {
    pub fn new(command: TransactionCommand) -> Self {
        Self { command }
    }
}

/// SQL statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    Insert(InsertStmt),
    Select(SelectStmt),
    CreateIndex(CreateIndexStmt),
    DropIndex(DropIndexStmt),
    Delete(DeleteStmt),
    Update(UpdateStmt),
    Transaction(TransactionStmt),
}
