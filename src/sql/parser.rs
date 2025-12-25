use super::ast::{
    AggregateExpr, AggregateFunc, AggregateTarget, AlterTableAction, AlterTableStmt, BinaryOp,
    ColumnDef, ColumnRef, CreateIndexStmt, CreateTableStmt, DataType, DeleteStmt, DropIndexStmt,
    DropTableStmt, Expr, ForeignKeyRef, FromClause, IndexType, InsertStmt, Literal, OrderByExpr,
    SelectColumn, SelectItem, SelectStmt, Statement, TransactionCommand, TransactionStmt,
    UpdateStmt,
};

/// Parse errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    UnexpectedToken { expected: String, found: String },
    UnexpectedEof,
    InvalidSyntax(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnexpectedToken { expected, found } => {
                write!(f, "Expected {}, found {}", expected, found)
            }
            ParseError::UnexpectedEof => write!(f, "Unexpected end of input"),
            ParseError::InvalidSyntax(msg) => write!(f, "Invalid syntax: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

/// Token types
#[derive(Debug, Clone)]
pub(crate) enum Token {
    // Keywords
    Create,
    Drop,
    Alter,
    Table,
    Rename,
    Begin,
    Commit,
    Rollback,
    Transaction,
    Insert,
    Into,
    Values,
    Add,
    Column,
    Integer,
    Varchar,
    Boolean,
    Float,
    Unsigned,
    Date,
    Timestamp,
    Decimal,
    Numeric,
    Order,
    By,
    Group,
    Limit,
    Offset,
    Asc,
    Desc,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Distinct,
    In,
    To,
    True,
    False,
    Select,
    From,
    Where,
    Index,
    On,
    Using,
    Join,
    And,
    Delete,
    Dot,
    Update,
    Set,
    Semicolon,
    Primary,
    Key,
    Unique,
    References,
    Not,
    Null,
    Check,

    // Symbols
    LeftParen,
    RightParen,
    Comma,
    Asterisk,
    Equals,
    NotEquals,
    LessThan,
    LessThanEquals,
    GreaterThan,
    GreaterThanEquals,

    // Literals
    Identifier(String),
    IntegerLiteral(i128),
    FloatLiteral(f64),
    StringLiteral(String),

    // End of input
    Eof,
}

impl PartialEq for Token {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Token::Create, Token::Create)
            | (Token::Drop, Token::Drop)
            | (Token::Alter, Token::Alter)
            | (Token::Rename, Token::Rename)
            | (Token::Table, Token::Table)
            | (Token::Begin, Token::Begin)
            | (Token::Commit, Token::Commit)
            | (Token::Rollback, Token::Rollback)
            | (Token::Transaction, Token::Transaction)
            | (Token::Insert, Token::Insert)
            | (Token::Into, Token::Into)
            | (Token::Values, Token::Values)
            | (Token::Add, Token::Add)
            | (Token::Column, Token::Column)
            | (Token::Integer, Token::Integer)
            | (Token::Unsigned, Token::Unsigned)
            | (Token::Float, Token::Float)
            | (Token::Varchar, Token::Varchar)
            | (Token::Boolean, Token::Boolean)
            | (Token::Date, Token::Date)
            | (Token::Timestamp, Token::Timestamp)
            | (Token::Decimal, Token::Decimal)
            | (Token::Numeric, Token::Numeric)
            | (Token::Order, Token::Order)
            | (Token::By, Token::By)
            | (Token::Group, Token::Group)
            | (Token::Limit, Token::Limit)
            | (Token::Offset, Token::Offset)
            | (Token::Asc, Token::Asc)
            | (Token::Desc, Token::Desc)
            | (Token::Count, Token::Count)
            | (Token::Sum, Token::Sum)
            | (Token::Avg, Token::Avg)
            | (Token::Min, Token::Min)
            | (Token::Max, Token::Max)
            | (Token::Distinct, Token::Distinct)
            | (Token::In, Token::In)
            | (Token::To, Token::To)
            | (Token::True, Token::True)
            | (Token::False, Token::False)
            | (Token::Select, Token::Select)
            | (Token::From, Token::From)
            | (Token::Where, Token::Where)
            | (Token::Index, Token::Index)
            | (Token::On, Token::On)
            | (Token::Using, Token::Using)
            | (Token::Join, Token::Join)
            | (Token::And, Token::And)
            | (Token::Delete, Token::Delete)
            | (Token::Dot, Token::Dot)
            | (Token::Update, Token::Update)
            | (Token::Set, Token::Set)
            | (Token::Semicolon, Token::Semicolon)
            | (Token::Primary, Token::Primary)
            | (Token::Key, Token::Key)
            | (Token::Unique, Token::Unique)
            | (Token::References, Token::References)
            | (Token::Not, Token::Not)
            | (Token::Null, Token::Null)
            | (Token::Check, Token::Check)
            | (Token::LeftParen, Token::LeftParen)
            | (Token::RightParen, Token::RightParen)
            | (Token::Comma, Token::Comma)
            | (Token::Asterisk, Token::Asterisk)
            | (Token::Equals, Token::Equals)
            | (Token::NotEquals, Token::NotEquals)
            | (Token::LessThan, Token::LessThan)
            | (Token::LessThanEquals, Token::LessThanEquals)
            | (Token::GreaterThan, Token::GreaterThan)
            | (Token::GreaterThanEquals, Token::GreaterThanEquals)
            | (Token::Eof, Token::Eof) => true,
            (Token::Identifier(a), Token::Identifier(b)) => a == b,
            (Token::IntegerLiteral(a), Token::IntegerLiteral(b)) => a == b,
            (Token::FloatLiteral(a), Token::FloatLiteral(b)) => a.to_bits() == b.to_bits(),
            (Token::StringLiteral(a), Token::StringLiteral(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Token {}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Create => write!(f, "CREATE"),
            Token::Drop => write!(f, "DROP"),
            Token::Alter => write!(f, "ALTER"),
            Token::Rename => write!(f, "RENAME"),
            Token::Table => write!(f, "TABLE"),
            Token::Begin => write!(f, "BEGIN"),
            Token::Commit => write!(f, "COMMIT"),
            Token::Rollback => write!(f, "ROLLBACK"),
            Token::Transaction => write!(f, "TRANSACTION"),
            Token::Insert => write!(f, "INSERT"),
            Token::Into => write!(f, "INTO"),
            Token::Values => write!(f, "VALUES"),
            Token::Add => write!(f, "ADD"),
            Token::Column => write!(f, "COLUMN"),
            Token::Integer => write!(f, "INTEGER"),
            Token::Varchar => write!(f, "VARCHAR"),
            Token::Boolean => write!(f, "BOOLEAN"),
            Token::Unsigned => write!(f, "UNSIGNED"),
            Token::Float => write!(f, "FLOAT"),
            Token::Date => write!(f, "DATE"),
            Token::Timestamp => write!(f, "TIMESTAMP"),
            Token::Decimal => write!(f, "DECIMAL"),
            Token::Numeric => write!(f, "NUMERIC"),
            Token::Order => write!(f, "ORDER"),
            Token::By => write!(f, "BY"),
            Token::Group => write!(f, "GROUP"),
            Token::Limit => write!(f, "LIMIT"),
            Token::Offset => write!(f, "OFFSET"),
            Token::Asc => write!(f, "ASC"),
            Token::Desc => write!(f, "DESC"),
            Token::Count => write!(f, "COUNT"),
            Token::Sum => write!(f, "SUM"),
            Token::Avg => write!(f, "AVG"),
            Token::Min => write!(f, "MIN"),
            Token::Max => write!(f, "MAX"),
            Token::Distinct => write!(f, "DISTINCT"),
            Token::In => write!(f, "IN"),
            Token::To => write!(f, "TO"),
            Token::Select => write!(f, "SELECT"),
            Token::From => write!(f, "FROM"),
            Token::Where => write!(f, "WHERE"),
            Token::Index => write!(f, "INDEX"),
            Token::On => write!(f, "ON"),
            Token::Using => write!(f, "USING"),
            Token::Join => write!(f, "JOIN"),
            Token::And => write!(f, "AND"),
            Token::Delete => write!(f, "DELETE"),
            Token::Dot => write!(f, "."),
            Token::Update => write!(f, "UPDATE"),
            Token::Set => write!(f, "SET"),
            Token::Semicolon => write!(f, ";"),
            Token::Primary => write!(f, "PRIMARY"),
            Token::Key => write!(f, "KEY"),
            Token::Unique => write!(f, "UNIQUE"),
            Token::References => write!(f, "REFERENCES"),
            Token::Not => write!(f, "NOT"),
            Token::Null => write!(f, "NULL"),
            Token::Check => write!(f, "CHECK"),
            Token::True => write!(f, "TRUE"),
            Token::False => write!(f, "FALSE"),
            Token::LeftParen => write!(f, "("),
            Token::RightParen => write!(f, ")"),
            Token::Comma => write!(f, ","),
            Token::Asterisk => write!(f, "*"),
            Token::Equals => write!(f, "="),
            Token::NotEquals => write!(f, "!="),
            Token::LessThan => write!(f, "<"),
            Token::LessThanEquals => write!(f, "<="),
            Token::GreaterThan => write!(f, ">"),
            Token::GreaterThanEquals => write!(f, ">="),
            Token::Identifier(s) => write!(f, "identifier '{}'", s),
            Token::IntegerLiteral(i) => write!(f, "integer {}", i),
            Token::FloatLiteral(fv) => write!(f, "float {}", fv),
            Token::StringLiteral(s) => write!(f, "string '{}'", s),
            Token::Eof => write!(f, "end of input"),
        }
    }
}

/// Tokenizer (lexer)
pub(crate) struct Tokenizer {
    input: Vec<char>,
    position: usize,
}

enum NumberToken {
    Integer(i128),
    Float(f64),
}

impl Tokenizer {
    pub(crate) fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            position: 0,
        }
    }

    fn current(&self) -> Option<char> {
        self.input.get(self.position).copied()
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.current() {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn read_identifier(&mut self) -> String {
        let mut result = String::new();
        while let Some(ch) = self.current() {
            if ch.is_alphanumeric() || ch == '_' {
                result.push(ch);
                self.advance();
            } else {
                break;
            }
        }
        result
    }

    fn read_number(&mut self) -> Result<NumberToken, ParseError> {
        let mut result = String::new();
        let mut is_negative = false;

        if self.current() == Some('-') {
            is_negative = true;
            self.advance();
        }

        while let Some(ch) = self.current() {
            if ch.is_ascii_digit() {
                result.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        let mut is_float = false;

        if self.current() == Some('.') {
            if self
                .input
                .get(self.position + 1)
                .is_some_and(|c| c.is_ascii_digit())
            {
                is_float = true;
                result.push('.');
                self.advance();
                while let Some(ch) = self.current() {
                    if ch.is_ascii_digit() {
                        result.push(ch);
                        self.advance();
                    } else {
                        break;
                    }
                }
            }
        }

        if let Some(ch) = self.current() {
            if ch == 'e' || ch == 'E' {
                is_float = true;
                result.push(ch);
                self.advance();
                if let Some(sign) = self.current() {
                    if sign == '+' || sign == '-' {
                        result.push(sign);
                        self.advance();
                    }
                }
                let mut has_exponent_digit = false;
                while let Some(d) = self.current() {
                    if d.is_ascii_digit() {
                        has_exponent_digit = true;
                        result.push(d);
                        self.advance();
                    } else {
                        break;
                    }
                }
                if !has_exponent_digit {
                    return Err(ParseError::InvalidSyntax(
                        "Invalid number: missing exponent digits".to_string(),
                    ));
                }
            }
        }

        if result.is_empty() {
            return Err(ParseError::InvalidSyntax("Invalid number".to_string()));
        }

        if is_float {
            let num: f64 = result
                .parse()
                .map_err(|_| ParseError::InvalidSyntax("Invalid number".to_string()))?;
            Ok(NumberToken::Float(if is_negative { -num } else { num }))
        } else {
            let num: i128 = result
                .parse()
                .map_err(|_| ParseError::InvalidSyntax("Invalid number".to_string()))?;
            Ok(NumberToken::Integer(if is_negative { -num } else { num }))
        }
    }

    fn read_string(&mut self) -> Result<String, ParseError> {
        // Skip opening quote
        self.advance();

        let mut result = String::new();
        while let Some(ch) = self.current() {
            if ch == '\'' {
                // Check for escaped quote
                self.advance();
                if self.current() == Some('\'') {
                    result.push('\'');
                    self.advance();
                } else {
                    // End of string
                    return Ok(result);
                }
            } else {
                result.push(ch);
                self.advance();
            }
        }

        Err(ParseError::InvalidSyntax("Unterminated string".to_string()))
    }

    fn next_token(&mut self) -> Result<Token, ParseError> {
        self.skip_whitespace();

        match self.current() {
            None => Ok(Token::Eof),
            Some('(') => {
                self.advance();
                Ok(Token::LeftParen)
            }
            Some(')') => {
                self.advance();
                Ok(Token::RightParen)
            }
            Some(',') => {
                self.advance();
                Ok(Token::Comma)
            }
            Some(';') => {
                self.advance();
                Ok(Token::Semicolon)
            }
            Some('.') => {
                self.advance();
                Ok(Token::Dot)
            }
            Some('*') => {
                self.advance();
                Ok(Token::Asterisk)
            }
            Some('=') => {
                self.advance();
                Ok(Token::Equals)
            }
            Some('!') => {
                self.advance();
                if self.current() == Some('=') {
                    self.advance();
                    Ok(Token::NotEquals)
                } else {
                    Err(ParseError::InvalidSyntax(
                        "Expected '=' after '!'".to_string(),
                    ))
                }
            }
            Some('<') => {
                self.advance();
                if self.current() == Some('=') {
                    self.advance();
                    Ok(Token::LessThanEquals)
                } else {
                    Ok(Token::LessThan)
                }
            }
            Some('>') => {
                self.advance();
                if self.current() == Some('=') {
                    self.advance();
                    Ok(Token::GreaterThanEquals)
                } else {
                    Ok(Token::GreaterThan)
                }
            }
            Some('\'') => {
                let s = self.read_string()?;
                Ok(Token::StringLiteral(s))
            }
            Some(ch) if ch.is_ascii_digit() || ch == '-' => {
                let num = self.read_number()?;
                match num {
                    NumberToken::Integer(i) => Ok(Token::IntegerLiteral(i)),
                    NumberToken::Float(f) => Ok(Token::FloatLiteral(f)),
                }
            }
            Some(ch) if ch.is_alphabetic() || ch == '_' => {
                let ident = self.read_identifier();
                let token = match ident.to_uppercase().as_str() {
                    "CREATE" => Token::Create,
                    "DROP" => Token::Drop,
                    "ALTER" => Token::Alter,
                    "RENAME" => Token::Rename,
                    "TABLE" => Token::Table,
                    "BEGIN" => Token::Begin,
                    "COMMIT" => Token::Commit,
                    "ROLLBACK" => Token::Rollback,
                    "TRANSACTION" => Token::Transaction,
                    "INSERT" => Token::Insert,
                    "INTO" => Token::Into,
                    "VALUES" => Token::Values,
                    "ADD" => Token::Add,
                    "COLUMN" => Token::Column,
                    "INTEGER" => Token::Integer,
                    "UNSIGNED" => Token::Unsigned,
                    "FLOAT" => Token::Float,
                    "VARCHAR" => Token::Varchar,
                    "BOOLEAN" | "BOOL" => Token::Boolean,
                    "DATE" => Token::Date,
                    "TIMESTAMP" => Token::Timestamp,
                    "DECIMAL" => Token::Decimal,
                    "NUMERIC" => Token::Numeric,
                    "ORDER" => Token::Order,
                    "BY" => Token::By,
                    "GROUP" => Token::Group,
                    "LIMIT" => Token::Limit,
                    "OFFSET" => Token::Offset,
                    "ASC" => Token::Asc,
                    "DESC" => Token::Desc,
                    "COUNT" => Token::Count,
                    "SUM" => Token::Sum,
                    "AVG" => Token::Avg,
                    "MIN" => Token::Min,
                    "MAX" => Token::Max,
                    "DISTINCT" => Token::Distinct,
                    "IN" => Token::In,
                    "TO" => Token::To,
                    "SELECT" => Token::Select,
                    "FROM" => Token::From,
                    "WHERE" => Token::Where,
                    "INDEX" => Token::Index,
                    "ON" => Token::On,
                    "USING" => Token::Using,
                    "JOIN" => Token::Join,
                    "AND" => Token::And,
                    "DELETE" => Token::Delete,
                    "UPDATE" => Token::Update,
                    "SET" => Token::Set,
                    "TRUE" => Token::True,
                    "FALSE" => Token::False,
                    "PRIMARY" => Token::Primary,
                    "KEY" => Token::Key,
                    "UNIQUE" => Token::Unique,
                    "REFERENCES" => Token::References,
                    "NOT" => Token::Not,
                    "NULL" => Token::Null,
                    "CHECK" => Token::Check,
                    _ => Token::Identifier(ident),
                };
                Ok(token)
            }
            Some(ch) => Err(ParseError::InvalidSyntax(format!(
                "Unexpected character: {}",
                ch
            ))),
        }
    }

    pub(crate) fn tokenize(&mut self) -> Result<Vec<Token>, ParseError> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }
        Ok(tokens)
    }
}

/// Parser
struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    fn current(&self) -> &Token {
        self.tokens.get(self.position).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }

    fn expect(&mut self, expected: Token) -> Result<(), ParseError> {
        let current = self.current().clone();
        if std::mem::discriminant(&current) == std::mem::discriminant(&expected) {
            self.advance();
            Ok(())
        } else {
            Err(ParseError::UnexpectedToken {
                expected: format!("{}", expected),
                found: format!("{}", current),
            })
        }
    }

    fn parse_data_type(&mut self) -> Result<DataType, ParseError> {
        let token = self.current().clone();
        match token {
            Token::Integer => {
                self.advance();
                Ok(DataType::Integer)
            }
            Token::Unsigned => {
                self.advance();
                Ok(DataType::Unsigned)
            }
            Token::Float => {
                self.advance();
                Ok(DataType::Float)
            }
            Token::Varchar => {
                self.advance();
                Ok(DataType::Varchar)
            }
            Token::Boolean => {
                self.advance();
                Ok(DataType::Boolean)
            }
            Token::Date => {
                self.advance();
                Ok(DataType::Date)
            }
            Token::Timestamp => {
                self.advance();
                Ok(DataType::Timestamp)
            }
            Token::Decimal | Token::Numeric => {
                self.advance();
                Ok(DataType::Decimal)
            }
            _ => Err(ParseError::UnexpectedToken {
                expected: "data type (INTEGER, UNSIGNED, FLOAT, BOOLEAN, VARCHAR, DATE, TIMESTAMP, or DECIMAL)".to_string(),
                found: format!("{}", token),
            }),
        }
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        let name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "column name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        let data_type = self.parse_data_type()?;

        let mut column = ColumnDef::new(name, data_type);
        loop {
            match self.current() {
                Token::Primary => {
                    self.advance();
                    self.expect(Token::Key)?;
                    column.is_primary_key = true;
                    column.is_unique = true;
                    column.is_not_null = true;
                }
                Token::Unique => {
                    self.advance();
                    column.is_unique = true;
                }
                Token::Not => {
                    self.advance();
                    self.expect(Token::Null)?;
                    column.is_not_null = true;
                }
                Token::References => {
                    self.advance();
                    let table = match self.current() {
                        Token::Identifier(s) => {
                            let name = s.clone();
                            self.advance();
                            name
                        }
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "referenced table name".to_string(),
                                found: format!("{}", self.current()),
                            });
                        }
                    };
                    self.expect(Token::LeftParen)?;
                    let column_name = match self.current() {
                        Token::Identifier(s) => {
                            let name = s.clone();
                            self.advance();
                            name
                        }
                        _ => {
                            return Err(ParseError::UnexpectedToken {
                                expected: "referenced column name".to_string(),
                                found: format!("{}", self.current()),
                            });
                        }
                    };
                    self.expect(Token::RightParen)?;
                    column.references = Some(ForeignKeyRef::new(table, column_name));
                }
                Token::Check => {
                    self.advance();
                    self.expect(Token::LeftParen)?;
                    let expr = self.parse_expression()?;
                    self.expect(Token::RightParen)?;
                    column.check = Some(expr);
                }
                _ => break,
            }
        }

        Ok(column)
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStmt, ParseError> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;

        let table_name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "table name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        self.expect(Token::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            columns.push(self.parse_column_def()?);

            if matches!(self.current(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        self.expect(Token::RightParen)?;

        Ok(CreateTableStmt::new(table_name, columns))
    }

    fn parse_create_index(&mut self) -> Result<CreateIndexStmt, ParseError> {
        self.expect(Token::Create)?;
        self.expect(Token::Index)?;

        let index_name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "index name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        self.expect(Token::On)?;

        let table_name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "table name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        let mut index_type = IndexType::BTree;
        if matches!(self.current(), Token::Using) {
            self.advance();
            index_type = match self.current() {
                Token::Identifier(s) if s.eq_ignore_ascii_case("hash") => IndexType::Hash,
                Token::Identifier(s) if s.eq_ignore_ascii_case("btree") => IndexType::BTree,
                token => {
                    return Err(ParseError::InvalidSyntax(format!(
                        "Unsupported index type {}",
                        token
                    )));
                }
            };
            self.advance();
        }

        self.expect(Token::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            let column_name = match self.current() {
                Token::Identifier(s) => {
                    let name = s.clone();
                    self.advance();
                    name
                }
                _ => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "column name".to_string(),
                        found: format!("{}", self.current()),
                    });
                }
            };
            columns.push(column_name);

            match self.current() {
                Token::Comma => self.advance(),
                _ => break,
            }
        }

        self.expect(Token::RightParen)?;

        Ok(CreateIndexStmt::with_type(
            index_name, table_name, columns, index_type,
        ))
    }

    fn parse_drop_table(&mut self) -> Result<DropTableStmt, ParseError> {
        self.expect(Token::Drop)?;
        self.expect(Token::Table)?;

        let table_name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "table name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        Ok(DropTableStmt::new(table_name))
    }

    fn parse_drop_index(&mut self) -> Result<DropIndexStmt, ParseError> {
        self.expect(Token::Drop)?;
        self.expect(Token::Index)?;

        let index_name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "index name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        Ok(DropIndexStmt::new(index_name))
    }

    fn parse_transaction_stmt(
        &mut self,
        command: TransactionCommand,
    ) -> Result<TransactionStmt, ParseError> {
        match command {
            TransactionCommand::Begin => self.expect(Token::Begin)?,
            TransactionCommand::Commit => self.expect(Token::Commit)?,
            TransactionCommand::Rollback => self.expect(Token::Rollback)?,
        }

        if matches!(self.current(), Token::Transaction) {
            self.advance();
        }

        Ok(TransactionStmt::new(command))
    }

    fn parse_delete(&mut self) -> Result<DeleteStmt, ParseError> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;

        let table_name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "table name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        // Parse optional WHERE clause
        let where_clause = if matches!(self.current(), Token::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(DeleteStmt::new(table_name, where_clause))
    }

    fn parse_update(&mut self) -> Result<UpdateStmt, ParseError> {
        self.expect(Token::Update)?;

        let table_name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "table name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        self.expect(Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column_name = match self.current() {
                Token::Identifier(s) => {
                    let name = s.clone();
                    self.advance();
                    name
                }
                _ => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "column name".to_string(),
                        found: format!("{}", self.current()),
                    });
                }
            };

            self.expect(Token::Equals)?;
            let expr = self.parse_expression()?;
            if matches!(expr, Expr::BinaryOp { .. }) {
                return Err(ParseError::InvalidSyntax(
                    "SET expressions must be a column or literal".to_string(),
                ));
            }

            assignments.push((column_name, expr));

            if matches!(self.current(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        let where_clause = if matches!(self.current(), Token::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(UpdateStmt::new(table_name, assignments, where_clause))
    }

    fn parse_literal(&mut self) -> Result<Literal, ParseError> {
        let token = self.current().clone();
        match token {
            Token::IntegerLiteral(i) => {
                self.advance();
                Ok(Literal::Integer(i))
            }
            Token::FloatLiteral(fv) => {
                self.advance();
                Ok(Literal::Float(fv))
            }
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Literal::String(s))
            }
            Token::True => {
                self.advance();
                Ok(Literal::Boolean(true))
            }
            Token::False => {
                self.advance();
                Ok(Literal::Boolean(false))
            }
            Token::Null => {
                self.advance();
                Ok(Literal::Null)
            }
            Token::Date => {
                self.advance();
                let literal = match self.current() {
                    Token::StringLiteral(s) => {
                        let value = s.clone();
                        self.advance();
                        value
                    }
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "date string literal".to_string(),
                            found: format!("{}", self.current()),
                        });
                    }
                };
                Ok(Literal::Date(literal))
            }
            Token::Timestamp => {
                self.advance();
                let literal = match self.current() {
                    Token::StringLiteral(s) => {
                        let value = s.clone();
                        self.advance();
                        value
                    }
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "timestamp string literal".to_string(),
                            found: format!("{}", self.current()),
                        });
                    }
                };
                Ok(Literal::Timestamp(literal))
            }
            Token::Decimal | Token::Numeric => {
                self.advance();
                let literal = match self.current() {
                    Token::StringLiteral(s) => {
                        let value = s.clone();
                        self.advance();
                        value
                    }
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "decimal string literal".to_string(),
                            found: format!("{}", self.current()),
                        });
                    }
                };
                Ok(Literal::Decimal(literal))
            }
            _ => Err(ParseError::UnexpectedToken {
                expected: "literal value".to_string(),
                found: format!("{}", token),
            }),
        }
    }

    fn parse_insert(&mut self) -> Result<InsertStmt, ParseError> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;

        let table_name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "table name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        self.expect(Token::Values)?;
        let mut rows = Vec::new();
        loop {
            self.expect(Token::LeftParen)?;

            let mut values = Vec::new();
            loop {
                values.push(self.parse_literal()?);

                if matches!(self.current(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }

            self.expect(Token::RightParen)?;
            rows.push(values);

            if matches!(self.current(), Token::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        Ok(InsertStmt::new(table_name, rows))
    }

    fn parse_binary_op(&mut self) -> Result<BinaryOp, ParseError> {
        let token = self.current().clone();
        let op = match token {
            Token::Equals => BinaryOp::Eq,
            Token::NotEquals => BinaryOp::NotEq,
            Token::LessThan => BinaryOp::Lt,
            Token::LessThanEquals => BinaryOp::LtEq,
            Token::GreaterThan => BinaryOp::Gt,
            Token::GreaterThanEquals => BinaryOp::GtEq,
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "comparison operator (=, !=, <, <=, >, >=)".to_string(),
                    found: format!("{}", token),
                });
            }
        };
        self.advance();
        Ok(op)
    }

    fn parse_primary_expr(&mut self) -> Result<Expr, ParseError> {
        let token = self.current().clone();
        match token {
            Token::Identifier(_) => {
                let col_ref = self.parse_column_ref()?;
                Ok(Expr::Column(col_ref))
            }
            Token::IntegerLiteral(i) => {
                self.advance();
                Ok(Expr::Literal(Literal::Integer(i)))
            }
            Token::FloatLiteral(fv) => {
                self.advance();
                Ok(Expr::Literal(Literal::Float(fv)))
            }
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Expr::Literal(Literal::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(false)))
            }
            Token::Null | Token::Date | Token::Timestamp | Token::Decimal | Token::Numeric => {
                let literal = self.parse_literal()?;
                Ok(Expr::Literal(literal))
            }
            _ => Err(ParseError::UnexpectedToken {
                expected: "column name or literal".to_string(),
                found: format!("{}", token),
            }),
        }
    }

    fn parse_expression(&mut self) -> Result<Expr, ParseError> {
        let mut expr = self.parse_comparison_expr()?;

        while matches!(self.current(), Token::And) {
            self.advance();
            let right = self.parse_comparison_expr()?;
            expr = Expr::binary_op(expr, BinaryOp::And, right);
        }

        Ok(expr)
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_primary_expr()?;

        // Check if there's a binary operator
        match self.current() {
            Token::Equals
            | Token::NotEquals
            | Token::LessThan
            | Token::LessThanEquals
            | Token::GreaterThan
            | Token::GreaterThanEquals => {
                let op = self.parse_binary_op()?;
                let right = self.parse_primary_expr()?;
                Ok(Expr::binary_op(left, op, right))
            }
            Token::In => {
                self.advance();
                self.expect(Token::LeftParen)?;
                let subquery = self.parse_select()?;
                self.expect(Token::RightParen)?;
                Ok(Expr::in_subquery(left, subquery))
            }
            _ => Ok(left),
        }
    }

    fn parse_column_ref(&mut self) -> Result<ColumnRef, ParseError> {
        let base_name = match self.current() {
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "column name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        if matches!(self.current(), Token::Dot) {
            self.advance();
            let column_name = match self.current() {
                Token::Identifier(name) => {
                    let name = name.clone();
                    self.advance();
                    name
                }
                _ => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "column name".to_string(),
                        found: format!("{}", self.current()),
                    });
                }
            };

            Ok(ColumnRef::new(Some(base_name), column_name))
        } else {
            Ok(ColumnRef::new(None, base_name))
        }
    }

    fn parse_select(&mut self) -> Result<SelectStmt, ParseError> {
        self.expect(Token::Select)?;

        let distinct = if matches!(self.current(), Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        // Parse column list or *
        let columns = if matches!(self.current(), Token::Asterisk) {
            self.advance();
            SelectColumn::All
        } else {
            let mut items = Vec::new();
            loop {
                items.push(self.parse_select_item()?);

                if matches!(self.current(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            SelectColumn::Items(items)
        };

        self.expect(Token::From)?;

        let left_table = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "table name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        let mut from = FromClause::Table(left_table.clone());

        if matches!(self.current(), Token::Join) {
            self.advance();

            let right_table = match self.current() {
                Token::Identifier(s) => {
                    let name = s.clone();
                    self.advance();
                    name
                }
                _ => {
                    return Err(ParseError::UnexpectedToken {
                        expected: "table name".to_string(),
                        found: format!("{}", self.current()),
                    });
                }
            };

            self.expect(Token::On)?;
            let left_column = self.parse_column_ref()?;
            let op = self.parse_binary_op()?;
            if op != BinaryOp::Eq {
                return Err(ParseError::InvalidSyntax(
                    "JOIN only supports equality conditions".to_string(),
                ));
            }
            let right_column = self.parse_column_ref()?;

            from = FromClause::Join {
                left_table,
                right_table,
                left_column,
                right_column,
            };
        }

        // Parse optional WHERE clause
        let where_clause = if matches!(self.current(), Token::Where) {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        let mut group_by = Vec::new();
        if matches!(self.current(), Token::Group) {
            self.advance();
            self.expect(Token::By)?;
            loop {
                group_by.push(self.parse_column_ref()?);
                if matches!(self.current(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        let mut order_by = Vec::new();
        if matches!(self.current(), Token::Order) {
            self.advance();
            self.expect(Token::By)?;
            loop {
                let col_ref = self.parse_column_ref()?;
                let ascending = match self.current() {
                    Token::Asc => {
                        self.advance();
                        true
                    }
                    Token::Desc => {
                        self.advance();
                        false
                    }
                    _ => true,
                };
                order_by.push(OrderByExpr::new(col_ref, ascending));

                if matches!(self.current(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        let mut limit = None;
        let mut offset = None;
        if matches!(self.current(), Token::Limit) {
            self.advance();
            limit = Some(self.parse_non_negative_usize("LIMIT")?);
        }
        if matches!(self.current(), Token::Offset) {
            self.advance();
            offset = Some(self.parse_non_negative_usize("OFFSET")?);
        }

        Ok(SelectStmt::new(
            columns,
            from,
            where_clause,
            group_by,
            distinct,
            order_by,
            limit,
            offset,
        ))
    }

    fn parse_alter_table(&mut self) -> Result<AlterTableStmt, ParseError> {
        self.expect(Token::Alter)?;
        self.expect(Token::Table)?;

        let table_name = match self.current() {
            Token::Identifier(s) => {
                let name = s.clone();
                self.advance();
                name
            }
            _ => {
                return Err(ParseError::UnexpectedToken {
                    expected: "table name".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        match self.current() {
            Token::Add => {
                self.advance();
                if matches!(self.current(), Token::Column) {
                    self.advance();
                }
                let column_def = self.parse_column_def()?;
                if column_def.is_primary_key
                    || column_def.is_unique
                    || column_def.is_not_null
                    || column_def.check.is_some()
                    || column_def.references.is_some()
                {
                    return Err(ParseError::InvalidSyntax(
                        "ALTER TABLE ADD COLUMN does not support constraints yet".to_string(),
                    ));
                }
                Ok(AlterTableStmt::new(
                    table_name,
                    AlterTableAction::AddColumn(column_def),
                ))
            }
            Token::Drop => {
                self.advance();
                if matches!(self.current(), Token::Column) {
                    self.advance();
                }
                let column_name = match self.current() {
                    Token::Identifier(name) => {
                        let name = name.clone();
                        self.advance();
                        name
                    }
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "column name".to_string(),
                            found: format!("{}", self.current()),
                        });
                    }
                };
                Ok(AlterTableStmt::new(
                    table_name,
                    AlterTableAction::DropColumn(column_name),
                ))
            }
            Token::Rename => {
                self.advance();
                if matches!(self.current(), Token::Column) {
                    self.advance();
                }
                let from_name = match self.current() {
                    Token::Identifier(name) => {
                        let name = name.clone();
                        self.advance();
                        name
                    }
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "column name".to_string(),
                            found: format!("{}", self.current()),
                        });
                    }
                };
                self.expect(Token::To)?;
                let to_name = match self.current() {
                    Token::Identifier(name) => {
                        let name = name.clone();
                        self.advance();
                        name
                    }
                    _ => {
                        return Err(ParseError::UnexpectedToken {
                            expected: "column name".to_string(),
                            found: format!("{}", self.current()),
                        });
                    }
                };
                Ok(AlterTableStmt::new(
                    table_name,
                    AlterTableAction::RenameColumn {
                        from: from_name,
                        to: to_name,
                    },
                ))
            }
            _ => Err(ParseError::UnexpectedToken {
                expected: "ADD, DROP, or RENAME".to_string(),
                found: format!("{}", self.current()),
            }),
        }
    }

    fn parse_select_item(&mut self) -> Result<SelectItem, ParseError> {
        let token = self.current().clone();
        match token {
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                let func = match token {
                    Token::Count => AggregateFunc::Count,
                    Token::Sum => AggregateFunc::Sum,
                    Token::Avg => AggregateFunc::Avg,
                    Token::Min => AggregateFunc::Min,
                    Token::Max => AggregateFunc::Max,
                    _ => unreachable!("aggregate token matched above"),
                };
                self.advance();
                self.expect(Token::LeftParen)?;
                let target = if matches!(self.current(), Token::Asterisk) {
                    if func != AggregateFunc::Count {
                        return Err(ParseError::InvalidSyntax(
                            "Only COUNT supports '*'".to_string(),
                        ));
                    }
                    self.advance();
                    AggregateTarget::All
                } else {
                    let col = self.parse_column_ref()?;
                    AggregateTarget::Column(col)
                };
                self.expect(Token::RightParen)?;
                Ok(SelectItem::Aggregate(AggregateExpr::new(func, target)))
            }
            _ => {
                let col = self.parse_column_ref()?;
                Ok(SelectItem::Column(col))
            }
        }
    }

    fn parse_non_negative_usize(&mut self, label: &str) -> Result<usize, ParseError> {
        let token = self.current().clone();
        match token {
            Token::IntegerLiteral(value) => {
                self.advance();
                if value < 0 {
                    return Err(ParseError::InvalidSyntax(format!(
                        "{} must be non-negative",
                        label
                    )));
                }
                value.try_into().map_err(|_| {
                    ParseError::InvalidSyntax(format!("{} value too large", label))
                })
            }
            _ => Err(ParseError::UnexpectedToken {
                expected: format!("{} integer literal", label),
                found: format!("{}", token),
            }),
        }
    }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.current() {
            Token::Create => {
                // Peek at next token to determine if it's TABLE or INDEX
                self.advance();
                match self.current() {
                    Token::Table => {
                        // Rewind to CREATE
                        self.position -= 1;
                        let stmt = self.parse_create_table()?;
                        Ok(Statement::CreateTable(stmt))
                    }
                    Token::Index => {
                        // Rewind to CREATE
                        self.position -= 1;
                        let stmt = self.parse_create_index()?;
                        Ok(Statement::CreateIndex(stmt))
                    }
                    token => Err(ParseError::UnexpectedToken {
                        expected: "TABLE or INDEX".to_string(),
                        found: format!("{}", token),
                    }),
                }
            }
            Token::Drop => {
                // Peek at next token to determine if it's TABLE or INDEX
                self.advance();
                match self.current() {
                    Token::Table => {
                        // Rewind to DROP
                        self.position -= 1;
                        let stmt = self.parse_drop_table()?;
                        Ok(Statement::DropTable(stmt))
                    }
                    Token::Index => {
                        // Rewind to DROP
                        self.position -= 1;
                        let stmt = self.parse_drop_index()?;
                        Ok(Statement::DropIndex(stmt))
                    }
                    token => Err(ParseError::UnexpectedToken {
                        expected: "TABLE or INDEX".to_string(),
                        found: format!("{}", token),
                    }),
                }
            }
            Token::Alter => {
                let stmt = self.parse_alter_table()?;
                Ok(Statement::AlterTable(stmt))
            }
            Token::Begin => {
                let stmt = self.parse_transaction_stmt(TransactionCommand::Begin)?;
                Ok(Statement::Transaction(stmt))
            }
            Token::Commit => {
                let stmt = self.parse_transaction_stmt(TransactionCommand::Commit)?;
                Ok(Statement::Transaction(stmt))
            }
            Token::Rollback => {
                let stmt = self.parse_transaction_stmt(TransactionCommand::Rollback)?;
                Ok(Statement::Transaction(stmt))
            }
            Token::Insert => {
                let stmt = self.parse_insert()?;
                Ok(Statement::Insert(stmt))
            }
            Token::Select => {
                let stmt = self.parse_select()?;
                Ok(Statement::Select(stmt))
            }
            Token::Delete => {
                let stmt = self.parse_delete()?;
                Ok(Statement::Delete(stmt))
            }
            Token::Update => {
                let stmt = self.parse_update()?;
                Ok(Statement::Update(stmt))
            }
            Token::Eof => Err(ParseError::UnexpectedEof),
            token => Err(ParseError::UnexpectedToken {
                expected: "SQL statement".to_string(),
                found: format!("{}", token),
            }),
        }
    }
}

/// Parse SQL string into a Statement
pub fn parse_sql(sql: &str) -> Result<Statement, ParseError> {
    let mut tokenizer = Tokenizer::new(sql);
    let tokens = tokenizer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement()?;
    while matches!(parser.current(), Token::Semicolon) {
        parser.advance();
    }
    if !matches!(parser.current(), Token::Eof) {
        return Err(ParseError::UnexpectedToken {
            expected: "end of input".to_string(),
            found: format!("{}", parser.current()),
        });
    }
    Ok(stmt)
}

/// Parse SQL input into multiple statements separated by semicolons.
pub fn parse_sql_statements(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let mut tokenizer = Tokenizer::new(sql);
    let tokens = tokenizer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let mut statements = Vec::new();

    loop {
        while matches!(parser.current(), Token::Semicolon) {
            parser.advance();
        }

        if matches!(parser.current(), Token::Eof) {
            break;
        }

        let stmt = parser.parse_statement()?;
        statements.push(stmt);

        while matches!(parser.current(), Token::Semicolon) {
            parser.advance();
        }
    }

    Ok(statements)
}
