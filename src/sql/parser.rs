use super::ast::{
    BinaryOp, ColumnDef, ColumnRef, CreateIndexStmt, CreateTableStmt, DataType, DeleteStmt,
    DropIndexStmt, DropTableStmt, Expr, FromClause, InsertStmt, Literal, SelectColumn, SelectStmt,
    Statement, UpdateStmt,
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
    Table,
    Insert,
    Into,
    Values,
    Integer,
    Varchar,
    Boolean,
    Float,
    Unsigned,
    True,
    False,
    Select,
    From,
    Where,
    Index,
    On,
    Join,
    And,
    Delete,
    Dot,
    Update,
    Set,

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
            | (Token::Table, Token::Table)
            | (Token::Insert, Token::Insert)
            | (Token::Into, Token::Into)
            | (Token::Values, Token::Values)
            | (Token::Integer, Token::Integer)
            | (Token::Unsigned, Token::Unsigned)
            | (Token::Float, Token::Float)
            | (Token::Varchar, Token::Varchar)
            | (Token::Boolean, Token::Boolean)
            | (Token::True, Token::True)
            | (Token::False, Token::False)
            | (Token::Select, Token::Select)
            | (Token::From, Token::From)
            | (Token::Where, Token::Where)
            | (Token::Index, Token::Index)
            | (Token::On, Token::On)
            | (Token::Join, Token::Join)
            | (Token::And, Token::And)
            | (Token::Delete, Token::Delete)
            | (Token::Dot, Token::Dot)
            | (Token::Update, Token::Update)
            | (Token::Set, Token::Set)
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
            Token::Table => write!(f, "TABLE"),
            Token::Insert => write!(f, "INSERT"),
            Token::Into => write!(f, "INTO"),
            Token::Values => write!(f, "VALUES"),
            Token::Integer => write!(f, "INTEGER"),
            Token::Varchar => write!(f, "VARCHAR"),
            Token::Boolean => write!(f, "BOOLEAN"),
            Token::Unsigned => write!(f, "UNSIGNED"),
            Token::Float => write!(f, "FLOAT"),
            Token::Select => write!(f, "SELECT"),
            Token::From => write!(f, "FROM"),
            Token::Where => write!(f, "WHERE"),
            Token::Index => write!(f, "INDEX"),
            Token::On => write!(f, "ON"),
            Token::Join => write!(f, "JOIN"),
            Token::And => write!(f, "AND"),
            Token::Delete => write!(f, "DELETE"),
            Token::Dot => write!(f, "."),
            Token::Update => write!(f, "UPDATE"),
            Token::Set => write!(f, "SET"),
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
                    "TABLE" => Token::Table,
                    "INSERT" => Token::Insert,
                    "INTO" => Token::Into,
                    "VALUES" => Token::Values,
                    "INTEGER" => Token::Integer,
                    "UNSIGNED" => Token::Unsigned,
                    "FLOAT" => Token::Float,
                    "VARCHAR" => Token::Varchar,
                    "BOOLEAN" | "BOOL" => Token::Boolean,
                    "SELECT" => Token::Select,
                    "FROM" => Token::From,
                    "WHERE" => Token::Where,
                    "INDEX" => Token::Index,
                    "ON" => Token::On,
                    "JOIN" => Token::Join,
                    "AND" => Token::And,
                    "DELETE" => Token::Delete,
                    "UPDATE" => Token::Update,
                    "SET" => Token::Set,
                    "TRUE" => Token::True,
                    "FALSE" => Token::False,
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
            _ => Err(ParseError::UnexpectedToken {
                expected: "data type (INTEGER, UNSIGNED, FLOAT, BOOLEAN, or VARCHAR)".to_string(),
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

        Ok(ColumnDef::new(name, data_type))
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

        Ok(CreateIndexStmt::new(index_name, table_name, columns))
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

        // Parse column list or *
        let columns = if matches!(self.current(), Token::Asterisk) {
            self.advance();
            SelectColumn::All
        } else {
            let mut column_names = Vec::new();
            loop {
                column_names.push(self.parse_column_ref()?);

                if matches!(self.current(), Token::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            SelectColumn::Columns(column_names)
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

        Ok(SelectStmt::new(columns, from, where_clause))
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
    parser.parse_statement()
}
