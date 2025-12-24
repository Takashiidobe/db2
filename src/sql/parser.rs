use super::ast::{
    BinaryOp, ColumnDef, ColumnRef, CreateIndexStmt, CreateTableStmt, DataType, Expr, FromClause, InsertStmt,
    Literal, SelectColumn, SelectStmt, Statement,
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
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    // Keywords
    Create,
    Table,
    Insert,
    Into,
    Values,
    Integer,
    Varchar,
    Select,
    From,
    Where,
    Index,
    On,
    Join,
    Dot,

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
    IntegerLiteral(i64),
    StringLiteral(String),

    // End of input
    Eof,
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Create => write!(f, "CREATE"),
            Token::Table => write!(f, "TABLE"),
            Token::Insert => write!(f, "INSERT"),
            Token::Into => write!(f, "INTO"),
            Token::Values => write!(f, "VALUES"),
            Token::Integer => write!(f, "INTEGER"),
            Token::Varchar => write!(f, "VARCHAR"),
            Token::Select => write!(f, "SELECT"),
            Token::From => write!(f, "FROM"),
            Token::Where => write!(f, "WHERE"),
            Token::Index => write!(f, "INDEX"),
            Token::On => write!(f, "ON"),
            Token::Join => write!(f, "JOIN"),
            Token::Dot => write!(f, "."),
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
            Token::StringLiteral(s) => write!(f, "string '{}'", s),
            Token::Eof => write!(f, "end of input"),
        }
    }
}

/// Tokenizer (lexer)
struct Tokenizer {
    input: Vec<char>,
    position: usize,
}

impl Tokenizer {
    fn new(input: &str) -> Self {
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

    fn read_number(&mut self) -> Result<i64, ParseError> {
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

        if result.is_empty() {
            return Err(ParseError::InvalidSyntax("Invalid number".to_string()));
        }

        let num: i64 = result
            .parse()
            .map_err(|_| ParseError::InvalidSyntax("Invalid number".to_string()))?;

        Ok(if is_negative { -num } else { num })
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
                    Err(ParseError::InvalidSyntax("Expected '=' after '!'".to_string()))
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
                Ok(Token::IntegerLiteral(num))
            }
            Some(ch) if ch.is_alphabetic() || ch == '_' => {
                let ident = self.read_identifier();
                let token = match ident.to_uppercase().as_str() {
                    "CREATE" => Token::Create,
                    "TABLE" => Token::Table,
                    "INSERT" => Token::Insert,
                    "INTO" => Token::Into,
                    "VALUES" => Token::Values,
                    "INTEGER" => Token::Integer,
                    "VARCHAR" => Token::Varchar,
                    "SELECT" => Token::Select,
                    "FROM" => Token::From,
                    "WHERE" => Token::Where,
                    "INDEX" => Token::Index,
                    "ON" => Token::On,
                    "JOIN" => Token::Join,
                    _ => Token::Identifier(ident),
                };
                Ok(token)
            }
            Some(ch) => Err(ParseError::InvalidSyntax(format!("Unexpected character: {}", ch))),
        }
    }

    fn tokenize(&mut self) -> Result<Vec<Token>, ParseError> {
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
        Self { tokens, position: 0 }
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
            Token::Varchar => {
                self.advance();
                Ok(DataType::Varchar)
            }
            _ => Err(ParseError::UnexpectedToken {
                expected: "data type (INTEGER or VARCHAR)".to_string(),
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
                })
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
                })
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
                })
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
                })
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
                    expected: "column name".to_string(),
                    found: format!("{}", self.current()),
                })
            }
        };

        self.expect(Token::RightParen)?;

        Ok(CreateIndexStmt::new(index_name, table_name, column_name))
    }

    fn parse_literal(&mut self) -> Result<Literal, ParseError> {
        let token = self.current().clone();
        match token {
            Token::IntegerLiteral(i) => {
                self.advance();
                Ok(Literal::Integer(i))
            }
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Literal::String(s))
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
                })
            }
        };

        self.expect(Token::Values)?;
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

        Ok(InsertStmt::new(table_name, values))
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
                })
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
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Expr::Literal(Literal::String(s)))
            }
            _ => Err(ParseError::UnexpectedToken {
                expected: "column name or literal".to_string(),
                found: format!("{}", token),
            }),
        }
    }

    fn parse_expression(&mut self) -> Result<Expr, ParseError> {
        let left = self.parse_primary_expr()?;

        // Check if there's a binary operator
        match self.current() {
            Token::Equals | Token::NotEquals | Token::LessThan | Token::LessThanEquals
            | Token::GreaterThan | Token::GreaterThanEquals => {
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
                })
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
                    })
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
                })
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
                    })
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
            Token::Insert => {
                let stmt = self.parse_insert()?;
                Ok(Statement::Insert(stmt))
            }
            Token::Select => {
                let stmt = self.parse_select()?;
                Ok(Statement::Select(stmt))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_create_table() {
        let mut tokenizer = Tokenizer::new("CREATE TABLE users (id INTEGER, name VARCHAR)");
        let tokens = tokenizer.tokenize().unwrap();

        assert_eq!(tokens[0], Token::Create);
        assert_eq!(tokens[1], Token::Table);
        assert_eq!(tokens[2], Token::Identifier("users".to_string()));
    }

    #[test]
    fn test_parse_create_table_simple() {
        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table_name, "users");
                assert_eq!(create.columns.len(), 2);
                assert_eq!(create.columns[0].name, "id");
                assert_eq!(create.columns[0].data_type, DataType::Integer);
                assert_eq!(create.columns[1].name, "name");
                assert_eq!(create.columns[1].data_type, DataType::Varchar);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_create_table_case_insensitive() {
        let sql = "create table Users (ID integer, Name varchar)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table_name, "Users");
                assert_eq!(create.columns.len(), 2);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO users VALUES (1, 'Alice')";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.table_name, "users");
                assert_eq!(insert.values.len(), 2);
                assert_eq!(insert.values[0], Literal::Integer(1));
                assert_eq!(insert.values[1], Literal::String("Alice".to_string()));
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_insert_multiple_values() {
        let sql = "INSERT INTO test VALUES (42, 'hello', -100, 'world')";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.values.len(), 4);
                assert_eq!(insert.values[0], Literal::Integer(42));
                assert_eq!(insert.values[1], Literal::String("hello".to_string()));
                assert_eq!(insert.values[2], Literal::Integer(-100));
                assert_eq!(insert.values[3], Literal::String("world".to_string()));
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_string_with_escaped_quote() {
        let sql = "INSERT INTO test VALUES ('it''s working')";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Insert(insert) => {
                assert_eq!(insert.values[0], Literal::String("it's working".to_string()));
            }
            _ => panic!("Expected Insert statement"),
        }
    }

    #[test]
    fn test_parse_create_table_single_column() {
        let sql = "CREATE TABLE test (id INTEGER)";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.columns.len(), 1);
                assert_eq!(create.columns[0].name, "id");
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_error_missing_paren() {
        let sql = "CREATE TABLE test (id INTEGER";
        let result = parse_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_error_invalid_type() {
        let sql = "CREATE TABLE test (id INVALID)";
        let result = parse_sql(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_whitespace_handling() {
        let sql = "  CREATE   TABLE   users  (  id   INTEGER  ,  name  VARCHAR  )  ";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::CreateTable(create) => {
                assert_eq!(create.table_name, "users");
                assert_eq!(create.columns.len(), 2);
            }
            _ => panic!("Expected CreateTable statement"),
        }
    }

    #[test]
    fn test_parse_select_join() {
        let sql = "SELECT users.id, orders.amount FROM users JOIN orders ON users.id = orders.user_id WHERE orders.amount > 10";
        let stmt = parse_sql(sql).unwrap();

        match stmt {
            Statement::Select(select) => {
                if let SelectColumn::Columns(cols) = select.columns {
                    assert_eq!(cols.len(), 2);
                    assert_eq!(cols[0].table.as_deref(), Some("users"));
                    assert_eq!(cols[0].column, "id");
                    assert_eq!(cols[1].table.as_deref(), Some("orders"));
                    assert_eq!(cols[1].column, "amount");
                } else {
                    panic!("Expected column list");
                }

                match select.from {
                    FromClause::Join { left_table, right_table, left_column, right_column } => {
                        assert_eq!(left_table, "users");
                        assert_eq!(right_table, "orders");
                        assert_eq!(left_column.table.as_deref(), Some("users"));
                        assert_eq!(left_column.column, "id");
                        assert_eq!(right_column.table.as_deref(), Some("orders"));
                        assert_eq!(right_column.column, "user_id");
                    }
                    _ => panic!("Expected JOIN in FROM"),
                }
            }
            _ => panic!("Expected Select statement"),
        }
    }
}
