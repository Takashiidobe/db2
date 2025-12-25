use db::sql::{parse_sql, Executor, ExecutionResult};
use std::io;
use std::path::Path;
use tempfile::TempDir;

/// Test database wrapper for integration tests
pub struct TestDb {
    executor: Executor,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl TestDb {
    /// Create a new test database with a temporary directory
    pub fn new() -> io::Result<Self> {
        let temp_dir = TempDir::new()?;
        let executor = Executor::new(temp_dir.path(), 100)?;
        Ok(Self { executor, temp_dir })
    }

    /// Execute a SQL statement and return the result
    pub fn execute(&mut self, sql: &str) -> io::Result<ExecutionResult> {
        let stmt = parse_sql(sql).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        self.executor.execute(stmt)
    }

    /// Execute a SQL statement and expect success
    pub fn execute_ok(&mut self, sql: &str) -> ExecutionResult {
        self.execute(sql)
            .unwrap_or_else(|e| panic!("Expected SQL to succeed but got error: {}\nSQL: {}", e, sql))
    }

    /// Execute a SQL statement and expect failure
    pub fn execute_err(&mut self, sql: &str) -> io::Error {
        self.execute(sql)
            .expect_err(&format!("Expected SQL to fail but it succeeded\nSQL: {}", sql))
    }

    /// Get the path to the database directory
    pub fn path(&self) -> &Path {
        self.temp_dir.path()
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<(String, db::types::Schema)> {
        self.executor.list_tables()
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<(String, String, Vec<String>)> {
        self.executor.list_indexes()
    }

    /// Flush all data to disk
    pub fn flush(&mut self) -> io::Result<()> {
        self.executor.flush_all()
    }
}

/// Helper macro for asserting execution results
#[macro_export]
macro_rules! assert_create_table {
    ($result:expr, $table_name:expr) => {
        match $result {
            ExecutionResult::CreateTable { table_name } => {
                assert_eq!(table_name, $table_name);
            }
            other => panic!("Expected CreateTable result, got: {:?}", other),
        }
    };
}

#[macro_export]
macro_rules! assert_drop_table {
    ($result:expr, $table_name:expr) => {
        match $result {
            ExecutionResult::DropTable { table_name } => {
                assert_eq!(table_name, $table_name);
            }
            other => panic!("Expected DropTable result, got: {:?}", other),
        }
    };
}

#[macro_export]
macro_rules! assert_insert {
    ($result:expr, $row_count:expr) => {
        match $result {
            ExecutionResult::Insert { row_ids } => {
                assert_eq!(row_ids.len(), $row_count);
            }
            other => panic!("Expected Insert result with {} rows, got: {:?}", $row_count, other),
        }
    };
}

#[macro_export]
macro_rules! assert_select {
    ($result:expr, $row_count:expr) => {
        match &$result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), $row_count, "Expected {} rows, got {}", $row_count, rows.len());
            }
            other => panic!("Expected Select result, got: {:?}", other),
        }
    };
}

#[macro_export]
macro_rules! assert_create_index {
    ($result:expr, $index_name:expr, $table_name:expr) => {
        match $result {
            ExecutionResult::CreateIndex { index_name, table_name, .. } => {
                assert_eq!(index_name, $index_name);
                assert_eq!(table_name, $table_name);
            }
            other => panic!("Expected CreateIndex result, got: {:?}", other),
        }
    };
}
