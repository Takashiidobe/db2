use super::ast::{
    BinaryOp, ColumnRef, CreateIndexStmt, CreateTableStmt, Expr, InsertStmt, Literal, SelectColumn, SelectStmt,
    Statement,
};
use crate::index::BPlusTree;
use crate::optimizer::planner::{FromClausePlan, JoinPlan, Planner, ScanPlan};
use crate::table::{HeapTable, RowId, TableScan};
use crate::types::{Column, DataType as DbDataType, Schema, Value};
use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

/// Execution result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionResult {
    /// Table created successfully
    CreateTable { table_name: String },
    /// Row inserted successfully
    Insert { row_id: RowId },
    /// SELECT query result
    Select {
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    /// Index created successfully
    CreateIndex {
        index_name: String,
        table_name: String,
        column_name: String,
    },
}

impl std::fmt::Display for ExecutionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionResult::CreateTable { table_name } => {
                write!(f, "Table '{}' created successfully", table_name)
            }
            ExecutionResult::Insert { row_id } => {
                write!(
                    f,
                    "Row inserted (page: {}, slot: {})",
                    row_id.page_id(),
                    row_id.slot_id()
                )
            }
            ExecutionResult::Select { column_names, rows } => {
                // Print column headers
                writeln!(f, "{}", column_names.join(" | "))?;
                writeln!(f, "{}", "-".repeat(column_names.len() * 10))?;

                // Print rows
                for row in rows {
                    let row_str: Vec<String> = row.iter().map(|v| format!("{}", v)).collect();
                    writeln!(f, "{}", row_str.join(" | "))?;
                }

                write!(f, "{} row(s) returned", rows.len())
            }
            ExecutionResult::CreateIndex {
                index_name,
                table_name,
                column_name,
            } => {
                write!(
                    f,
                    "Index '{}' created on {}.{}",
                    index_name, table_name, column_name
                )
            }
        }
    }
}

/// Index key: (table_name, column_name)
type IndexKey = (String, String);

/// Database executor with catalog
///
/// Manages tables and executes SQL statements.
pub struct Executor {
    /// Database directory
    db_path: PathBuf,
    /// Buffer pool size for each table
    buffer_pool_size: usize,
    /// Table catalog (maps table name to HeapTable)
    tables: HashMap<String, HeapTable>,
    /// Index catalog (maps (table_name, column_name) to B+ tree index)
    /// Only supports INTEGER column indexes for now
    indexes: HashMap<IndexKey, BPlusTree<i64, RowId>>,
}

impl Executor {
    /// Create a new executor
    ///
    /// # Arguments
    /// * `db_path` - Directory for database files
    /// * `buffer_pool_size` - Size of buffer pool for each table
    pub fn new(db_path: impl AsRef<Path>, buffer_pool_size: usize) -> io::Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();

        // Create database directory if it doesn't exist
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }

        Ok(Self {
            db_path,
            buffer_pool_size,
            tables: HashMap::new(),
            indexes: HashMap::new(),
        })
    }

    /// Execute a SQL statement
    ///
    /// # Arguments
    /// * `stmt` - Parsed SQL statement
    ///
    /// # Returns
    /// Execution result
    ///
    /// # Errors
    /// Returns error if execution fails
    pub fn execute(&mut self, stmt: Statement) -> io::Result<ExecutionResult> {
        match stmt {
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Select(select) => self.execute_select(select),
            Statement::CreateIndex(create_index) => self.execute_create_index(create_index),
        }
    }

    /// Execute CREATE TABLE statement
    fn execute_create_table(&mut self, stmt: CreateTableStmt) -> io::Result<ExecutionResult> {
        // Check if table already exists
        if self.tables.contains_key(&stmt.table_name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Table '{}' already exists", stmt.table_name),
            ));
        }

        // Convert AST column definitions to database schema
        let columns: Vec<Column> = stmt
            .columns
            .iter()
            .map(|col| {
                let db_type = match col.data_type {
                    super::ast::DataType::Integer => DbDataType::Integer,
                    super::ast::DataType::Varchar => DbDataType::String,
                };
                Column::new(&col.name, db_type)
            })
            .collect();

        let schema = Schema::new(columns);

        // Create table file path
        let table_path = self.db_path.join(format!("{}.db", stmt.table_name));

        // Create the heap table
        let table = HeapTable::create(
            &stmt.table_name,
            schema,
            table_path,
            self.buffer_pool_size,
        )?;

        let table_name = stmt.table_name.clone();
        self.tables.insert(stmt.table_name, table);

        Ok(ExecutionResult::CreateTable { table_name })
    }

    /// Execute INSERT statement
    fn execute_insert(&mut self, stmt: InsertStmt) -> io::Result<ExecutionResult> {
        // Get the table
        let table = self.tables.get_mut(&stmt.table_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' does not exist", stmt.table_name),
            )
        })?;

        // Convert literals to values
        let values: Vec<Value> = stmt
            .values
            .iter()
            .map(|lit| match lit {
                Literal::Integer(i) => Value::Integer(*i),
                Literal::String(s) => Value::String(s.clone()),
            })
            .collect();

        // Insert the row
        let row_id = table.insert(&values)?;

        // Update any indexes on this table
        let schema = table.schema().clone();
        for ((idx_table, idx_column), index) in &mut self.indexes {
            if idx_table == &stmt.table_name {
                // Find the column index
                if let Some((col_idx, _)) = schema.find_column(idx_column) {
                    // Only support INTEGER indexes for now
                    if let Value::Integer(key) = &values[col_idx] {
                        index.insert(*key, row_id);
                    }
                }
            }
        }

        Ok(ExecutionResult::Insert { row_id })
    }

    /// Execute CREATE INDEX statement
    fn execute_create_index(&mut self, stmt: CreateIndexStmt) -> io::Result<ExecutionResult> {
        // Check if table exists
        let table = self.tables.get_mut(&stmt.table_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' does not exist", stmt.table_name),
            )
        })?;

        let schema = table.schema().clone();

        // Find the column
        let (col_idx, column) = schema.find_column(&stmt.column_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Column '{}' not found in table '{}'", stmt.column_name, stmt.table_name),
            )
        })?;

        // Only support INTEGER columns for now
        if column.data_type() != DbDataType::Integer {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Only INTEGER columns can be indexed",
            ));
        }

        // Check if index already exists
        let index_key = (stmt.table_name.clone(), stmt.column_name.clone());
        if self.indexes.contains_key(&index_key) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Index on {}.{} already exists", stmt.table_name, stmt.column_name),
            ));
        }

        // Create the index and populate it with existing data
        let mut index = BPlusTree::new();

        // Scan the table and add all existing rows to the index
        let mut scan = TableScan::new(table);
        while let Some((row_id, row)) = scan.next()? {
            if let Value::Integer(key) = &row[col_idx] {
                index.insert(*key, row_id);
            }
        }

        // Store the index
        self.indexes.insert(index_key, index);

        Ok(ExecutionResult::CreateIndex {
            index_name: stmt.index_name,
            table_name: stmt.table_name,
            column_name: stmt.column_name,
        })
    }

    /// Execute SELECT statement
    fn execute_select(&mut self, stmt: SelectStmt) -> io::Result<ExecutionResult> {
        let planner = Planner::new(self.index_metadata());
        let plan = planner.plan_select(&stmt);

        match plan.from {
            FromClausePlan::Single { table, scan } => {
                self.execute_select_single_table_plan(plan.columns, table, plan.filter, scan)
            }
            FromClausePlan::Join(join_plan) => {
                self.execute_select_join_plan(plan.columns, join_plan, plan.filter)
            }
        }
    }

    fn execute_select_single_table_plan(
        &mut self,
        columns: SelectColumn,
        table_name: String,
        where_clause: Option<Expr>,
        scan_plan: ScanPlan,
    ) -> io::Result<ExecutionResult> {
        // Get schema first (before any mutable borrows)
        let schema = {
            let table = self.tables.get(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;
            table.schema().clone()
        };

        let columns_meta = Self::build_column_metadata_for_table(&table_name, &schema);
        let (column_indices, column_names) =
            Self::build_projection(&columns_meta, &columns, false)?;

        let row_ids = match scan_plan {
            ScanPlan::IndexScan { column, op, value } => {
                self.index_scan(&table_name, &column, op, &value)?
            }
            ScanPlan::SeqScan => None,
        };

        // Get the table again for mutable access
        let table = self.tables.get_mut(&table_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' does not exist", table_name),
            )
        })?;

        let mut result_rows = Vec::new();

        if let Some(row_ids) = row_ids {
            // Index scan: fetch specific rows
            for row_id in row_ids {
                let row = table.get(row_id)?;

                if let Some(ref where_expr) = where_clause
                    && !Self::evaluate_predicate_static(where_expr, &row, &columns_meta)?
                {
                    continue;
                }

                // Project selected columns
                let projected_row: Vec<Value> = column_indices
                    .iter()
                    .map(|&idx| row[idx].clone())
                    .collect();

                result_rows.push(projected_row);
            }
        } else {
            // Table scan: scan all rows and filter
            let mut scan = TableScan::new(table);

            while let Some((_row_id, row)) = scan.next()? {
                // Apply WHERE clause filter if present
                if let Some(ref where_expr) = where_clause
                    && !Self::evaluate_predicate_static(where_expr, &row, &columns_meta)?
                {
                    continue;
                }

                // Project selected columns
                let projected_row: Vec<Value> = column_indices
                    .iter()
                    .map(|&idx| row[idx].clone())
                    .collect();

                result_rows.push(projected_row);
            }
        }

        Ok(ExecutionResult::Select {
            column_names,
            rows: result_rows,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_select_join_plan(
        &mut self,
        columns: SelectColumn,
        join_plan: JoinPlan,
        where_clause: Option<Expr>,
    ) -> io::Result<ExecutionResult> {
        // Fetch schemas before mutable borrows
        let left_schema = {
            let table = self.tables.get(&join_plan.outer_table).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", join_plan.outer_table),
                )
            })?;
            table.schema().clone()
        };
        let right_schema = {
            let table = self.tables.get(&join_plan.inner_table).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", join_plan.inner_table),
                )
            })?;
            table.schema().clone()
        };

        // Resolve join columns
        let left_join_idx =
            Self::resolve_schema_column_index(&left_schema, &join_plan.outer_table, &join_plan.outer_column)?;
        let right_join_idx =
            Self::resolve_schema_column_index(&right_schema, &join_plan.inner_table, &join_plan.inner_column)?;

        let combined_meta = Self::build_join_column_metadata(
            &join_plan.outer_table,
            &left_schema,
            &join_plan.inner_table,
            &right_schema,
        );
        let (column_indices, column_names) =
            Self::build_projection(&combined_meta, &columns, true)?;

        // Preload left rows (outer loop)
        let left_rows = {
            let mut rows = Vec::new();
            let left_table_ref = self.tables.get_mut(&join_plan.outer_table).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", join_plan.outer_table),
                )
            })?;
            let mut scan = TableScan::new(left_table_ref);
            while let Some((_row_id, row)) = scan.next()? {
                rows.push(row);
            }
            rows
        };

        let index_key = (
            join_plan.inner_table.clone(),
            right_schema.columns()[right_join_idx].name().to_string(),
        );
        let right_join_is_integer =
            right_schema.columns()[right_join_idx].data_type() == DbDataType::Integer;
        let use_right_index = join_plan.inner_has_index && right_join_is_integer && self.indexes.contains_key(&index_key);

        // If not using index, load right rows once
        let right_rows_cache: Option<Vec<Vec<Value>>> = if use_right_index {
            None
        } else {
            let mut rows = Vec::new();
            let right_table_ref = self.tables.get_mut(&join_plan.inner_table).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", join_plan.inner_table),
                )
            })?;
            let mut scan = TableScan::new(right_table_ref);
            while let Some((_row_id, row)) = scan.next()? {
                rows.push(row);
            }
            Some(rows)
        };

        let mut result_rows = Vec::new();

        for left_row in left_rows {
            let left_key = left_row[left_join_idx].clone();
            let mut matching_right_rows = Vec::new();

            if use_right_index {
                if let Value::Integer(key) = left_key {
                    // Look up matching row IDs via index first
                    let mut matched_ids = Vec::new();
                    if let Some(index) = self.indexes.get(&index_key) {
                        if let Some(&row_id) = index.search(&key) {
                            matched_ids.push(row_id);
                        }
                    }

                    for row_id in matched_ids {
                        let right_row = {
                            let right_table_ref =
                                self.tables.get_mut(&join_plan.inner_table).ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::NotFound,
                                        format!("Table '{}' does not exist", join_plan.inner_table),
                                    )
                                })?;
                            right_table_ref.get(row_id)?
                        };
                        matching_right_rows.push(right_row);
                    }
                }
            } else if let Some(ref right_rows) = right_rows_cache {
                for right_row in right_rows {
                    if right_row[right_join_idx] == left_key {
                        matching_right_rows.push(right_row.clone());
                    }
                }
            }

            for right_row in matching_right_rows {
                let mut combined_row = Vec::new();
                combined_row.extend(left_row.clone());
                combined_row.extend(right_row);

                if let Some(ref where_expr) = where_clause
                    && !Self::evaluate_predicate_static(where_expr, &combined_row, &combined_meta)?
                {
                    continue;
                }

                let projected_row: Vec<Value> = column_indices
                    .iter()
                    .map(|&idx| combined_row[idx].clone())
                    .collect();

                result_rows.push(projected_row);
            }
        }

        Ok(ExecutionResult::Select {
            column_names,
            rows: result_rows,
        })
    }


    /// Use an index for a simple predicate if available.
    /// Returns Some(row_ids) if an index can be used, None otherwise.
    fn index_scan(
        &self,
        table_name: &str,
        column_name: &str,
        op: BinaryOp,
        literal_value: &Literal,
    ) -> io::Result<Option<Vec<RowId>>> {
        let index_key = (table_name.to_string(), column_name.to_string());
        if let Some(index) = self.indexes.get(&index_key) {
            if let Literal::Integer(key) = literal_value {
                let mut row_ids = Vec::new();

                match op {
                    BinaryOp::Eq => {
                        if let Some(&row_id) = index.search(key) {
                            row_ids.push(row_id);
                        }
                    }
                    BinaryOp::Lt => {
                        for (_k, v) in index.range_scan(&i64::MIN, &(key - 1)) {
                            row_ids.push(v);
                        }
                    }
                    BinaryOp::LtEq => {
                        for (_k, v) in index.range_scan(&i64::MIN, key) {
                            row_ids.push(v);
                        }
                    }
                    BinaryOp::Gt => {
                        for (_k, v) in index.range_scan(&(key + 1), &i64::MAX) {
                            row_ids.push(v);
                        }
                    }
                    BinaryOp::GtEq => {
                        for (_k, v) in index.range_scan(key, &i64::MAX) {
                            row_ids.push(v);
                        }
                    }
                    BinaryOp::NotEq => {
                        for (_k, v) in index.range_scan(&i64::MIN, &(key - 1)) {
                            row_ids.push(v);
                        }
                        for (_k, v) in index.range_scan(&(key + 1), &i64::MAX) {
                            row_ids.push(v);
                        }
                    }
                }

                return Ok(Some(row_ids));
            }
        }

        Ok(None)
    }

    /// Evaluate a predicate expression against a row (static version)
    fn evaluate_predicate_static(
        expr: &Expr,
        row: &[Value],
        columns: &[(Option<String>, String)],
    ) -> io::Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = Self::evaluate_expr_static(left, row, columns)?;
                let right_val = Self::evaluate_expr_static(right, row, columns)?;

                let result = match op {
                    BinaryOp::Eq => left_val == right_val,
                    BinaryOp::NotEq => left_val != right_val,
                    BinaryOp::Lt => left_val < right_val,
                    BinaryOp::LtEq => left_val <= right_val,
                    BinaryOp::Gt => left_val > right_val,
                    BinaryOp::GtEq => left_val >= right_val,
                };

                Ok(result)
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WHERE clause must be a comparison expression",
            )),
        }
    }

    /// Evaluate an expression to a value (static version)
    fn evaluate_expr_static(
        expr: &Expr,
        row: &[Value],
        columns: &[(Option<String>, String)],
    ) -> io::Result<Value> {
        match expr {
            Expr::Column(col_ref) => Self::resolve_column_value(row, columns, col_ref),
            Expr::Literal(lit) => {
                let val = match lit {
                    Literal::Integer(i) => Value::Integer(*i),
                    Literal::String(s) => Value::String(s.clone()),
                };
                Ok(val)
            }
            Expr::BinaryOp { .. } => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Binary operations cannot be directly evaluated as values",
            )),
        }
    }

    /// Get a table by name
    pub fn get_table(&mut self, name: &str) -> Option<&mut HeapTable> {
        self.tables.get_mut(name)
    }

    fn index_metadata(&self) -> HashSet<(String, String)> {
        self.indexes
            .keys()
            .cloned()
            .collect()
    }

    fn build_column_metadata_for_table(table_name: &str, schema: &Schema) -> Vec<(Option<String>, String)> {
        schema
            .columns()
            .iter()
            .map(|col| (Some(table_name.to_string()), col.name().to_string()))
            .collect()
    }

    fn build_join_column_metadata(
        left_table: &str,
        left_schema: &Schema,
        right_table: &str,
        right_schema: &Schema,
    ) -> Vec<(Option<String>, String)> {
        let mut columns = Self::build_column_metadata_for_table(left_table, left_schema);
        columns.extend(Self::build_column_metadata_for_table(right_table, right_schema));
        columns
    }

    fn format_column_name(meta: &(Option<String>, String), use_qualified: bool) -> String {
        match (&meta.0, use_qualified) {
            (Some(table), true) => format!("{}.{}", table, meta.1),
            _ => meta.1.clone(),
        }
    }

    fn build_projection(
        columns_meta: &[(Option<String>, String)],
        selection: &SelectColumn,
        use_qualified: bool,
    ) -> io::Result<(Vec<usize>, Vec<String>)> {
        match selection {
            SelectColumn::All => {
                let indices: Vec<usize> = (0..columns_meta.len()).collect();
                let names: Vec<String> = columns_meta
                    .iter()
                    .map(|meta| Self::format_column_name(meta, use_qualified))
                    .collect();
                Ok((indices, names))
            }
            SelectColumn::Columns(cols) => {
                let mut indices = Vec::new();
                let mut names = Vec::new();
                for col_ref in cols {
                    let idx = Self::resolve_column_index(columns_meta, col_ref)?;
                    indices.push(idx);
                    names.push(Self::format_column_name(
                        &columns_meta[idx],
                        use_qualified,
                    ));
                }
                Ok((indices, names))
            }
        }
    }

    fn resolve_column_index(
        columns_meta: &[(Option<String>, String)],
        col_ref: &ColumnRef,
    ) -> io::Result<usize> {
        let mut matches = columns_meta
            .iter()
            .enumerate()
            .filter(|(_, (table, name))| match &col_ref.table {
                Some(t) => table.as_deref() == Some(t) && name == &col_ref.column,
                None => name == &col_ref.column,
            });

        let first = matches.next();
        let second = matches.next();

        match (first, second) {
            (Some((idx, _)), None) => Ok(idx),
            (None, _) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Column '{}' not found", col_ref.column),
            )),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Column reference '{}' is ambiguous", col_ref.column),
            )),
        }
    }

    fn resolve_column_value(
        row: &[Value],
        columns_meta: &[(Option<String>, String)],
        col_ref: &ColumnRef,
    ) -> io::Result<Value> {
        let idx = Self::resolve_column_index(columns_meta, col_ref)?;
        Ok(row[idx].clone())
    }

    fn resolve_schema_column_index(
        schema: &Schema,
        table_name: &str,
        col_ref: &ColumnRef,
    ) -> io::Result<usize> {
        if let Some(ref table) = col_ref.table {
            if table != table_name {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Column '{}' does not belong to table '{}'", col_ref.column, table_name),
                ));
            }
        }

        schema
            .find_column(&col_ref.column)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Column '{}' not found in table '{}'", col_ref.column, table_name),
                )
            })
    }

    /// Flush all tables
    pub fn flush_all(&mut self) -> io::Result<()> {
        for table in self.tables.values_mut() {
            table.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::parse_sql;
    use tempfile::TempDir;

    #[test]
    fn test_create_table() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(sql).unwrap();
        let result = executor.execute(stmt).unwrap();

        match result {
            ExecutionResult::CreateTable { table_name } => {
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected CreateTable result"),
        }

        // Verify table exists
        assert!(executor.get_table("users").is_some());
    }

    #[test]
    fn test_create_table_duplicate() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(sql).unwrap();
        executor.execute(stmt).unwrap();

        // Try to create again
        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(sql).unwrap();
        let result = executor.execute(stmt);

        assert!(result.is_err());
    }

    #[test]
    fn test_insert() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(create_sql).unwrap();
        executor.execute(stmt).unwrap();

        // Insert row
        let insert_sql = "INSERT INTO users VALUES (1, 'Alice')";
        let stmt = parse_sql(insert_sql).unwrap();
        let result = executor.execute(stmt).unwrap();

        match result {
            ExecutionResult::Insert { row_id } => {
                assert_eq!(row_id.page_id(), 1); // First data page
                assert_eq!(row_id.slot_id(), 0); // First slot
            }
            _ => panic!("Expected Insert result"),
        }
    }

    #[test]
    fn test_insert_multiple_rows() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)";
        let stmt = parse_sql(create_sql).unwrap();
        executor.execute(stmt).unwrap();

        // Insert multiple rows
        let inserts = vec![
            "INSERT INTO users VALUES (1, 'Alice', 30)",
            "INSERT INTO users VALUES (2, 'Bob', 25)",
            "INSERT INTO users VALUES (3, 'Charlie', 35)",
        ];

        for insert_sql in inserts {
            let stmt = parse_sql(insert_sql).unwrap();
            executor.execute(stmt).unwrap();
        }

        // Verify we can retrieve rows
        let table = executor.get_table("users").unwrap();
        let row = table.get(RowId::new(1, 0)).unwrap();
        assert_eq!(row[0], Value::Integer(1));
        assert_eq!(row[1], Value::String("Alice".to_string()));
        assert_eq!(row[2], Value::Integer(30));
    }

    #[test]
    fn test_insert_nonexistent_table() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        let insert_sql = "INSERT INTO nonexistent VALUES (1, 'Alice')";
        let stmt = parse_sql(insert_sql).unwrap();
        let result = executor.execute(stmt);

        assert!(result.is_err());
    }

    #[test]
    fn test_insert_schema_validation() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR)";
        let stmt = parse_sql(create_sql).unwrap();
        executor.execute(stmt).unwrap();

        // Try to insert wrong number of values
        let insert_sql = "INSERT INTO users VALUES (1)";
        let stmt = parse_sql(insert_sql).unwrap();
        let result = executor.execute(stmt);

        assert!(result.is_err());
    }

    #[test]
    fn test_end_to_end() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        let create_sql = "CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER)";
        executor.execute(parse_sql(create_sql).unwrap()).unwrap();

        // Insert products
        let products = vec![
            "INSERT INTO products VALUES (1, 'Laptop', 1000)",
            "INSERT INTO products VALUES (2, 'Mouse', 25)",
            "INSERT INTO products VALUES (3, 'Keyboard', 75)",
        ];

        for sql in products {
            executor.execute(parse_sql(sql).unwrap()).unwrap();
        }

        // Verify data
        let table = executor.get_table("products").unwrap();

        let laptop = table.get(RowId::new(1, 0)).unwrap();
        assert_eq!(laptop[1], Value::String("Laptop".to_string()));
        assert_eq!(laptop[2], Value::Integer(1000));

        let mouse = table.get(RowId::new(1, 1)).unwrap();
        assert_eq!(mouse[1], Value::String("Mouse".to_string()));
        assert_eq!(mouse[2], Value::Integer(25));
    }

    #[test]
    fn test_flush_all() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor.execute(parse_sql("CREATE TABLE test (id INTEGER)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO test VALUES (42)").unwrap()).unwrap();

        // Flush
        executor.flush_all().unwrap();
    }

    #[test]
    fn test_select_all() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (3, 'Charlie')").unwrap()).unwrap();

        // SELECT * FROM users
        let result = executor.execute(parse_sql("SELECT * FROM users").unwrap()).unwrap();

        match result {
            ExecutionResult::Select { column_names, rows } => {
                assert_eq!(column_names, vec!["id", "name"]);
                assert_eq!(rows.len(), 3);
                assert_eq!(rows[0], vec![Value::Integer(1), Value::String("Alice".to_string())]);
                assert_eq!(rows[1], vec![Value::Integer(2), Value::String("Bob".to_string())]);
                assert_eq!(rows[2], vec![Value::Integer(3), Value::String("Charlie".to_string())]);
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_columns() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap()).unwrap();

        // SELECT name, age FROM users
        let result = executor.execute(parse_sql("SELECT name, age FROM users").unwrap()).unwrap();

        match result {
            ExecutionResult::Select { column_names, rows } => {
                assert_eq!(column_names, vec!["name", "age"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0], vec![Value::String("Alice".to_string()), Value::Integer(30)]);
                assert_eq!(rows[1], vec![Value::String("Bob".to_string()), Value::Integer(25)]);
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_where_equal() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (3, 'Charlie', 30)").unwrap()).unwrap();

        // SELECT * FROM users WHERE age = 30
        let result = executor.execute(parse_sql("SELECT * FROM users WHERE age = 30").unwrap()).unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::String("Alice".to_string()));
                assert_eq!(rows[1][1], Value::String("Charlie".to_string()));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_where_comparison() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor.execute(parse_sql("CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (1, 'Laptop', 1000)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (2, 'Mouse', 25)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (3, 'Keyboard', 75)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (4, 'Monitor', 300)").unwrap()).unwrap();

        // SELECT * FROM products WHERE price > 100
        let result = executor.execute(parse_sql("SELECT * FROM products WHERE price > 100").unwrap()).unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::String("Laptop".to_string()));
                assert_eq!(rows[1][1], Value::String("Monitor".to_string()));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_where_string() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (3, 'Alice')").unwrap()).unwrap();

        // SELECT * FROM users WHERE name = 'Alice'
        let result = executor.execute(parse_sql("SELECT * FROM users WHERE name = 'Alice'").unwrap()).unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Integer(1));
                assert_eq!(rows[1][0], Value::Integer(3));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_select_empty_result() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();

        // SELECT * FROM users WHERE id = 999
        let result = executor.execute(parse_sql("SELECT * FROM users WHERE id = 999").unwrap()).unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_create_index() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap()).unwrap();

        // Create index
        let result = executor.execute(parse_sql("CREATE INDEX idx_id ON users(id)").unwrap()).unwrap();

        match result {
            ExecutionResult::CreateIndex { index_name, table_name, column_name } => {
                assert_eq!(index_name, "idx_id");
                assert_eq!(table_name, "users");
                assert_eq!(column_name, "id");
            }
            _ => panic!("Expected CreateIndex result"),
        }
    }

    #[test]
    fn test_index_scan() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create and populate table
        executor.execute(parse_sql("CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (1, 'Laptop', 1000)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (2, 'Mouse', 25)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (3, 'Keyboard', 75)").unwrap()).unwrap();

        // Create index on id
        executor.execute(parse_sql("CREATE INDEX idx_id ON products(id)").unwrap()).unwrap();

        // Query using index (WHERE id = 2)
        let result = executor.execute(parse_sql("SELECT * FROM products WHERE id = 2").unwrap()).unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(2));
                assert_eq!(rows[0][1], Value::String("Mouse".to_string()));
                assert_eq!(rows[0][2], Value::Integer(25));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_index_maintained_on_insert() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table and index
        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap()).unwrap();
        executor.execute(parse_sql("CREATE INDEX idx_id ON users(id)").unwrap()).unwrap();

        // Insert rows after creating index
        executor.execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (3, 'Charlie')").unwrap()).unwrap();

        // Query using index
        let result = executor.execute(parse_sql("SELECT * FROM users WHERE id = 2").unwrap()).unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(2));
                assert_eq!(rows[0][1], Value::String("Bob".to_string()));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_index_range_queries() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table, populate, and create index
        executor.execute(parse_sql("CREATE TABLE products (id INTEGER, price INTEGER)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (1, 100)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (2, 200)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (3, 300)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO products VALUES (4, 400)").unwrap()).unwrap();
        executor.execute(parse_sql("CREATE INDEX idx_price ON products(price)").unwrap()).unwrap();

        // Test > operator
        let result = executor.execute(parse_sql("SELECT * FROM products WHERE price > 200").unwrap()).unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::Integer(300));
                assert_eq!(rows[1][1], Value::Integer(400));
            }
            _ => panic!("Expected Select result"),
        }

        // Test >= operator
        let result = executor.execute(parse_sql("SELECT * FROM products WHERE price >= 200").unwrap()).unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Select result"),
        }

        // Test < operator
        let result = executor.execute(parse_sql("SELECT * FROM products WHERE price < 300").unwrap()).unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::Integer(100));
                assert_eq!(rows[1][1], Value::Integer(200));
            }
            _ => panic!("Expected Select result"),
        }

        // Test <= operator
        let result = executor.execute(parse_sql("SELECT * FROM products WHERE price <= 200").unwrap()).unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Select result"),
        }

        // Test != operator
        let result = executor.execute(parse_sql("SELECT * FROM products WHERE price != 200").unwrap()).unwrap();
        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_create_index_varchar_fails() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table
        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap()).unwrap();

        // Try to create index on VARCHAR column (should fail)
        let result = executor.execute(parse_sql("CREATE INDEX idx_name ON users(name)").unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_create_index_duplicate() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        // Create table and index
        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap()).unwrap();
        executor.execute(parse_sql("CREATE INDEX idx_id ON users(id)").unwrap()).unwrap();

        // Try to create same index again (should fail)
        let result = executor.execute(parse_sql("CREATE INDEX idx_id2 ON users(id)").unwrap());
        assert!(result.is_err());
    }

    #[test]
    fn test_join_nested_loop() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap()).unwrap();
        executor.execute(parse_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)").unwrap()).unwrap();

        executor.execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap()).unwrap();

        executor.execute(parse_sql("INSERT INTO orders VALUES (100, 1, 50)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO orders VALUES (101, 2, 20)").unwrap()).unwrap();

        let result = executor.execute(parse_sql(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
        ).unwrap()).unwrap();

        match result {
            ExecutionResult::Select { column_names, rows } => {
                assert_eq!(
                    column_names,
                    vec![
                        "users.id".to_string(),
                        "users.name".to_string(),
                        "orders.id".to_string(),
                        "orders.user_id".to_string(),
                        "orders.amount".to_string(),
                    ]
                );
                assert_eq!(rows.len(), 2);
                assert!(rows.iter().any(|r| r[0] == Value::Integer(1) && r[2] == Value::Integer(100)));
                assert!(rows.iter().any(|r| r[0] == Value::Integer(2) && r[2] == Value::Integer(101)));
            }
            _ => panic!("Expected Select result"),
        }
    }

    #[test]
    fn test_join_with_index_on_inner() {
        let temp_dir = TempDir::new().unwrap();
        let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

        executor.execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap()).unwrap();
        executor.execute(parse_sql("CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)").unwrap()).unwrap();

        executor.execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO users VALUES (2, 'Bob')").unwrap()).unwrap();

        executor.execute(parse_sql("INSERT INTO orders VALUES (100, 1, 50)").unwrap()).unwrap();
        executor.execute(parse_sql("INSERT INTO orders VALUES (101, 2, 20)").unwrap()).unwrap();

        executor.execute(parse_sql("CREATE INDEX idx_orders_user_id ON orders(user_id)").unwrap()).unwrap();

        let result = executor.execute(parse_sql(
            "SELECT orders.amount, users.name FROM users JOIN orders ON users.id = orders.user_id",
        ).unwrap()).unwrap();

        match result {
            ExecutionResult::Select { column_names, rows } => {
                assert_eq!(column_names, vec!["orders.amount".to_string(), "users.name".to_string()]);
                assert_eq!(rows.len(), 2);
                assert!(rows.iter().any(|r| r[0] == Value::Integer(50) && r[1] == Value::String("Alice".to_string())));
                assert!(rows.iter().any(|r| r[0] == Value::Integer(20) && r[1] == Value::String("Bob".to_string())));
            }
            _ => panic!("Expected Select result"),
        }
    }
}
