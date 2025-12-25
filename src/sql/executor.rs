use super::ast::{
    BinaryOp, ColumnRef, CreateIndexStmt, CreateTableStmt, DeleteStmt, DropIndexStmt,
    DropTableStmt, Expr, InsertStmt, Literal, SelectColumn, SelectStmt, Statement, UpdateStmt,
};
use crate::index::BPlusTree;
use crate::optimizer::planner::{
    FromClausePlan, IndexMetadata, JoinPlan, JoinStrategy, Planner, ScanPlan,
};
use crate::table::{HeapTable, RowId, TableScan};
use crate::types::{Column, DataType as DbDataType, Schema, Value};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Execution result
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionResult {
    /// Table created successfully
    CreateTable { table_name: String },
    /// Table dropped successfully
    DropTable { table_name: String },
    /// Row inserted successfully
    Insert { row_ids: Vec<RowId> },
    /// SELECT query result
    Select {
        column_names: Vec<String>,
        rows: Vec<Vec<Value>>,
        plan: Vec<String>,
    },
    /// Index created successfully
    CreateIndex {
        index_name: String,
        table_name: String,
        columns: Vec<String>,
    },
    /// Index dropped successfully
    DropIndex { index_name: String },
    /// Rows deleted successfully
    Delete { rows_deleted: usize },
    /// Rows updated successfully
    Update { rows_updated: usize },
}

impl std::fmt::Display for ExecutionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionResult::CreateTable { table_name } => {
                write!(f, "Table '{}' created successfully", table_name)
            }
            ExecutionResult::DropTable { table_name } => {
                write!(f, "Table '{}' dropped successfully", table_name)
            }
            ExecutionResult::Insert { row_ids } => {
                if row_ids.len() == 1 {
                    let row_id = row_ids[0];
                    write!(
                        f,
                        "Row inserted (page: {}, slot: {})",
                        row_id.page_id(),
                        row_id.slot_id()
                    )
                } else {
                    write!(f, "{} rows inserted", row_ids.len())
                }
            }
            ExecutionResult::Select {
                column_names,
                rows,
                plan,
            } => {
                if !plan.is_empty() {
                    write!(f, "Plan:")?;
                    for step in plan {
                        writeln!(f)?;
                        write!(f, "  - {}", step)?;
                    }
                    writeln!(f)?;
                    writeln!(f)?;
                }
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
                columns,
            } => {
                write!(
                    f,
                    "Index '{}' created on {}({})",
                    index_name,
                    table_name,
                    columns.join(", ")
                )
            }
            ExecutionResult::DropIndex { index_name } => {
                write!(f, "Index '{}' dropped successfully", index_name)
            }
            ExecutionResult::Delete { rows_deleted } => {
                if *rows_deleted == 1 {
                    write!(f, "1 row deleted")
                } else {
                    write!(f, "{} rows deleted", rows_deleted)
                }
            }
            ExecutionResult::Update { rows_updated } => {
                if *rows_updated == 1 {
                    write!(f, "1 row updated")
                } else {
                    write!(f, "{} rows updated", rows_updated)
                }
            }
        }
    }
}

/// Index key: (table_name, column_name)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct IndexKey {
    table: String,
    columns: Vec<String>,
}

struct IndexEntry {
    name: String,
    key: IndexKey,
    column_indices: Vec<usize>,
    column_types: Vec<DbDataType>,
    tree: BPlusTree<CompositeKey, RowId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CompositeKey {
    values: Vec<IndexValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum IndexValue {
    Signed(i64),
    Unsigned(u64),
}

impl CompositeKey {
    fn new(values: Vec<IndexValue>) -> Self {
        Self { values }
    }

    fn min_values(types: &[DbDataType]) -> Self {
        Self {
            values: types.iter().map(IndexValue::min_value).collect(),
        }
    }

    fn max_values(types: &[DbDataType]) -> Self {
        Self {
            values: types.iter().map(IndexValue::max_value).collect(),
        }
    }
}

impl IndexValue {
    fn min_value(data_type: &DbDataType) -> Self {
        match data_type {
            DbDataType::Integer => IndexValue::Signed(i64::MIN),
            DbDataType::Unsigned => IndexValue::Unsigned(0),
            _ => unreachable!("IndexValue only used for integer types"),
        }
    }

    fn max_value(data_type: &DbDataType) -> Self {
        match data_type {
            DbDataType::Integer => IndexValue::Signed(i64::MAX),
            DbDataType::Unsigned => IndexValue::Unsigned(u64::MAX),
            _ => unreachable!("IndexValue only used for integer types"),
        }
    }

    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Integer(i) => Some(IndexValue::Signed(*i)),
            Value::Unsigned(u) => Some(IndexValue::Unsigned(*u)),
            _ => None,
        }
    }

    fn from_literal(lit: &Literal, data_type: &DbDataType) -> Option<Self> {
        match (lit, data_type) {
            (Literal::Integer(i), DbDataType::Integer) => {
                (*i).try_into().ok().map(IndexValue::Signed)
            }
            (Literal::Integer(i), DbDataType::Unsigned) if *i >= 0 => {
                (*i).try_into().ok().map(IndexValue::Unsigned)
            }
            (Literal::Float(fv), DbDataType::Integer) if fv.fract() == 0.0 => {
                (*fv as i128).try_into().ok().map(IndexValue::Signed)
            }
            (Literal::Float(fv), DbDataType::Unsigned) if fv.fract() == 0.0 && *fv >= 0.0 => {
                (*fv as i128).try_into().ok().map(IndexValue::Unsigned)
            }
            _ => None,
        }
    }

    fn saturating_sub_one(&self) -> Self {
        match self {
            IndexValue::Signed(v) => IndexValue::Signed(v.saturating_sub(1)),
            IndexValue::Unsigned(v) => IndexValue::Unsigned(v.saturating_sub(1)),
        }
    }

    fn saturating_add_one(&self) -> Self {
        match self {
            IndexValue::Signed(v) => IndexValue::Signed(v.saturating_add(1)),
            IndexValue::Unsigned(v) => IndexValue::Unsigned(v.saturating_add(1)),
        }
    }
}

impl PartialOrd for CompositeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompositeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.values.cmp(&other.values)
    }
}

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
    /// Index catalog (multi-column B+Tree over integer columns)
    indexes: Vec<IndexEntry>,
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

        // Load existing heap tables from disk
        let mut tables = HashMap::new();
        for entry in fs::read_dir(&db_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) == Some("db") {
                let table = HeapTable::open(&path, buffer_pool_size)?;
                tables.insert(table.name().to_string(), table);
            }
        }

        let mut executor = Self {
            db_path,
            buffer_pool_size,
            tables,
            indexes: Vec::new(),
        };

        executor.load_indexes_from_metadata()?;

        Ok(executor)
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
            Statement::DropTable(drop) => self.execute_drop_table(drop),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Select(select) => self.execute_select(select),
            Statement::CreateIndex(create_index) => self.execute_create_index(create_index),
            Statement::DropIndex(drop_index) => self.execute_drop_index(drop_index),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::Update(update) => self.execute_update(update),
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
                    super::ast::DataType::Unsigned => DbDataType::Unsigned,
                    super::ast::DataType::Float => DbDataType::Float,
                    super::ast::DataType::Boolean => DbDataType::Boolean,
                    super::ast::DataType::Varchar => DbDataType::String,
                };
                Column::new(&col.name, db_type)
            })
            .collect();

        let schema = Schema::new(columns);

        // Create table file path
        let table_path = self.db_path.join(format!("{}.db", stmt.table_name));

        // Create the heap table
        let table = HeapTable::create(&stmt.table_name, schema, table_path, self.buffer_pool_size)?;

        let table_name = stmt.table_name.clone();
        self.tables.insert(stmt.table_name, table);

        Ok(ExecutionResult::CreateTable { table_name })
    }

    /// Execute DROP TABLE statement
    fn execute_drop_table(&mut self, stmt: DropTableStmt) -> io::Result<ExecutionResult> {
        // Check if table exists
        if !self.tables.contains_key(&stmt.table_name) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' does not exist", stmt.table_name),
            ));
        }

        // Remove table from catalog (this also drops the HeapTable, flushing any dirty pages)
        self.tables.remove(&stmt.table_name);

        // Remove any indexes that reference this table
        self.indexes.retain(|idx| idx.key.table != stmt.table_name);

        // Persist updated index metadata
        self.persist_index_metadata()?;

        // Delete the table file from disk
        let table_path = self.db_path.join(format!("{}.db", stmt.table_name));
        if table_path.exists() {
            fs::remove_file(table_path)?;
        }

        Ok(ExecutionResult::DropTable {
            table_name: stmt.table_name,
        })
    }

    /// Execute DELETE statement
    fn execute_delete(&mut self, stmt: DeleteStmt) -> io::Result<ExecutionResult> {
        let table_name = stmt.table_name;
        let where_clause = stmt.where_clause;

        // Validate table exists and capture schema for predicate evaluation
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

        // Choose scan strategy using existing planner
        let planner = Planner::new(self.index_metadata());
        let scan_plan = planner.plan_scan(&table_name, where_clause.as_ref());
        let row_ids = match scan_plan {
            ScanPlan::IndexScan {
                index_columns,
                predicates,
            } => self.index_scan(&table_name, &index_columns, &predicates)?,
            ScanPlan::SeqScan => None,
        };

        // Collect target rows
        let rows_to_delete: Vec<RowId> = {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            let mut matches = Vec::new();

            if let Some(row_ids) = row_ids {
                for row_id in row_ids {
                    let row = match table.get(row_id) {
                        Ok(row) => row,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                        Err(e) => return Err(e),
                    };

                    if let Some(ref expr) = where_clause
                        && !Self::evaluate_predicate_static(expr, &row, &columns_meta)?
                    {
                        continue;
                    }

                    matches.push(row_id);
                }
            } else {
                let mut scan = TableScan::new(table);
                while let Some((row_id, row)) = scan.next()? {
                    if let Some(ref expr) = where_clause
                        && !Self::evaluate_predicate_static(expr, &row, &columns_meta)?
                    {
                        continue;
                    }
                    matches.push(row_id);
                }
            }

            matches
        };

        // Apply deletions
        let mut rows_deleted = 0;
        {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            for row_id in rows_to_delete {
                match table.delete(row_id) {
                    Ok(()) => rows_deleted += 1,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        // Rebuild indexes to drop stale entries
        if rows_deleted > 0 {
            self.rebuild_indexes_for_table(&table_name)?;
        }

        Ok(ExecutionResult::Delete { rows_deleted })
    }

    /// Execute UPDATE statement
    fn execute_update(&mut self, stmt: UpdateStmt) -> io::Result<ExecutionResult> {
        if stmt.assignments.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UPDATE must specify at least one column",
            ));
        }

        let table_name = stmt.table_name;
        let where_clause = stmt.where_clause;

        // Fetch schema for validation and column resolution
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

        // Resolve assignment targets and pre-validate literals
        let mut seen_columns = HashSet::new();
        let mut assignments = Vec::new();
        for (col_name, expr) in stmt.assignments {
            let (idx, column) = schema.find_column(&col_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Column '{}' not found in table '{}'", col_name, table_name),
                )
            })?;

            if !seen_columns.insert(idx) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Duplicate assignment for column '{}'", col_name),
                ));
            }

            if let Expr::Literal(ref lit) = expr {
                Self::literal_to_typed_value(lit, column.data_type())?;
            }

            assignments.push((idx, expr));
        }

        let planner = Planner::new(self.index_metadata());
        let scan_plan = planner.plan_scan(&table_name, where_clause.as_ref());
        let row_ids = match scan_plan {
            ScanPlan::IndexScan {
                index_columns,
                predicates,
            } => self.index_scan(&table_name, &index_columns, &predicates)?,
            ScanPlan::SeqScan => None,
        };

        let mut rows_updated = 0;
        {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            let mut pending_updates: Vec<(RowId, Vec<Value>)> = Vec::new();

            if let Some(row_ids) = row_ids {
                for row_id in row_ids {
                    let row = match table.get(row_id) {
                        Ok(row) => row,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                        Err(e) => return Err(e),
                    };

                    if let Some(ref expr) = where_clause
                        && !Self::evaluate_predicate_static(expr, &row, &columns_meta)?
                    {
                        continue;
                    }

                    let new_row =
                        Self::apply_assignments(&row, &assignments, &schema, &columns_meta)?;
                    pending_updates.push((row_id, new_row));
                }
            } else {
                let mut scan = TableScan::new(table);
                while let Some((row_id, row)) = scan.next()? {
                    if let Some(ref expr) = where_clause
                        && !Self::evaluate_predicate_static(expr, &row, &columns_meta)?
                    {
                        continue;
                    }

                    let new_row =
                        Self::apply_assignments(&row, &assignments, &schema, &columns_meta)?;
                    pending_updates.push((row_id, new_row));
                }
            }

            for (row_id, new_row) in pending_updates {
                match table.update(row_id, &new_row) {
                    Ok(_) => rows_updated += 1,
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        if rows_updated > 0 {
            self.rebuild_indexes_for_table(&table_name)?;
        }

        Ok(ExecutionResult::Update { rows_updated })
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

        let mut row_ids = Vec::new();

        for row_values in stmt.values {
            if row_values.len() != table.schema().column_count() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Row does not match table schema",
                ));
            }

            let values: Vec<Value> = row_values
                .iter()
                .zip(table.schema().columns())
                .map(|(lit, col)| Self::literal_to_typed_value(lit, col.data_type()))
                .collect::<io::Result<_>>()?;

            let row_id = table.insert(&values)?;

            for index in self
                .indexes
                .iter_mut()
                .filter(|idx| idx.key.table == stmt.table_name)
            {
                let key =
                    Self::build_composite_key(&values, &index.column_indices, &index.column_types)?;
                index.tree.insert(key, row_id);
            }

            row_ids.push(row_id);
        }

        Ok(ExecutionResult::Insert { row_ids })
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

        if stmt.columns.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Index must include at least one column",
            ));
        }

        // Ensure index name and column set are unique
        if self.indexes.iter().any(|idx| idx.name == stmt.index_name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Index '{}' already exists", stmt.index_name),
            ));
        }

        if self
            .indexes
            .iter()
            .any(|idx| idx.key.table == stmt.table_name && idx.key.columns == stmt.columns)
        {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "Index on {}({}) already exists",
                    stmt.table_name,
                    stmt.columns.join(", ")
                ),
            ));
        }

        // Resolve and validate columns
        let mut column_indices = Vec::new();
        let mut column_types = Vec::new();
        for col_name in &stmt.columns {
            let (idx, column) = schema.find_column(col_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "Column '{}' not found in table '{}'",
                        col_name, stmt.table_name
                    ),
                )
            })?;

            if !matches!(
                column.data_type(),
                DbDataType::Integer | DbDataType::Unsigned
            ) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Only INTEGER or UNSIGNED columns can be indexed",
                ));
            }

            column_indices.push(idx);
            column_types.push(column.data_type());
        }

        // Create the index and populate it with existing data
        let mut index = BPlusTree::new();

        // Scan the table and add all existing rows to the index
        let mut scan = TableScan::new(table);
        while let Some((row_id, row)) = scan.next()? {
            let key = Self::build_composite_key(&row, &column_indices, &column_types)?;
            index.insert(key, row_id);
        }

        let entry = IndexEntry {
            name: stmt.index_name.clone(),
            key: IndexKey {
                table: stmt.table_name.clone(),
                columns: stmt.columns.clone(),
            },
            column_indices,
            column_types,
            tree: index,
        };

        self.indexes.push(entry);
        self.persist_index_metadata()?;

        Ok(ExecutionResult::CreateIndex {
            index_name: stmt.index_name,
            table_name: stmt.table_name,
            columns: stmt.columns,
        })
    }

    /// Execute DROP INDEX statement
    fn execute_drop_index(&mut self, stmt: DropIndexStmt) -> io::Result<ExecutionResult> {
        // Find the index by name
        let index_pos = self
            .indexes
            .iter()
            .position(|idx| idx.name == stmt.index_name)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Index '{}' does not exist", stmt.index_name),
                )
            })?;

        // Remove the index
        self.indexes.remove(index_pos);

        // Persist updated index metadata
        self.persist_index_metadata()?;

        Ok(ExecutionResult::DropIndex {
            index_name: stmt.index_name,
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
        let mut plan_steps = Vec::new();
        plan_steps.push(Self::describe_scan(&table_name, &scan_plan));
        if let Some(ref predicate) = where_clause {
            plan_steps.push(format!("Filter: {}", Self::describe_expr(predicate)));
        }

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
            ScanPlan::IndexScan {
                index_columns,
                predicates,
            } => self.index_scan(&table_name, &index_columns, &predicates)?,
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
                let projected_row: Vec<Value> =
                    column_indices.iter().map(|&idx| row[idx].clone()).collect();

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
                let projected_row: Vec<Value> =
                    column_indices.iter().map(|&idx| row[idx].clone()).collect();

                result_rows.push(projected_row);
            }
        }

        Ok(ExecutionResult::Select {
            column_names,
            rows: result_rows,
            plan: plan_steps,
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
        let left_join_idx = Self::resolve_schema_column_index(
            &left_schema,
            &join_plan.outer_table,
            &join_plan.outer_column,
        )?;
        let right_join_idx = Self::resolve_schema_column_index(
            &right_schema,
            &join_plan.inner_table,
            &join_plan.inner_column,
        )?;

        let combined_meta = Self::build_join_column_metadata(
            &join_plan.outer_table,
            &left_schema,
            &join_plan.inner_table,
            &right_schema,
        );
        let (column_indices, column_names) =
            Self::build_projection(&combined_meta, &columns, true)?;

        match join_plan.strategy {
            JoinStrategy::NestedLoop { inner_has_index } => self.execute_nested_loop_join(
                join_plan,
                where_clause,
                left_join_idx,
                right_join_idx,
                &right_schema,
                &combined_meta,
                column_indices,
                column_names,
                inner_has_index,
            ),
            JoinStrategy::MergeJoin => self.execute_merge_join(
                join_plan,
                where_clause,
                left_join_idx,
                right_join_idx,
                &combined_meta,
                column_indices,
                column_names,
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_nested_loop_join(
        &mut self,
        join_plan: JoinPlan,
        where_clause: Option<Expr>,
        left_join_idx: usize,
        right_join_idx: usize,
        right_schema: &Schema,
        combined_meta: &[(Option<String>, String)],
        column_indices: Vec<usize>,
        column_names: Vec<String>,
        inner_has_index: bool,
    ) -> io::Result<ExecutionResult> {
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
        let right_join_is_integer = matches!(
            right_schema.columns()[right_join_idx].data_type(),
            DbDataType::Integer | DbDataType::Unsigned
        );
        let use_right_index = inner_has_index
            && right_join_is_integer
            && self
                .find_index_on_first_column(&index_key.0, &index_key.1)
                .is_some();

        let mut plan_steps = Vec::new();
        plan_steps.push(format!("Seq scan outer table {}", join_plan.outer_table));
        plan_steps.push(format!(
            "Nested loop join outer={} inner={} on {} = {}",
            join_plan.outer_table,
            join_plan.inner_table,
            Self::format_column_ref(&join_plan.outer_column),
            Self::format_column_ref(&join_plan.inner_column),
        ));
        if use_right_index {
            plan_steps.push(format!(
                "Use index on {}.{} for inner lookups",
                join_plan.inner_table, join_plan.inner_column.column
            ));
        } else {
            plan_steps.push(format!("Seq scan inner table {}", join_plan.inner_table));
        }
        if let Some(ref predicate) = where_clause {
            plan_steps.push(format!("Filter: {}", Self::describe_expr(predicate)));
        }

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
                // Look up matching row IDs via index first
                let mut matched_ids = Vec::new();
                if let Some(index) = self.find_index_on_first_column(&index_key.0, &index_key.1) {
                    let coerced_key =
                        Self::coerce_value_to_type(left_key.clone(), index.column_types[0])?;
                    let Some(index_value) = IndexValue::from_value(&coerced_key) else {
                        continue;
                    };
                    let mut start = CompositeKey::min_values(&index.column_types);
                    let mut end = CompositeKey::max_values(&index.column_types);
                    start.values[0] = index_value.clone();
                    end.values[0] = index_value;

                    for (_k, row_id) in index.tree.range_scan(&start, &end) {
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
                    && !Self::evaluate_predicate_static(where_expr, &combined_row, combined_meta)?
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
            plan: plan_steps,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_merge_join(
        &mut self,
        join_plan: JoinPlan,
        where_clause: Option<Expr>,
        left_join_idx: usize,
        right_join_idx: usize,
        combined_meta: &[(Option<String>, String)],
        column_indices: Vec<usize>,
        column_names: Vec<String>,
    ) -> io::Result<ExecutionResult> {
        let mut plan_steps = Vec::new();
        plan_steps.push(format!(
            "Merge join on {} = {}",
            Self::format_column_ref(&join_plan.outer_column),
            Self::format_column_ref(&join_plan.inner_column),
        ));
        plan_steps.push(format!("Sort {} on join key", join_plan.outer_table));
        plan_steps.push(format!("Sort {} on join key", join_plan.inner_table));
        if let Some(ref predicate) = where_clause {
            plan_steps.push(format!("Filter: {}", Self::describe_expr(predicate)));
        }

        // Load and sort both sides by join key
        let mut left_rows = self.load_sorted_rows(&join_plan.outer_table, left_join_idx)?;
        let mut right_rows = self.load_sorted_rows(&join_plan.inner_table, right_join_idx)?;

        left_rows.sort_by(|a, b| a.0.cmp(&b.0));
        right_rows.sort_by(|a, b| a.0.cmp(&b.0));

        let mut i = 0usize;
        let mut j = 0usize;
        let mut result_rows = Vec::new();

        while i < left_rows.len() && j < right_rows.len() {
            let left_key = &left_rows[i].0;
            let right_key = &right_rows[j].0;

            match left_key.cmp(right_key) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    let i_end = Self::advance_run_end(&left_rows, i);
                    let j_end = Self::advance_run_end(&right_rows, j);

                    for li in i..i_end {
                        for rj in j..j_end {
                            let mut combined = Vec::new();
                            combined.extend(left_rows[li].1.clone());
                            combined.extend(right_rows[rj].1.clone());

                            if let Some(ref where_expr) = where_clause
                                && !Self::evaluate_predicate_static(
                                    where_expr,
                                    &combined,
                                    combined_meta,
                                )?
                            {
                                continue;
                            }

                            let projected_row: Vec<Value> = column_indices
                                .iter()
                                .map(|&idx| combined[idx].clone())
                                .collect();
                            result_rows.push(projected_row);
                        }
                    }

                    i = i_end;
                    j = j_end;
                }
            }
        }

        Ok(ExecutionResult::Select {
            column_names,
            rows: result_rows,
            plan: plan_steps,
        })
    }

    fn load_sorted_rows(
        &mut self,
        table_name: &str,
        join_idx: usize,
    ) -> io::Result<Vec<(Value, Vec<Value>)>> {
        let table_ref = self.tables.get_mut(table_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' does not exist", table_name),
            )
        })?;

        let mut scan = TableScan::new(table_ref);
        let mut rows = Vec::new();
        while let Some((_row_id, row)) = scan.next()? {
            if join_idx >= row.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Join column index {} out of bounds for table {}",
                        join_idx, table_name
                    ),
                ));
            }
            rows.push((row[join_idx].clone(), row));
        }
        Ok(rows)
    }

    fn advance_run_end(rows: &[(Value, Vec<Value>)], start: usize) -> usize {
        let key = &rows[start].0;
        let mut idx = start + 1;
        while idx < rows.len() && rows[idx].0 == *key {
            idx += 1;
        }
        idx
    }

    /// Use an index for a simple predicate if available.
    /// Returns Some(row_ids) if an index can be used, None otherwise.
    fn index_scan(
        &self,
        table_name: &str,
        index_columns: &[String],
        predicates: &[(String, BinaryOp, Literal)],
    ) -> io::Result<Option<Vec<RowId>>> {
        let index = match self.find_index(table_name, index_columns) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let ranges = Self::build_ranges(index, predicates)?;
        if ranges.is_empty() {
            return Ok(None);
        }

        let mut row_ids = Vec::new();
        for (start, end) in ranges {
            for (_k, v) in index.tree.range_scan(&start, &end) {
                row_ids.push(v);
            }
        }

        Ok(Some(row_ids))
    }

    fn apply_assignments(
        row: &[Value],
        assignments: &[(usize, Expr)],
        schema: &Schema,
        columns_meta: &[(Option<String>, String)],
    ) -> io::Result<Vec<Value>> {
        let mut new_row = row.to_vec();

        for (idx, expr) in assignments {
            let column = schema.column(*idx).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Invalid column index {}", idx),
                )
            })?;

            let typed_value = match expr {
                Expr::Literal(lit) => Self::literal_to_typed_value(lit, column.data_type())?,
                _ => {
                    let value = Self::evaluate_expr_static(expr, row, columns_meta)?;
                    Self::coerce_value_to_type(value, column.data_type())?
                }
            };

            new_row[*idx] = typed_value;
        }

        Ok(new_row)
    }

    fn literal_to_value(lit: &Literal) -> io::Result<Value> {
        match lit {
            Literal::Integer(i) => {
                if *i < i64::MIN as i128 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Integer literal below supported range",
                    ));
                }

                if *i < 0 {
                    Ok(Value::Integer(*i as i64))
                } else if *i <= i64::MAX as i128 {
                    Ok(Value::Integer(*i as i64))
                } else if *i <= u64::MAX as i128 {
                    Ok(Value::Unsigned(*i as u64))
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Integer literal exceeds u64::MAX",
                    ))
                }
            }
            Literal::Float(fv) => Ok(Value::Float(*fv)),
            Literal::Boolean(b) => Ok(Value::Boolean(*b)),
            Literal::String(s) => Ok(Value::String(s.clone())),
        }
    }

    fn literal_to_typed_value(lit: &Literal, data_type: DbDataType) -> io::Result<Value> {
        let value = Self::literal_to_value(lit)?;
        Self::coerce_value_to_type(value, data_type)
    }

    fn coerce_value_to_type(value: Value, data_type: DbDataType) -> io::Result<Value> {
        match (data_type, value) {
            (DbDataType::Integer, Value::Integer(i)) => Ok(Value::Integer(i)),
            (DbDataType::Integer, Value::Unsigned(u)) if u <= i64::MAX as u64 => {
                Ok(Value::Integer(u as i64))
            }
            (DbDataType::Integer, Value::Float(fv))
                if fv.fract() == 0.0 && fv <= i64::MAX as f64 =>
            {
                Ok(Value::Integer(fv as i64))
            }
            (DbDataType::Unsigned, Value::Unsigned(u)) => Ok(Value::Unsigned(u)),
            (DbDataType::Unsigned, Value::Integer(i)) if i >= 0 => Ok(Value::Unsigned(i as u64)),
            (DbDataType::Unsigned, Value::Float(fv)) if fv.fract() == 0.0 && fv >= 0.0 => {
                Ok(Value::Unsigned(fv as u64))
            }
            (DbDataType::Float, Value::Float(fv)) => Ok(Value::Float(fv)),
            (DbDataType::Float, Value::Integer(i)) => Ok(Value::Float(i as f64)),
            (DbDataType::Float, Value::Unsigned(u)) => Ok(Value::Float(u as f64)),
            (DbDataType::Boolean, Value::Boolean(b)) => Ok(Value::Boolean(b)),
            (DbDataType::String, Value::String(s)) => Ok(Value::String(s)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Type mismatch: expected {}", data_type),
            )),
        }
    }

    /// Evaluate a predicate expression against a row (static version)
    fn evaluate_predicate_static(
        expr: &Expr,
        row: &[Value],
        columns: &[(Option<String>, String)],
    ) -> io::Result<bool> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                if *op == BinaryOp::And {
                    let left_result = Self::evaluate_predicate_static(left, row, columns)?;
                    if !left_result {
                        return Ok(false);
                    }
                    return Self::evaluate_predicate_static(right, row, columns);
                }

                let left_val = Self::evaluate_expr_static(left, row, columns)?;
                let right_val = Self::evaluate_expr_static(right, row, columns)?;

                let result = match op {
                    BinaryOp::Eq => left_val == right_val,
                    BinaryOp::NotEq => left_val != right_val,
                    BinaryOp::Lt => left_val < right_val,
                    BinaryOp::LtEq => left_val <= right_val,
                    BinaryOp::Gt => left_val > right_val,
                    BinaryOp::GtEq => left_val >= right_val,
                    BinaryOp::And => unreachable!(),
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
            Expr::Literal(lit) => Self::literal_to_value(lit),
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

    fn rebuild_indexes_for_table(&mut self, table_name: &str) -> io::Result<()> {
        let rows = {
            let table = self.tables.get_mut(table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            let mut scan = TableScan::new(table);
            let mut rows = Vec::new();
            while let Some((row_id, row)) = scan.next()? {
                rows.push((row_id, row));
            }
            rows
        };

        for index in self
            .indexes
            .iter_mut()
            .filter(|idx| idx.key.table == table_name)
        {
            let mut tree = BPlusTree::new();
            for (row_id, row) in &rows {
                let key =
                    Self::build_composite_key(row, &index.column_indices, &index.column_types)?;
                tree.insert(key, *row_id);
            }
            index.tree = tree;
        }

        Ok(())
    }

    fn index_metadata(&self) -> Vec<IndexMetadata> {
        self.indexes
            .iter()
            .map(|idx| IndexMetadata {
                table: idx.key.table.clone(),
                columns: idx.key.columns.clone(),
            })
            .collect()
    }

    fn build_column_metadata_for_table(
        table_name: &str,
        schema: &Schema,
    ) -> Vec<(Option<String>, String)> {
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
        columns.extend(Self::build_column_metadata_for_table(
            right_table,
            right_schema,
        ));
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
                    names.push(Self::format_column_name(&columns_meta[idx], use_qualified));
                }
                Ok((indices, names))
            }
        }
    }

    fn resolve_column_index(
        columns_meta: &[(Option<String>, String)],
        col_ref: &ColumnRef,
    ) -> io::Result<usize> {
        let mut matches =
            columns_meta
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
        if let Some(ref table) = col_ref.table
            && table != table_name
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Column '{}' does not belong to table '{}'",
                    col_ref.column, table_name
                ),
            ));
        }

        schema
            .find_column(&col_ref.column)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Column '{}' not found in table '{}'",
                        col_ref.column, table_name
                    ),
                )
            })
    }

    fn build_composite_key(
        row: &[Value],
        column_indices: &[usize],
        column_types: &[DbDataType],
    ) -> io::Result<CompositeKey> {
        let mut values = Vec::with_capacity(column_indices.len());
        for (&idx, data_type) in column_indices.iter().zip(column_types.iter()) {
            let value = row.get(idx).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Column index {} out of bounds for row", idx),
                )
            })?;

            let index_value = match (data_type, value) {
                (DbDataType::Integer, Value::Integer(i)) => IndexValue::Signed(*i),
                (DbDataType::Integer, Value::Unsigned(u)) if *u <= i64::MAX as u64 => {
                    IndexValue::Signed(*u as i64)
                }
                (DbDataType::Unsigned, Value::Unsigned(u)) => IndexValue::Unsigned(*u),
                (DbDataType::Unsigned, Value::Integer(i)) if *i >= 0 => {
                    IndexValue::Unsigned(*i as u64)
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Indexing currently only supports INTEGER or UNSIGNED columns",
                    ));
                }
            };
            values.push(index_value);
        }
        Ok(CompositeKey::new(values))
    }

    fn find_index_on_first_column(&self, table: &str, column: &str) -> Option<&IndexEntry> {
        self.indexes.iter().find(|idx| {
            idx.key.table == table && idx.key.columns.first().is_some_and(|c| c == column)
        })
    }

    fn find_index(&self, table: &str, columns: &[String]) -> Option<&IndexEntry> {
        self.indexes
            .iter()
            .find(|idx| idx.key.table == table && idx.key.columns == columns)
    }

    fn build_ranges(
        index: &IndexEntry,
        predicates: &[(String, BinaryOp, Literal)],
    ) -> io::Result<Vec<(CompositeKey, CompositeKey)>> {
        if predicates.is_empty() {
            return Ok(Vec::new());
        }

        let mut start = CompositeKey::min_values(&index.column_types);
        let mut end = CompositeKey::max_values(&index.column_types);
        let mut eq_prefix = true;

        for (i, col_name) in index.key.columns.iter().enumerate() {
            let pred = predicates.iter().find(|(c, _, _)| c == col_name);
            let Some((_, op, lit)) = pred else {
                break;
            };

            let Some(value) = IndexValue::from_literal(lit, &index.column_types[i]) else {
                return Ok(Vec::new());
            };

            if !eq_prefix {
                break;
            }

            match op {
                BinaryOp::Eq => {
                    start.values[i] = value.clone();
                    end.values[i] = value;
                }
                BinaryOp::Lt => {
                    end.values[i] = value.saturating_sub_one();
                    eq_prefix = false;
                }
                BinaryOp::LtEq => {
                    end.values[i] = value;
                    eq_prefix = false;
                }
                BinaryOp::Gt => {
                    start.values[i] = value.saturating_add_one();
                    eq_prefix = false;
                }
                BinaryOp::GtEq => {
                    start.values[i] = value;
                    eq_prefix = false;
                }
                BinaryOp::NotEq => {
                    if i == 0 {
                        let mut ranges = Vec::new();
                        if value > IndexValue::min_value(&index.column_types[0]) {
                            let mut left_end = end.clone();
                            left_end.values[0] = value.saturating_sub_one();
                            ranges.push((start.clone(), left_end));
                        }
                        if value < IndexValue::max_value(&index.column_types[0]) {
                            let mut right_start = start.clone();
                            right_start.values[0] = value.saturating_add_one();
                            ranges
                                .push((right_start, CompositeKey::max_values(&index.column_types)));
                        }
                        return Ok(ranges);
                    } else {
                        return Ok(Vec::new());
                    }
                }
                BinaryOp::And => unreachable!(),
            }
        }

        Ok(vec![(start, end)])
    }

    fn persist_index_metadata(&self) -> io::Result<()> {
        let path = self.db_path.join("indexes.meta");
        let mut buf = String::new();
        for idx in &self.indexes {
            buf.push_str(&format!(
                "{}|{}|{}\n",
                idx.name,
                idx.key.table,
                idx.key.columns.join(",")
            ));
        }
        fs::write(path, buf)
    }

    fn load_indexes_from_metadata(&mut self) -> io::Result<()> {
        let path = self.db_path.join("indexes.meta");
        if !path.exists() {
            return Ok(());
        }

        let data = fs::read_to_string(&path)?;
        for line in data.lines() {
            let mut parts = line.split('|');
            let (Some(name), Some(table), Some(cols_str)) =
                (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };

            let columns: Vec<String> = cols_str
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if columns.is_empty() {
                continue;
            }

            let table_ref = match self.tables.get_mut(table) {
                Some(t) => t,
                None => continue,
            };
            let schema = table_ref.schema().clone();

            let mut column_indices = Vec::new();
            let mut column_types = Vec::new();
            for col in &columns {
                if let Some((idx, column)) = schema.find_column(col) {
                    if !matches!(
                        column.data_type(),
                        DbDataType::Integer | DbDataType::Unsigned
                    ) {
                        column_indices.clear();
                        column_types.clear();
                        break;
                    }
                    column_indices.push(idx);
                    column_types.push(column.data_type());
                } else {
                    column_indices.clear();
                    column_types.clear();
                    break;
                }
            }
            if column_indices.is_empty() {
                continue;
            }

            let mut tree = BPlusTree::new();
            let mut scan = TableScan::new(table_ref);
            while let Some((row_id, row)) = scan.next()? {
                let key = Self::build_composite_key(&row, &column_indices, &column_types)?;
                tree.insert(key, row_id);
            }

            self.indexes.push(IndexEntry {
                name: name.to_string(),
                key: IndexKey {
                    table: table.to_string(),
                    columns: columns.clone(),
                },
                column_indices,
                column_types,
                tree,
            });
        }

        Ok(())
    }

    fn describe_scan(table: &str, scan_plan: &ScanPlan) -> String {
        match scan_plan {
            ScanPlan::SeqScan => format!("Seq scan on {}", table),
            ScanPlan::IndexScan {
                index_columns,
                predicates,
            } => {
                let pred_str = predicates
                    .iter()
                    .map(|(col, op, lit)| {
                        format!("{} {} {}", col, Self::format_binary_op(*op), lit)
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!(
                    "Index scan on {} using ({}) with {}",
                    table,
                    index_columns.join(", "),
                    pred_str
                )
            }
        }
    }

    fn describe_expr(expr: &Expr) -> String {
        match expr {
            Expr::Column(col_ref) => Self::format_column_ref(col_ref),
            Expr::Literal(lit) => lit.to_string(),
            Expr::BinaryOp { left, op, right } => format!(
                "{} {} {}",
                Self::describe_expr(left),
                Self::format_binary_op(*op),
                Self::describe_expr(right)
            ),
        }
    }

    fn format_binary_op(op: BinaryOp) -> &'static str {
        match op {
            BinaryOp::Eq => "=",
            BinaryOp::NotEq => "!=",
            BinaryOp::Lt => "<",
            BinaryOp::LtEq => "<=",
            BinaryOp::Gt => ">",
            BinaryOp::GtEq => ">=",
            BinaryOp::And => "AND",
        }
    }

    fn format_column_ref(col_ref: &ColumnRef) -> String {
        match &col_ref.table {
            Some(table) => format!("{}.{}", table, col_ref.column),
            None => col_ref.column.clone(),
        }
    }

    /// Flush all tables
    pub fn flush_all(&mut self) -> io::Result<()> {
        for table in self.tables.values_mut() {
            table.flush()?;
        }
        Ok(())
    }

    /// Return table names and schemas currently loaded.
    pub fn list_tables(&self) -> Vec<(String, Schema)> {
        self.tables
            .iter()
            .map(|(name, table)| (name.clone(), table.schema().clone()))
            .collect()
    }

    /// Return index metadata currently loaded.
    pub fn list_indexes(&self) -> Vec<(String, String, Vec<String>)> {
        self.indexes
            .iter()
            .map(|idx| {
                (
                    idx.name.clone(),
                    idx.key.table.clone(),
                    idx.key.columns.clone(),
                )
            })
            .collect()
    }
}
