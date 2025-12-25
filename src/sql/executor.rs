use super::ast::{
    BinaryOp, ColumnRef, CreateIndexStmt, CreateTableStmt, DeleteStmt, DropIndexStmt,
    AggregateExpr, AggregateFunc, AggregateTarget, DropTableStmt, Expr, IndexType, InsertStmt,
    Literal, OrderByExpr, SelectColumn, SelectItem, SelectStmt, Statement, TransactionCommand,
    TransactionStmt, UpdateStmt,
};
use super::parser::parse_sql;
use crate::index::{BPlusTree, HashIndex};
use crate::optimizer::planner::{
    FromClausePlan, IndexMetadata, JoinPlan, JoinStrategy, Planner, ScanPlan,
};
use crate::serialization::{RowMetadata, RowSerializer};
use crate::table::{HeapTable, RowId, TableScan};
use crate::types::{Column, DataType as DbDataType, Schema, Value};
use crate::wal::{WalFile, WalRecord, TxnId};
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
        index_type: IndexType,
    },
    /// Index dropped successfully
    DropIndex { index_name: String },
    /// Rows deleted successfully
    Delete { rows_deleted: usize },
    /// Rows updated successfully
    Update { rows_updated: usize },
    /// Transaction control statement
    Transaction { command: TransactionCommand },
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
                index_type,
            } => {
                write!(
                    f,
                    "Index '{}' ({}) created on {}({})",
                    index_name,
                    index_type,
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
            ExecutionResult::Transaction { command } => match command {
                TransactionCommand::Begin => write!(f, "Transaction started"),
                TransactionCommand::Commit => write!(f, "Transaction committed"),
                TransactionCommand::Rollback => write!(f, "Transaction rolled back"),
            },
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
    index_type: IndexType,
    data: IndexData,
}

enum IndexData {
    BTree(BPlusTree<CompositeKey, RowId>),
    Hash(HashIndex<CompositeKey, RowId>),
}

impl IndexEntry {
    fn insert(&mut self, key: CompositeKey, row_id: RowId) {
        match &mut self.data {
            IndexData::BTree(tree) => tree.insert(key, row_id),
            IndexData::Hash(index) => index.insert(key, row_id),
        }
    }

    fn lookup_range(&self, ranges: &[(CompositeKey, CompositeKey)]) -> Vec<RowId> {
        let mut row_ids = Vec::new();
        if let IndexData::BTree(tree) = &self.data {
            for (start, end) in ranges {
                for (_k, v) in tree.range_scan(start, end) {
                    row_ids.push(v);
                }
            }
        }
        row_ids
    }

    fn lookup_eq(&self, key: &CompositeKey) -> Vec<RowId> {
        match &self.data {
            IndexData::BTree(tree) => tree.range_scan(key, key).map(|(_k, v)| v).collect(),
            IndexData::Hash(index) => index.get(key).copied().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CompositeKey {
    values: Vec<IndexValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
            Value::Null => None,
            _ => None,
        }
    }

    fn from_literal(lit: &Literal, data_type: &DbDataType) -> Option<Self> {
        match (lit, data_type) {
            (Literal::Null, _) => None,
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
    /// Index catalog (in-memory B-Tree or hash indexes over integer columns)
    indexes: Vec<IndexEntry>,
    /// Transaction state (syntax-only for now).
    in_transaction: bool,
    /// Active transaction identifier (when in_transaction).
    current_txn_id: Option<TxnId>,
    /// Next transaction identifier to allocate.
    next_txn_id: TxnId,
    /// Write-ahead log handle.
    wal: WalFile,
    /// In-memory log for undo on rollback.
    txn_log: Vec<WalRecord>,
    /// Active transactions for snapshotting.
    active_txns: HashSet<TxnId>,
    /// Per-transaction snapshots.
    snapshots: HashMap<TxnId, Snapshot>,
    /// Transaction state table (active/committed/aborted).
    txn_states: HashMap<TxnId, TxnState>,
    /// Table constraints (primary/unique/foreign keys).
    constraints: HashMap<String, TableConstraints>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub xmin: TxnId,
    pub xmax: TxnId,
    pub active: HashSet<TxnId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TableConstraints {
    primary_key: Option<String>,
    unique: HashSet<String>,
    not_null: HashSet<String>,
    foreign_keys: Vec<ForeignKey>,
    checks: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ForeignKey {
    column: String,
    ref_table: String,
    ref_column: String,
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

        let wal_path = db_path.join("wal.log");
        let mut executor = Self {
            db_path,
            buffer_pool_size,
            tables,
            indexes: Vec::new(),
            in_transaction: false,
            current_txn_id: None,
            next_txn_id: 1,
            wal: WalFile::new(wal_path),
            txn_log: Vec::new(),
            active_txns: HashSet::new(),
            snapshots: HashMap::new(),
            txn_states: HashMap::new(),
            constraints: HashMap::new(),
        };

        executor.recover_from_wal()?;
        executor.load_indexes_from_metadata()?;
        executor.load_constraints_metadata()?;

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
            Statement::Transaction(txn) => self.execute_transaction(txn),
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
                    super::ast::DataType::Date => DbDataType::Date,
                    super::ast::DataType::Timestamp => DbDataType::Timestamp,
                    super::ast::DataType::Decimal => DbDataType::Decimal,
                };
                Column::new(&col.name, db_type)
            })
            .collect();

        let schema = Schema::new(columns);

        let mut primary_key: Option<String> = None;
        let mut unique: HashSet<String> = HashSet::new();
        let mut not_null = HashSet::new();
        let mut foreign_keys = Vec::new();
        let mut checks = Vec::new();

        for col_def in &stmt.columns {
            if col_def.is_primary_key {
                if primary_key.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Only one PRIMARY KEY is supported",
                    ));
                }
                primary_key = Some(col_def.name.clone());
                unique.insert(col_def.name.clone());
                not_null.insert(col_def.name.clone());
            } else if col_def.is_unique {
                unique.insert(col_def.name.clone());
            }
            if col_def.is_not_null {
                not_null.insert(col_def.name.clone());
            }

            if let Some(ref expr) = col_def.check {
                checks.push(expr.clone());
            }

            if let Some(ref fk) = col_def.references {
                let referenced = self.tables.get(&fk.table).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Referenced table '{}' does not exist", fk.table),
                    )
                })?;
                let (_, ref_col) = referenced
                    .schema()
                    .find_column(&fk.column)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "Referenced column '{}.{}' does not exist",
                                fk.table, fk.column
                            ),
                        )
                    })?;

                let (_, column) = schema.find_column(&col_def.name).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Column '{}' not found in schema", col_def.name),
                    )
                })?;
                if column.data_type() != ref_col.data_type() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Foreign key type mismatch: {} ({}) references {}.{} ({})",
                            col_def.name,
                            column.data_type(),
                            fk.table,
                            fk.column,
                            ref_col.data_type()
                        ),
                    ));
                }

                foreign_keys.push(ForeignKey {
                    column: col_def.name.clone(),
                    ref_table: fk.table.clone(),
                    ref_column: fk.column.clone(),
                });
            }
        }

        // Create table file path
        let table_path = self.db_path.join(format!("{}.db", stmt.table_name));

        // Create the heap table
        let table = HeapTable::create(&stmt.table_name, schema, table_path, self.buffer_pool_size)?;

        let table_name = stmt.table_name.clone();
        self.tables.insert(stmt.table_name, table);
        self.constraints.insert(
            table_name.clone(),
            TableConstraints {
                primary_key,
                unique,
                not_null,
                foreign_keys,
                checks,
            },
        );
        self.persist_constraints_metadata()?;

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
        self.constraints.remove(&stmt.table_name);

        // Persist updated index metadata
        self.persist_index_metadata()?;
        self.persist_constraints_metadata()?;

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
                index_type,
                predicates,
            } => self.index_scan(&table_name, &index_columns, index_type, &predicates)?,
            ScanPlan::SeqScan => None,
        };

        let snapshot = self.current_snapshot();
        let current_txn_id = self.current_txn_id;
        let txn_states = self.txn_states.clone();

        // Collect target rows
        let mut conflict_row: Option<RowId> = None;
        let rows_to_delete: Vec<(RowId, Vec<Value>)> = {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            let mut matches = Vec::new();

            if let Some(row_ids) = row_ids {
                for row_id in row_ids {
                    let (meta, row) = match table.get_with_metadata(row_id) {
                        Ok((meta, row)) => {
                            if !Self::is_visible_for_snapshot(
                                &meta,
                                snapshot.as_ref(),
                                current_txn_id,
                                &txn_states,
                            ) {
                                continue;
                            }
                            (meta, row)
                        }
                        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                        Err(e) => return Err(e),
                    };

                    if let Some(ref expr) = where_clause
                        && !Self::evaluate_predicate_static(expr, &row, &columns_meta)?
                    {
                        continue;
                    }

                    if Self::has_write_conflict(&meta, current_txn_id, &txn_states) {
                        conflict_row = Some(row_id);
                        break;
                    }

                    matches.push((row_id, row));
                }
            } else {
                let mut scan = TableScan::new(table);
                while let Some((row_id, meta, row)) = scan.next_with_metadata()? {
                    if !Self::is_visible_for_snapshot(
                        &meta,
                        snapshot.as_ref(),
                        current_txn_id,
                        &txn_states,
                    ) {
                        continue;
                    }
                    if let Some(ref expr) = where_clause
                        && !Self::evaluate_predicate_static(expr, &row, &columns_meta)?
                    {
                        continue;
                    }
                    if Self::has_write_conflict(&meta, current_txn_id, &txn_states) {
                        conflict_row = Some(row_id);
                        break;
                    }

                    matches.push((row_id, row));
                }
            }

            matches
        };

        if let Some(row_id) = conflict_row {
            if self.in_transaction {
                self.abort_current_transaction()?;
            }
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Write conflict detected on row {:?}", row_id),
            ));
        }

        for (_row_id, row) in &rows_to_delete {
            self.enforce_no_fk_references(&table_name, row)?;
        }

        // Apply deletions
        let mut rows_deleted = 0;
        let wal_context = if rows_to_delete.is_empty() {
            None
        } else {
            Some(self.wal_txn_for_mutation()?)
        };
        let track_txn = wal_context.as_ref().is_some_and(|(_, implicit)| !*implicit);
        let mut wal_records = Vec::new();
        {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            for (row_id, row) in rows_to_delete {
                let (txn_id, _) = match wal_context {
                    Some(context) => context,
                    None => continue,
                };

                let (mut meta, values) = table.get_with_metadata(row_id)?;
                meta.xmax = txn_id;
                let serialized = RowSerializer::serialize_with_metadata(
                    &values,
                    Some(table.schema()),
                    meta,
                )
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let page = table.buffer_pool_mut().fetch_page(row_id.page_id())?;
                page.update_row(row_id.slot_id(), &serialized)
                    .map_err(io::Error::from)?;
                table
                    .buffer_pool_mut()
                    .unpin_page(row_id.page_id(), true);

                rows_deleted += 1;
                wal_records.push(WalRecord::Delete {
                    txn_id,
                    table: table_name.clone(),
                    row_id,
                    values: row,
                });
            }
        }

        for record in wal_records {
            self.wal.append(&record)?;
            if track_txn {
                self.txn_log.push(record);
            }
        }

        if let Some((txn_id, implicit)) = wal_context {
            if implicit {
                self.wal.append(&WalRecord::Commit { txn_id })?;
                self.set_txn_state(txn_id, TxnState::Committed);
            }
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
                index_type,
                predicates,
            } => self.index_scan(&table_name, &index_columns, index_type, &predicates)?,
            ScanPlan::SeqScan => None,
        };

        let snapshot = self.current_snapshot();
        let current_txn_id = self.current_txn_id;
        let txn_states = self.txn_states.clone();
        let mut rows_updated = 0;
        let mut conflict_row: Option<RowId> = None;
        let pending_updates: Vec<(RowId, Vec<Value>, Vec<Value>)> = {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            let mut pending = Vec::new();

            if let Some(row_ids) = row_ids {
                for row_id in row_ids {
                    let (meta, row) = match table.get_with_metadata(row_id) {
                        Ok((meta, row)) => {
                            if !Self::is_visible_for_snapshot(
                                &meta,
                                snapshot.as_ref(),
                                current_txn_id,
                                &txn_states,
                            ) {
                                continue;
                            }
                            (meta, row)
                        }
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
                    if Self::has_write_conflict(&meta, current_txn_id, &txn_states) {
                        conflict_row = Some(row_id);
                        break;
                    }

                    pending.push((row_id, row, new_row));
                }
            } else {
                let mut scan = TableScan::new(table);
                while let Some((row_id, meta, row)) = scan.next_with_metadata()? {
                    if !Self::is_visible_for_snapshot(
                        &meta,
                        snapshot.as_ref(),
                        current_txn_id,
                        &txn_states,
                    ) {
                        continue;
                    }
                    if let Some(ref expr) = where_clause
                        && !Self::evaluate_predicate_static(expr, &row, &columns_meta)?
                    {
                        continue;
                    }

                    let new_row =
                        Self::apply_assignments(&row, &assignments, &schema, &columns_meta)?;
                    if Self::has_write_conflict(&meta, current_txn_id, &txn_states) {
                        conflict_row = Some(row_id);
                        break;
                    }

                    pending.push((row_id, row, new_row));
                }
            }

            pending
        };

        if let Some(row_id) = conflict_row {
            if self.in_transaction {
                self.abort_current_transaction()?;
            }
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Write conflict detected on row {:?}", row_id),
            ));
        }

        let updated_rows: Vec<Vec<Value>> = pending_updates
            .iter()
            .map(|(_, _, new_row)| new_row.clone())
            .collect();
        self.validate_batch_uniques(&table_name, &updated_rows)?;
        for (row_id, before_row, new_row) in &pending_updates {
            self.enforce_constraints_for_row(&table_name, new_row, Some(*row_id))?;
            self.enforce_no_fk_references_on_update(&table_name, before_row, new_row)?;
        }

        let wal_context = if pending_updates.is_empty() {
            None
        } else {
            Some(self.wal_txn_for_mutation()?)
        };
        let track_txn = wal_context.as_ref().is_some_and(|(_, implicit)| !*implicit);
        let mut wal_records = Vec::new();
        {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            for (row_id, before_row, new_row) in pending_updates {
                let (txn_id, _) = match wal_context {
                    Some(context) => context,
                    None => continue,
                };

                let (mut old_meta, old_values) = table.get_with_metadata(row_id)?;
                let new_meta = RowMetadata { xmin: txn_id, xmax: 0 };
                let _new_row_id = table.insert_with_metadata(&new_row, new_meta)?;

                old_meta.xmax = txn_id;
                let serialized = RowSerializer::serialize_with_metadata(
                    &old_values,
                    Some(table.schema()),
                    old_meta,
                )
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                let page = table.buffer_pool_mut().fetch_page(row_id.page_id())?;
                page.update_row(row_id.slot_id(), &serialized)
                    .map_err(io::Error::from)?;
                table
                    .buffer_pool_mut()
                    .unpin_page(row_id.page_id(), true);

                rows_updated += 1;
                wal_records.push(WalRecord::Update {
                    txn_id,
                    table: table_name.clone(),
                    row_id,
                    before: before_row,
                    after: new_row,
                });
            }
        }

        for record in wal_records {
            self.wal.append(&record)?;
            if track_txn {
                self.txn_log.push(record);
            }
        }

        if let Some((txn_id, implicit)) = wal_context {
            if implicit {
                self.wal.append(&WalRecord::Commit { txn_id })?;
                self.set_txn_state(txn_id, TxnState::Committed);
            }
        }

        if rows_updated > 0 {
            self.rebuild_indexes_for_table(&table_name)?;
        }

        Ok(ExecutionResult::Update { rows_updated })
    }

    fn execute_transaction(&mut self, stmt: TransactionStmt) -> io::Result<ExecutionResult> {
        match stmt.command {
            TransactionCommand::Begin => {
                if self.in_transaction {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Transaction already in progress",
                    ));
                }
                let txn_id = self.allocate_txn_id();
                let snapshot_active = self.active_txns.clone();
                let xmin = snapshot_active.iter().copied().min().unwrap_or(txn_id);
                let snapshot = Snapshot {
                    xmin,
                    xmax: self.next_txn_id,
                    active: snapshot_active.clone(),
                };
                self.snapshots.insert(txn_id, snapshot);
                self.wal.append(&WalRecord::Begin { txn_id })?;
                self.set_txn_state(txn_id, TxnState::Active);
                self.in_transaction = true;
                self.current_txn_id = Some(txn_id);
                self.txn_log.clear();
            }
            TransactionCommand::Commit => {
                if !self.in_transaction {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "No active transaction to commit",
                    ));
                }
                let txn_id = self
                    .current_txn_id
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing txn id"))?;
                self.wal.append(&WalRecord::Commit { txn_id })?;
                self.set_txn_state(txn_id, TxnState::Committed);
                self.snapshots.remove(&txn_id);
                self.in_transaction = false;
                self.current_txn_id = None;
                self.txn_log.clear();
            }
            TransactionCommand::Rollback => {
                if !self.in_transaction {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "No active transaction to rollback",
                    ));
                }
                let txn_id = self
                    .current_txn_id
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing txn id"))?;
                self.undo_transaction()?;
                self.wal.append(&WalRecord::Rollback { txn_id })?;
                self.set_txn_state(txn_id, TxnState::Aborted);
                self.snapshots.remove(&txn_id);
                self.in_transaction = false;
                self.current_txn_id = None;
                self.txn_log.clear();
            }
        }

        Ok(ExecutionResult::Transaction {
            command: stmt.command,
        })
    }

    /// Execute INSERT statement
    fn execute_insert(&mut self, stmt: InsertStmt) -> io::Result<ExecutionResult> {
        let mut row_ids = Vec::new();
        let table_name = stmt.table_name;

        let schema = {
            let table = self.tables.get(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;
            table.schema().clone()
        };

        let mut prepared_rows = Vec::new();
        for row_values in stmt.values {
            if row_values.len() != schema.column_count() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Row does not match table schema",
                ));
            }

            let values: Vec<Value> = row_values
                .iter()
                .zip(schema.columns())
                .map(|(lit, col)| Self::literal_to_typed_value(lit, col.data_type()))
                .collect::<io::Result<_>>()?;
            prepared_rows.push(values);
        }

        if !prepared_rows.is_empty() {
            self.validate_batch_uniques(&table_name, &prepared_rows)?;
            for row in &prepared_rows {
                self.enforce_constraints_for_row(&table_name, row, None)?;
            }
        }

        let has_values = !prepared_rows.is_empty();
        let wal_context = if has_values {
            Some(self.wal_txn_for_mutation()?)
        } else {
            None
        };
        let track_txn = wal_context.as_ref().is_some_and(|(_, implicit)| !*implicit);
        let mut wal_records = Vec::new();

        {
            let table = self.tables.get_mut(&table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            for values in prepared_rows {
                let row_id = table.insert(&values)?;

                for index in self
                    .indexes
                    .iter_mut()
                    .filter(|idx| idx.key.table == table_name)
                {
                    let key = Self::build_composite_key(
                        &values,
                        &index.column_indices,
                        &index.column_types,
                    )?;
                    index.insert(key, row_id);
                }

                if let Some((txn_id, _)) = wal_context {
                    wal_records.push(WalRecord::Insert {
                        txn_id,
                        table: table_name.clone(),
                        row_id,
                        values,
                    });
                }

                row_ids.push(row_id);
            }
        }

        for record in wal_records {
            self.wal.append(&record)?;
            if track_txn {
                self.txn_log.push(record);
            }
        }

        if let Some((txn_id, implicit)) = wal_context {
            if implicit {
                self.wal.append(&WalRecord::Commit { txn_id })?;
                self.set_txn_state(txn_id, TxnState::Committed);
            }
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
        let mut data = match stmt.index_type {
            IndexType::BTree => IndexData::BTree(BPlusTree::new()),
            IndexType::Hash => IndexData::Hash(HashIndex::new()),
        };

        // Scan the table and add all existing rows to the index
        let mut scan = TableScan::new(table);
        while let Some((row_id, row)) = scan.next()? {
            let key = Self::build_composite_key(&row, &column_indices, &column_types)?;
            match &mut data {
                IndexData::BTree(tree) => tree.insert(key, row_id),
                IndexData::Hash(index) => index.insert(key, row_id),
            }
        }

        let entry = IndexEntry {
            name: stmt.index_name.clone(),
            key: IndexKey {
                table: stmt.table_name.clone(),
                columns: stmt.columns.clone(),
            },
            column_indices,
            column_types,
            index_type: stmt.index_type,
            data,
        };

        self.indexes.push(entry);
        self.persist_index_metadata()?;

        Ok(ExecutionResult::CreateIndex {
            index_name: stmt.index_name,
            table_name: stmt.table_name,
            columns: stmt.columns,
            index_type: stmt.index_type,
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
                self.execute_select_single_table_plan(
                    plan.columns,
                    table,
                    plan.filter,
                    scan,
                    &stmt.group_by,
                    stmt.distinct,
                    &stmt.order_by,
                    stmt.limit,
                    stmt.offset,
                )
            }
            FromClausePlan::Join(join_plan) => {
                self.execute_select_join_plan(
                    plan.columns,
                    join_plan,
                    plan.filter,
                    &stmt.group_by,
                    stmt.distinct,
                    &stmt.order_by,
                    stmt.limit,
                    stmt.offset,
                )
            }
        }
    }

    fn execute_select_single_table_plan(
        &mut self,
        columns: SelectColumn,
        table_name: String,
        where_clause: Option<Expr>,
        scan_plan: ScanPlan,
        group_by: &[ColumnRef],
        distinct: bool,
        order_by: &[OrderByExpr],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> io::Result<ExecutionResult> {
        let mut plan_steps = Vec::new();
        plan_steps.push(Self::describe_scan(&table_name, &scan_plan));
        if let Some(ref predicate) = where_clause {
            plan_steps.push(format!("Filter: {}", Self::describe_expr(predicate)));
        }
        if !order_by.is_empty() {
            let order_desc = order_by
                .iter()
                .map(|expr| {
                    format!(
                        "{} {}",
                        Self::format_column_ref(&expr.column),
                        if expr.ascending { "ASC" } else { "DESC" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            plan_steps.push(format!("Order by: {}", order_desc));
        }
        if let Some(limit) = limit {
            plan_steps.push(format!("Limit: {}", limit));
        }
        if let Some(offset) = offset {
            plan_steps.push(format!("Offset: {}", offset));
        }
        if !order_by.is_empty() {
            let order_desc = order_by
                .iter()
                .map(|expr| {
                    format!(
                        "{} {}",
                        Self::format_column_ref(&expr.column),
                        if expr.ascending { "ASC" } else { "DESC" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            plan_steps.push(format!("Order by: {}", order_desc));
        }
        if let Some(limit) = limit {
            plan_steps.push(format!("Limit: {}", limit));
        }
        if let Some(offset) = offset {
            plan_steps.push(format!("Offset: {}", offset));
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

        let snapshot = self.current_snapshot();
        let current_txn_id = self.current_txn_id;
        let txn_states = self.txn_states.clone();

        let row_ids = match scan_plan {
            ScanPlan::IndexScan {
                index_columns,
                index_type,
                predicates,
            } => self.index_scan(&table_name, &index_columns, index_type, &predicates)?,
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
                let (meta, row) = table.get_with_metadata(row_id)?;
                if !Self::is_visible_for_snapshot(
                    &meta,
                    snapshot.as_ref(),
                    current_txn_id,
                    &txn_states,
                ) {
                    continue;
                }

                if let Some(ref where_expr) = where_clause
                    && !Self::evaluate_predicate_static(where_expr, &row, &columns_meta)?
                {
                    continue;
                }

                result_rows.push(row);
            }
        } else {
            // Table scan: scan all rows and filter
            let mut scan = TableScan::new(table);

            while let Some((_row_id, meta, row)) = scan.next_with_metadata()? {
                if !Self::is_visible_for_snapshot(
                    &meta,
                    snapshot.as_ref(),
                    current_txn_id,
                    &txn_states,
                ) {
                    continue;
                }
                // Apply WHERE clause filter if present
                if let Some(ref where_expr) = where_clause
                    && !Self::evaluate_predicate_static(where_expr, &row, &columns_meta)?
                {
                    continue;
                }

                result_rows.push(row);
            }
        }

        let (column_names, mut result_rows, output_meta) =
            Self::apply_select_items(result_rows, &columns_meta, &columns, group_by, false)?;
        if distinct {
            Self::apply_distinct(&mut result_rows);
        }

        let output_indices: Vec<usize> = (0..output_meta.len()).collect();
        Self::apply_order_limit(
            &mut result_rows,
            &output_meta,
            &output_indices,
            order_by,
            limit,
            offset,
        )?;

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
        group_by: &[ColumnRef],
        distinct: bool,
        order_by: &[OrderByExpr],
        limit: Option<usize>,
        offset: Option<usize>,
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

        match join_plan.strategy {
            JoinStrategy::NestedLoop { inner_has_index } => self.execute_nested_loop_join(
                join_plan,
                where_clause,
                left_join_idx,
                right_join_idx,
                &right_schema,
                &combined_meta,
                &columns,
                group_by,
                inner_has_index,
                distinct,
                order_by,
                limit,
                offset,
            ),
            JoinStrategy::MergeJoin => self.execute_merge_join(
                join_plan,
                where_clause,
                left_join_idx,
                right_join_idx,
                &combined_meta,
                &columns,
                group_by,
                distinct,
                order_by,
                limit,
                offset,
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
        columns: &SelectColumn,
        group_by: &[ColumnRef],
        inner_has_index: bool,
        distinct: bool,
        order_by: &[OrderByExpr],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> io::Result<ExecutionResult> {
        let snapshot = self.current_snapshot();
        let current_txn_id = self.current_txn_id;
        let txn_states = self.txn_states.clone();

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
            while let Some((_row_id, meta, row)) = scan.next_with_metadata()? {
                if !Self::is_visible_for_snapshot(
                    &meta,
                    snapshot.as_ref(),
                    current_txn_id,
                    &txn_states,
                ) {
                    continue;
                }
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
        if !order_by.is_empty() {
            let order_desc = order_by
                .iter()
                .map(|expr| {
                    format!(
                        "{} {}",
                        Self::format_column_ref(&expr.column),
                        if expr.ascending { "ASC" } else { "DESC" }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            plan_steps.push(format!("Order by: {}", order_desc));
        }
        if let Some(limit) = limit {
            plan_steps.push(format!("Limit: {}", limit));
        }
        if let Some(offset) = offset {
            plan_steps.push(format!("Offset: {}", offset));
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
            while let Some((_row_id, meta, row)) = scan.next_with_metadata()? {
                if !Self::is_visible_for_snapshot(
                    &meta,
                    snapshot.as_ref(),
                    current_txn_id,
                    &txn_states,
                ) {
                    continue;
                }
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
                    let key = CompositeKey::new(vec![index_value.clone()]);
                    matched_ids.extend(index.lookup_eq(&key));
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
                        let (meta, row) = right_table_ref.get_with_metadata(row_id)?;
                        if !Self::is_visible_for_snapshot(
                            &meta,
                            snapshot.as_ref(),
                            current_txn_id,
                            &txn_states,
                        ) {
                            continue;
                        }
                        row
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

                result_rows.push(combined_row);
            }
        }

        let (column_names, mut result_rows, output_meta) = Self::apply_select_items(
            result_rows,
            combined_meta,
            columns,
            group_by,
            true,
        )?;
        if distinct {
            Self::apply_distinct(&mut result_rows);
        }

        let output_indices: Vec<usize> = (0..output_meta.len()).collect();
        Self::apply_order_limit(
            &mut result_rows,
            &output_meta,
            &output_indices,
            order_by,
            limit,
            offset,
        )?;

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
        columns: &SelectColumn,
        group_by: &[ColumnRef],
        distinct: bool,
        order_by: &[OrderByExpr],
        limit: Option<usize>,
        offset: Option<usize>,
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

                            result_rows.push(combined);
                        }
                    }

                    i = i_end;
                    j = j_end;
                }
            }
        }

        let (column_names, mut result_rows, output_meta) = Self::apply_select_items(
            result_rows,
            combined_meta,
            columns,
            group_by,
            true,
        )?;
        if distinct {
            Self::apply_distinct(&mut result_rows);
        }

        let output_indices: Vec<usize> = (0..output_meta.len()).collect();
        Self::apply_order_limit(
            &mut result_rows,
            &output_meta,
            &output_indices,
            order_by,
            limit,
            offset,
        )?;

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
        let snapshot = self.current_snapshot();
        let current_txn_id = self.current_txn_id;
        let txn_states = self.txn_states.clone();
        let table_ref = self.tables.get_mut(table_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' does not exist", table_name),
            )
        })?;

        let mut scan = TableScan::new(table_ref);
        let mut rows = Vec::new();
        while let Some((_row_id, meta, row)) = scan.next_with_metadata()? {
            if !Self::is_visible_for_snapshot(
                &meta,
                snapshot.as_ref(),
                current_txn_id,
                &txn_states,
            ) {
                continue;
            }
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
        index_type: IndexType,
        predicates: &[(String, BinaryOp, Literal)],
    ) -> io::Result<Option<Vec<RowId>>> {
        let index = match self.find_index(table_name, index_columns, index_type) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        match index.index_type {
            IndexType::BTree => {
                let ranges = Self::build_ranges(index, predicates)?;
                if ranges.is_empty() {
                    return Ok(None);
                }
                Ok(Some(index.lookup_range(&ranges)))
            }
            IndexType::Hash => {
                let Some(key) = Self::build_hash_key(index, predicates)? else {
                    return Ok(None);
                };
                Ok(Some(index.lookup_eq(&key)))
            }
        }
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
            Literal::Date(s) => {
                let date = crate::types::Date::parse(s).map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidInput, format!("Invalid DATE: {}", e))
                })?;
                Ok(Value::Date(date))
            }
            Literal::Timestamp(s) => {
                let timestamp = crate::types::Timestamp::parse(s).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid TIMESTAMP: {}", e),
                    )
                })?;
                Ok(Value::Timestamp(timestamp))
            }
            Literal::Decimal(s) => {
                let decimal = crate::types::Decimal::parse(s).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Invalid DECIMAL: {}", e),
                    )
                })?;
                Ok(Value::Decimal(decimal))
            }
            Literal::Null => Ok(Value::Null),
        }
    }

    fn literal_to_typed_value(lit: &Literal, data_type: DbDataType) -> io::Result<Value> {
        let value = Self::literal_to_value(lit)?;
        Self::coerce_value_to_type(value, data_type)
    }

    fn coerce_value_to_type(value: Value, data_type: DbDataType) -> io::Result<Value> {
        match (data_type, value) {
            (_, Value::Null) => Ok(Value::Null),
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
            (DbDataType::Date, Value::Date(d)) => Ok(Value::Date(d)),
            (DbDataType::Date, Value::String(s)) => crate::types::Date::parse(&s)
                .map(Value::Date)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e)),
            (DbDataType::Timestamp, Value::Timestamp(t)) => Ok(Value::Timestamp(t)),
            (DbDataType::Timestamp, Value::String(s)) => crate::types::Timestamp::parse(&s)
                .map(Value::Timestamp)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e)),
            (DbDataType::Decimal, Value::Decimal(d)) => Ok(Value::Decimal(d)),
            (DbDataType::Decimal, Value::Integer(i)) => {
                Ok(Value::Decimal(crate::types::Decimal::from_i128(i as i128)))
            }
            (DbDataType::Decimal, Value::Unsigned(u)) => Ok(Value::Decimal(
                crate::types::Decimal::from_i128(u as i128),
            )),
            (DbDataType::Decimal, Value::Float(fv)) => crate::types::Decimal::from_f64(fv)
                .map(Value::Decimal)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "Invalid decimal literal")
                }),
            (DbDataType::Decimal, Value::String(s)) => crate::types::Decimal::parse(&s)
                .map(Value::Decimal)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e)),
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

                if left_val.is_null() || right_val.is_null() {
                    return Ok(false);
                }

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
        let snapshot = self.current_snapshot();
        let current_txn_id = self.current_txn_id;
        let txn_states = self.txn_states.clone();
        let rows = {
            let table = self.tables.get_mut(table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            let mut scan = TableScan::new(table);
            let mut rows = Vec::new();
            while let Some((row_id, meta, row)) = scan.next_with_metadata()? {
                if !Self::is_visible_for_snapshot(
                    &meta,
                    snapshot.as_ref(),
                    current_txn_id,
                    &txn_states,
                ) {
                    continue;
                }
                rows.push((row_id, row));
            }
            rows
        };

        for index in self
            .indexes
            .iter_mut()
            .filter(|idx| idx.key.table == table_name)
        {
            let mut data = match index.index_type {
                IndexType::BTree => IndexData::BTree(BPlusTree::new()),
                IndexType::Hash => IndexData::Hash(HashIndex::new()),
            };

            for (row_id, row) in &rows {
                let key =
                    Self::build_composite_key(row, &index.column_indices, &index.column_types)?;
                match &mut data {
                    IndexData::BTree(tree) => tree.insert(key, *row_id),
                    IndexData::Hash(hash) => hash.insert(key, *row_id),
                }
            }

            index.data = data;
        }

        Ok(())
    }

    fn index_metadata(&self) -> Vec<IndexMetadata> {
        self.indexes
            .iter()
            .map(|idx| IndexMetadata {
                table: idx.key.table.clone(),
                columns: idx.key.columns.clone(),
                index_type: idx.index_type,
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
            SelectColumn::Items(items) => {
                let mut indices = Vec::new();
                let mut names = Vec::new();
                for item in items {
                    match item {
                        SelectItem::Column(col_ref) => {
                            let idx = Self::resolve_column_index(columns_meta, col_ref)?;
                            indices.push(idx);
                            names.push(Self::format_column_name(&columns_meta[idx], use_qualified));
                        }
                        SelectItem::Aggregate(_) => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "Cannot project aggregate without GROUP BY",
                            ));
                        }
                        SelectItem::All => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "Invalid '*' in select list",
                            ));
                        }
                    }
                }
                Ok((indices, names))
            }
        }
    }

    fn apply_order_limit(
        rows: &mut Vec<Vec<Value>>,
        columns_meta: &[(Option<String>, String)],
        column_indices: &[usize],
        order_by: &[OrderByExpr],
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> io::Result<()> {
        if !order_by.is_empty() {
            let mut order_indices = Vec::with_capacity(order_by.len());
            for expr in order_by {
                let full_idx = Self::resolve_column_index(columns_meta, &expr.column)?;
                let projected_idx = column_indices
                    .iter()
                    .position(|&idx| idx == full_idx)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "ORDER BY column '{}' must appear in select list",
                                Self::format_column_ref(&expr.column)
                            ),
                        )
                    })?;
                order_indices.push((projected_idx, expr.ascending));
            }

            rows.sort_by(|a, b| {
                for (idx, asc) in &order_indices {
                    let ord = a[*idx].cmp(&b[*idx]);
                    if ord != std::cmp::Ordering::Equal {
                        return if *asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        if limit.is_some() || offset.is_some() {
            let start = offset.unwrap_or(0);
            if start >= rows.len() {
                rows.clear();
                return Ok(());
            }
            let end = match limit {
                Some(limit) => start.saturating_add(limit).min(rows.len()),
                None => rows.len(),
            };
            let sliced = rows[start..end].to_vec();
            *rows = sliced;
        }

        Ok(())
    }

    fn apply_distinct(rows: &mut Vec<Vec<Value>>) {
        let mut seen = std::collections::BTreeSet::new();
        rows.retain(|row| seen.insert(row.clone()));
    }

    fn apply_select_items(
        rows: Vec<Vec<Value>>,
        columns_meta: &[(Option<String>, String)],
        selection: &SelectColumn,
        group_by: &[ColumnRef],
        use_qualified: bool,
    ) -> io::Result<(Vec<String>, Vec<Vec<Value>>, Vec<(Option<String>, String)>)> {
        let has_aggregate = matches!(selection, SelectColumn::Items(items)
            if items.iter().any(|item| matches!(item, SelectItem::Aggregate(_)))
        );

        if matches!(selection, SelectColumn::All) && (!group_by.is_empty() || has_aggregate) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SELECT * cannot be used with GROUP BY or aggregates",
            ));
        }

        if !has_aggregate && group_by.is_empty() {
            let (indices, column_names) = Self::build_projection(columns_meta, selection, use_qualified)?;
            let output_rows = rows
                .into_iter()
                .map(|row| indices.iter().map(|&idx| row[idx].clone()).collect())
                .collect();
            let output_meta = indices
                .iter()
                .map(|&idx| columns_meta[idx].clone())
                .collect();
            return Ok((column_names, output_rows, output_meta));
        }

        let items = match selection {
            SelectColumn::Items(items) => items,
            SelectColumn::All => unreachable!("handled above"),
        };

        let mut group_by_indices = Vec::with_capacity(group_by.len());
        for col in group_by {
            let idx = Self::resolve_column_index(columns_meta, col)?;
            group_by_indices.push(idx);
        }

        #[derive(Clone)]
        struct AggSpec {
            func: AggregateFunc,
            target_index: Option<usize>,
            count_all: bool,
        }

        #[derive(Clone)]
        enum AggState {
            Count(i64),
            Sum { sum: f64, count: u64 },
            Avg { sum: f64, count: u64 },
            Min(Option<Value>),
            Max(Option<Value>),
        }

        impl AggState {
            fn new(spec: &AggSpec) -> Self {
                match spec.func {
                    AggregateFunc::Count => AggState::Count(0),
                    AggregateFunc::Sum => AggState::Sum { sum: 0.0, count: 0 },
                    AggregateFunc::Avg => AggState::Avg { sum: 0.0, count: 0 },
                    AggregateFunc::Min => AggState::Min(None),
                    AggregateFunc::Max => AggState::Max(None),
                }
            }

            fn finish(self) -> Value {
                match self {
                    AggState::Count(count) => Value::Integer(count),
                    AggState::Sum { sum, count } => {
                        if count == 0 {
                            Value::Null
                        } else {
                            Value::Float(sum)
                        }
                    }
                    AggState::Avg { sum, count } => {
                        if count == 0 {
                            Value::Null
                        } else {
                            Value::Float(sum / count as f64)
                        }
                    }
                    AggState::Min(value) => value.unwrap_or(Value::Null),
                    AggState::Max(value) => value.unwrap_or(Value::Null),
                }
            }
        }

        let mut agg_specs = Vec::new();
        let mut item_to_agg_index = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Aggregate(agg) => {
                    let (count_all, target_index) = match &agg.target {
                        AggregateTarget::All => (true, None),
                        AggregateTarget::Column(col) => {
                            let idx = Self::resolve_column_index(columns_meta, col)?;
                            (false, Some(idx))
                        }
                    };
                    agg_specs.push(AggSpec {
                        func: agg.func,
                        target_index,
                        count_all,
                    });
                    item_to_agg_index.push(Some(agg_specs.len() - 1));
                }
                _ => item_to_agg_index.push(None),
            }
        }

        for (item, agg_index) in items.iter().zip(item_to_agg_index.iter()) {
            if let SelectItem::Column(col) = item {
                if group_by_indices.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Non-aggregate columns require GROUP BY",
                    ));
                }
                let idx = Self::resolve_column_index(columns_meta, col)?;
                if !group_by_indices.contains(&idx) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Column '{}' must appear in GROUP BY", col.column),
                    ));
                }
            }
            if let (SelectItem::Aggregate(_), None) = (item, agg_index) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid aggregate expression",
                ));
            }
        }

        let mut output_meta = Vec::with_capacity(items.len());
        let mut column_names = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Column(col) => {
                    let idx = Self::resolve_column_index(columns_meta, col)?;
                    output_meta.push(columns_meta[idx].clone());
                    column_names.push(Self::format_column_name(&columns_meta[idx], use_qualified));
                }
                SelectItem::Aggregate(agg) => {
                    let name = Self::format_aggregate_name(agg);
                    output_meta.push((None, name.clone()));
                    column_names.push(name);
                }
                SelectItem::All => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Invalid '*' in select list",
                    ));
                }
            }
        }

        let mut groups: std::collections::BTreeMap<Vec<Value>, Vec<AggState>> =
            std::collections::BTreeMap::new();
        if group_by_indices.is_empty() && has_aggregate {
            groups.entry(Vec::new()).or_insert_with(|| {
                agg_specs.iter().map(AggState::new).collect::<Vec<_>>()
            });
        }

        for row in rows {
            let key: Vec<Value> = group_by_indices
                .iter()
                .map(|&idx| row[idx].clone())
                .collect();

            let states = groups.entry(key).or_insert_with(|| {
                agg_specs.iter().map(AggState::new).collect::<Vec<_>>()
            });

            for (idx, spec) in agg_specs.iter().enumerate() {
                let value_opt = if spec.count_all {
                    None
                } else {
                    spec.target_index.map(|col_idx| row[col_idx].clone())
                };

                match (&mut states[idx], spec.func) {
                    (AggState::Count(count), AggregateFunc::Count) => {
                        if spec.count_all {
                            *count += 1;
                        } else if let Some(value) = value_opt {
                            if !value.is_null() {
                                *count += 1;
                            }
                        }
                    }
                    (AggState::Sum { sum, count }, AggregateFunc::Sum) => {
                        if let Some(value) = value_opt {
                            if let Some(num) = Self::numeric_to_f64(&value)? {
                                *sum += num;
                                *count += 1;
                            }
                        }
                    }
                    (AggState::Avg { sum, count }, AggregateFunc::Avg) => {
                        if let Some(value) = value_opt {
                            if let Some(num) = Self::numeric_to_f64(&value)? {
                                *sum += num;
                                *count += 1;
                            }
                        }
                    }
                    (AggState::Min(current), AggregateFunc::Min) => {
                        if let Some(value) = value_opt {
                            if value.is_null() {
                                continue;
                            }
                            let replace = match current {
                                None => true,
                                Some(existing) => value < *existing,
                            };
                            if replace {
                                *current = Some(value);
                            }
                        }
                    }
                    (AggState::Max(current), AggregateFunc::Max) => {
                        if let Some(value) = value_opt {
                            if value.is_null() {
                                continue;
                            }
                            let replace = match current {
                                None => true,
                                Some(existing) => value > *existing,
                            };
                            if replace {
                                *current = Some(value);
                            }
                        }
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Invalid aggregate state",
                        ));
                    }
                }
            }
        }

        let mut output_rows = Vec::new();
        for (group_key, agg_states) in groups {
            let mut row = Vec::with_capacity(items.len());
            for (item, agg_index) in items.iter().zip(item_to_agg_index.iter()) {
                match (item, agg_index) {
                    (SelectItem::Column(col), _) => {
                        let idx = Self::resolve_column_index(columns_meta, col)?;
                        let group_pos = group_by_indices
                            .iter()
                            .position(|&gidx| gidx == idx)
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidInput,
                                    "GROUP BY column missing",
                                )
                            })?;
                        row.push(group_key[group_pos].clone());
                    }
                    (SelectItem::Aggregate(_), Some(agg_idx)) => {
                        row.push(agg_states[*agg_idx].clone().finish());
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "Invalid select item",
                        ));
                    }
                }
            }
            output_rows.push(row);
        }

        Ok((column_names, output_rows, output_meta))
    }

    fn format_aggregate_name(agg: &AggregateExpr) -> String {
        let func = match agg.func {
            AggregateFunc::Count => "COUNT",
            AggregateFunc::Sum => "SUM",
            AggregateFunc::Avg => "AVG",
            AggregateFunc::Min => "MIN",
            AggregateFunc::Max => "MAX",
        };
        let target = match &agg.target {
            AggregateTarget::All => "*".to_string(),
            AggregateTarget::Column(col) => Self::format_column_ref(col),
        };
        format!("{}({})", func, target)
    }

    fn numeric_to_f64(value: &Value) -> io::Result<Option<f64>> {
        match value {
            Value::Null => Ok(None),
            Value::Integer(i) => Ok(Some(*i as f64)),
            Value::Unsigned(u) => Ok(Some(*u as f64)),
            Value::Float(fv) => Ok(Some(*fv)),
            Value::Decimal(d) => d.to_f64().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "Invalid decimal value")
            }).map(Some),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Aggregate expects numeric values",
            )),
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

    fn find_index(
        &self,
        table: &str,
        columns: &[String],
        index_type: IndexType,
    ) -> Option<&IndexEntry> {
        self.indexes.iter().find(|idx| {
            idx.key.table == table && idx.key.columns == columns && idx.index_type == index_type
        })
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

    fn build_hash_key(
        index: &IndexEntry,
        predicates: &[(String, BinaryOp, Literal)],
    ) -> io::Result<Option<CompositeKey>> {
        let mut values = Vec::with_capacity(index.key.columns.len());

        for (i, col_name) in index.key.columns.iter().enumerate() {
            let pred = predicates.iter().find(|(c, _, _)| c == col_name);
            let Some((_, op, lit)) = pred else {
                return Ok(None);
            };

            if *op != BinaryOp::Eq {
                return Ok(None);
            }

            let Some(value) = IndexValue::from_literal(lit, &index.column_types[i]) else {
                return Ok(None);
            };
            values.push(value);
        }

        Ok(Some(CompositeKey::new(values)))
    }

    fn persist_index_metadata(&self) -> io::Result<()> {
        let path = self.db_path.join("indexes.meta");
        let mut buf = String::new();
        for idx in &self.indexes {
            buf.push_str(&format!(
                "{}|{}|{}|{}\n",
                idx.name,
                idx.key.table,
                idx.index_type,
                idx.key.columns.join(",")
            ));
        }
        fs::write(path, buf)
    }

    fn persist_constraints_metadata(&self) -> io::Result<()> {
        let path = self.db_path.join("constraints.meta");
        let mut buf = String::new();
        for (table, constraints) in &self.constraints {
            let primary = constraints.primary_key.clone().unwrap_or_default();
            let mut unique: Vec<String> = constraints.unique.iter().cloned().collect();
            unique.sort();
            let unique_str = unique.join(",");
            let mut not_null: Vec<String> = constraints.not_null.iter().cloned().collect();
            not_null.sort();
            let not_null_str = not_null.join(",");
            let fk_str = constraints
                .foreign_keys
                .iter()
                .map(|fk| format!("{}->{}.{}", fk.column, fk.ref_table, fk.ref_column))
                .collect::<Vec<_>>()
                .join(";");
            let check_str = constraints
                .checks
                .iter()
                .map(Self::describe_expr)
                .collect::<Vec<_>>()
                .join(";");
            buf.push_str(&format!(
                "{}|{}|{}|{}|{}|{}\n",
                table, primary, unique_str, fk_str, not_null_str, check_str
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
            let parts: Vec<&str> = line.split('|').collect();
            if parts.len() < 3 {
                continue;
            }

            let (name, table, index_type, cols_str) = if parts.len() >= 4 {
                let parsed_type = IndexType::from_str(parts[2]).unwrap_or(IndexType::BTree);
                (parts[0], parts[1], parsed_type, parts[3])
            } else {
                (parts[0], parts[1], IndexType::BTree, parts[2])
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

            let mut data = match index_type {
                IndexType::BTree => IndexData::BTree(BPlusTree::new()),
                IndexType::Hash => IndexData::Hash(HashIndex::new()),
            };
            let mut scan = TableScan::new(table_ref);
            while let Some((row_id, row)) = scan.next()? {
                let key = Self::build_composite_key(&row, &column_indices, &column_types)?;
                match &mut data {
                    IndexData::BTree(tree) => tree.insert(key, row_id),
                    IndexData::Hash(index) => index.insert(key, row_id),
                }
            }

            self.indexes.push(IndexEntry {
                name: name.to_string(),
                key: IndexKey {
                    table: table.to_string(),
                    columns: columns.clone(),
                },
                column_indices,
                column_types,
                index_type,
                data,
            });
        }

        Ok(())
    }

    fn load_constraints_metadata(&mut self) -> io::Result<()> {
        let path = self.db_path.join("constraints.meta");
        if !path.exists() {
            return Ok(());
        }

        let data = fs::read_to_string(&path)?;
        for line in data.lines() {
            let parts: Vec<&str> = line.split('|').collect();
            if parts.is_empty() {
                continue;
            }
            let table = parts[0].to_string();
            if !self.tables.contains_key(&table) {
                continue;
            }

            let primary = parts.get(1).and_then(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some((*s).to_string())
                }
            });
            let mut unique = HashSet::new();
            if let Some(unique_str) = parts.get(2) {
                if !unique_str.is_empty() {
                    for col in unique_str.split(',') {
                        if !col.is_empty() {
                            unique.insert(col.to_string());
                        }
                    }
                }
            }
            if let Some(ref pk) = primary {
                unique.insert(pk.clone());
            }

            let mut foreign_keys = Vec::new();
            if let Some(fk_str) = parts.get(3) {
                if !fk_str.is_empty() {
                    for entry in fk_str.split(';') {
                        let mut split = entry.split("->");
                        let Some(column) = split.next() else { continue };
                        let Some(target) = split.next() else { continue };
                        let mut target_split = target.split('.');
                        let Some(ref_table) = target_split.next() else { continue };
                        let Some(ref_column) = target_split.next() else { continue };
                        if column.is_empty() || ref_table.is_empty() || ref_column.is_empty() {
                            continue;
                        }
                        foreign_keys.push(ForeignKey {
                            column: column.to_string(),
                            ref_table: ref_table.to_string(),
                            ref_column: ref_column.to_string(),
                        });
                    }
                }
            }

            let mut not_null = HashSet::new();
            if let Some(not_null_str) = parts.get(4) {
                if !not_null_str.is_empty() {
                    for col in not_null_str.split(',') {
                        if !col.is_empty() {
                            not_null.insert(col.to_string());
                        }
                    }
                }
            }
            if let Some(ref pk) = primary {
                not_null.insert(pk.clone());
            }

            let mut checks = Vec::new();
            if let Some(check_str) = parts.get(5) {
                if !check_str.is_empty() {
                    for expr_str in check_str.split(';') {
                        if expr_str.trim().is_empty() {
                            continue;
                        }
                        let expr = self.parse_check_expr(expr_str)?;
                        checks.push(expr);
                    }
                }
            }

            self.constraints.insert(
                table,
                TableConstraints {
                    primary_key: primary,
                    unique,
                    not_null,
                    foreign_keys,
                    checks,
                },
            );
        }

        Ok(())
    }

    fn parse_check_expr(&self, expr_str: &str) -> io::Result<Expr> {
        let stmt = parse_sql(&format!("SELECT * FROM t WHERE {}", expr_str)).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to parse CHECK expression '{}': {}", expr_str, e),
            )
        })?;
        match stmt {
            Statement::Select(select) => select.where_clause.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Missing CHECK expression '{}'", expr_str),
                )
            }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid CHECK expression '{}'", expr_str),
            )),
        }
    }

    fn describe_scan(table: &str, scan_plan: &ScanPlan) -> String {
        match scan_plan {
            ScanPlan::SeqScan => format!("Seq scan on {}", table),
            ScanPlan::IndexScan {
                index_columns,
                index_type,
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
                    "Index scan on {} using {} ({}) with {}",
                    table,
                    index_type,
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

    fn is_visible_for_snapshot(
        meta: &RowMetadata,
        snapshot: Option<&Snapshot>,
        current_txn_id: Option<TxnId>,
        txn_states: &HashMap<TxnId, TxnState>,
    ) -> bool {
        let creator = meta.xmin;
        if creator != 0 {
            if Some(creator) != current_txn_id {
                let creator_state = txn_states
                    .get(&creator)
                    .copied()
                    .unwrap_or(TxnState::Committed);
                if creator_state != TxnState::Committed {
                    return false;
                }
                if let Some(snapshot) = snapshot {
                    if creator >= snapshot.xmax || snapshot.active.contains(&creator) {
                        return false;
                    }
                }
            }
        }

        let deleter = meta.xmax;
        if deleter == 0 {
            return true;
        }

        if Some(deleter) == current_txn_id {
            return false;
        }

        let deleter_state = txn_states
            .get(&deleter)
            .copied()
            .unwrap_or(TxnState::Committed);

        match snapshot {
            None => deleter_state != TxnState::Committed,
            Some(snapshot) => match deleter_state {
                TxnState::Committed => {
                    if deleter < snapshot.xmax && !snapshot.active.contains(&deleter) {
                        return false;
                    }
                    true
                }
                TxnState::Active | TxnState::Aborted => true,
            },
        }
    }

    fn validate_batch_uniques(
        &self,
        table_name: &str,
        rows: &[Vec<Value>],
    ) -> io::Result<()> {
        if rows.len() < 2 {
            return Ok(());
        }
        let Some(constraints) = self.constraints.get(table_name) else {
            return Ok(());
        };
        if constraints.unique.is_empty() {
            return Ok(());
        }
        let schema = self
            .tables
            .get(table_name)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?
            .schema()
            .clone();

        for col in &constraints.unique {
            let (idx, _) = schema.find_column(col).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Column '{}' not found in table '{}'", col, table_name),
                )
            })?;
            for i in 0..rows.len() {
                for j in (i + 1)..rows.len() {
                    if rows[i][idx].is_null() || rows[j][idx].is_null() {
                        continue;
                    }
                    if rows[i][idx] == rows[j][idx] {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("Unique constraint violated on {}", col),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    fn enforce_constraints_for_row(
        &mut self,
        table_name: &str,
        row: &[Value],
        exclude_row: Option<RowId>,
    ) -> io::Result<()> {
        let Some(constraints) = self.constraints.get(table_name).cloned() else {
            return Ok(());
        };
        if constraints.unique.is_empty()
            && constraints.foreign_keys.is_empty()
            && constraints.not_null.is_empty()
            && constraints.checks.is_empty()
        {
            return Ok(());
        }

        let schema = {
            let table = self.tables.get(table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;
            table.schema().clone()
        };

        for col in &constraints.not_null {
            let (idx, _) = schema.find_column(col).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Column '{}' not found in table '{}'", col, table_name),
                )
            })?;
            if row[idx].is_null() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("NOT NULL constraint violated on {}", col),
                ));
            }
        }

        let snapshot = self.current_snapshot();
        let current_txn_id = self.current_txn_id;
        let txn_states = self.txn_states.clone();

        for col in constraints.unique {
            let (idx, _) = schema.find_column(&col).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Column '{}' not found in table '{}'", col, table_name),
                )
            })?;
            let value = &row[idx];
            if value.is_null() {
                continue;
            }
            let table = self.tables.get_mut(table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;
            let mut scan = TableScan::new(table);
            while let Some((row_id, meta, existing)) = scan.next_with_metadata()? {
                if Some(row_id) == exclude_row {
                    continue;
                }
                if !Self::is_visible_for_snapshot(
                    &meta,
                    snapshot.as_ref(),
                    current_txn_id,
                    &txn_states,
                ) {
                    continue;
                }
                if existing[idx] == *value {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Unique constraint violated on {}", col),
                    ));
                }
            }
        }

        for fk in constraints.foreign_keys {
            let (idx, _) = schema.find_column(&fk.column).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Column '{}' not found in table '{}'", fk.column, table_name),
                )
            })?;
            let value = &row[idx];
            if value.is_null() {
                continue;
            }
            let ref_schema = {
                let table = self.tables.get(&fk.ref_table).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Referenced table '{}' does not exist", fk.ref_table),
                    )
                })?;
                table.schema().clone()
            };
            let (ref_idx, _) = ref_schema.find_column(&fk.ref_column).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Referenced column '{}.{}' does not exist",
                        fk.ref_table, fk.ref_column
                    ),
                )
            })?;
            let table = self.tables.get_mut(&fk.ref_table).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Referenced table '{}' does not exist", fk.ref_table),
                )
            })?;
            let mut scan = TableScan::new(table);
            let mut found = false;
            while let Some((_row_id, meta, existing)) = scan.next_with_metadata()? {
                if !Self::is_visible_for_snapshot(
                    &meta,
                    snapshot.as_ref(),
                    current_txn_id,
                    &txn_states,
                ) {
                    continue;
                }
                if existing[ref_idx] == *value {
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Foreign key violation on {}.{} -> {}.{}",
                        table_name, fk.column, fk.ref_table, fk.ref_column
                    ),
                ));
            }
        }

        if !constraints.checks.is_empty() {
            let columns_meta = Self::build_column_metadata_for_table(table_name, &schema);
            for expr in constraints.checks {
                let ok = Self::evaluate_predicate_static(&expr, row, &columns_meta)?;
                if !ok {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "CHECK constraint violated",
                    ));
                }
            }
        }

        Ok(())
    }

    fn has_referencing_rows(
        &mut self,
        ref_table: &str,
        ref_column: &str,
        value: &Value,
    ) -> io::Result<bool> {
        let constraints = self.constraints.clone();
        if constraints.is_empty() {
            return Ok(false);
        }

        let snapshot = self.current_snapshot();
        let current_txn_id = self.current_txn_id;
        let txn_states = self.txn_states.clone();

        for (child_table, table_constraints) in constraints {
            for fk in table_constraints.foreign_keys {
                if fk.ref_table != ref_table || fk.ref_column != ref_column {
                    continue;
                }
                let child_schema = {
                    let table = self.tables.get(&child_table).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("Table '{}' does not exist", child_table),
                        )
                    })?;
                    table.schema().clone()
                };
                let (child_idx, _) = child_schema.find_column(&fk.column).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Foreign key column '{}' not found in table '{}'",
                            fk.column, child_table
                        ),
                    )
                })?;
                let table = self.tables.get_mut(&child_table).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Table '{}' does not exist", child_table),
                    )
                })?;
                let mut scan = TableScan::new(table);
                while let Some((_row_id, meta, row)) = scan.next_with_metadata()? {
                    if !Self::is_visible_for_snapshot(
                        &meta,
                        snapshot.as_ref(),
                        current_txn_id,
                        &txn_states,
                    ) {
                        continue;
                    }
                    if row[child_idx] == *value {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    fn enforce_no_fk_references(&mut self, table_name: &str, row: &[Value]) -> io::Result<()> {
        let schema = {
            let table = self.tables.get(table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;
            table.schema().clone()
        };

        let constraints = self.constraints.clone();
        for (_child_table, table_constraints) in &constraints {
            for fk in &table_constraints.foreign_keys {
                if fk.ref_table != table_name {
                    continue;
                }
                let (ref_idx, _) = schema.find_column(&fk.ref_column).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Referenced column '{}.{}' does not exist",
                            fk.ref_table, fk.ref_column
                        ),
                    )
                })?;
                let value = &row[ref_idx];
                if self.has_referencing_rows(table_name, &fk.ref_column, value)? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Foreign key restrict violation on {}.{}",
                            fk.ref_table, fk.ref_column
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    fn enforce_no_fk_references_on_update(
        &mut self,
        table_name: &str,
        before: &[Value],
        after: &[Value],
    ) -> io::Result<()> {
        let schema = {
            let table = self.tables.get(table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;
            table.schema().clone()
        };

        let constraints = self.constraints.clone();
        for (_child_table, table_constraints) in &constraints {
            for fk in &table_constraints.foreign_keys {
                if fk.ref_table != table_name {
                    continue;
                }
                let (ref_idx, _) = schema.find_column(&fk.ref_column).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Referenced column '{}.{}' does not exist",
                            fk.ref_table, fk.ref_column
                        ),
                    )
                })?;
                if before[ref_idx] != after[ref_idx]
                    && self.has_referencing_rows(table_name, &fk.ref_column, &before[ref_idx])?
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "Foreign key restrict violation on {}.{}",
                            fk.ref_table, fk.ref_column
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    fn has_write_conflict(
        meta: &RowMetadata,
        current_txn_id: Option<TxnId>,
        txn_states: &HashMap<TxnId, TxnState>,
    ) -> bool {
        let deleter = meta.xmax;
        if deleter == 0 || Some(deleter) == current_txn_id {
            return false;
        }
        let deleter_state = txn_states
            .get(&deleter)
            .copied()
            .unwrap_or(TxnState::Committed);
        deleter_state != TxnState::Aborted
    }

    fn abort_current_transaction(&mut self) -> io::Result<()> {
        if !self.in_transaction {
            return Ok(());
        }
        let txn_id = self
            .current_txn_id
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing txn id"))?;
        self.wal.append(&WalRecord::Rollback { txn_id })?;
        self.set_txn_state(txn_id, TxnState::Aborted);
        self.snapshots.remove(&txn_id);
        self.in_transaction = false;
        self.current_txn_id = None;
        self.txn_log.clear();
        Ok(())
    }

    /// Flush all tables
    pub fn flush_all(&mut self) -> io::Result<()> {
        for table in self.tables.values_mut() {
            table.flush()?;
        }
        self.checkpoint_wal()?;
        Ok(())
    }

    pub fn vacuum_table(&mut self, table_name: &str) -> io::Result<usize> {
        if !self.active_txns.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot vacuum with active transactions",
            ));
        }

        let txn_states = self.txn_states.clone();
        let row_ids = {
            let table = self.tables.get_mut(table_name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Table '{}' does not exist", table_name),
                )
            })?;

            let mut scan = TableScan::new(table);
            let mut dead_rows = Vec::new();
            while let Some((row_id, meta, _row)) = scan.next_with_metadata()? {
                if !Self::is_visible_for_snapshot(
                    &meta,
                    None,
                    None,
                    &txn_states,
                ) {
                    dead_rows.push(row_id);
                }
            }
            dead_rows
        };

        if row_ids.is_empty() {
            return Ok(0);
        }

        let table = self.tables.get_mut(table_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' does not exist", table_name),
            )
        })?;
        let mut removed = 0usize;
        for row_id in row_ids {
            match table.delete(row_id) {
                Ok(()) => removed += 1,
                Err(err)
                    if err.kind() == io::ErrorKind::NotFound
                        || err.kind() == io::ErrorKind::UnexpectedEof =>
                {}
                Err(err) => return Err(err),
            }
        }

        if removed > 0 {
            self.rebuild_indexes_for_table(table_name)?;
        }

        Ok(removed)
    }

    pub fn vacuum_all(&mut self) -> io::Result<usize> {
        let table_names: Vec<String> = self.tables.keys().cloned().collect();
        let mut removed = 0usize;
        for name in table_names {
            removed += self.vacuum_table(&name)?;
        }
        Ok(removed)
    }

    /// Return table names and schemas currently loaded.
    pub fn list_tables(&self) -> Vec<(String, Schema)> {
        self.tables
            .iter()
            .map(|(name, table)| (name.clone(), table.schema().clone()))
            .collect()
    }

    /// Report whether a transaction is active.
    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    pub fn current_txn_id(&self) -> Option<TxnId> {
        self.current_txn_id
    }

    pub fn transaction_state(&self, txn_id: TxnId) -> Option<TxnState> {
        self.txn_states.get(&txn_id).copied()
    }

    pub fn current_txn_state(&self) -> Option<TxnState> {
        self.current_txn_id
            .and_then(|txn_id| self.transaction_state(txn_id))
    }

    pub fn current_snapshot(&self) -> Option<Snapshot> {
        let txn_id = self.current_txn_id?;
        self.snapshots.get(&txn_id).cloned()
    }

    /// Return index metadata currently loaded.
    pub fn list_indexes(&self) -> Vec<(String, String, Vec<String>, IndexType)> {
        self.indexes
            .iter()
            .map(|idx| {
                (
                    idx.name.clone(),
                    idx.key.table.clone(),
                    idx.key.columns.clone(),
                    idx.index_type,
                )
            })
            .collect()
    }

    fn allocate_txn_id(&mut self) -> TxnId {
        let txn_id = self.next_txn_id;
        self.next_txn_id = self.next_txn_id.saturating_add(1);
        txn_id
    }

    fn set_txn_state(&mut self, txn_id: TxnId, state: TxnState) {
        match state {
            TxnState::Active => {
                self.active_txns.insert(txn_id);
            }
            TxnState::Committed | TxnState::Aborted => {
                self.active_txns.remove(&txn_id);
            }
        }
        self.txn_states.insert(txn_id, state);
    }

    fn wal_txn_for_mutation(&mut self) -> io::Result<(TxnId, bool)> {
        if self.in_transaction {
            let txn_id = self
                .current_txn_id
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Missing txn id"))?;
            Ok((txn_id, false))
        } else {
            let txn_id = self.allocate_txn_id();
            self.wal.append(&WalRecord::Begin { txn_id })?;
            self.set_txn_state(txn_id, TxnState::Active);
            Ok((txn_id, true))
        }
    }

    fn checkpoint_wal(&mut self) -> io::Result<()> {
        if self.in_transaction {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot checkpoint during an active transaction",
            ));
        }

        self.wal.truncate()
    }

    fn undo_transaction(&mut self) -> io::Result<()> {
        let mut row_map: HashMap<RowId, RowId> = HashMap::new();
        let mut affected_tables: HashSet<String> = HashSet::new();

        for record in self.txn_log.iter().rev() {
            match record {
                WalRecord::Insert {
                    table, row_id, ..
                } => {
                    let Some(table_ref) = self.tables.get_mut(table) else {
                        continue;
                    };
                    let resolved = row_map.get(row_id).copied().unwrap_or(*row_id);
                    match table_ref.delete(resolved) {
                        Ok(()) => {
                            affected_tables.insert(table.clone());
                            row_map.insert(*row_id, resolved);
                        }
                        Err(err)
                            if err.kind() == io::ErrorKind::NotFound
                                || err.kind() == io::ErrorKind::UnexpectedEof =>
                        {}
                        Err(err) => return Err(err),
                    }
                }
                WalRecord::Update {
                    table,
                    row_id,
                    before,
                    ..
                } => {
                    let Some(table_ref) = self.tables.get_mut(table) else {
                        continue;
                    };
                    let resolved = row_map.get(row_id).copied().unwrap_or(*row_id);
                    match table_ref.update(resolved, before) {
                        Ok(new_id) => {
                            affected_tables.insert(table.clone());
                            row_map.insert(*row_id, new_id);
                        }
                        Err(err)
                            if err.kind() == io::ErrorKind::NotFound
                                || err.kind() == io::ErrorKind::UnexpectedEof =>
                        {}
                        Err(err) => return Err(err),
                    }
                }
                WalRecord::Delete {
                    table, row_id, values, ..
                } => {
                    let Some(table_ref) = self.tables.get_mut(table) else {
                        continue;
                    };
                    let resolved = row_map.get(row_id).copied().unwrap_or(*row_id);
                    match table_ref.get(resolved) {
                        Ok(_) => {}
                        Err(err)
                            if err.kind() == io::ErrorKind::NotFound
                                || err.kind() == io::ErrorKind::UnexpectedEof =>
                        {
                            let new_id = table_ref.insert(values)?;
                            affected_tables.insert(table.clone());
                            row_map.insert(*row_id, new_id);
                        }
                        Err(err) => return Err(err),
                    }
                }
                _ => {}
            }
        }

        for table in affected_tables {
            self.rebuild_indexes_for_table(&table)?;
        }

        Ok(())
    }

    fn recover_from_wal(&mut self) -> io::Result<()> {
        let records = self.wal.read_all()?;
        if records.is_empty() {
            return Ok(());
        }

        let mut recovered_states: HashMap<TxnId, TxnState> = HashMap::new();
        let mut max_txn_id = 0;
        for record in &records {
            let txn_id = match record {
                WalRecord::Begin { txn_id }
                | WalRecord::Commit { txn_id }
                | WalRecord::Rollback { txn_id }
                | WalRecord::Insert { txn_id, .. }
                | WalRecord::Update { txn_id, .. }
                | WalRecord::Delete { txn_id, .. } => *txn_id,
            };
            max_txn_id = max_txn_id.max(txn_id);
            match record {
                WalRecord::Begin { txn_id } => {
                    recovered_states.insert(*txn_id, TxnState::Active);
                }
                WalRecord::Commit { txn_id } => {
                    recovered_states.insert(*txn_id, TxnState::Committed);
                }
                WalRecord::Rollback { txn_id } => {
                    recovered_states.insert(*txn_id, TxnState::Aborted);
                }
                _ => {}
            }
        }

        for state in recovered_states.values_mut() {
            if *state == TxnState::Active {
                *state = TxnState::Aborted;
            }
        }

        self.active_txns.clear();
        self.txn_states.clear();
        for (txn_id, state) in recovered_states {
            self.set_txn_state(txn_id, state);
        }
        if max_txn_id > 0 {
            self.next_txn_id = max_txn_id.saturating_add(1);
        }

        let committed: HashSet<TxnId> = self
            .txn_states
            .iter()
            .filter_map(|(txn_id, state)| {
                if *state == TxnState::Committed {
                    Some(*txn_id)
                } else {
                    None
                }
            })
            .collect();

        let mut mappings: HashMap<TxnId, HashMap<RowId, RowId>> = HashMap::new();

        for record in records {
            let (txn_id, record) = match record {
                WalRecord::Begin { txn_id }
                | WalRecord::Commit { txn_id }
                | WalRecord::Rollback { txn_id } => (txn_id, None),
                WalRecord::Insert {
                    txn_id,
                    table,
                    row_id,
                    values,
                } => (
                    txn_id,
                    Some(WalRecord::Insert {
                        txn_id,
                        table,
                        row_id,
                        values,
                    }),
                ),
                WalRecord::Update {
                    txn_id,
                    table,
                    row_id,
                    before,
                    after,
                } => (
                    txn_id,
                    Some(WalRecord::Update {
                        txn_id,
                        table,
                        row_id,
                        before,
                        after,
                    }),
                ),
                WalRecord::Delete {
                    txn_id,
                    table,
                    row_id,
                    values,
                } => (
                    txn_id,
                    Some(WalRecord::Delete {
                        txn_id,
                        table,
                        row_id,
                        values,
                    }),
                ),
            };

            if !committed.contains(&txn_id) {
                continue;
            }

            let Some(record) = record else {
                continue;
            };

            let entry = mappings.entry(txn_id).or_default();

            match record {
                WalRecord::Insert {
                    table,
                    row_id,
                    values,
                    ..
                } => {
                    let Some(table_ref) = self.tables.get_mut(&table) else {
                        continue;
                    };
                    let resolved = match table_ref.get(row_id) {
                        Ok(_) => row_id,
                        Err(err)
                            if err.kind() == io::ErrorKind::NotFound
                                || err.kind() == io::ErrorKind::UnexpectedEof =>
                        {
                            table_ref.insert(&values)?
                        }
                        Err(err) => return Err(err),
                    };
                    entry.insert(row_id, resolved);
                }
                WalRecord::Update {
                    table,
                    row_id,
                    after,
                    ..
                } => {
                    let Some(table_ref) = self.tables.get_mut(&table) else {
                        continue;
                    };
                    let resolved = entry.get(&row_id).copied().unwrap_or(row_id);
                    match table_ref.update(resolved, &after) {
                        Ok(new_id) => {
                            entry.insert(row_id, new_id);
                        }
                        Err(err)
                            if err.kind() == io::ErrorKind::NotFound
                                || err.kind() == io::ErrorKind::UnexpectedEof =>
                        {}
                        Err(err) => return Err(err),
                    }
                }
                WalRecord::Delete { table, row_id, .. } => {
                    let Some(table_ref) = self.tables.get_mut(&table) else {
                        continue;
                    };
                    let resolved = entry.get(&row_id).copied().unwrap_or(row_id);
                    match table_ref.delete(resolved) {
                        Ok(()) => {
                            entry.insert(row_id, resolved);
                        }
                        Err(err)
                            if err.kind() == io::ErrorKind::NotFound
                                || err.kind() == io::ErrorKind::UnexpectedEof =>
                        {}
                        Err(err) => return Err(err),
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }
}
