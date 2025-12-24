# Educational SQL Database - Implementation Plan

## Overview
Build an educational SQL database in Rust with custom binary serialization, page-based storage, B+ tree indexes, and query optimization. 15 incremental steps, each < 1 day of work.

Note the edition should always be 2024

**Selected Options:**
- Custom binary format (educational - learn data layout)
- SQL: SELECT with WHERE, INSERT, CREATE TABLE, JOINs
- Index: B+ Tree
- Data types: Integer (i64), String (VARCHAR)

---

## Architecture

```
src/
├── main.rs                 # CLI and demo
├── lib.rs                  # Public API
├── types/
│   ├── value.rs           # Integer and String value types
│   └── schema.rs          # Column and table schemas
├── serialization/
│   ├── column.rs          # Column serialization
│   ├── row.rs             # Row serialization
│   └── codec.rs           # Binary format utilities
├── storage/
│   ├── page.rs            # Page structure (8KB pages)
│   ├── buffer_pool.rs     # Dirty/clean page tracking
│   └── file.rs            # Disk I/O
├── index/
│   ├── btree.rs           # B+ tree implementation
│   └── btree_page.rs      # B+ tree on disk
├── table/
│   ├── heap.rs            # Heap table
│   └── scan.rs            # Table scanning iterator
├── sql/
│   ├── parser.rs          # SQL parser
│   ├── ast.rs             # Abstract syntax tree
│   └── executor.rs        # Query execution
└── optimizer/
    ├── rules.rs           # Optimization rules
    └── planner.rs         # Query planning
```

---

## Implementation Steps

### Step 1: Fix Edition + Basic Type System ✅ COMPLETED
**Goal:** Foundation with proper Rust edition and core data types

**Tasks:**
- Change `edition = "2024"` → `"2021"` in Cargo.toml
- Create `src/lib.rs` as library root
- Create `Value` enum: `Integer(i64)` and `String(String)`
- Implement Display, Debug, PartialEq, PartialOrd

**Files:**
- `src/types/value.rs` - Value enum with operations
- `src/types/mod.rs` - Module exports

**Tests:** Value operations, comparisons

---

### Step 2: Binary Serialization for Single Column ✅ COMPLETED
**Goal:** Custom binary format for column of values

**Format:**
```
[4 bytes: value_count]
[1 byte: type_tag (0=Integer, 1=String)]
[values...]

Integer: [8 bytes: i64]
String:  [4 bytes: length][length bytes: UTF-8]
```

**Tasks:**
- Create `ColumnSerializer`
- `serialize(values: &[Value]) -> Vec<u8>`
- `deserialize(bytes: &[u8]) -> Result<Vec<Value>>`
- Error handling (type mismatches, truncation)

**Files:**
- `src/serialization/column.rs` - Column serialization
- `src/serialization/codec.rs` - Helpers for primitives

**Tests:** Various data sizes, round-trips, error cases

---

### Step 3: Schema Definition and Row Serialization ✅ COMPLETED
**Goal:** Define schemas and serialize rows (multiple columns)

**Row Format:**
```
[2 bytes: column_count]
[for each column: serialized Value]
```

**Tasks:**
- Create `Schema` struct (column names + types)
- Row validation against schema
- `RowSerializer` with serialize/deserialize
- Handle mixed types in a row

**Files:**
- `src/types/schema.rs` - Schema and Column definitions
- `src/serialization/row.rs` - Row serialization

**Tests:** Multi-column rows with mixed types

---

### Step 4: Page-Based Storage ✅ COMPLETED
**Goal:** Fixed-size pages (8KB) as fundamental storage unit

**Page Layout:**
```
[2 bytes: page_type (0=heap, 1=btree_internal, 2=btree_leaf)]
[4 bytes: page_id]
[2 bytes: num_rows]
[2 bytes: free_space_offset]
[row_directory: array of (offset, length)]
[...free space...]
[rows stored bottom-up]
```

**Tasks:**
- Create `Page` struct
- `add_row(row_data: &[u8]) -> Result<SlotId>`
- `get_row(slot_id: SlotId) -> Option<&[u8]>`
- Serialize/deserialize page to/from bytes
- Handle page overflow

**Files:**
- `src/storage/page.rs` - Page structure and operations

**Tests:** Page filling, overflow detection, round-trips

---

### Step 5: Dirty/Clean Page Management ✅ COMPLETED
**Goal:** Buffer pool with dirty tracking and disk I/O

**Tasks:**
- Create `BufferPool` with LRU cache
- Track dirty/clean state per page
- Eviction policy (write dirty pages before evicting)
- `DiskManager` for file I/O
- `flush_page(page_id)` and `flush_all()`

**Files:**
- `src/storage/buffer_pool.rs` - Buffer pool
- `src/storage/file.rs` - Disk I/O

**Tests:** Use tempfile, verify dirty pages written correctly

---

### Step 6: Heap Table Implementation ✅ COMPLETED
**Goal:** Complete table structure using pages

**Tasks:**
- Create `HeapTable` with schema + BufferPool
- Metadata page (page 0) for schema/table info
- `create(name, schema) -> HeapTable`
- `insert(row: &[Value]) -> Result<RowId>`
- `get(row_id: RowId) -> Result<Vec<Value>>`
- `TableScan` iterator for sequential scans

**Files:**
- `src/table/heap.rs` - Heap table
- `src/table/scan.rs` - Table scanning

**Tests:** Create tables, insert rows, read back, scanning

---

### Step 7: SQL Parser (CREATE TABLE and INSERT) ✅ COMPLETED
**Goal:** Parse basic SQL into AST

**AST Nodes:**
- `Statement` enum (CreateTable, Insert, Select)
- `CreateTableStmt` - table name, columns
- `InsertStmt` - table name, values
- `DataType` enum (Integer, Varchar)

**Tasks:**
- Create AST definitions
- Write recursive descent parser or use `nom`
- `parse_create_table(sql: &str) -> Result<CreateTableStmt>`
- `parse_insert(sql: &str) -> Result<InsertStmt>`

**Files:**
- `src/sql/ast.rs` - AST definitions
- `src/sql/parser.rs` - Parser

**Tests:** Parse various SQL statements

---

### Step 8: SQL Executor (CREATE TABLE and INSERT) ✅ COMPLETED
**Goal:** Execute parsed SQL statements

**Tasks:**
- Create `Executor` with database catalog
- `execute(stmt: Statement) -> Result<ExecutionResult>`
- Execute CREATE TABLE (create HeapTable)
- Execute INSERT (parse values, insert)
- Database catalog to track tables
- Simple CLI in main.rs

**Files:**
- `src/sql/executor.rs` - Statement execution
- Update `src/main.rs` - CLI

**Tests:** Integration tests with full SQL commands

---

### Step 9: B+ Tree Implementation (In-Memory) ✅ COMPLETED
**Goal:** B+ tree index structure (memory first)

**Tasks:**
- Create `BPlusTree<K, V>` generic structure
- Internal nodes: keys + child pointers
- Leaf nodes: keys + values + sibling pointer
- `insert(key, value)`
- `search(key) -> Option<V>`
- `range_scan(start, end) -> Iterator<(K, V)>`
- Node splitting and rebalancing

**Files:**
- `src/index/btree.rs` - B+ tree

**Tests:** Insert, search, range queries, tree balancing

---

### Step 10: B+ Tree on Disk (Page-Based) ✅ COMPLETED
**Goal:** Persist B+ tree using pages

**Solution:** Implemented fixed-size node serialization with Page API `update_row()` method for in-place updates.

**Tasks:**
- Create `BTreePage` types (internal/leaf)
- Use `PageId` instead of memory pointers
- Load pages from BufferPool on demand
- Mark pages dirty when modified
- Persist and reload B+ tree

**Files:**
- `src/index/btree_page.rs` - B+ tree page layouts
- Update `src/index/btree.rs` - Work with pages

**Tests:** Insert, close, reopen, search (persistence)

---

### Step 11: SQL SELECT with WHERE ✅ COMPLETED
**Goal:** Implement SELECT queries with filtering

**Tasks:**
- Extend AST: `SelectStmt`, `Expr` enum
- Parse `SELECT col1, col2 FROM table WHERE col1 = 5`
- Execute SELECT with table scan
- Predicate evaluation (filter rows)
- Return result rows

**Files:**
- Update `src/sql/ast.rs` - SELECT nodes
- Update `src/sql/parser.rs` - SELECT parsing
- Update `src/sql/executor.rs` - SELECT execution

**Tests:** Various WHERE conditions (=, !=, <, <=, >, >=)

---

### Step 12: CREATE INDEX and Index-Backed Queries ✅ COMPLETED
**Goal:** Create indexes and use them in queries

**Tasks:**
- Parse `CREATE INDEX idx_name ON table(column)`
- Execute CREATE INDEX (create B+ tree)
- Store index in catalog
- Modify SELECT executor:
  - Check if WHERE can use index
  - Use index scan vs table scan
- Simple index selection logic

**Implementation:**
- Only INTEGER column indexes supported
- Indexes automatically maintained on INSERT
- Index used for simple equality predicates (WHERE col = value)
- Falls back to table scan for non-equality or non-indexed queries

**Files:**
- Update parser/executor for CREATE INDEX
- Update SELECT executor for index usage

**Tests:** Index creation, index scan, index maintenance, error cases

---

### Step 13: JOIN Implementation
**Goal:** Basic nested loop join

**Tasks:**
- Extend AST for JOIN
- Parse `SELECT * FROM t1 JOIN t2 ON t1.id = t2.id`
- Implement nested loop join:
  - Iterate outer table
  - For each row, scan inner table
  - Return combined rows
- Use index on join column if available

**Files:**
- Update parser/executor for JOINs

**Tests:** Multi-table queries

---

### Step 14: Query Optimizer
**Goal:** Cost-based query optimization

**Tasks:**
- Create logical/physical plan representations
- Optimization rules:
  - Predicate pushdown (evaluate WHERE early)
  - Index selection (use index when available)
  - Join ordering (smaller table first)
- Simple cost model:
  - Estimate rows scanned
  - Estimate I/O operations
  - Choose lowest cost plan
- Integrate between parser and executor

**Files:**
- `src/optimizer/planner.rs` - Query planner
- `src/optimizer/rules.rs` - Optimization rules

**Tests:** Verify optimizer chooses better plans

---

### Step 15: Testing, Documentation, and Polish
**Goal:** Comprehensive testing and documentation

**Tasks:**
- Integration tests (complex queries, large datasets, edge cases)
- Add documentation:
  - README with architecture overview
  - Module-level docs
  - Example programs in `examples/`
- Interactive SQL shell
- Optional: Performance benchmarks

**Files:**
- `README.md` - Architecture and usage
- `examples/` - Demo programs

---

## Key Design Decisions

**Binary Format:**
- Fixed-width for primitives (i64 = 8 bytes)
- Length-prefixed for strings
- Type tags for identification
- Little-endian byte order

**Page Size:** 8KB (good balance for learning)

**Buffer Pool:**
- Fixed size (e.g., 100 pages)
- LRU eviction
- Write-back with flush on evict

**B+ Tree:**
- Order 4-8 for learning (small enough to visualize)
- Higher order (100+) for production

**Optimizer:**
- Rule-based optimization
- Simple statistics (row counts)
- Focus on index selection and join ordering

---

## Critical Files

These 5 files form the backbone:

1. **`src/types/value.rs`** - Core data type system
2. **`src/serialization/row.rs`** - Binary serialization (understand data layout)
3. **`src/storage/page.rs`** - Page structure (fundamental unit)
4. **`src/storage/buffer_pool.rs`** - Buffer pool with dirty tracking (caching + durability)
5. **`src/index/btree.rs`** - B+ tree (key index structure)

---

## Testing Strategy

- **Unit tests:** Per module (values, serialization, pages, btree, parser)
- **Integration tests:** Full SQL end-to-end, persistence tests
- **Property-based (optional):** Use `proptest` for invariants

---

## Future Extensions

After Step 15, consider:
- Transactions (ACID, WAL)
- Concurrency (MVCC or locking)
- More data types (Float, Date, Boolean, NULL)
- Hash indexes
- Better joins (hash join, sort-merge join)
- Aggregations (COUNT, SUM, AVG, GROUP BY)
- Constraints (PRIMARY KEY, FOREIGN KEY)

---

## Dependencies

Start minimal, add as needed:

```toml
[dependencies]
# Add in Step 7 if using parser combinators:
# nom = "7"

[dev-dependencies]
tempfile = "3"  # For testing (Step 5+)
```

---

## Summary

This plan takes you from zero to a working educational SQL database with:
- ✅ Custom binary serialization
- ✅ Page-based storage with buffer management
- ✅ B+ tree indexes on disk
- ✅ SQL parsing and execution (SELECT, INSERT, CREATE TABLE, JOIN)
- ✅ Query optimization

Each step builds naturally on the previous, following the progression: column → row → table → index → optimizer.
