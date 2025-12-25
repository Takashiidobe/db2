# Overview

This repository implements an educational SQL database in Rust. It features a REPL interface, SQL parser, query planner with index selection, executor, and persistent heap tables with composite-key B+Tree indexes.

## Layout

- `src/main.rs`: REPL entry point with startup messaging and interactive query execution.
- `src/sql`: SQL surface area (AST definitions, parser, executor).
- `src/optimizer`: Query planner with index selection and join reordering.
- `src/table`: Heap table storage with sequential scans.
- `src/index`: In-memory B+Tree implementation with composite key support.
- `src/serialization`: Binary codecs for rows, columns, and schemas.
- `src/storage`: Page-based storage with buffer pool, LRU eviction, and disk manager.
- `src/types`: Logical data types (INTEGER, BOOLEAN, VARCHAR), schemas, and values.

## Capabilities

- **Tables**: Heap-organized pages persisted to disk at `./data/<table>.db`.
- **Data types**: `INTEGER` (i64), `BOOLEAN`, `VARCHAR` (UTF-8 strings).
- **SQL statements**:
  - `CREATE TABLE` with column definitions
  - `INSERT INTO ... VALUES (...)` (supports multiple tuples per statement)
  - `SELECT` with projection (`*` or column list), optional `WHERE` clause, and `JOIN`
  - `CREATE INDEX` over one or more integer columns
- **Indexing**: In-memory B+Tree indexes with composite-key support. Indexes are persisted as metadata in `./data/indexes.meta` and rebuilt on startup by scanning table data.
- **Query planning**: Chooses between sequential scans and index scans based on predicate analysis. For joins, reorders to place indexed table on inner side when beneficial. Supports merge join strategy when both join columns are indexed.
- **Predicates**: Supports `=`, `!=`, `<`, `<=`, `>`, `>=` and `AND` for combining column-literal comparisons.
- **REPL**: Interactive execution with explain-style plan output for SELECT queries. Data persists under `./data` and is automatically loaded on startup.

## Design Choices

### Storage Architecture
- **Fixed-size pages (8 KiB)**: Standardizes I/O and simplifies buffer pool management.
- **Slotted page layout**: Header + slot directory + variable-length rows. Supports efficient space utilization and avoids fragmentation within pages.
- **Buffer pool with LRU eviction**: Caches frequently accessed pages in memory with pin counting to prevent eviction of in-use pages.
- **Heap tables**: Unordered row storage for append-heavy workloads. Page 0 stores metadata (table name and schema).

### Indexing
- **In-memory B+Tree only**: Indexes are rebuilt on startup from table data. This simplifies implementation while preserving index definitions across sessions.
- **Composite keys**: Multi-column indexes use tuple ordering for range scans and prefix matching.
- **Integer columns only**: Simplifies key comparison and serialization.

### Query Execution
- **Volcano-style iterator model**: Sequential scans and index scans produce rows on demand.
- **Nested-loop joins**: Outer table drives inner lookups. When inner table has an index on the join column, uses index lookups instead of full scans.
- **Merge joins**: Used when both tables have indexes on their join columns.
- **No updates or deletes**: Tables are append-only after inserts.

### Limitations
- No transaction support or concurrency control.
- No aggregates, ordering, grouping, or limits.
- Only integer columns can be indexed.
- Indexes are in-memory and rebuilt on each startup (not page-based persistent structures).
- No query optimization beyond index selection and join reordering.
