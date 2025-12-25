# REPL Usage

## Starting the REPL

Run `cargo run` from the repository root. The REPL:
1. Opens or creates `./data` directory for database files
2. Loads all existing `.db` table files
3. Reads index metadata from `./data/indexes.meta`
4. Rebuilds in-memory B+Tree indexes by scanning table data
5. Displays loaded tables with their schemas and indexes
6. Shows built-in help text with SQL syntax examples

Startup output includes:
- Table list with column names and types (e.g., `users: id INTEGER, name VARCHAR`)
- Index list with table and column references (e.g., `idx_user_id on users(id)`)

## Commands

### DDL Statements
- `CREATE TABLE <name> (<col> <TYPE>, ...)` - Create a new table
  - Types: `INTEGER`, `BOOLEAN`, `VARCHAR`
  - Example: `CREATE TABLE users (id INTEGER, active BOOLEAN, name VARCHAR)`
- `DROP TABLE <name>` - Drop an existing table
  - Removes the table file from disk
  - Automatically removes all indexes on the table
  - Example: `DROP TABLE users`
- `CREATE INDEX <idx_name> ON <table>(<col1>[, <col2> ...])` - Create an index
  - Only INTEGER columns supported
  - Supports composite (multi-column) indexes
  - Example: `CREATE INDEX idx_user_id ON users(id)`

### DML Statements
- `INSERT INTO <table> VALUES (<v1>, <v2>, ...)[, (...)]` - Insert rows
  - Supports multiple tuples per statement
  - Example: `INSERT INTO users VALUES (1, true, 'Alice'), (2, false, 'Bob')`
- `DELETE FROM <table>[ WHERE <pred>]` - Remove rows
  - Without WHERE, deletes all rows from the table
  - Uses indexes when predicates match indexed INTEGER columns
- `SELECT <cols|*> FROM <table>[ JOIN <table> ON <lcol> = <rcol>][ WHERE <pred>]` - Query data
  - Projection: `*` for all columns or comma-separated column list
  - Join: equi-join with `ON` clause
  - Where: column-literal comparisons with `=`, `!=`, `<`, `<=`, `>`, `>=`, and `AND`
  - Column references may be qualified (`table.col`) or unqualified

### REPL Commands
- `.exit` - Flush all dirty pages to disk and exit

## Predicates

WHERE clauses support:
- **Comparison operators**: `=`, `!=`, `<`, `<=`, `>`, `>=`
- **Logical operators**: `AND` (chains multiple predicates)
- **Literals**:
  - Integers with optional leading `-` (e.g., `-42`, `100`)
  - Booleans: `true`, `false`
  - Strings with single quotes and `''` escaping (e.g., `'It''s working'`)
- **Column references**: qualified (`table.col`) or unqualified when unambiguous

Examples:
- `WHERE id = 5`
- `WHERE age >= 18 AND active = true`
- `WHERE name != 'admin'`

## Plan Output

Every SELECT query prints an explain-style plan before results:

```
Plan:
  - Index scan on users using idx_user_id with id = 5
  - Nested loop join: outer=orders, inner=users (indexed on id)
```

Plan descriptions include:
- **Scan type**: Sequential scan or index scan with predicates
- **Join strategy**: Nested loop (with or without inner index) or merge join
- **Index usage**: Which index columns are used and which predicates they satisfy

## Persistence and Reload

### Table Storage
- Tables are stored as `./data/<table>.db` heap files
- Page 0 contains metadata: table name and schema
- Data pages (1+) contain rows in heap order
- All dirty pages flush to disk on `.exit`

### Index Storage
- Index definitions persist in `./data/indexes.meta` as `name|table|col1,col2...` per line
- B+Tree nodes are in-memory only
- On startup, indexes are rebuilt by scanning table data according to metadata

### Reusing a Database
To reuse an existing database:
1. Keep `./data` directory intact
2. Restart the REPL - it will auto-load all `.db` files and rebuild indexes

## Current Limitations

- No `SHOW TABLES` or `DESCRIBE` commands (restart REPL to see schema)
- No `UPDATE` statements (row-level modifications)
- No `DROP INDEX` statement (must drop and recreate table to remove indexes)
- No transaction boundaries (`.exit` is the only flush point)
- All columns must be specified in INSERT (no default values)
