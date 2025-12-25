# SQL Surface

## Supported Statements

### DDL (Data Definition Language)
- `CREATE TABLE name (col TYPE [, ...])`
  - Defines table schema with column names and types
  - Table files are created at `./data/<name>.db`
- `CREATE INDEX idx_name ON table(col1[, col2 ...])`
  - Creates an in-memory B+Tree index
  - Supports composite (multi-column) keys
  - Only INTEGER columns can be indexed
  - Indexes are persisted as metadata and rebuilt on startup

### DML (Data Manipulation Language)
- `INSERT INTO name VALUES (v1, v2, ...)[, (...)]`
  - Inserts one or more rows into a table
  - Multiple tuples per statement supported
  - All columns must be provided (no partial inserts)
  - Values are validated against schema before insertion
- `SELECT <columns|*> FROM <table> [JOIN <table> ON <lcol> = <rcol>] [WHERE <pred>]`
  - Query data with optional filtering and joins
  - Prints explain-style plan before results

## Data Types

| SQL Type  | Storage    | Range/Notes                           |
|-----------|------------|---------------------------------------|
| `INTEGER` | i64        | -2^63 to 2^63-1                       |
| `BOOLEAN` | bool       | `true` or `false`                     |
| `VARCHAR` | String     | UTF-8 strings, variable-length        |

Type matching is strict: you cannot insert a string into an INTEGER column or vice versa.

## Expressions and Predicates

### Comparison Operators
- `=` (equality)
- `!=` (inequality)
- `<` (less than)
- `<=` (less than or equal)
- `>` (greater than)
- `>=` (greater than or equal)

### Logical Operators
- `AND` - Conjunctive combination of predicates

### Literals
- **Integers**: `42`, `-100`, `0`
- **Booleans**: `true`, `false` (case-insensitive)
- **Strings**: `'hello'`, `'It''s escaped'` (single quotes, `''` for literal quote)

### Column References
- Unqualified: `col_name` (must be unambiguous)
- Qualified: `table.col_name` (disambiguates in joins)

Predicates are limited to column-literal comparisons. Column-column comparisons (e.g., `a.x = b.y`) are only supported in JOIN ON clauses.

## SELECT Statement Details

### Projection
- `*` - All columns from all tables in FROM clause
- `col1, col2, ...` - Specific columns (qualified or unqualified)

### FROM Clause
- Single table: `FROM table_name`
- Equi-join: `FROM table1 JOIN table2 ON table1.col = table2.col`
  - Only equi-joins supported (equality condition)
  - Planner may reorder to place indexed table on inner side

### WHERE Clause
Optional filter with predicates:
- Column-literal comparisons: `WHERE id = 5`
- Combined with AND: `WHERE age >= 18 AND active = true`
- Applies after joins (post-filter on joined rows)

## Limitations

### Not Supported
- Subqueries or nested SELECT
- Aggregates (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
- DISTINCT
- UPDATE or DELETE statements
- Column-column comparisons in WHERE (only in JOIN ON)
- Non-equi joins (e.g., JOIN ON a.x < b.y)
- Outer joins (LEFT, RIGHT, FULL)
- Self-joins (table aliasing not implemented)
- NULL values or nullable columns
- DEFAULT values or auto-increment
- Constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK)

### Indexing Constraints
- Only INTEGER columns can be indexed
- Indexes are in-memory (rebuilt on startup)
- No partial or filtered indexes
- No index hints or forced index usage

### Query Execution
- Joins use nested loops or merge join (no hash join)
- No query result caching
- No parallel execution
- No query timeouts or resource limits
