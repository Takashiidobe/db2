# Indexes and Planner

## Index Implementation

The database supports in-memory B+Tree indexes with composite (multi-column) keys over INTEGER columns.

### B+Tree Structure (`src/index/btree.rs`)
In-memory B+Tree with generic key and value types:
- **Order**: 4 (max 3 keys per node, max 4 children per internal node)
- **Node types**:
  - Internal nodes: keys + child pointers for routing
  - Leaf nodes: keys + values, linked for range scans
- **Properties**:
  - All values stored in leaves
  - Leaves linked for efficient sequential access
  - Self-balancing (all leaves at same depth)

Operations:
- `insert(key, value)` - Insert or update. Splits nodes when full, propagates splits up to root.
- `search(key)` - Exact key lookup
- `range_scan(start, end)` - Iterator over keys in range `[start, end]`

### Composite Keys
Multi-column indexes use `CompositeKey`:
```rust
struct CompositeKey {
    values: Vec<i64>
}
```

Ordering is lexicographic (tuple ordering):
- Compare first value, then second if equal, etc.
- Example: `(1, 5)` < `(1, 10)` < `(2, 3)`

This enables prefix matching:
- Index on `(a, b, c)` can satisfy queries on `a`, `(a, b)`, or `(a, b, c)`
- But not queries on `b` or `c` alone

### Index Metadata Persistence
Indexes themselves are in-memory only, but their definitions persist:
- **File**: `./data/indexes.meta`
- **Format**: One line per index: `name|table|col1,col2,...`
- **Example**:
  ```
  idx_user_id|users|id
  idx_order_keys|orders|user_id,order_id
  ```

On startup:
1. Read `indexes.meta`
2. For each index definition:
   - Validate table and columns exist
   - Create empty B+Tree
   - Scan table data with `TableScan`
   - Insert each row into the B+Tree: `tree.insert(composite_key, row_id)`

### Supported Predicates
Indexes can satisfy predicates with comparison operators on indexed columns:
- `=` (equality)
- `!=` (inequality, first column only - splits into two range scans)
- `<`, `<=`, `>`, `>=` (range comparisons)

For composite indexes:
- All columns must form a prefix of the index
- Earlier columns use equality, later columns can use ranges
- Example: Index on `(a, b, c)` can use:
  - `a = 1` (prefix match on first column)
  - `a = 1 AND b < 5` (prefix match + range)
  - `a = 1 AND b = 2 AND c >= 10` (full prefix + range)
- Cannot use:
  - `b = 2` (not a prefix)
  - `a > 1 AND b = 2` (range on non-last column breaks prefix)

Range computation:
- Extract predicates on indexed columns
- Compute composite key bounds `[start, end]`
- Use B+Tree `range_scan(start, end)` to get matching `RowId`s

## Planner Behavior

The planner (`src/optimizer/planner.rs`) makes physical execution decisions based on index availability.

### Single-Table Scans
For `SELECT ... FROM table WHERE predicates`:
1. Extract indexable predicates (column-literal comparisons combined with AND)
2. Find all indexes on the table
3. For each index, compute longest matching prefix of predicates
4. Choose index with most matched columns
5. Fallback to sequential scan if no suitable index

Example:
```sql
-- Table: users (id, age, name)
-- Index: idx_user_id on users(id)
-- Index: idx_user_age on users(age)

SELECT * FROM users WHERE id = 5 AND age > 18
```

Planner chooses `idx_user_id` if both indexes are single-column (arbitrary tie-breaking). With composite index `idx_user_id_age on users(id, age)`, it would use both predicates.

Plan output:
```
Plan:
  - Index scan on users using idx_user_id with id = 5
```

### Join Execution
For `SELECT ... FROM a JOIN b ON a.x = b.y`:
1. Check if either table has an index on the join column
2. If right table (`b`) has index on `b.y`, use `(a, b)` order
3. If left table (`a`) has index on `a.x`, swap to `(b, a)` order
4. If neither has index, use original order `(a, b)`

Join strategies:
- **Nested loop with index**: Outer table drives, inner table uses index for lookups
  - Example: `FOR row_a IN scan(a): row_b = index_b.get(row_a.x)`
- **Merge join**: When both tables have indexes on join columns (not yet implemented in executor)

Plan output examples:
```
Plan:
  - Nested loop join: outer=orders, inner=users (indexed on id)
```

```
Plan:
  - Merge join on orders.user_id = users.id
```

### Predicate Extraction (`src/optimizer/rules.rs`)
The planner extracts indexable predicates from WHERE clauses:
- Walks the expression tree
- Identifies `BinaryOp { Column, op, Literal }` nodes
- Normalizes operand order (swaps operators if literal is on left)
- Collects all predicates connected by AND

Handles operand swapping:
- `5 < age` becomes `age > 5`
- `'admin' != role` becomes `role != 'admin'`

## Execution Paths

### Sequential Scan
1. Create `TableScan` iterator
2. For each row, evaluate WHERE predicates
3. Apply projection (select columns)
4. Return matching rows

### Index Scan
1. Compute range bounds from predicates
2. Call `tree.range_scan(start, end)` to get `RowId` iterator
3. For each `RowId`, fetch row from table via `table.get(row_id)`
4. Apply any remaining predicates (not satisfied by index)
5. Apply projection
6. Return matching rows

### Nested Loop Join (with index)
1. Sequential scan of outer table
2. For each outer row:
   - Extract join key value
   - Look up in inner table's index
   - Fetch matching inner rows
   - Combine outer + inner rows
3. Apply WHERE predicates to joined rows
4. Apply projection

### Nested Loop Join (without index)
1. Sequential scan of outer table
2. For each outer row:
   - Full sequential scan of inner table
   - Match on join condition
   - Combine rows
3. Apply WHERE predicates
4. Apply projection

Note: Inner table scan results are cached to avoid repeated full scans (though this is not yet optimized in the current implementation).

## Design Choices

### Why In-Memory B+Trees?
- Simplifies implementation (no page-based B+Tree code)
- Fast for small to medium datasets
- Index metadata persists, so rebuilding is acceptable
- Educational focus on core concepts rather than page-based index management

Trade-offs:
- Slower startup for large tables (must rebuild indexes)
- Higher memory usage (all indexes in RAM)
- No index-only scans (must always fetch from heap)

### Why Composite Keys?
- Supports multi-column indexes (common in real databases)
- Enables prefix matching for flexible query patterns
- Demonstrates tuple ordering and range computation

### Why Only INTEGER Columns?
- Simplifies key comparison and serialization
- Avoids complexities of string collation and case-sensitivity
- Focus on core indexing concepts

### Why Prefix Matching?
- Standard B+Tree behavior (used in PostgreSQL, MySQL, etc.)
- Allows one composite index to serve multiple query patterns
- Example: Index on `(user_id, created_at)` serves:
  - `WHERE user_id = 123`
  - `WHERE user_id = 123 AND created_at > '2024-01-01'`

### Planner Heuristics
Simple cost-based decisions:
- Prefer index scans when applicable (assumes indexes are selective)
- Prefer indexed inner table for joins (reduces inner lookups from O(n) to O(log n))
- Longest prefix match (more predicates satisfied = fewer post-filter rows)

Future improvements:
- Statistics (row counts, cardinality estimates)
- True cost estimation (I/O costs, CPU costs)
- Query hints or forced index usage
