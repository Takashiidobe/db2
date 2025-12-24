# Possible Extensions and Polish

- Richer cost model: maintain per-table/cardinality stats, histograms on indexed columns, selectivity estimates for better scan/join/index choices.
- Multiple indexes per query: choose best index per predicate, support index AND/OR via bitmap-style intersection/union, and combine with residual filters.
- Additional join algorithms: hash join for equality predicates, sort-merge join for ordered inputs, and nested-loop with block batching.
- New data types: booleans, floats (f32/f64), smaller ints (i8/i16/i32), unsigned variants where sensible, and longer VARCHAR with length limits.
- NULL semantics: add nullable columns, three-valued logic in predicates, and NULL-aware comparisons/joins.
- Secondary index types: hash indexes for equality lookups, covering indexes (include) to satisfy queries without table access, and unique indexes with enforcement.
- Expression engine: computed expressions in SELECT/WHERE, simple arithmetic, and type coercions between numeric types.
- Aggregations and grouping: COUNT/SUM/AVG/MIN/MAX, GROUP BY, HAVING, and basic DISTINCT support.
- Concurrency and durability: WAL, transaction boundaries, locks or MVCC, and crash recovery tests.
- Partitioning and clustering: clustered indexes/primary keys, table partitioning by range/hash, and page-level fill factor controls.
