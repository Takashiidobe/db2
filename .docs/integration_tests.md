# Integration Tests

## How to Write a New Test

Integration tests are located in `tests/` and organized by functionality. Each test uses a temporary database that's automatically cleaned up.

### Basic Test Structure

```rust
mod common;

use common::TestDb;
use db::sql::ExecutionResult;
use db::types::Value;

#[test]
fn test_my_feature() {
    // 1. Create a temporary test database
    let mut db = TestDb::new().unwrap();

    // 2. Set up test data
    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice')");

    // 3. Execute the operation you're testing
    let result = db.execute_ok("SELECT * FROM users WHERE id = 1");

    // 4. Assert the results
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(1));
            assert_eq!(rows[0][1], Value::String("Alice".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}
```

### Helper Methods

**`TestDb` methods:**
- `execute_ok(sql)` - Execute SQL, panic on error
- `execute_err(sql)` - Execute SQL, expect error, panic on success
- `execute(sql)` - Execute SQL, return Result
- `list_tables()` - Get all tables with schemas
- `list_indexes()` - Get all indexes

**Testing errors:**
```rust
let err = db.execute_err("DROP TABLE nonexistent");
assert!(err.to_string().contains("does not exist"));
```

### Where to Add Tests

- **DDL tests**: `tests/create_table.rs`, `tests/drop_table.rs`, `tests/create_index.rs`
- **DML tests**: `tests/insert.rs`, `tests/select.rs`, `tests/delete.rs`
- **New feature**: Create a new file `tests/my_feature.rs`

All test files must include `mod common;` at the top to access shared utilities.

### Running Tests

```bash
cargo test                          # Run all tests
cargo test --test select            # Run one test file
cargo test test_my_feature          # Run specific test
```
