mod common;

use common::TestDb;
use db::sql::ExecutionResult;

#[test]
fn test_drop_index_simple() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE INDEX idx_user_id ON users(id)");

    assert_eq!(db.list_indexes().len(), 1);

    let result = db.execute_ok("DROP INDEX idx_user_id");

    match result {
        ExecutionResult::DropIndex { index_name } => {
            assert_eq!(index_name, "idx_user_id");
        }
        other => panic!("Expected DropIndex result, got: {:?}", other),
    }

    assert_eq!(db.list_indexes().len(), 0);
}

#[test]
fn test_drop_index_nonexistent() {
    let mut db = TestDb::new().unwrap();

    let err = db.execute_err("DROP INDEX nonexistent");
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn test_drop_index_leaves_table() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE INDEX idx_user_id ON users(id)");

    db.execute_ok("DROP INDEX idx_user_id");

    // Verify table still exists
    assert_eq!(db.list_tables().len(), 1);
}

#[test]
fn test_drop_index_leaves_other_indexes() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, age INTEGER)");
    db.execute_ok("CREATE INDEX idx_id ON users(id)");
    db.execute_ok("CREATE INDEX idx_age ON users(age)");

    assert_eq!(db.list_indexes().len(), 2);

    db.execute_ok("DROP INDEX idx_id");

    let indexes = db.list_indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].0, "idx_age");
}

#[test]
fn test_drop_index_with_data() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
    db.execute_ok("CREATE INDEX idx_id ON users(id)");

    db.execute_ok("DROP INDEX idx_id");

    // Data should still be accessible
    let result = db.execute_ok("SELECT * FROM users");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_drop_index_persistence() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    {
        let mut executor = db::sql::Executor::new(&db_path, 100).unwrap();
        let stmt = db::sql::parse_sql("CREATE TABLE users (id INTEGER)").unwrap();
        executor.execute(stmt).unwrap();
        let stmt = db::sql::parse_sql("CREATE INDEX idx_id ON users(id)").unwrap();
        executor.execute(stmt).unwrap();
        executor.flush_all().unwrap();
    }

    {
        let mut executor = db::sql::Executor::new(&db_path, 100).unwrap();
        assert_eq!(executor.list_indexes().len(), 1);
        let stmt = db::sql::parse_sql("DROP INDEX idx_id").unwrap();
        executor.execute(stmt).unwrap();
        executor.flush_all().unwrap();
    }

    {
        let executor = db::sql::Executor::new(&db_path, 100).unwrap();
        assert_eq!(executor.list_indexes().len(), 0);
    }
}

#[test]
fn test_drop_index_and_recreate() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER)");
    db.execute_ok("CREATE INDEX idx_id ON users(id)");
    db.execute_ok("DROP INDEX idx_id");
    db.execute_ok("CREATE INDEX idx_id ON users(id)");

    let indexes = db.list_indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].0, "idx_id");
}

#[test]
fn test_drop_composite_index() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE orders (user_id INTEGER, order_id INTEGER)");
    db.execute_ok("CREATE INDEX idx_composite ON orders(user_id, order_id)");

    assert_eq!(db.list_indexes().len(), 1);

    db.execute_ok("DROP INDEX idx_composite");

    assert_eq!(db.list_indexes().len(), 0);
}

#[test]
fn test_drop_multiple_indexes() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE data (a INTEGER, b INTEGER, c INTEGER)");
    db.execute_ok("CREATE INDEX idx_a ON data(a)");
    db.execute_ok("CREATE INDEX idx_b ON data(b)");
    db.execute_ok("CREATE INDEX idx_c ON data(c)");

    assert_eq!(db.list_indexes().len(), 3);

    db.execute_ok("DROP INDEX idx_a");
    db.execute_ok("DROP INDEX idx_c");

    let indexes = db.list_indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].0, "idx_b");
}

#[test]
fn test_drop_index_affects_query_plan() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
    db.execute_ok("CREATE INDEX idx_id ON users(id)");

    // Query should use index
    let result = db.execute_ok("SELECT * FROM users WHERE id = 1");
    match &result {
        ExecutionResult::Select { plan, .. } => {
            assert!(plan.iter().any(|p| p.contains("Index scan")));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    // Drop index
    db.execute_ok("DROP INDEX idx_id");

    // Query should still work correctly, but without index
    let result = db.execute_ok("SELECT * FROM users WHERE id = 1");
    match &result {
        ExecutionResult::Select { plan, rows, .. } => {
            // Plan should not mention index scan anymore
            assert!(!plan.iter().any(|p| p.contains("Index scan")));
            assert_eq!(rows.len(), 1); // Data is still correct
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}
