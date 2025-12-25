mod common;

use common::TestDb;
use db2::sql::ExecutionResult;
use db2::types::Value;

#[test]
fn test_delete_all_rows() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");

    let result = db.execute_ok("DELETE FROM users");
    match result {
        ExecutionResult::Delete { rows_deleted } => assert_eq!(rows_deleted, 2),
        other => panic!("Expected Delete result, got: {:?}", other),
    }

    let remaining = db.execute_ok("SELECT * FROM users");
    match remaining {
        ExecutionResult::Select { rows, .. } => assert!(rows.is_empty()),
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_delete_with_where_clause() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER)");
    db.execute_ok("INSERT INTO numbers VALUES (10), (20), (30)");

    let result = db.execute_ok("DELETE FROM numbers WHERE val >= 20");
    match result {
        ExecutionResult::Delete { rows_deleted } => assert_eq!(rows_deleted, 2),
        other => panic!("Expected Delete result, got: {:?}", other),
    }

    let remaining = db.execute_ok("SELECT * FROM numbers");
    match remaining {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(10));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_delete_nonexistent_table() {
    let mut db = TestDb::new().unwrap();

    let err = db.execute_err("DELETE FROM missing");
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn test_delete_updates_indexes() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE INDEX idx_users_id ON users(id)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");

    let result = db.execute_ok("DELETE FROM users WHERE id = 1");
    match result {
        ExecutionResult::Delete { rows_deleted } => assert_eq!(rows_deleted, 1),
        other => panic!("Expected Delete result, got: {:?}", other),
    }

    let missing = db.execute_ok("SELECT * FROM users WHERE id = 1");
    match missing {
        ExecutionResult::Select { rows, .. } => assert!(rows.is_empty()),
        other => panic!("Expected Select result, got: {:?}", other),
    }

    let remaining = db.execute_ok("SELECT * FROM users WHERE id = 2");
    match remaining {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(2));
            assert_eq!(rows[0][1], Value::String("Bob".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}
