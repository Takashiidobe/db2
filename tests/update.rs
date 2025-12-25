mod common;

use common::TestDb;
use db::sql::ExecutionResult;
use db::types::Value;

#[test]
fn test_update_all_rows() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");

    let result = db.execute_ok("UPDATE users SET name = 'Updated'");
    match result {
        ExecutionResult::Update { rows_updated } => assert_eq!(rows_updated, 2),
        other => panic!("Expected Update result, got: {:?}", other),
    }

    let rows = db.execute_ok("SELECT * FROM users");
    match rows {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
            for row in rows {
                assert_eq!(row[1], Value::String("Updated".to_string()));
            }
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_update_with_where_clause() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER, label VARCHAR)");
    db.execute_ok("INSERT INTO numbers VALUES (10, 'ten'), (20, 'twenty'), (30, 'thirty')");

    let result = db.execute_ok("UPDATE numbers SET label = 'big' WHERE val >= 20");
    match result {
        ExecutionResult::Update { rows_updated } => assert_eq!(rows_updated, 2),
        other => panic!("Expected Update result, got: {:?}", other),
    }

    let remaining = db.execute_ok("SELECT * FROM numbers WHERE val = 10");
    match remaining {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], Value::String("ten".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    let updated = db.execute_ok("SELECT * FROM numbers WHERE val = 30");
    match updated {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], Value::String("big".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_update_rebuilds_indexes() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE metrics (id INTEGER, score INTEGER)");
    db.execute_ok("CREATE INDEX idx_metrics_score ON metrics(score)");
    db.execute_ok("INSERT INTO metrics VALUES (1, 10), (2, 20)");

    let result = db.execute_ok("UPDATE metrics SET score = 30 WHERE id = 2");
    match result {
        ExecutionResult::Update { rows_updated } => assert_eq!(rows_updated, 1),
        other => panic!("Expected Update result, got: {:?}", other),
    }

    let old = db.execute_ok("SELECT * FROM metrics WHERE score = 20");
    match old {
        ExecutionResult::Select { rows, .. } => assert!(rows.is_empty()),
        other => panic!("Expected Select result, got: {:?}", other),
    }

    let new = db.execute_ok("SELECT * FROM metrics WHERE score = 30");
    match new {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(2));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_update_supports_growing_rows() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE docs (id INTEGER, body VARCHAR)");
    db.execute_ok("INSERT INTO docs VALUES (1, 'a')");

    let result = db.execute_ok("UPDATE docs SET body = 'a much longer document body'");
    match result {
        ExecutionResult::Update { rows_updated } => assert_eq!(rows_updated, 1),
        other => panic!("Expected Update result, got: {:?}", other),
    }

    let rows = db.execute_ok("SELECT * FROM docs");
    match rows {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0][1],
                Value::String("a much longer document body".to_string())
            );
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_update_nonexistent_table() {
    let mut db = TestDb::new().unwrap();

    let err = db.execute_err("UPDATE missing SET name = 'nope'");
    assert!(err.to_string().contains("does not exist"));
}

