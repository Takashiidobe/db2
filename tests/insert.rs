mod common;

use common::TestDb;
use db2::sql::ExecutionResult;
use db2::types::Value;

#[test]
fn test_insert_single_row() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    let result = db.execute_ok("INSERT INTO users VALUES (1, 'Alice')");

    match result {
        ExecutionResult::Insert { row_ids } => {
            assert_eq!(row_ids.len(), 1);
        }
        other => panic!("Expected Insert result, got: {:?}", other),
    }

    let result = db.execute_ok("SELECT * FROM users");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(1));
            assert_eq!(rows[0][1], Value::String("Alice".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_insert_multiple_rows() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    let result = db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    match result {
        ExecutionResult::Insert { row_ids } => {
            assert_eq!(row_ids.len(), 3);
        }
        other => panic!("Expected Insert result, got: {:?}", other),
    }

    let result = db.execute_ok("SELECT * FROM users");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 3);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_insert_all_types() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE data (int_col INTEGER, bool_col BOOLEAN, str_col VARCHAR)");
    db.execute_ok("INSERT INTO data VALUES (42, true, 'hello')");

    let result = db.execute_ok("SELECT * FROM data");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(42));
            assert_eq!(rows[0][1], Value::Boolean(true));
            assert_eq!(rows[0][2], Value::String("hello".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_insert_boolean_values() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE flags (id INTEGER, active BOOLEAN)");
    db.execute_ok("INSERT INTO flags VALUES (1, true), (2, false)");

    let result = db.execute_ok("SELECT * FROM flags");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][1], Value::Boolean(true));
            assert_eq!(rows[1][1], Value::Boolean(false));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_insert_negative_integers() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER)");
    db.execute_ok("INSERT INTO numbers VALUES (-100), (0), (100)");

    let result = db.execute_ok("SELECT * FROM numbers");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0][0], Value::Integer(-100));
            assert_eq!(rows[1][0], Value::Integer(0));
            assert_eq!(rows[2][0], Value::Integer(100));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_insert_unsigned_values() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val UNSIGNED)");
    db.execute_ok("INSERT INTO numbers VALUES (0), (18446744073709551615)");

    let result = db.execute_ok("SELECT * FROM numbers WHERE val = 18446744073709551615");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Unsigned(18446744073709551615));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_insert_negative_into_unsigned_rejected() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val UNSIGNED)");
    let err = db.execute_err("INSERT INTO numbers VALUES (-1)");
    assert!(err.to_string().to_lowercase().contains("type mismatch"));
}

#[test]
fn test_insert_strings_with_quotes() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE messages (text VARCHAR)");
    db.execute_ok("INSERT INTO messages VALUES ('It''s working')");

    let result = db.execute_ok("SELECT * FROM messages");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::String("It's working".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_insert_into_nonexistent_table() {
    let mut db = TestDb::new().unwrap();

    let err = db.execute_err("INSERT INTO nonexistent VALUES (1, 'test')");
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn test_insert_wrong_column_count() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    let err = db.execute_err("INSERT INTO users VALUES (1)");
    assert!(err.to_string().contains("does not match"));
}

#[test]
fn test_insert_updates_index() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE INDEX idx_id ON users(id)");

    db.execute_ok("INSERT INTO users VALUES (5, 'Alice')");

    let result = db.execute_ok("SELECT * FROM users WHERE id = 5");
    match &result {
        ExecutionResult::Select { rows, plan, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(plan.iter().any(|p| p.contains("Index scan")));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_insert_persistence() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    {
        let mut executor = db2::sql::Executor::new(&db_path, 100).unwrap();
        let stmt = db2::sql::parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap();
        executor.execute(stmt).unwrap();
        let stmt =
            db2::sql::parse_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
        executor.execute(stmt).unwrap();
        executor.flush_all().unwrap();
    }

    {
        let mut executor = db2::sql::Executor::new(&db_path, 100).unwrap();
        let stmt = db2::sql::parse_sql("SELECT * FROM users").unwrap();
        let result = executor.execute(stmt).unwrap();

        match result {
            ExecutionResult::Select { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            other => panic!("Expected Select result, got: {:?}", other),
        }
    }
}

#[test]
fn test_insert_many_rows() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER)");

    // Insert 100 rows one statement at a time
    for i in 0..100 {
        db.execute_ok(&format!("INSERT INTO numbers VALUES ({})", i));
    }

    let result = db.execute_ok("SELECT * FROM numbers");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 100);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_insert_empty_string() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE data (text VARCHAR)");
    db.execute_ok("INSERT INTO data VALUES ('')");

    let result = db.execute_ok("SELECT * FROM data");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::String("".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}
