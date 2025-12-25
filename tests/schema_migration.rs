use db2::sql::{parse_sql, ExecutionResult, Executor};
use db2::types::Value;
use std::io;
use tempfile::TempDir;

fn execute_ok(executor: &mut Executor, sql: &str) -> ExecutionResult {
    let stmt = parse_sql(sql).unwrap_or_else(|e| {
        panic!("Expected SQL to parse but got error: {}\nSQL: {}", e, sql)
    });
    executor.execute(stmt).unwrap_or_else(|e| {
        panic!("Expected SQL to succeed but got error: {}\nSQL: {}", e, sql)
    })
}

fn execute_err(executor: &mut Executor, sql: &str) -> io::Error {
    let stmt = parse_sql(sql).unwrap_or_else(|e| {
        panic!("Expected SQL to parse but got error: {}\nSQL: {}", e, sql)
    });
    executor.execute(stmt).expect_err(&format!(
        "Expected SQL to fail but it succeeded\nSQL: {}",
        sql
    ))
}

#[test]
fn test_schema_add_column_persists_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    {
        let mut executor = Executor::new(temp_dir.path(), 100).unwrap();
        execute_ok(
            &mut executor,
            "CREATE TABLE users (id INTEGER, name VARCHAR)",
        );
        execute_ok(&mut executor, "INSERT INTO users VALUES (1, 'Alice')");
        execute_ok(&mut executor, "ALTER TABLE users ADD COLUMN age INTEGER");
        execute_ok(&mut executor, "INSERT INTO users VALUES (2, 'Bob', 30)");
        executor.flush_all().unwrap();
    }

    let mut executor = Executor::new(temp_dir.path(), 100).unwrap();
    let result = execute_ok(
        &mut executor,
        "SELECT id, name, age FROM users ORDER BY id ASC",
    );
    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0], vec![Value::Integer(1), Value::String("Alice".to_string()), Value::Null]);
            assert_eq!(rows[1], vec![Value::Integer(2), Value::String("Bob".to_string()), Value::Integer(30)]);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_schema_drop_column_persists_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    {
        let mut executor = Executor::new(temp_dir.path(), 100).unwrap();
        execute_ok(
            &mut executor,
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)",
        );
        execute_ok(&mut executor, "INSERT INTO users VALUES (1, 'Alice', 25)");
        execute_ok(&mut executor, "ALTER TABLE users DROP COLUMN age");
        executor.flush_all().unwrap();
    }

    let mut executor = Executor::new(temp_dir.path(), 100).unwrap();
    let result = execute_ok(&mut executor, "SELECT id, name FROM users");
    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0], vec![Value::Integer(1), Value::String("Alice".to_string())]);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    let err = execute_err(&mut executor, "SELECT age FROM users");
    assert!(err.to_string().contains("Column"));
}

#[test]
fn test_schema_rename_column_persists_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    {
        let mut executor = Executor::new(temp_dir.path(), 100).unwrap();
        execute_ok(
            &mut executor,
            "CREATE TABLE users (id INTEGER, full_name VARCHAR)",
        );
        execute_ok(&mut executor, "INSERT INTO users VALUES (1, 'Alice')");
        execute_ok(
            &mut executor,
            "ALTER TABLE users RENAME COLUMN full_name TO name",
        );
        executor.flush_all().unwrap();
    }

    let mut executor = Executor::new(temp_dir.path(), 100).unwrap();
    let result = execute_ok(&mut executor, "SELECT id, name FROM users");
    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0], vec![Value::Integer(1), Value::String("Alice".to_string())]);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    let err = execute_err(&mut executor, "SELECT full_name FROM users");
    assert!(err.to_string().contains("Column"));
}

#[test]
fn test_schema_rename_table_persists_after_reopen() {
    let temp_dir = TempDir::new().unwrap();
    {
        let mut executor = Executor::new(temp_dir.path(), 100).unwrap();
        execute_ok(
            &mut executor,
            "CREATE TABLE users (id INTEGER, name VARCHAR)",
        );
        execute_ok(&mut executor, "INSERT INTO users VALUES (1, 'Alice')");
        execute_ok(&mut executor, "ALTER TABLE users RENAME TO customers");
        executor.flush_all().unwrap();
    }

    let mut executor = Executor::new(temp_dir.path(), 100).unwrap();
    let result = execute_ok(&mut executor, "SELECT id, name FROM customers");
    match result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0], vec![Value::Integer(1), Value::String("Alice".to_string())]);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    let err = execute_err(&mut executor, "SELECT id FROM users");
    assert!(err.to_string().contains("does not exist"));

    let old_path = temp_dir.path().join("users.db");
    let new_path = temp_dir.path().join("customers.db");
    assert!(!old_path.exists());
    assert!(new_path.exists());
}
