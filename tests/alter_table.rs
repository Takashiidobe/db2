mod common;

use common::TestDb;
use db2::sql::ExecutionResult;
use db2::types::Value;

#[test]
fn test_alter_table_add_column() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");

    let result = db.execute_ok("ALTER TABLE users ADD COLUMN age INTEGER");
    match result {
        ExecutionResult::AlterTable { table_name } => {
            assert_eq!(table_name, "users");
        }
        other => panic!("Expected AlterTable result, got: {:?}", other),
    }

    let result = db.execute_ok("SELECT id, name, age FROM users ORDER BY id ASC");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::Integer(1));
            assert_eq!(rows[0][2], Value::Null);
            assert_eq!(rows[1][0], Value::Integer(2));
            assert_eq!(rows[1][2], Value::Null);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_alter_table_drop_column() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice', 30)");

    let result = db.execute_ok("ALTER TABLE users DROP COLUMN age");
    match result {
        ExecutionResult::AlterTable { table_name } => {
            assert_eq!(table_name, "users");
        }
        other => panic!("Expected AlterTable result, got: {:?}", other),
    }

    let result = db.execute_ok("SELECT id, name FROM users");
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
fn test_alter_table_rename_column() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice')");

    let result = db.execute_ok("ALTER TABLE users RENAME COLUMN name TO full_name");
    match result {
        ExecutionResult::AlterTable { table_name } => {
            assert_eq!(table_name, "users");
        }
        other => panic!("Expected AlterTable result, got: {:?}", other),
    }

    let result = db.execute_ok("SELECT id, full_name FROM users");
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
fn test_alter_table_rename_table() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE INDEX idx_users_id ON users (id)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice')");

    let result = db.execute_ok("ALTER TABLE users RENAME TO customers");
    match result {
        ExecutionResult::AlterTable { table_name } => {
            assert_eq!(table_name, "users");
        }
        other => panic!("Expected AlterTable result, got: {:?}", other),
    }

    let result = db.execute_ok("SELECT id, name FROM customers");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(1));
            assert_eq!(rows[0][1], Value::String("Alice".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    let err = db.execute_err("SELECT id FROM users");
    assert!(err.to_string().contains("does not exist"));

    let tables = db.list_tables();
    assert!(tables.iter().any(|(name, _)| name == "customers"));
    assert!(!tables.iter().any(|(name, _)| name == "users"));

    let indexes = db.list_indexes();
    assert!(indexes.iter().any(|(name, table, cols, _)| {
        name == "idx_users_id" && table == "customers" && cols == &vec!["id".to_string()]
    }));

    let old_path = db.path().join("users.db");
    let new_path = db.path().join("customers.db");
    assert!(!old_path.exists());
    assert!(new_path.exists());
}
