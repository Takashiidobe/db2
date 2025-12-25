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
