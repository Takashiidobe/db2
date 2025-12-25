mod common;

use common::TestDb;
use db::sql::ExecutionResult;

#[test]
fn test_drop_table_simple() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    assert_eq!(db.list_tables().len(), 1);

    let result = db.execute_ok("DROP TABLE users");

    match result {
        ExecutionResult::DropTable { table_name } => {
            assert_eq!(table_name, "users");
        }
        other => panic!("Expected DropTable result, got: {:?}", other),
    }

    assert_eq!(db.list_tables().len(), 0);
}

#[test]
fn test_drop_table_removes_indexes() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, age INTEGER)");
    db.execute_ok("CREATE INDEX idx_user_id ON users(id)");
    db.execute_ok("CREATE INDEX idx_user_age ON users(age)");

    assert_eq!(db.list_indexes().len(), 2);

    db.execute_ok("DROP TABLE users");

    assert_eq!(db.list_tables().len(), 0);
    assert_eq!(db.list_indexes().len(), 0);
}

#[test]
fn test_drop_table_nonexistent() {
    let mut db = TestDb::new().unwrap();

    let err = db.execute_err("DROP TABLE nonexistent");
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn test_drop_table_with_data() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice')");
    db.execute_ok("INSERT INTO users VALUES (2, 'Bob')");

    let result = db.execute_ok("SELECT * FROM users");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    db.execute_ok("DROP TABLE users");
    assert_eq!(db.list_tables().len(), 0);
}

#[test]
fn test_drop_table_and_recreate() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER)");
    db.execute_ok("DROP TABLE users");
    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR)");

    let tables = db.list_tables();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].1.column_count(), 3);
}

#[test]
fn test_drop_table_multiple() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER)");
    db.execute_ok("CREATE TABLE orders (id INTEGER)");
    db.execute_ok("CREATE TABLE products (id INTEGER)");
    assert_eq!(db.list_tables().len(), 3);

    db.execute_ok("DROP TABLE orders");
    assert_eq!(db.list_tables().len(), 2);

    let table_names: Vec<String> = db.list_tables().iter().map(|(name, _)| name.clone()).collect();
    assert!(table_names.contains(&"users".to_string()));
    assert!(table_names.contains(&"products".to_string()));
    assert!(!table_names.contains(&"orders".to_string()));

    db.execute_ok("DROP TABLE users");
    db.execute_ok("DROP TABLE products");
    assert_eq!(db.list_tables().len(), 0);
}

#[test]
fn test_drop_table_persistence() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    {
        let mut executor = db::sql::Executor::new(&db_path, 100).unwrap();
        let stmt = db::sql::parse_sql("CREATE TABLE persistent (id INTEGER)").unwrap();
        executor.execute(stmt).unwrap();
        executor.flush_all().unwrap();
    }

    {
        let mut executor = db::sql::Executor::new(&db_path, 100).unwrap();
        let stmt = db::sql::parse_sql("DROP TABLE persistent").unwrap();
        executor.execute(stmt).unwrap();
        executor.flush_all().unwrap();
    }

    {
        let executor = db::sql::Executor::new(&db_path, 100).unwrap();
        let tables = executor.list_tables();
        assert_eq!(tables.len(), 0);
    }
}

#[test]
fn test_drop_table_leaves_other_indexes() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER)");
    db.execute_ok("CREATE TABLE orders (id INTEGER)");
    db.execute_ok("CREATE INDEX idx_user_id ON users(id)");
    db.execute_ok("CREATE INDEX idx_order_id ON orders(id)");

    assert_eq!(db.list_indexes().len(), 2);

    db.execute_ok("DROP TABLE users");

    let indexes = db.list_indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].0, "idx_order_id");
    assert_eq!(indexes[0].1, "orders");
}
