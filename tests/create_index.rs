mod common;

use common::TestDb;
use db2::sql::{ExecutionResult, IndexType};

#[test]
fn test_create_index_simple() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    let result = db.execute_ok("CREATE INDEX idx_user_id ON users(id)");

    match result {
        ExecutionResult::CreateIndex {
            index_name,
            table_name,
            columns,
            index_type,
        } => {
            assert_eq!(index_name, "idx_user_id");
            assert_eq!(table_name, "users");
            assert_eq!(columns, vec!["id"]);
            assert_eq!(index_type, IndexType::BTree);
        }
        other => panic!("Expected CreateIndex result, got: {:?}", other),
    }

    let indexes = db.list_indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].0, "idx_user_id");
    assert_eq!(indexes[0].1, "users");
    assert_eq!(indexes[0].2, vec!["id"]);
    assert_eq!(indexes[0].3, IndexType::BTree);
}

#[test]
fn test_create_hash_index() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    let result = db.execute_ok("CREATE INDEX idx_user_id_hash ON users USING HASH (id)");

    match result {
        ExecutionResult::CreateIndex { index_type, .. } => {
            assert_eq!(index_type, IndexType::Hash);
        }
        other => panic!("Expected CreateIndex result, got: {:?}", other),
    }

    let indexes = db.list_indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].3, IndexType::Hash);
}

#[test]
fn test_create_index_composite() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE orders (user_id INTEGER, order_id INTEGER, amount INTEGER)");
    db.execute_ok("CREATE INDEX idx_user_order ON orders(user_id, order_id)");

    let indexes = db.list_indexes();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0].2, vec!["user_id", "order_id"]);
}

#[test]
fn test_create_index_multiple_on_same_table() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, age INTEGER, score INTEGER)");
    db.execute_ok("CREATE INDEX idx_id ON users(id)");
    db.execute_ok("CREATE INDEX idx_age ON users(age)");
    db.execute_ok("CREATE INDEX idx_score ON users(score)");

    let indexes = db.list_indexes();
    assert_eq!(indexes.len(), 3);
}

#[test]
fn test_create_index_on_nonexistent_table() {
    let mut db = TestDb::new().unwrap();

    let err = db.execute_err("CREATE INDEX idx ON nonexistent(id)");
    assert!(err.to_string().contains("does not exist"));
}

#[test]
fn test_create_index_on_nonexistent_column() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    let err = db.execute_err("CREATE INDEX idx ON users(nonexistent)");
    assert!(err.to_string().contains("not found"));
}

#[test]
fn test_create_index_duplicate_name() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, age INTEGER)");
    db.execute_ok("CREATE INDEX idx_users ON users(id)");

    let err = db.execute_err("CREATE INDEX idx_users ON users(age)");
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn test_create_index_duplicate_columns() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, age INTEGER)");
    db.execute_ok("CREATE INDEX idx1 ON users(id)");

    let err = db.execute_err("CREATE INDEX idx2 ON users(id)");
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn test_create_index_on_varchar_column() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    let err = db.execute_err("CREATE INDEX idx ON users(name)");
    assert!(err.to_string().contains("INTEGER"));
}

#[test]
fn test_create_index_on_boolean_column() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, active BOOLEAN)");
    let err = db.execute_err("CREATE INDEX idx ON users(active)");
    assert!(err.to_string().contains("INTEGER"));
}

#[test]
fn test_create_index_persistence() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    {
        let mut executor = db2::sql::Executor::new(&db_path, 100).unwrap();
        let stmt = db2::sql::parse_sql("CREATE TABLE users (id INTEGER, age INTEGER)").unwrap();
        executor.execute(stmt).unwrap();
        let stmt = db2::sql::parse_sql("CREATE INDEX idx_id ON users(id)").unwrap();
        executor.execute(stmt).unwrap();
        executor.flush_all().unwrap();
    }

    {
        let executor = db2::sql::Executor::new(&db_path, 100).unwrap();
        let indexes = executor.list_indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].0, "idx_id");
        assert_eq!(indexes[0].3, IndexType::BTree);
    }
}

#[test]
fn test_create_hash_index_persistence() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    {
        let mut executor = db2::sql::Executor::new(&db_path, 100).unwrap();
        let stmt = db2::sql::parse_sql("CREATE TABLE users (id INTEGER)").unwrap();
        executor.execute(stmt).unwrap();
        let stmt =
            db2::sql::parse_sql("CREATE INDEX idx_id_hash ON users USING HASH (id)").unwrap();
        executor.execute(stmt).unwrap();
        executor.flush_all().unwrap();
    }

    {
        let executor = db2::sql::Executor::new(&db_path, 100).unwrap();
        let indexes = executor.list_indexes();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].0, "idx_id_hash");
        assert_eq!(indexes[0].3, IndexType::Hash);
    }
}

#[test]
fn test_create_index_populates_existing_data() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice')");
    db.execute_ok("INSERT INTO users VALUES (2, 'Bob')");
    db.execute_ok("INSERT INTO users VALUES (3, 'Charlie')");

    db.execute_ok("CREATE INDEX idx_id ON users(id)");

    let result = db.execute_ok("SELECT * FROM users WHERE id = 2");
    match &result {
        ExecutionResult::Select { rows, plan, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(plan.iter().any(|p| p.contains("Index scan")));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}
