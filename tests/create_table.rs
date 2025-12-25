mod common;

use common::TestDb;
use db2::sql::ExecutionResult;
use db2::types::DataType;

#[test]
fn test_create_table_simple() {
    let mut db = TestDb::new().unwrap();

    let result = db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");

    match result {
        ExecutionResult::CreateTable { table_name } => {
            assert_eq!(table_name, "users");
        }
        other => panic!("Expected CreateTable result, got: {:?}", other),
    }

    let tables = db.list_tables();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].0, "users");

    let schema = &tables[0].1;
    assert_eq!(schema.column_count(), 2);

    let col0 = schema.column(0).unwrap();
    assert_eq!(col0.name(), "id");
    assert_eq!(col0.data_type(), DataType::Integer);

    let col1 = schema.column(1).unwrap();
    assert_eq!(col1.name(), "name");
    assert_eq!(col1.data_type(), DataType::String);
}

#[test]
fn test_create_table_all_types() {
    let mut db = TestDb::new().unwrap();

    let result = db.execute_ok(
        "CREATE TABLE all_types (int_col INTEGER, uint_col UNSIGNED, float_col FLOAT, bool_col BOOLEAN, str_col VARCHAR)",
    );

    match result {
        ExecutionResult::CreateTable { table_name } => {
            assert_eq!(table_name, "all_types");
        }
        other => panic!("Expected CreateTable result, got: {:?}", other),
    }

    let tables = db.list_tables();
    let schema = &tables[0].1;
    assert_eq!(schema.column_count(), 5);
    assert_eq!(schema.column(0).unwrap().data_type(), DataType::Integer);
    assert_eq!(schema.column(1).unwrap().data_type(), DataType::Unsigned);
    assert_eq!(schema.column(2).unwrap().data_type(), DataType::Float);
    assert_eq!(schema.column(3).unwrap().data_type(), DataType::Boolean);
    assert_eq!(schema.column(4).unwrap().data_type(), DataType::String);
}

#[test]
fn test_create_table_duplicate_name() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER)");

    let err = db.execute_err("CREATE TABLE users (name VARCHAR)");
    assert!(err.to_string().contains("already exists"));
}

#[test]
fn test_create_table_persistence() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();

    {
        let mut executor = db2::sql::Executor::new(&db_path, 100).unwrap();
        let stmt =
            db2::sql::parse_sql("CREATE TABLE persistent (id INTEGER, data VARCHAR)").unwrap();
        executor.execute(stmt).unwrap();
        executor.flush_all().unwrap();
    }

    {
        let executor = db2::sql::Executor::new(&db_path, 100).unwrap();
        let tables = executor.list_tables();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].0, "persistent");
        assert_eq!(tables[0].1.column_count(), 2);
    }
}

#[test]
fn test_create_multiple_tables() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE TABLE orders (order_id INTEGER, user_id INTEGER)");
    db.execute_ok("CREATE TABLE products (product_id INTEGER, price INTEGER)");

    let tables = db.list_tables();
    assert_eq!(tables.len(), 3);

    let table_names: Vec<&str> = tables.iter().map(|(name, _)| name.as_str()).collect();
    assert!(table_names.contains(&"users"));
    assert!(table_names.contains(&"orders"));
    assert!(table_names.contains(&"products"));
}

#[test]
fn test_create_table_single_column() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE single (col INTEGER)");

    let tables = db.list_tables();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].1.column_count(), 1);
}

#[test]
fn test_create_table_many_columns() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok(
        "CREATE TABLE many_cols (c1 INTEGER, c2 INTEGER, c3 VARCHAR, c4 BOOLEAN, c5 INTEGER, c6 VARCHAR)"
    );

    let tables = db.list_tables();
    assert_eq!(tables[0].1.column_count(), 6);
}
