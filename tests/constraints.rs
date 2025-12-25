use db2::sql::{ExecutionResult, Executor, parse_sql};
use tempfile::TempDir;

#[test]
fn test_primary_key_and_unique_constraints() {
    let temp_dir = TempDir::new().unwrap();
    let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

    executor
        .execute(parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)").unwrap())
        .unwrap();
    executor
        .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
        .unwrap();

    let err = executor
        .execute(parse_sql("INSERT INTO users VALUES (1, 'Bob')").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("Unique constraint"));

    let err = executor
        .execute(parse_sql("INSERT INTO users VALUES (2, 'Alice')").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("Unique constraint"));
}

#[test]
fn test_foreign_key_constraints() {
    let temp_dir = TempDir::new().unwrap();
    let mut executor = Executor::new(temp_dir.path(), 10).unwrap();

    executor
        .execute(parse_sql("CREATE TABLE orgs (id INTEGER PRIMARY KEY)").unwrap())
        .unwrap();
    executor
        .execute(
            parse_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, org_id INTEGER REFERENCES orgs(id))")
                .unwrap(),
        )
        .unwrap();

    executor
        .execute(parse_sql("INSERT INTO orgs VALUES (1)").unwrap())
        .unwrap();
    executor
        .execute(parse_sql("INSERT INTO users VALUES (10, 1)").unwrap())
        .unwrap();

    let err = executor
        .execute(parse_sql("INSERT INTO users VALUES (11, 2)").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("Foreign key violation"));

    let err = executor
        .execute(parse_sql("DELETE FROM orgs WHERE id = 1").unwrap())
        .unwrap_err();
    assert!(err.to_string().contains("Foreign key restrict"));

    let result = executor
        .execute(parse_sql("SELECT * FROM users WHERE id = 10").unwrap())
        .unwrap();
    match result {
        ExecutionResult::Select { rows, .. } => assert_eq!(rows.len(), 1),
        _ => panic!("Expected Select result"),
    }
}
