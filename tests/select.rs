mod common;

use common::TestDb;
use db2::sql::ExecutionResult;
use db2::types::Value;

// Basic SELECT tests

#[test]
fn test_select_star() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");

    let result = db.execute_ok("SELECT * FROM users");
    match &result {
        ExecutionResult::Select {
            column_names, rows, ..
        } => {
            assert_eq!(column_names.len(), 2);
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_specific_columns() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice', 30)");

    let result = db.execute_ok("SELECT name, id FROM users");
    match &result {
        ExecutionResult::Select {
            column_names, rows, ..
        } => {
            assert_eq!(column_names, &vec!["name", "id"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::String("Alice".to_string()));
            assert_eq!(rows[0][1], Value::Integer(1));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_empty_table() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");

    let result = db.execute_ok("SELECT * FROM users");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 0);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_from_nonexistent_table() {
    let mut db = TestDb::new().unwrap();

    let err = db.execute_err("SELECT * FROM nonexistent");
    assert!(err.to_string().contains("does not exist"));
}

// WHERE clause tests

#[test]
fn test_select_where_equals() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    let result = db.execute_ok("SELECT * FROM users WHERE id = 2");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(2));
            assert_eq!(rows[0][1], Value::String("Bob".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_where_not_equals() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER)");
    db.execute_ok("INSERT INTO users VALUES (1), (2), (3)");

    let result = db.execute_ok("SELECT * FROM users WHERE id != 2");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_where_less_than() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER)");
    db.execute_ok("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)");

    let result = db.execute_ok("SELECT * FROM numbers WHERE val < 30");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_where_greater_than() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER)");
    db.execute_ok("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)");

    let result = db.execute_ok("SELECT * FROM numbers WHERE val > 30");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_where_less_than_equals() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER)");
    db.execute_ok("INSERT INTO numbers VALUES (10), (20), (30)");

    let result = db.execute_ok("SELECT * FROM numbers WHERE val <= 20");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_where_greater_than_equals() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER)");
    db.execute_ok("INSERT INTO numbers VALUES (10), (20), (30)");

    let result = db.execute_ok("SELECT * FROM numbers WHERE val >= 20");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_where_and() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, age INTEGER)");
    db.execute_ok("INSERT INTO users VALUES (1, 25), (2, 30), (3, 35), (4, 40)");

    let result = db.execute_ok("SELECT * FROM users WHERE id > 1 AND age < 40");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_where_boolean() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, active BOOLEAN)");
    db.execute_ok("INSERT INTO users VALUES (1, true), (2, false), (3, true)");

    let result = db.execute_ok("SELECT * FROM users WHERE active = true");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_where_string() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (name VARCHAR)");
    db.execute_ok("INSERT INTO users VALUES ('Alice'), ('Bob'), ('Charlie')");

    let result = db.execute_ok("SELECT * FROM users WHERE name = 'Bob'");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::String("Bob".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_where_no_matches() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER)");
    db.execute_ok("INSERT INTO users VALUES (1), (2), (3)");

    let result = db.execute_ok("SELECT * FROM users WHERE id = 999");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 0);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

// Index scan tests

#[test]
fn test_select_uses_index() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE INDEX idx_id ON users(id)");
    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')");

    let result = db.execute_ok("SELECT * FROM users WHERE id = 2");
    match &result {
        ExecutionResult::Select { rows, plan, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(plan.iter().any(|p| p.contains("Index scan")));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_index_range() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER)");
    db.execute_ok("CREATE INDEX idx_val ON numbers(val)");
    db.execute_ok("INSERT INTO numbers VALUES (10), (20), (30), (40), (50)");

    let result = db.execute_ok("SELECT * FROM numbers WHERE val > 20");
    match &result {
        ExecutionResult::Select { rows, plan, .. } => {
            assert_eq!(rows.len(), 3);
            assert!(plan.iter().any(|p| p.contains("Index scan")));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_unsigned_index() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val UNSIGNED)");
    db.execute_ok("CREATE INDEX idx_val_unsigned ON numbers(val)");
    db.execute_ok("INSERT INTO numbers VALUES (1), (5), (18446744073709551615)");

    let result = db.execute_ok("SELECT * FROM numbers WHERE val = 18446744073709551615");
    match &result {
        ExecutionResult::Select { rows, plan, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Unsigned(18446744073709551615));
            assert!(plan.iter().any(|p| p.contains("Index scan")));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

// JOIN tests

#[test]
fn test_select_join_simple() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE TABLE orders (user_id INTEGER, product VARCHAR)");

    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
    db.execute_ok("INSERT INTO orders VALUES (1, 'Book'), (2, 'Pen'), (1, 'Notebook')");

    let result = db.execute_ok("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 3);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_join_with_index() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE TABLE orders (user_id INTEGER, product VARCHAR)");
    db.execute_ok("CREATE INDEX idx_user_id ON orders(user_id)");

    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
    db.execute_ok("INSERT INTO orders VALUES (1, 'Book'), (2, 'Pen')");

    let result = db.execute_ok("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    match &result {
        ExecutionResult::Select { rows, plan, .. } => {
            assert_eq!(rows.len(), 2);
            // Verify planner mentions the join (plan is populated)
            assert!(!plan.is_empty());
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_join_with_where() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE TABLE orders (user_id INTEGER, amount INTEGER)");

    db.execute_ok("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')");
    db.execute_ok("INSERT INTO orders VALUES (1, 100), (2, 200), (1, 150)");

    let result = db.execute_ok(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE amount > 120",
    );
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_join_no_matches() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER)");
    db.execute_ok("CREATE TABLE orders (user_id INTEGER)");

    db.execute_ok("INSERT INTO users VALUES (1), (2)");
    db.execute_ok("INSERT INTO orders VALUES (3), (4)");

    let result = db.execute_ok("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 0);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_select_specific_columns_after_join() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR)");
    db.execute_ok("CREATE TABLE orders (user_id INTEGER, product VARCHAR)");

    db.execute_ok("INSERT INTO users VALUES (1, 'Alice')");
    db.execute_ok("INSERT INTO orders VALUES (1, 'Book')");

    let result =
        db.execute_ok("SELECT name, product FROM users JOIN orders ON users.id = orders.user_id");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 2);
            // Verify we got the right data
            assert_eq!(rows[0][0], Value::String("Alice".to_string()));
            assert_eq!(rows[0][1], Value::String("Book".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}
