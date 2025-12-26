mod common;

use common::TestDb;
use db2::sql::ExecutionResult;
use db2::types::Value;

#[test]
fn test_query_features_combined() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE users (id INTEGER, name VARCHAR, region VARCHAR)");
    db.execute_ok("CREATE TABLE orders (user_id INTEGER, amount INTEGER)");
    db.execute_ok(
        "INSERT INTO users VALUES (1, 'Alice', 'east'), (2, 'Bob', 'west'), (3, 'Cara', 'east')",
    );
    db.execute_ok("INSERT INTO orders VALUES (1, 10), (1, 5), (3, 7), (2, 1)");

    let result =
        db.execute_ok("SELECT region, COUNT(*) FROM users GROUP BY region ORDER BY region ASC");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][0], Value::String("east".to_string()));
            assert_eq!(rows[0][1], Value::Integer(2));
            assert_eq!(rows[1][0], Value::String("west".to_string()));
            assert_eq!(rows[1][1], Value::Integer(1));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    let result =
        db.execute_ok("SELECT DISTINCT region FROM users ORDER BY region DESC LIMIT 1 OFFSET 0");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::String("west".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    let result = db.execute_ok(
        "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders) ORDER BY name ASC",
    );
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0][0], Value::String("Alice".to_string()));
            assert_eq!(rows[1][0], Value::String("Bob".to_string()));
            assert_eq!(rows[2][0], Value::String("Cara".to_string()));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}

#[test]
fn test_query_features_aggregates() {
    let mut db = TestDb::new().unwrap();

    db.execute_ok("CREATE TABLE numbers (val INTEGER)");
    db.execute_ok("INSERT INTO numbers VALUES (1), (2), (3), (4)");

    let result =
        db.execute_ok("SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM numbers");
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Value::Integer(4));
            match &rows[0][1] {
                Value::Float(v) => assert!((*v - 10.0).abs() < f64::EPSILON),
                other => panic!("Expected float sum, got {:?}", other),
            }
            match &rows[0][2] {
                Value::Float(v) => assert!((*v - 2.5).abs() < f64::EPSILON),
                other => panic!("Expected float avg, got {:?}", other),
            }
            assert_eq!(rows[0][3], Value::Integer(1));
            assert_eq!(rows[0][4], Value::Integer(4));
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }
}
