mod common;

use common::TestDb;
use db2::sql::ExecutionResult;
use db2::types::{Date, Decimal, Timestamp, Value};

#[test]
fn test_date_timestamp_decimal_types() {
    let mut db = TestDb::new().expect("test db");
    let result = db.execute_ok(
        "CREATE TABLE events (id INTEGER, d DATE, ts TIMESTAMP, amount DECIMAL, note VARCHAR)",
    );
    match result {
        ExecutionResult::CreateTable { table_name } => {
            assert_eq!(table_name, "events");
        }
        other => panic!("Expected CreateTable result, got: {:?}", other),
    }

    let result = db.execute_ok(
        "INSERT INTO events VALUES (1, DATE '2025-01-02', TIMESTAMP '2025-01-02 03:04:05', DECIMAL '12.34', 'launch')",
    );
    match result {
        ExecutionResult::Insert { row_ids } => {
            assert_eq!(row_ids.len(), 1);
        }
        other => panic!("Expected Insert result, got: {:?}", other),
    }

    let result = db.execute_ok(
        "SELECT id, d, ts, amount, note FROM events WHERE d = DATE '2025-01-02' AND amount > DECIMAL '10.00'",
    );
    match &result {
        ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
        }
        other => panic!("Expected Select result, got: {:?}", other),
    }

    match result {
        db2::sql::ExecutionResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0],
                vec![
                    Value::Integer(1),
                    Value::Date(Date::parse("2025-01-02").expect("valid date")),
                    Value::Timestamp(Timestamp::parse("2025-01-02 03:04:05").expect("valid ts")),
                    Value::Decimal(Decimal::parse("12.34").expect("valid decimal")),
                    Value::String("launch".to_string()),
                ]
            );
        }
        _ => panic!("Expected Select result"),
    }
}
