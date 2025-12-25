use db2::sql::{ExecutionResult, Executor, parse_sql};
use db2::table::RowId;
use db2::types::Value;
use db2::wal::{WalFile, WalRecord};
use tempfile::TempDir;

#[test]
fn test_wal_records_persist_on_disk() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal.log");
    let wal = WalFile::new(&wal_path);

    let records = vec![
        WalRecord::Begin { txn_id: 1 },
        WalRecord::Insert {
            txn_id: 1,
            table: "users".to_string(),
            row_id: RowId::new(2, 5),
            values: vec![Value::Integer(1), Value::String("Alice".to_string())],
        },
        WalRecord::Update {
            txn_id: 1,
            table: "users".to_string(),
            row_id: RowId::new(2, 5),
            before: vec![Value::Integer(1), Value::String("Alice".to_string())],
            after: vec![Value::Integer(2), Value::String("Bob".to_string())],
        },
        WalRecord::Delete {
            txn_id: 1,
            table: "users".to_string(),
            row_id: RowId::new(2, 5),
            values: vec![Value::Integer(2), Value::String("Bob".to_string())],
        },
        WalRecord::Commit { txn_id: 1 },
    ];

    for record in &records {
        wal.append(record).unwrap();
    }

    assert!(wal_path.exists());
    assert!(std::fs::metadata(&wal_path).unwrap().len() > 0);

    let loaded = wal.read_all().unwrap();
    assert_eq!(loaded, records);
}

#[test]
fn test_wal_records_from_executor_transactions() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    let mut executor = Executor::new(&db_path, 10).unwrap();

    executor
        .execute(parse_sql("CREATE TABLE users (id INTEGER, name VARCHAR)").unwrap())
        .unwrap();
    executor.execute(parse_sql("BEGIN").unwrap()).unwrap();

    let insert_result = executor
        .execute(parse_sql("INSERT INTO users VALUES (1, 'Alice')").unwrap())
        .unwrap();
    let insert_row_id = match insert_result {
        ExecutionResult::Insert { row_ids } => row_ids[0],
        _ => panic!("Expected insert result"),
    };

    executor
        .execute(parse_sql("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap())
        .unwrap();
    executor
        .execute(parse_sql("DELETE FROM users WHERE id = 1").unwrap())
        .unwrap();
    executor.execute(parse_sql("COMMIT").unwrap()).unwrap();

    let wal = WalFile::new(db_path.join("wal.log"));
    let records = wal.read_all().unwrap();

    assert_eq!(records.len(), 5);

    let txn_id = match &records[0] {
        WalRecord::Begin { txn_id } => *txn_id,
        _ => panic!("Expected Begin record"),
    };

    match &records[1] {
        WalRecord::Insert {
            txn_id: rec_txn,
            table,
            row_id,
            values,
        } => {
            assert_eq!(*rec_txn, txn_id);
            assert_eq!(table, "users");
            assert_eq!(*row_id, insert_row_id);
            assert_eq!(values[0], Value::Integer(1));
            assert_eq!(values[1], Value::String("Alice".to_string()));
        }
        _ => panic!("Expected Insert record"),
    }

    match &records[2] {
        WalRecord::Update {
            txn_id: rec_txn,
            table,
            row_id,
            before,
            after,
        } => {
            assert_eq!(*rec_txn, txn_id);
            assert_eq!(table, "users");
            assert_eq!(*row_id, insert_row_id);
            assert_eq!(before[1], Value::String("Alice".to_string()));
            assert_eq!(after[1], Value::String("Bob".to_string()));
        }
        _ => panic!("Expected Update record"),
    }

    match &records[3] {
        WalRecord::Delete {
            txn_id: rec_txn,
            table,
            row_id,
            values,
        } => {
            assert_eq!(*rec_txn, txn_id);
            assert_eq!(table, "users");
            assert_eq!(*row_id, insert_row_id);
            assert_eq!(values[1], Value::String("Bob".to_string()));
        }
        _ => panic!("Expected Delete record"),
    }

    match &records[4] {
        WalRecord::Commit { txn_id: rec_txn } => {
            assert_eq!(*rec_txn, txn_id);
        }
        _ => panic!("Expected Commit record"),
    }
}
