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
