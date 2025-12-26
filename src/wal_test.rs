mod tests {
    use crate::table::RowId;
    use crate::types::Value;
    use crate::wal::{TxnId, WalRecord, WalRecord::*};

    fn roundtrip(record: WalRecord) -> WalRecord {
        let bytes = record.serialize().unwrap();
        WalRecord::deserialize(&bytes).unwrap()
    }

    #[test]
    fn test_roundtrip_control_records() {
        let begin = Begin { txn_id: 1 as TxnId };
        let commit = Commit { txn_id: 2 as TxnId };
        let rollback = Rollback { txn_id: 3 as TxnId };

        assert_eq!(roundtrip(begin.clone()), begin);
        assert_eq!(roundtrip(commit.clone()), commit);
        assert_eq!(roundtrip(rollback.clone()), rollback);
    }

    #[test]
    fn test_roundtrip_insert_update_delete() {
        let row_id = RowId::new(10, 3);
        let values = vec![
            Value::Integer(-7),
            Value::Unsigned(42),
            Value::Float(3.5),
            Value::Boolean(true),
            Value::String("wal".to_string()),
        ];

        let insert = Insert {
            txn_id: 9,
            table: "users".to_string(),
            row_id,
            values: values.clone(),
        };

        let update = Update {
            txn_id: 9,
            table: "users".to_string(),
            row_id,
            before: values.clone(),
            after: vec![
                Value::Integer(8),
                Value::Unsigned(100),
                Value::Float(1.25),
                Value::Boolean(false),
                Value::String("redo".to_string()),
            ],
        };

        let delete = Delete {
            txn_id: 9,
            table: "users".to_string(),
            row_id,
            values,
        };

        assert_eq!(roundtrip(insert.clone()), insert);
        assert_eq!(roundtrip(update.clone()), update);
        assert_eq!(roundtrip(delete.clone()), delete);
    }
}
