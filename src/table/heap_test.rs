use super::heap::{deserialize_schema, serialize_schema, HeapTable};
use crate::types::{Column, DataType, Schema, Value};
use tempfile::NamedTempFile;

fn create_test_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Integer),
        Column::new("name", DataType::String),
        Column::new("age", DataType::Integer),
    ])
}

#[test]
fn test_create_table() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = create_test_schema();

    let table = HeapTable::create("users", schema, temp_file.path(), 10);
    assert!(table.is_ok());

    let table = table.unwrap();
    assert_eq!(table.name(), "users");
    assert_eq!(table.schema().column_count(), 3);
}

#[test]
fn test_insert_and_get() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = create_test_schema();
    let mut table = HeapTable::create("users", schema, temp_file.path(), 10).unwrap();

    let row = vec![
        Value::Integer(1),
        Value::String("Alice".to_string()),
        Value::Integer(30),
    ];

    let row_id = table.insert(&row).unwrap();
    let retrieved = table.get(row_id).unwrap();

    assert_eq!(retrieved, row);
}

#[test]
fn test_insert_multiple_rows() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = create_test_schema();
    let mut table = HeapTable::create("users", schema, temp_file.path(), 10).unwrap();

    let rows = vec![
        vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
            Value::Integer(30),
        ],
        vec![
            Value::Integer(2),
            Value::String("Bob".to_string()),
            Value::Integer(25),
        ],
        vec![
            Value::Integer(3),
            Value::String("Charlie".to_string()),
            Value::Integer(35),
        ],
    ];

    let mut row_ids = Vec::new();
    for row in &rows {
        row_ids.push(table.insert(row).unwrap());
    }

    for (i, row_id) in row_ids.iter().enumerate() {
        let retrieved = table.get(*row_id).unwrap();
        assert_eq!(retrieved, rows[i]);
    }
}

#[test]
fn test_schema_validation() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = create_test_schema();
    let mut table = HeapTable::create("users", schema, temp_file.path(), 10).unwrap();

    // Wrong number of columns
    let bad_row = vec![Value::Integer(1), Value::String("Alice".to_string())];
    assert!(table.insert(&bad_row).is_err());

    // Wrong type
    let bad_row = vec![
        Value::String("wrong".to_string()),
        Value::String("Alice".to_string()),
        Value::Integer(30),
    ];
    assert!(table.insert(&bad_row).is_err());
}

#[test]
fn test_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_owned();

    let row = vec![
        Value::Integer(42),
        Value::String("Test".to_string()),
        Value::Integer(100),
    ];

    let row_id = {
        let schema = create_test_schema();
        let mut table = HeapTable::create("test", schema, &path, 10).unwrap();
        let row_id = table.insert(&row).unwrap();
        table.flush().unwrap();
        row_id
    };

    // Open table again and verify data persisted
    {
        let mut table = HeapTable::open(&path, 10).unwrap();
        assert_eq!(table.name(), "test");
        assert_eq!(table.schema().column_count(), 3);

        let retrieved = table.get(row_id).unwrap();
        assert_eq!(retrieved, row);
    }
}

#[test]
fn test_multiple_pages() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = Schema::new(vec![
        Column::new("id", DataType::Integer),
        Column::new("data", DataType::String),
    ]);
    let mut table = HeapTable::create("large_table", schema, temp_file.path(), 10).unwrap();

    // Insert many rows to fill multiple pages
    let mut row_ids = Vec::new();
    for i in 0..100 {
        let row = vec![
            Value::Integer(i),
            Value::String(format!("Data row {}", i)),
        ];
        row_ids.push(table.insert(&row).unwrap());
    }

    // Verify all rows can be retrieved
    for (i, row_id) in row_ids.iter().enumerate() {
        let retrieved = table.get(*row_id).unwrap();
        assert_eq!(retrieved[0], Value::Integer(i as i64));
        assert_eq!(
            retrieved[1],
            Value::String(format!("Data row {}", i))
        );
    }
}

#[test]
fn test_schema_serialization() {
    let schema = create_test_schema();
    let serialized = serialize_schema(&schema);
    let deserialized = deserialize_schema(&serialized).unwrap();

    assert_eq!(deserialized.column_count(), schema.column_count());
    for i in 0..schema.column_count() {
        let orig = schema.column(i).unwrap();
        let deser = deserialized.column(i).unwrap();
        assert_eq!(orig.name(), deser.name());
        assert_eq!(orig.data_type(), deser.data_type());
    }
}
