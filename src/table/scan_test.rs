use super::heap::HeapTable;
use super::scan::TableScan;
use crate::types::{Column, DataType, Schema, Value};
use tempfile::NamedTempFile;

#[test]
fn test_scan_empty_table() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = Schema::new(vec![
        Column::new("id", DataType::Integer),
        Column::new("name", DataType::String),
    ]);

    let mut table = HeapTable::create("test", schema, temp_file.path(), 10).unwrap();

    let mut scan = TableScan::new(&mut table);
    assert!(scan.next().unwrap().is_none());
}

#[test]
fn test_scan_single_row() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = Schema::new(vec![
        Column::new("id", DataType::Integer),
        Column::new("name", DataType::String),
    ]);

    let mut table = HeapTable::create("test", schema, temp_file.path(), 10).unwrap();

    let row = vec![Value::Integer(1), Value::String("Alice".to_string())];
    table.insert(&row).unwrap();

    let mut scan = TableScan::new(&mut table);
    let result = scan.next().unwrap();
    assert!(result.is_some());

    let (row_id, values) = result.unwrap();
    assert_eq!(values, row);
    assert_eq!(row_id.page_id(), 1); // First data page

    assert!(scan.next().unwrap().is_none());
}

#[test]
fn test_scan_multiple_rows() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = Schema::new(vec![
        Column::new("id", DataType::Integer),
        Column::new("name", DataType::String),
    ]);

    let mut table = HeapTable::create("test", schema, temp_file.path(), 10).unwrap();

    // Insert multiple rows
    let rows = vec![
        vec![Value::Integer(1), Value::String("Alice".to_string())],
        vec![Value::Integer(2), Value::String("Bob".to_string())],
        vec![Value::Integer(3), Value::String("Charlie".to_string())],
    ];

    for row in &rows {
        table.insert(row).unwrap();
    }

    // Scan and collect all rows
    let mut scan = TableScan::new(&mut table);
    let mut scanned_rows = Vec::new();

    while let Some((_, values)) = scan.next().unwrap() {
        scanned_rows.push(values);
    }

    assert_eq!(scanned_rows.len(), 3);
    assert_eq!(scanned_rows, rows);
}

#[test]
fn test_scan_many_rows() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = Schema::new(vec![
        Column::new("id", DataType::Integer),
        Column::new("data", DataType::String),
    ]);

    let mut table = HeapTable::create("test", schema, temp_file.path(), 10).unwrap();

    // Insert many rows
    for i in 0..50 {
        let row = vec![
            Value::Integer(i),
            Value::String(format!("Row {}", i)),
        ];
        table.insert(&row).unwrap();
    }

    // Scan and verify count
    let mut scan = TableScan::new(&mut table);
    let mut count = 0;

    while let Some((_, values)) = scan.next().unwrap() {
        assert_eq!(values[0], Value::Integer(count));
        assert_eq!(values[1], Value::String(format!("Row {}", count)));
        count += 1;
    }

    assert_eq!(count, 50);
}

#[test]
fn test_scan_multiple_pages() {
    let temp_file = NamedTempFile::new().unwrap();
    let schema = Schema::new(vec![
        Column::new("id", DataType::Integer),
        Column::new("large_data", DataType::String),
    ]);

    let mut table = HeapTable::create("test", schema, temp_file.path(), 10).unwrap();

    // Insert rows with large data to span multiple pages
    let large_string = "X".repeat(1000);
    for i in 0..20 {
        let row = vec![Value::Integer(i), Value::String(large_string.clone())];
        table.insert(&row).unwrap();
    }

    // Scan and verify all rows
    let mut scan = TableScan::new(&mut table);
    let mut count = 0;
    let mut seen_multiple_pages = false;
    let mut last_page_id = None;

    while let Some((row_id, values)) = scan.next().unwrap() {
        assert_eq!(values[0], Value::Integer(count));
        assert_eq!(values[1], Value::String(large_string.clone()));

        if let Some(last_pid) = last_page_id
            && row_id.page_id() != last_pid {
                seen_multiple_pages = true;
            }
        last_page_id = Some(row_id.page_id());

        count += 1;
    }

    assert_eq!(count, 20);
    assert!(seen_multiple_pages, "Should have spanned multiple pages");
}
