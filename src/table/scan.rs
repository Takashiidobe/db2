use super::heap::{HeapTable, RowId};
use crate::serialization::RowSerializer;
use crate::storage::PageId;
use crate::types::Value;
use std::io;

/// Sequential table scanner
///
/// Iterates through all rows in a heap table in physical storage order.
/// Skips page 0 (metadata page) and only scans data pages.
pub struct TableScan<'a> {
    table: &'a mut HeapTable,
    current_page_id: PageId,
    current_slot_id: u16,
    finished: bool,
}

impl<'a> TableScan<'a> {
    /// Create a new table scanner
    pub fn new(table: &'a mut HeapTable) -> Self {
        Self {
            table,
            current_page_id: 1, // Start at page 1 (skip metadata page 0)
            current_slot_id: 0,
            finished: false,
        }
    }

    /// Get the next row from the table
    ///
    /// Returns (RowId, Vec<Value>) for each row, or None when done
    pub fn next(&mut self) -> io::Result<Option<(RowId, Vec<Value>)>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            // Try to fetch the current page
            let page = match self.table.buffer_pool_mut().fetch_page(self.current_page_id) {
                Ok(page) => page,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // No more pages
                    self.finished = true;
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };

            let num_rows = page.num_rows();

            // Check if we have more rows on this page
            if self.current_slot_id < num_rows {
                // Get the row data
                let row_data = match page.get_row(self.current_slot_id) {
                    Some(data) => data.to_vec(),
                    None => {
                        self.table
                            .buffer_pool_mut()
                            .unpin_page(self.current_page_id, false);
                        self.current_slot_id += 1;
                        continue;
                    }
                };

                let row_id = RowId::new(self.current_page_id, self.current_slot_id);

                self.table
                    .buffer_pool_mut()
                    .unpin_page(self.current_page_id, false);

                // Deserialize the row
                let values = RowSerializer::deserialize(&row_data, self.table.schema())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

                // Move to next slot
                self.current_slot_id += 1;

                return Ok(Some((row_id, values)));
            } else {
                // No more rows on this page, move to next page
                self.table
                    .buffer_pool_mut()
                    .unpin_page(self.current_page_id, false);

                self.current_page_id += 1;
                self.current_slot_id = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Column, DataType, Schema};
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
}
