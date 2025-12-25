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
