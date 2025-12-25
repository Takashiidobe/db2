use crate::serialization::RowSerializer;
use crate::storage::{BufferPool, PageError, PageId, PageType, SlotId};
use crate::types::{Schema, Value};
use std::io;
use std::path::Path;

/// Row identifier (page_id, slot_id)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowId {
    page_id: PageId,
    slot_id: SlotId,
}

impl RowId {
    /// Create a new RowId
    pub fn new(page_id: PageId, slot_id: SlotId) -> Self {
        Self { page_id, slot_id }
    }

    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get the slot ID
    pub fn slot_id(&self) -> SlotId {
        self.slot_id
    }
}

/// Heap table - unordered collection of rows stored in pages
///
/// The first page (page 0) is reserved for metadata (table name and schema).
/// Data pages are allocated as needed starting from page 1.
pub struct HeapTable {
    name: String,
    schema: Schema,
    buffer_pool: BufferPool,
    /// ID of the last data page (for quick appends)
    last_page_id: Option<PageId>,
}

impl HeapTable {
    /// Create a new heap table
    ///
    /// # Arguments
    /// * `name` - Table name
    /// * `schema` - Table schema
    /// * `db_path` - Path to database file
    /// * `buffer_pool_size` - Size of buffer pool
    ///
    /// # Errors
    /// Returns error if buffer pool creation or metadata page write fails
    pub fn create(
        name: impl Into<String>,
        schema: Schema,
        db_path: impl AsRef<Path>,
        buffer_pool_size: usize,
    ) -> io::Result<Self> {
        let name = name.into();
        let mut buffer_pool = BufferPool::new(buffer_pool_size, db_path)?;

        // Create metadata page (page 0)
        let metadata_page = buffer_pool.new_page(PageType::Heap)?;
        let metadata = format!("TABLE:{}\n", name);
        metadata_page.add_row(metadata.as_bytes())?;

        // Store schema information
        let schema_data = serialize_schema(&schema);
        metadata_page.add_row(&schema_data)?;

        buffer_pool.unpin_page(0, true);
        buffer_pool.flush_page(0)?;

        Ok(Self {
            name,
            schema,
            buffer_pool,
            last_page_id: None,
        })
    }

    /// Open an existing heap table
    ///
    /// # Arguments
    /// * `db_path` - Path to database file
    /// * `buffer_pool_size` - Size of buffer pool
    ///
    /// # Errors
    /// Returns error if buffer pool creation or metadata read fails
    pub fn open(db_path: impl AsRef<Path>, buffer_pool_size: usize) -> io::Result<Self> {
        let mut buffer_pool = BufferPool::new(buffer_pool_size, db_path)?;

        // Read metadata page
        let metadata_page = buffer_pool.fetch_page(0)?;

        // Read table name
        let name_bytes = metadata_page.get_row(0).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Missing table name in metadata")
        })?;
        let name_str = std::str::from_utf8(name_bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let name = name_str
            .strip_prefix("TABLE:")
            .and_then(|s| s.strip_suffix('\n'))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid table name format"))?
            .to_string();

        // Read schema
        let schema_bytes = metadata_page.get_row(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "Missing schema in metadata")
        })?;
        let schema = deserialize_schema(schema_bytes)?;

        buffer_pool.unpin_page(0, false);

        Ok(Self {
            name,
            schema,
            buffer_pool,
            last_page_id: None,
        })
    }

    /// Get the table name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the table schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Insert a row into the table
    ///
    /// # Arguments
    /// * `row` - Row values to insert
    ///
    /// # Returns
    /// RowId of the inserted row
    ///
    /// # Errors
    /// Returns error if:
    /// - Row doesn't match schema
    /// - Serialization fails
    /// - Page operations fail
    pub fn insert(&mut self, row: &[Value]) -> io::Result<RowId> {
        // Validate row against schema
        self.schema.validate_row(row).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Schema validation failed: {}", e),
            )
        })?;

        // Serialize the row
        let row_data = RowSerializer::serialize(row, Some(&self.schema))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Try to insert into the last page first
        if let Some(last_page_id) = self.last_page_id {
            let page = self.buffer_pool.fetch_page(last_page_id)?;
            match page.add_row(&row_data) {
                Ok(slot_id) => {
                    self.buffer_pool.unpin_page(last_page_id, true);
                    return Ok(RowId::new(last_page_id, slot_id));
                }
                Err(_) => {
                    // Page is full, unpin and create a new one
                    self.buffer_pool.unpin_page(last_page_id, false);
                }
            }
        }

        // Create a new data page
        let page = self.buffer_pool.new_page(PageType::Heap)?;
        let page_id = page.page_id();
        let slot_id = page.add_row(&row_data)?;

        self.buffer_pool.unpin_page(page_id, true);
        self.last_page_id = Some(page_id);

        Ok(RowId::new(page_id, slot_id))
    }

    /// Get a row by its RowId
    ///
    /// # Arguments
    /// * `row_id` - ID of the row to retrieve
    ///
    /// # Returns
    /// Vector of values representing the row
    ///
    /// # Errors
    /// Returns error if:
    /// - Page cannot be fetched
    /// - Row doesn't exist
    /// - Deserialization fails
    pub fn get(&mut self, row_id: RowId) -> io::Result<Vec<Value>> {
        let page = self.buffer_pool.fetch_page(row_id.page_id)?;

        let row_data = page.get_row(row_id.slot_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Row not found: {:?}", row_id),
            )
        })?;

        let values = RowSerializer::deserialize(row_data, &self.schema)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.buffer_pool.unpin_page(row_id.page_id, false);

        Ok(values)
    }

    /// Delete a row from the table
    ///
    /// # Arguments
    /// * `row_id` - ID of the row to delete
    ///
    /// # Errors
    /// Returns error if:
    /// - Page cannot be fetched
    /// - Row doesn't exist
    pub fn delete(&mut self, row_id: RowId) -> io::Result<()> {
        let page = self.buffer_pool.fetch_page(row_id.page_id)?;

        page.delete_row(row_id.slot_id)
            .map_err(|e| io::Error::new(io::ErrorKind::NotFound, e))?;

        self.buffer_pool.unpin_page(row_id.page_id, true);

        Ok(())
    }

    /// Update an existing row.
    ///
    /// Attempts to update in place; if the new row is larger than the existing slot,
    /// falls back to deleting and reinserting the row (which may change the RowId).
    pub fn update(&mut self, row_id: RowId, new_row: &[Value]) -> io::Result<RowId> {
        // Validate and serialize the new row
        self.schema.validate_row(new_row).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Schema validation failed: {}", e),
            )
        })?;

        let row_data = RowSerializer::serialize(new_row, Some(&self.schema))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Try to update in place first
        let page = self.buffer_pool.fetch_page(row_id.page_id)?;
        match page.update_row(row_id.slot_id, &row_data) {
            Ok(()) => {
                self.buffer_pool.unpin_page(row_id.page_id, true);
                return Ok(row_id);
            }
            Err(PageError::PageFull) => {
                // Fall through to move the row
                self.buffer_pool.unpin_page(row_id.page_id, false);
            }
            Err(e) => {
                self.buffer_pool.unpin_page(row_id.page_id, false);
                return Err(io::Error::from(e));
            }
        }

        // If we couldn't update in place, delete and re-insert the row
        self.delete(row_id)?;
        let new_row_id = self.insert(new_row)?;
        Ok(new_row_id)
    }

    /// Flush all dirty pages to disk
    pub fn flush(&mut self) -> io::Result<()> {
        self.buffer_pool.flush_all()
    }

    /// Get a reference to the buffer pool (for scanning)
    pub(crate) fn buffer_pool_mut(&mut self) -> &mut BufferPool {
        &mut self.buffer_pool
    }
}

/// Serialize schema to bytes
pub(crate) fn serialize_schema(schema: &Schema) -> Vec<u8> {
    use crate::serialization::codec;
    let mut buf = Vec::new();

    // Write column count
    codec::write_u16(&mut buf, schema.column_count() as u16).unwrap();

    // Write each column
    for column in schema.columns() {
        codec::write_string(&mut buf, column.name()).unwrap();
        let type_byte = match column.data_type() {
            crate::types::DataType::Integer => 0u8,
            crate::types::DataType::String => 1u8,
            crate::types::DataType::Boolean => 2u8,
            crate::types::DataType::Unsigned => 3u8,
            crate::types::DataType::Float => 4u8,
        };
        codec::write_u8(&mut buf, type_byte).unwrap();
    }

    buf
}

/// Deserialize schema from bytes
pub(crate) fn deserialize_schema(bytes: &[u8]) -> io::Result<Schema> {
    use crate::serialization::codec;
    use crate::types::{Column, DataType};
    use std::io::Cursor;

    let mut cursor = Cursor::new(bytes);

    // Read column count
    let column_count = codec::read_u16(&mut cursor)? as usize;

    // Read each column
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        let name = codec::read_string(&mut cursor)?;
        let type_byte = codec::read_u8(&mut cursor)?;
        let data_type = match type_byte {
            0 => DataType::Integer,
            1 => DataType::String,
            2 => DataType::Boolean,
            3 => DataType::Unsigned,
            4 => DataType::Float,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid data type: {}", type_byte),
                ));
            }
        };
        columns.push(Column::new(name, data_type));
    }

    Ok(Schema::new(columns))
}
