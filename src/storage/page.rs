use std::io::{self, Cursor};

/// Page size: 8KB
pub const PAGE_SIZE: usize = 8192;

/// Page header size:
/// - 2 bytes: page_type
/// - 4 bytes: page_id
/// - 2 bytes: num_rows
/// - 2 bytes: free_space_offset
pub(super) const PAGE_HEADER_SIZE: usize = 10;

/// Size of each slot directory entry (offset + length)
pub(super) const SLOT_ENTRY_SIZE: usize = 4;

/// Page ID type
pub type PageId = u32;

/// Slot ID type (index into the slot directory)
pub type SlotId = u16;

/// Types of pages in the database
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum PageType {
    Heap = 0,
    BTreeInternal = 1,
    BTreeLeaf = 2,
}

impl PageType {
    fn from_u16(value: u16) -> Result<Self, PageError> {
        match value {
            0 => Ok(PageType::Heap),
            1 => Ok(PageType::BTreeInternal),
            2 => Ok(PageType::BTreeLeaf),
            _ => Err(PageError::InvalidPageType(value)),
        }
    }
}

/// Errors that can occur during page operations
#[derive(Debug)]
pub enum PageError {
    IoError(io::Error),
    InvalidPageType(u16),
    PageFull,
    InvalidSlotId(SlotId),
    InvalidPageSize { expected: usize, found: usize },
}

impl From<io::Error> for PageError {
    fn from(err: io::Error) -> Self {
        PageError::IoError(err)
    }
}

impl std::fmt::Display for PageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageError::IoError(e) => write!(f, "I/O error: {}", e),
            PageError::InvalidPageType(t) => write!(f, "Invalid page type: {}", t),
            PageError::PageFull => write!(f, "Page is full"),
            PageError::InvalidSlotId(id) => write!(f, "Invalid slot ID: {}", id),
            PageError::InvalidPageSize { expected, found } => {
                write!(
                    f,
                    "Invalid page size: expected {}, found {}",
                    expected, found
                )
            }
        }
    }
}

impl std::error::Error for PageError {}

impl From<PageError> for io::Error {
    fn from(err: PageError) -> Self {
        match err {
            PageError::IoError(e) => e,
            other => io::Error::other(other),
        }
    }
}

/// Slot directory entry
#[derive(Debug, Clone, Copy)]
struct SlotEntry {
    offset: u16,
    length: u16,
}

/// A fixed-size page (8KB) that stores rows
///
/// Page Layout:
/// ```text
/// [2 bytes: page_type]
/// [4 bytes: page_id]
/// [2 bytes: num_rows]
/// [2 bytes: free_space_offset]
/// [slot_directory: array of (offset: u16, length: u16)]
/// [...free space...]
/// [rows stored bottom-up]
/// ```
#[derive(Clone)]
pub struct Page {
    page_type: PageType,
    page_id: PageId,
    num_rows: u16,
    free_space_offset: u16,
    data: [u8; PAGE_SIZE],
}

impl Page {
    /// Create a new empty page
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let mut page = Self {
            page_type,
            page_id,
            num_rows: 0,
            free_space_offset: PAGE_SIZE as u16,
            data: [0; PAGE_SIZE],
        };
        page.write_header();
        page
    }

    /// Get the page ID
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Get the page type
    pub fn page_type(&self) -> PageType {
        self.page_type
    }

    /// Get the number of rows in the page
    pub fn num_rows(&self) -> u16 {
        self.num_rows
    }

    /// Write the page header to the data array
    fn write_header(&mut self) {
        let mut cursor = Cursor::new(&mut self.data[..]);
        use std::io::Write;

        cursor
            .write_all(&(self.page_type as u16).to_le_bytes())
            .unwrap();
        cursor.write_all(&self.page_id.to_le_bytes()).unwrap();
        cursor.write_all(&self.num_rows.to_le_bytes()).unwrap();
        cursor
            .write_all(&self.free_space_offset.to_le_bytes())
            .unwrap();
    }

    /// Read the page header from the data array
    fn read_header(&mut self) {
        use std::io::Read;
        let mut cursor = Cursor::new(&self.data[..]);

        let mut buf = [0u8; 2];
        cursor.read_exact(&mut buf).unwrap();
        let page_type_value = u16::from_le_bytes(buf);
        self.page_type = PageType::from_u16(page_type_value).unwrap();

        let mut buf = [0u8; 4];
        cursor.read_exact(&mut buf).unwrap();
        self.page_id = u32::from_le_bytes(buf);

        let mut buf = [0u8; 2];
        cursor.read_exact(&mut buf).unwrap();
        self.num_rows = u16::from_le_bytes(buf);

        let mut buf = [0u8; 2];
        cursor.read_exact(&mut buf).unwrap();
        self.free_space_offset = u16::from_le_bytes(buf);
    }

    /// Get the offset for a slot entry in the directory
    fn slot_entry_offset(&self, slot_id: SlotId) -> usize {
        PAGE_HEADER_SIZE + (slot_id as usize) * SLOT_ENTRY_SIZE
    }

    /// Read a slot entry from the directory
    fn read_slot_entry(&self, slot_id: SlotId) -> Option<SlotEntry> {
        if slot_id >= self.num_rows {
            return None;
        }

        let offset = self.slot_entry_offset(slot_id);
        let offset_bytes = [self.data[offset], self.data[offset + 1]];
        let length_bytes = [self.data[offset + 2], self.data[offset + 3]];

        Some(SlotEntry {
            offset: u16::from_le_bytes(offset_bytes),
            length: u16::from_le_bytes(length_bytes),
        })
    }

    /// Write a slot entry to the directory
    fn write_slot_entry(&mut self, slot_id: SlotId, entry: SlotEntry) {
        let offset = self.slot_entry_offset(slot_id);
        self.data[offset..offset + 2].copy_from_slice(&entry.offset.to_le_bytes());
        self.data[offset + 2..offset + 4].copy_from_slice(&entry.length.to_le_bytes());
    }

    /// Calculate available free space in the page
    pub(super) fn free_space(&self) -> usize {
        let directory_end = PAGE_HEADER_SIZE + (self.num_rows as usize) * SLOT_ENTRY_SIZE;
        let data_start = self.free_space_offset as usize;

        data_start.saturating_sub(directory_end)
    }

    /// Add a row to the page
    ///
    /// Returns the SlotId where the row was stored
    ///
    /// # Errors
    /// Returns `PageError::PageFull` if there's not enough space
    pub fn add_row(&mut self, row_data: &[u8]) -> Result<SlotId, PageError> {
        let row_length = row_data.len();

        // Check if there's enough space (need space for slot entry + row data)
        let required_space = SLOT_ENTRY_SIZE + row_length;
        if self.free_space() < required_space {
            return Err(PageError::PageFull);
        }

        // Allocate space for the row (from bottom up)
        let new_offset = self.free_space_offset as usize - row_length;
        self.data[new_offset..new_offset + row_length].copy_from_slice(row_data);

        // Add slot entry
        let slot_id = self.num_rows;
        let entry = SlotEntry {
            offset: new_offset as u16,
            length: row_length as u16,
        };
        self.write_slot_entry(slot_id, entry);

        // Update header
        self.num_rows += 1;
        self.free_space_offset = new_offset as u16;
        self.write_header();

        Ok(slot_id)
    }

    /// Update an existing row in the page
    ///
    /// The new data must fit in the space allocated for the existing row.
    /// This is designed for fixed-size row updates where the size doesn't change.
    ///
    /// # Errors
    /// Returns `PageError::InvalidSlotId` if the slot doesn't exist
    /// Returns `PageError::PageFull` if the new data is larger than the old data
    pub fn update_row(&mut self, slot_id: SlotId, row_data: &[u8]) -> Result<(), PageError> {
        let entry = self
            .read_slot_entry(slot_id)
            .ok_or(PageError::InvalidSlotId(slot_id))?;

        // Check that new data fits in the allocated space
        if row_data.len() > entry.length as usize {
            return Err(PageError::PageFull);
        }

        // Update the data in place
        let start = entry.offset as usize;
        let end = start + row_data.len();
        self.data[start..end].copy_from_slice(row_data);

        // If new data is smaller, zero out the rest
        if row_data.len() < entry.length as usize {
            let zero_start = start + row_data.len();
            let zero_end = start + entry.length as usize;
            self.data[zero_start..zero_end].fill(0);
        }

        // Update the slot entry with the new length
        let new_entry = SlotEntry {
            offset: entry.offset,
            length: row_data.len() as u16,
        };
        self.write_slot_entry(slot_id, new_entry);

        Ok(())
    }

    /// Get a row from the page by its SlotId
    ///
    /// Returns a reference to the row data, or None if the slot is invalid or deleted
    pub fn get_row(&self, slot_id: SlotId) -> Option<&[u8]> {
        let entry = self.read_slot_entry(slot_id)?;
        // Treat length 0 as deleted row
        if entry.length == 0 {
            return None;
        }
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        Some(&self.data[start..end])
    }

    /// Delete a row from the page by marking it as deleted
    ///
    /// This doesn't reclaim space, but marks the slot as deleted by setting length to 0
    ///
    /// # Errors
    /// Returns `PageError::InvalidSlotId` if the slot doesn't exist
    pub fn delete_row(&mut self, slot_id: SlotId) -> Result<(), PageError> {
        let entry = self
            .read_slot_entry(slot_id)
            .ok_or(PageError::InvalidSlotId(slot_id))?;

        // Mark as deleted by setting length to 0
        let deleted_entry = SlotEntry {
            offset: entry.offset,
            length: 0,
        };
        self.write_slot_entry(slot_id, deleted_entry);

        Ok(())
    }

    /// Serialize the page to bytes
    pub fn to_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Deserialize a page from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PageError> {
        if bytes.len() != PAGE_SIZE {
            return Err(PageError::InvalidPageSize {
                expected: PAGE_SIZE,
                found: bytes.len(),
            });
        }

        let mut data = [0u8; PAGE_SIZE];
        data.copy_from_slice(bytes);

        let mut page = Self {
            page_type: PageType::Heap,
            page_id: 0,
            num_rows: 0,
            free_space_offset: PAGE_SIZE as u16,
            data,
        };

        page.read_header();
        Ok(page)
    }
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("page_type", &self.page_type)
            .field("page_id", &self.page_id)
            .field("num_rows", &self.num_rows)
            .field("free_space_offset", &self.free_space_offset)
            .field("free_space", &self.free_space())
            .finish()
    }
}
