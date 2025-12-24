use std::io::{self, Cursor};

/// Page size: 8KB
pub const PAGE_SIZE: usize = 8192;

/// Page header size:
/// - 2 bytes: page_type
/// - 4 bytes: page_id
/// - 2 bytes: num_rows
/// - 2 bytes: free_space_offset
const PAGE_HEADER_SIZE: usize = 10;

/// Size of each slot directory entry (offset + length)
const SLOT_ENTRY_SIZE: usize = 4;

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
                write!(f, "Invalid page size: expected {}, found {}", expected, found)
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

        cursor.write_all(&(self.page_type as u16).to_le_bytes()).unwrap();
        cursor.write_all(&self.page_id.to_le_bytes()).unwrap();
        cursor.write_all(&self.num_rows.to_le_bytes()).unwrap();
        cursor.write_all(&self.free_space_offset.to_le_bytes()).unwrap();
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
    fn free_space(&self) -> usize {
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
        let entry = self.read_slot_entry(slot_id)
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
    /// Returns a reference to the row data, or None if the slot is invalid
    pub fn get_row(&self, slot_id: SlotId) -> Option<&[u8]> {
        let entry = self.read_slot_entry(slot_id)?;
        let start = entry.offset as usize;
        let end = start + entry.length as usize;
        Some(&self.data[start..end])
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page() {
        let page = Page::new(1, PageType::Heap);
        assert_eq!(page.page_id(), 1);
        assert_eq!(page.page_type(), PageType::Heap);
        assert_eq!(page.num_rows(), 0);
        assert_eq!(page.free_space(), PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    #[test]
    fn test_add_and_get_row() {
        let mut page = Page::new(1, PageType::Heap);
        let row_data = b"Hello, World!";

        let slot_id = page.add_row(row_data).unwrap();
        assert_eq!(slot_id, 0);
        assert_eq!(page.num_rows(), 1);

        let retrieved = page.get_row(slot_id).unwrap();
        assert_eq!(retrieved, row_data);
    }

    #[test]
    fn test_add_multiple_rows() {
        let mut page = Page::new(1, PageType::Heap);

        let row1 = b"First row";
        let row2 = b"Second row";
        let row3 = b"Third row";

        let slot1 = page.add_row(row1).unwrap();
        let slot2 = page.add_row(row2).unwrap();
        let slot3 = page.add_row(row3).unwrap();

        assert_eq!(slot1, 0);
        assert_eq!(slot2, 1);
        assert_eq!(slot3, 2);
        assert_eq!(page.num_rows(), 3);

        assert_eq!(page.get_row(slot1).unwrap(), row1);
        assert_eq!(page.get_row(slot2).unwrap(), row2);
        assert_eq!(page.get_row(slot3).unwrap(), row3);
    }

    #[test]
    fn test_page_serialization() {
        let mut page = Page::new(42, PageType::BTreeLeaf);
        page.add_row(b"Row 1").unwrap();
        page.add_row(b"Row 2").unwrap();

        let bytes = page.to_bytes();
        let deserialized = Page::from_bytes(bytes).unwrap();

        assert_eq!(deserialized.page_id(), 42);
        assert_eq!(deserialized.page_type(), PageType::BTreeLeaf);
        assert_eq!(deserialized.num_rows(), 2);
        assert_eq!(deserialized.get_row(0).unwrap(), b"Row 1");
        assert_eq!(deserialized.get_row(1).unwrap(), b"Row 2");
    }

    #[test]
    fn test_page_full() {
        let mut page = Page::new(1, PageType::Heap);

        // Create a large row that will fill most of the page
        let large_row = vec![0u8; PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE - 10];
        page.add_row(&large_row).unwrap();

        // Try to add another row that doesn't fit
        let another_row = vec![0u8; 100];
        let result = page.add_row(&another_row);

        assert!(matches!(result, Err(PageError::PageFull)));
    }

    #[test]
    fn test_invalid_slot_id() {
        let page = Page::new(1, PageType::Heap);
        assert!(page.get_row(0).is_none());
        assert!(page.get_row(999).is_none());
    }

    #[test]
    fn test_free_space_calculation() {
        let mut page = Page::new(1, PageType::Heap);
        let initial_free = page.free_space();
        assert_eq!(initial_free, PAGE_SIZE - PAGE_HEADER_SIZE);

        let row = b"Test data";
        page.add_row(row).unwrap();

        let free_after = page.free_space();
        assert_eq!(free_after, initial_free - row.len() - SLOT_ENTRY_SIZE);
    }

    #[test]
    fn test_different_page_types() {
        let heap_page = Page::new(1, PageType::Heap);
        let btree_internal = Page::new(2, PageType::BTreeInternal);
        let btree_leaf = Page::new(3, PageType::BTreeLeaf);

        assert_eq!(heap_page.page_type(), PageType::Heap);
        assert_eq!(btree_internal.page_type(), PageType::BTreeInternal);
        assert_eq!(btree_leaf.page_type(), PageType::BTreeLeaf);
    }

    #[test]
    fn test_empty_row() {
        let mut page = Page::new(1, PageType::Heap);
        let slot_id = page.add_row(b"").unwrap();
        assert_eq!(page.get_row(slot_id).unwrap(), b"");
    }

    #[test]
    fn test_max_rows() {
        let mut page = Page::new(1, PageType::Heap);
        let mut count = 0;

        // Add tiny rows until we run out of space
        loop {
            match page.add_row(b"x") {
                Ok(_) => count += 1,
                Err(PageError::PageFull) => break,
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
        }

        assert!(count > 0);
        assert_eq!(page.num_rows() as usize, count);
    }

    #[test]
    fn test_round_trip_with_various_sizes() {
        let mut page = Page::new(10, PageType::Heap);

        let rows = vec![
            vec![1u8; 10],
            vec![2u8; 100],
            vec![3u8; 1000],
            vec![4u8; 50],
        ];

        for row in &rows {
            page.add_row(row).unwrap();
        }

        let bytes = page.to_bytes();
        let restored = Page::from_bytes(bytes).unwrap();

        for (i, expected_row) in rows.iter().enumerate() {
            let actual_row = restored.get_row(i as SlotId).unwrap();
            assert_eq!(actual_row, expected_row.as_slice());
        }
    }

    #[test]
    fn test_invalid_page_size() {
        let short_bytes = vec![0u8; 100];
        let result = Page::from_bytes(&short_bytes);
        assert!(matches!(
            result,
            Err(PageError::InvalidPageSize { expected: PAGE_SIZE, found: 100 })
        ));
    }
}
