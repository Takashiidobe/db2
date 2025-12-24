use super::page::{PAGE_SIZE, Page, PageId, PageType};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

/// Manages disk I/O for pages
///
/// Handles reading and writing fixed-size pages to/from a database file.
pub struct DiskManager {
    file: File,
}

impl DiskManager {
    /// Open or create a database file
    ///
    /// # Errors
    /// Returns error if file cannot be opened or created
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        Ok(Self { file })
    }

    /// Read a page from disk
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to read
    ///
    /// # Errors
    /// Returns error if:
    /// - Seek fails
    /// - Read fails
    /// - Page data is invalid
    pub fn read_page(&mut self, page_id: PageId) -> io::Result<Page> {
        let offset = (page_id as u64) * (PAGE_SIZE as u64);
        self.file.seek(SeekFrom::Start(offset))?;

        let mut buffer = vec![0u8; PAGE_SIZE];
        self.file.read_exact(&mut buffer)?;

        Page::from_bytes(&buffer).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Write a page to disk
    ///
    /// # Arguments
    /// * `page` - Page to write
    ///
    /// # Errors
    /// Returns error if:
    /// - Seek fails
    /// - Write fails
    pub fn write_page(&mut self, page: &Page) -> io::Result<()> {
        let page_id = page.page_id();
        let offset = (page_id as u64) * (PAGE_SIZE as u64);

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(page.to_bytes())?;
        self.file.sync_data()?;

        Ok(())
    }

    /// Allocate a new page on disk
    ///
    /// Returns the ID of the newly allocated page
    ///
    /// # Errors
    /// Returns error if file metadata cannot be read or page cannot be written
    pub fn allocate_page(&mut self, page_type: PageType) -> io::Result<PageId> {
        let file_len = self.file.metadata()?.len();
        let page_id = (file_len / PAGE_SIZE as u64) as PageId;

        let page = Page::new(page_id, page_type);
        self.write_page(&page)?;

        Ok(page_id)
    }

    /// Get the total number of pages in the file
    pub fn num_pages(&mut self) -> io::Result<u32> {
        let file_len = self.file.metadata()?.len();
        Ok((file_len / PAGE_SIZE as u64) as u32)
    }

    /// Flush all writes to disk
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_open_disk_manager() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::open(temp_file.path());
        assert!(dm.is_ok());
    }

    #[test]
    fn test_allocate_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::open(temp_file.path()).unwrap();

        let page_id = dm.allocate_page(PageType::Heap).unwrap();
        assert_eq!(page_id, 0);

        let page_id2 = dm.allocate_page(PageType::Heap).unwrap();
        assert_eq!(page_id2, 1);
    }

    #[test]
    fn test_write_and_read_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::open(temp_file.path()).unwrap();

        // Create a page with some data
        let mut page = Page::new(0, PageType::Heap);
        page.add_row(b"Hello, World!").unwrap();
        page.add_row(b"Test data").unwrap();

        // Write the page
        dm.write_page(&page).unwrap();

        // Read it back
        let read_page = dm.read_page(0).unwrap();

        assert_eq!(read_page.page_id(), 0);
        assert_eq!(read_page.page_type(), PageType::Heap);
        assert_eq!(read_page.num_rows(), 2);
        assert_eq!(read_page.get_row(0).unwrap(), b"Hello, World!");
        assert_eq!(read_page.get_row(1).unwrap(), b"Test data");
    }

    #[test]
    fn test_multiple_pages() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::open(temp_file.path()).unwrap();

        // Write several pages
        for i in 0..5 {
            let mut page = Page::new(i, PageType::Heap);
            page.add_row(format!("Page {}", i).as_bytes()).unwrap();
            dm.write_page(&page).unwrap();
        }

        // Read them back in different order
        for i in [2, 0, 4, 1, 3] {
            let page = dm.read_page(i).unwrap();
            assert_eq!(page.page_id(), i);
            assert_eq!(page.get_row(0).unwrap(), format!("Page {}", i).as_bytes());
        }
    }

    #[test]
    fn test_num_pages() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::open(temp_file.path()).unwrap();

        assert_eq!(dm.num_pages().unwrap(), 0);

        dm.allocate_page(PageType::Heap).unwrap();
        assert_eq!(dm.num_pages().unwrap(), 1);

        dm.allocate_page(PageType::Heap).unwrap();
        dm.allocate_page(PageType::Heap).unwrap();
        assert_eq!(dm.num_pages().unwrap(), 3);
    }

    #[test]
    fn test_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_owned();

        // Write data in first session
        {
            let mut dm = DiskManager::open(&path).unwrap();
            let mut page = Page::new(0, PageType::Heap);
            page.add_row(b"Persistent data").unwrap();
            dm.write_page(&page).unwrap();
        }

        // Read data in second session
        {
            let mut dm = DiskManager::open(&path).unwrap();
            let page = dm.read_page(0).unwrap();
            assert_eq!(page.get_row(0).unwrap(), b"Persistent data");
        }
    }

    #[test]
    fn test_overwrite_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut dm = DiskManager::open(temp_file.path()).unwrap();

        // Write initial data
        let mut page = Page::new(0, PageType::Heap);
        page.add_row(b"Original").unwrap();
        dm.write_page(&page).unwrap();

        // Overwrite with new data
        let mut page2 = Page::new(0, PageType::Heap);
        page2.add_row(b"Updated").unwrap();
        dm.write_page(&page2).unwrap();

        // Read back and verify
        let read_page = dm.read_page(0).unwrap();
        assert_eq!(read_page.num_rows(), 1);
        assert_eq!(read_page.get_row(0).unwrap(), b"Updated");
    }
}
