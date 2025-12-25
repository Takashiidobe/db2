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
