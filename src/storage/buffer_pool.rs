use super::file::DiskManager;
use super::page::{Page, PageId, PageType};
use std::collections::HashMap;
use std::io;
use std::path::Path;

/// Frame ID in the buffer pool
type FrameId = usize;

/// Buffer pool entry
struct Frame {
    page: Page,
    is_dirty: bool,
    pin_count: usize,
}

/// Buffer pool with LRU eviction policy
///
/// Manages a fixed-size cache of pages in memory with dirty tracking.
/// Pages are evicted using LRU (Least Recently Used) policy.
pub struct BufferPool {
    /// Storage for page frames
    frames: Vec<Option<Frame>>,
    /// Maps page_id to frame_id
    page_table: HashMap<PageId, FrameId>,
    /// LRU tracking: list of frame_ids in order of use (most recent at back)
    lru_list: Vec<FrameId>,
    /// Disk manager for I/O
    disk_manager: DiskManager,
}

impl BufferPool {
    /// Create a new buffer pool
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of pages to cache
    /// * `db_path` - Path to the database file
    ///
    /// # Errors
    /// Returns error if database file cannot be opened
    pub fn new(capacity: usize, db_path: impl AsRef<Path>) -> io::Result<Self> {
        let disk_manager = DiskManager::open(db_path)?;
        let frames = (0..capacity).map(|_| None).collect();

        Ok(Self {
            frames,
            page_table: HashMap::new(),
            lru_list: Vec::new(),
            disk_manager,
        })
    }

    /// Fetch a page from the buffer pool
    ///
    /// If the page is not in the pool, it's loaded from disk.
    /// The page is pinned and marked as recently used.
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to fetch
    ///
    /// # Returns
    /// Mutable reference to the page
    ///
    /// # Errors
    /// Returns error if:
    /// - All frames are pinned (cannot evict)
    /// - Disk I/O fails
    pub fn fetch_page(&mut self, page_id: PageId) -> io::Result<&mut Page> {
        // Check if page is already in buffer pool
        if let Some(&frame_id) = self.page_table.get(&page_id) {
            self.mark_recently_used(frame_id);
            let frame = self.frames[frame_id].as_mut().unwrap();
            frame.pin_count += 1;
            return Ok(&mut frame.page);
        }

        // Need to load from disk - find a frame
        let frame_id = self.find_victim_frame()?;

        // Load page from disk
        let page = self.disk_manager.read_page(page_id)?;

        // Insert into frame
        self.frames[frame_id] = Some(Frame {
            page,
            is_dirty: false,
            pin_count: 1,
        });

        self.page_table.insert(page_id, frame_id);
        self.mark_recently_used(frame_id);

        Ok(&mut self.frames[frame_id].as_mut().unwrap().page)
    }

    /// Create a new page
    ///
    /// Allocates a new page on disk and loads it into the buffer pool.
    ///
    /// # Arguments
    /// * `page_type` - Type of page to create
    ///
    /// # Returns
    /// Mutable reference to the newly created page
    ///
    /// # Errors
    /// Returns error if allocation or fetch fails
    pub fn new_page(&mut self, page_type: PageType) -> io::Result<&mut Page> {
        let page_id = self.disk_manager.allocate_page(page_type)?;
        self.fetch_page(page_id)
    }

    /// Unpin a page
    ///
    /// Decreases the pin count. When pin count reaches 0, the page can be evicted.
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to unpin
    /// * `is_dirty` - Whether the page was modified
    pub fn unpin_page(&mut self, page_id: PageId, is_dirty: bool) {
        if let Some(&frame_id) = self.page_table.get(&page_id)
            && let Some(frame) = &mut self.frames[frame_id]
        {
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
            if is_dirty {
                frame.is_dirty = true;
            }
        }
    }

    /// Flush a specific page to disk
    ///
    /// # Arguments
    /// * `page_id` - ID of the page to flush
    ///
    /// # Errors
    /// Returns error if:
    /// - Page is not in buffer pool
    /// - Disk write fails
    pub fn flush_page(&mut self, page_id: PageId) -> io::Result<()> {
        let frame_id = self
            .page_table
            .get(&page_id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Page not in buffer pool"))?;

        if let Some(frame) = &mut self.frames[*frame_id]
            && frame.is_dirty
        {
            self.disk_manager.write_page(&frame.page)?;
            frame.is_dirty = false;
        }

        Ok(())
    }

    /// Flush all dirty pages to disk
    ///
    /// # Errors
    /// Returns error if any disk write fails
    pub fn flush_all(&mut self) -> io::Result<()> {
        for frame in self.frames.iter_mut().flatten() {
            if frame.is_dirty {
                self.disk_manager.write_page(&frame.page)?;
                frame.is_dirty = false;
            }
        }
        self.disk_manager.flush()
    }

    /// Find a victim frame for eviction using LRU policy
    ///
    /// # Returns
    /// Frame ID of the victim frame
    ///
    /// # Errors
    /// Returns error if all frames are pinned
    fn find_victim_frame(&mut self) -> io::Result<FrameId> {
        // First, try to find an empty frame
        for (frame_id, frame) in self.frames.iter().enumerate() {
            if frame.is_none() {
                return Ok(frame_id);
            }
        }

        // No empty frames - use LRU to find victim
        // Iterate from front (least recently used) to back
        for &frame_id in &self.lru_list {
            if let Some(frame) = &self.frames[frame_id]
                && frame.pin_count == 0
            {
                // Found a victim - evict it
                return self.evict_frame(frame_id);
            }
        }

        Err(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "All frames are pinned - cannot evict",
        ))
    }

    /// Evict a frame
    ///
    /// If the frame is dirty, writes it to disk first.
    ///
    /// # Arguments
    /// * `frame_id` - Frame to evict
    ///
    /// # Returns
    /// The frame_id (now available for reuse)
    fn evict_frame(&mut self, frame_id: FrameId) -> io::Result<FrameId> {
        if let Some(frame) = &self.frames[frame_id] {
            // Write to disk if dirty
            if frame.is_dirty {
                self.disk_manager.write_page(&frame.page)?;
            }

            // Remove from page table
            self.page_table.remove(&frame.page.page_id());
        }

        // Clear the frame
        self.frames[frame_id] = None;

        // Remove from LRU list
        self.lru_list.retain(|&id| id != frame_id);

        Ok(frame_id)
    }

    /// Mark a frame as recently used (move to back of LRU list)
    fn mark_recently_used(&mut self, frame_id: FrameId) {
        // Remove from current position
        self.lru_list.retain(|&id| id != frame_id);
        // Add to back (most recent)
        self.lru_list.push(frame_id);
    }

    /// Get the number of pages in the buffer pool
    pub fn size(&self) -> usize {
        self.page_table.len()
    }
}
