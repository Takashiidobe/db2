use super::*;
use tempfile::NamedTempFile;

#[test]
fn test_create_buffer_pool() {
    let temp_file = NamedTempFile::new().unwrap();
    let pool = BufferPool::new(10, temp_file.path());
    assert!(pool.is_ok());
}

#[test]
fn test_new_page() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut pool = BufferPool::new(10, temp_file.path()).unwrap();

    let page = pool.new_page(PageType::Heap).unwrap();
    assert_eq!(page.page_id(), 0);
    assert_eq!(page.page_type(), PageType::Heap);
}

#[test]
fn test_fetch_and_unpin() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut pool = BufferPool::new(10, temp_file.path()).unwrap();

    // Create a page
    let page = pool.new_page(PageType::Heap).unwrap();
    let page_id = page.page_id();

    // Unpin it
    pool.unpin_page(page_id, false);

    // Fetch it again
    let fetched = pool.fetch_page(page_id).unwrap();
    assert_eq!(fetched.page_id(), page_id);
}

#[test]
fn test_dirty_page_written_on_flush() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_owned();

    {
        let mut pool = BufferPool::new(10, &path).unwrap();

        // Create and modify a page
        let page = pool.new_page(PageType::Heap).unwrap();
        let page_id = page.page_id();
        page.add_row(b"Test data").unwrap();

        // Unpin as dirty
        pool.unpin_page(page_id, true);

        // Flush
        pool.flush_page(page_id).unwrap();
    }

    // Verify data persisted
    {
        let mut pool = BufferPool::new(10, &path).unwrap();
        let page = pool.fetch_page(0).unwrap();
        assert_eq!(page.get_row(0).unwrap(), b"Test data");
    }
}

#[test]
fn test_lru_eviction() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut pool = BufferPool::new(3, temp_file.path()).unwrap();

    // Create 3 pages (fills the pool)
    let page0 = pool.new_page(PageType::Heap).unwrap();
    page0.add_row(b"Page 0").unwrap();
    pool.unpin_page(0, true);

    let page1 = pool.new_page(PageType::Heap).unwrap();
    page1.add_row(b"Page 1").unwrap();
    pool.unpin_page(1, true);

    let page2 = pool.new_page(PageType::Heap).unwrap();
    page2.add_row(b"Page 2").unwrap();
    pool.unpin_page(2, true);

    assert_eq!(pool.size(), 3);

    // Access page 0 to make it more recently used
    pool.fetch_page(0).unwrap();
    pool.unpin_page(0, false);

    // Create page 3 - should evict page 1 (least recently used)
    let page3 = pool.new_page(PageType::Heap).unwrap();
    page3.add_row(b"Page 3").unwrap();
    pool.unpin_page(3, true);

    assert_eq!(pool.size(), 3);

    // Verify page 0, 2, 3 are in pool (page 1 was evicted)
    assert!(pool.fetch_page(0).is_ok());
    pool.unpin_page(0, false);

    assert!(pool.fetch_page(2).is_ok());
    pool.unpin_page(2, false);

    assert!(pool.fetch_page(3).is_ok());
    pool.unpin_page(3, false);
}

#[test]
fn test_pinned_pages_not_evicted() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut pool = BufferPool::new(2, temp_file.path()).unwrap();

    // Create 2 pages and keep them both pinned
    pool.new_page(PageType::Heap).unwrap();
    // Leave page 0 pinned (don't unpin)

    pool.new_page(PageType::Heap).unwrap();
    // Leave page 1 pinned too (don't unpin)

    // Try to create a third page - should fail (both pages are pinned, can't evict)
    let result = pool.new_page(PageType::Heap);
    assert!(result.is_err());
}

#[test]
fn test_flush_all() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_owned();

    {
        let mut pool = BufferPool::new(10, &path).unwrap();

        // Create multiple dirty pages
        for i in 0..5 {
            let page = pool.new_page(PageType::Heap).unwrap();
            page.add_row(format!("Page {}", i).as_bytes()).unwrap();
            pool.unpin_page(i, true);
        }

        // Flush all
        pool.flush_all().unwrap();
    }

    // Verify all persisted
    {
        let mut pool = BufferPool::new(10, &path).unwrap();
        for i in 0..5 {
            let page = pool.fetch_page(i).unwrap();
            assert_eq!(page.get_row(0).unwrap(), format!("Page {}", i).as_bytes());
        }
    }
}

#[test]
fn test_dirty_eviction_writes_to_disk() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut pool = BufferPool::new(2, temp_file.path()).unwrap();

    // Create page 0 and modify it
    let page0 = pool.new_page(PageType::Heap).unwrap();
    page0.add_row(b"Modified page 0").unwrap();
    pool.unpin_page(0, true); // Mark as dirty

    // Create page 1
    pool.new_page(PageType::Heap).unwrap();
    pool.unpin_page(1, false);

    // Create page 2 - should evict page 0 (and write it because it's dirty)
    pool.new_page(PageType::Heap).unwrap();
    pool.unpin_page(2, false);

    // Fetch page 0 again - should load from disk with modifications intact
    let reloaded = pool.fetch_page(0).unwrap();
    assert_eq!(reloaded.get_row(0).unwrap(), b"Modified page 0");
}

#[test]
fn test_multiple_pins() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut pool = BufferPool::new(10, temp_file.path()).unwrap();

    pool.new_page(PageType::Heap).unwrap();

    // Pin multiple times
    pool.fetch_page(0).unwrap();
    pool.fetch_page(0).unwrap();

    // Need to unpin multiple times
    pool.unpin_page(0, false);
    pool.unpin_page(0, false);
    pool.unpin_page(0, false); // Original pin from new_page

    assert_eq!(pool.size(), 1);
}
