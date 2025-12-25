mod tests {
    use crate::storage::{
        PAGE_SIZE, Page, PageError, PageType, SlotId,
        page::{PAGE_HEADER_SIZE, SLOT_ENTRY_SIZE},
    };

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
            Err(PageError::InvalidPageSize {
                expected: PAGE_SIZE,
                found: 100
            })
        ));
    }
}
