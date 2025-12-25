mod tests {
    use crate::{index::BTreePageIndex, storage::PageId};
    use tempfile::NamedTempFile;

    #[test]
    fn test_create_btree() {
        let temp_file = NamedTempFile::new().unwrap();
        let btree = BTreePageIndex::create(temp_file.path(), 10);
        assert!(btree.is_ok());
    }

    #[test]
    fn test_insert_and_search() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut btree = BTreePageIndex::create(temp_file.path(), 10).unwrap();

        btree.insert(1, 100).unwrap();
        btree.insert(2, 200).unwrap();
        btree.insert(3, 300).unwrap();

        assert_eq!(btree.search(1).unwrap(), Some(100));
        assert_eq!(btree.search(2).unwrap(), Some(200));
        assert_eq!(btree.search(3).unwrap(), Some(300));
        assert_eq!(btree.search(4).unwrap(), None);
    }

    #[test]
    fn test_persistence() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_owned();

        let root_page_id = {
            let mut btree = BTreePageIndex::create(&path, 10).unwrap();
            btree.insert(1, 100).unwrap();
            btree.insert(2, 200).unwrap();
            btree.flush().unwrap();
            btree.root_page_id()
        };

        // Reopen and verify
        {
            let mut btree = BTreePageIndex::open(&path, 10, root_page_id).unwrap();
            assert_eq!(btree.search(1).unwrap(), Some(100));
            assert_eq!(btree.search(2).unwrap(), Some(200));
        }
    }

    #[test]
    fn test_many_insertions() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut btree = BTreePageIndex::create(temp_file.path(), 20).unwrap();

        for i in 1..=20 {
            btree.insert(i, i as PageId * 10).unwrap();
        }

        for i in 1..=20 {
            assert_eq!(btree.search(i).unwrap(), Some(i as PageId * 10));
        }
    }
}
