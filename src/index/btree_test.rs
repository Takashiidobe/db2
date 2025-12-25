mod tests {
    use crate::index::BPlusTree;

    #[test]
    fn test_new_tree() {
        let tree: BPlusTree<i64, String> = BPlusTree::new();
        assert!(tree.search(&1).is_none());
    }

    #[test]
    fn test_insert_and_search() {
        let mut tree = BPlusTree::new();
        tree.insert(1, "one");
        tree.insert(2, "two");
        tree.insert(3, "three");

        assert_eq!(tree.search(&1), Some(&"one"));
        assert_eq!(tree.search(&2), Some(&"two"));
        assert_eq!(tree.search(&3), Some(&"three"));
        assert_eq!(tree.search(&4), None);
    }

    #[test]
    fn test_insert_order() {
        let mut tree = BPlusTree::new();

        // Insert in reverse order
        tree.insert(5, "five");
        tree.insert(3, "three");
        tree.insert(7, "seven");
        tree.insert(1, "one");

        assert_eq!(tree.search(&1), Some(&"one"));
        assert_eq!(tree.search(&3), Some(&"three"));
        assert_eq!(tree.search(&5), Some(&"five"));
        assert_eq!(tree.search(&7), Some(&"seven"));
    }

    #[test]
    fn test_leaf_split() {
        let mut tree = BPlusTree::new();

        // Insert enough values to cause a leaf split
        for i in 1..=10 {
            tree.insert(i, format!("value{}", i));
        }

        // Verify all values are accessible
        for i in 1..=10 {
            assert_eq!(tree.search(&i), Some(&format!("value{}", i)));
        }
    }

    #[test]
    fn test_internal_node_creation() {
        let mut tree = BPlusTree::new();

        // Insert enough values to create internal nodes
        for i in 1..=20 {
            tree.insert(i, i * 10);
        }

        // Verify all values
        for i in 1..=20 {
            assert_eq!(tree.search(&i), Some(&(i * 10)));
        }
    }

    #[test]
    fn test_range_scan() {
        let mut tree = BPlusTree::new();

        for i in 1..=10 {
            tree.insert(i, i * 10);
        }

        let results: Vec<_> = tree.range_scan(&3, &7).collect();

        assert_eq!(results.len(), 5);
        assert_eq!(results[0], (3, 30));
        assert_eq!(results[1], (4, 40));
        assert_eq!(results[2], (5, 50));
        assert_eq!(results[3], (6, 60));
        assert_eq!(results[4], (7, 70));
    }

    #[test]
    fn test_range_scan_empty() {
        let tree: BPlusTree<i64, i64> = BPlusTree::new();
        let results: Vec<_> = tree.range_scan(&1, &10).collect();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_range_scan_single() {
        let mut tree = BPlusTree::new();
        tree.insert(5, 50);

        let results: Vec<_> = tree.range_scan(&5, &5).collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (5, 50));
    }

    #[test]
    fn test_range_scan_across_leaves() {
        let mut tree = BPlusTree::new();

        // Insert enough to span multiple leaves
        for i in 1..=20 {
            tree.insert(i, i * 10);
        }

        let results: Vec<_> = tree.range_scan(&5, &15).collect();
        assert_eq!(results.len(), 11);

        for (i, (key, value)) in results.iter().enumerate() {
            let expected_key = i as i64 + 5;
            assert_eq!(*key, expected_key);
            assert_eq!(*value, expected_key * 10);
        }
    }

    #[test]
    fn test_many_insertions() {
        let mut tree = BPlusTree::new();

        // Insert many values to stress test splitting
        for i in 0..100 {
            tree.insert(i, format!("val{}", i));
        }

        // Verify all values
        for i in 0..100 {
            assert_eq!(tree.search(&i), Some(&format!("val{}", i)));
        }
    }

    #[test]
    fn test_string_keys() {
        let mut tree = BPlusTree::new();

        tree.insert("apple".to_string(), 1);
        tree.insert("banana".to_string(), 2);
        tree.insert("cherry".to_string(), 3);

        assert_eq!(tree.search(&"apple".to_string()), Some(&1));
        assert_eq!(tree.search(&"banana".to_string()), Some(&2));
        assert_eq!(tree.search(&"cherry".to_string()), Some(&3));
        assert_eq!(tree.search(&"date".to_string()), None);
    }

    #[test]
    fn test_range_scan_full() {
        let mut tree = BPlusTree::new();

        for i in 1..=10 {
            tree.insert(i, i);
        }

        let results: Vec<_> = tree.range_scan(&1, &10).collect();
        assert_eq!(results.len(), 10);
    }

    #[test]
    fn test_duplicate_keys() {
        let mut tree = BPlusTree::new();

        tree.insert(5, "first");
        tree.insert(5, "second");

        // Later insertion should appear
        assert_eq!(tree.search(&5), Some(&"second"));
    }
}
