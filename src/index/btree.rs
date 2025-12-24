use std::fmt::Debug;

/// B+ Tree order (maximum number of children per internal node)
/// For learning purposes, we use a small order to make tree operations visible
const ORDER: usize = 4;

/// Maximum keys in internal node = ORDER - 1
const MAX_KEYS_INTERNAL: usize = ORDER - 1;

/// Maximum keys in leaf node = ORDER - 1
const MAX_KEYS_LEAF: usize = ORDER - 1;

/// Node ID type
type NodeId = usize;

/// Internal node in B+ tree
#[derive(Debug, Clone)]
struct InternalNode<K> {
    keys: Vec<K>,
    children: Vec<NodeId>,
}

impl<K> InternalNode<K> {
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            children: Vec::new(),
        }
    }
}

/// Leaf node in B+ tree
#[derive(Debug, Clone)]
struct LeafNode<K, V> {
    keys: Vec<K>,
    values: Vec<V>,
    next: Option<NodeId>,
}

impl<K, V> LeafNode<K, V> {
    fn new() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            next: None,
        }
    }
}

/// B+ tree node
#[derive(Debug, Clone)]
enum Node<K, V> {
    Internal(InternalNode<K>),
    Leaf(LeafNode<K, V>),
}

/// In-memory B+ tree
///
/// A B+ tree is a balanced tree data structure that maintains sorted data
/// and allows for efficient insertions, deletions, and range queries.
///
/// Properties:
/// - All values are stored in leaf nodes
/// - Internal nodes only store keys for routing
/// - Leaf nodes are linked for efficient range scans
/// - Tree remains balanced (all leaves at same depth)
pub struct BPlusTree<K, V> {
    root: NodeId,
    nodes: Vec<Node<K, V>>,
    next_id: NodeId,
}

impl<K: Ord + Clone + Debug, V: Clone + Debug> BPlusTree<K, V> {
    /// Create a new B+ tree
    pub fn new() -> Self {
        let root = LeafNode::new();
        let nodes = vec![Node::Leaf(root)];

        Self {
            root: 0,
            nodes,
            next_id: 1,
        }
    }

    /// Allocate a new node ID
    fn alloc_node(&mut self, node: Node<K, V>) -> NodeId {
        let id = self.next_id;
        self.next_id += 1;
        self.nodes.push(node);
        id
    }

    /// Search for a value by key
    pub fn search(&self, key: &K) -> Option<&V> {
        let mut node_id = self.root;

        loop {
            match &self.nodes[node_id] {
                Node::Internal(internal) => {
                    // Find the child to descend to
                    // If key is found at pos, go to children[pos+1] (keys >= split_key)
                    // If not found, binary_search returns Err(pos) where pos is insertion point
                    let child_idx = match internal.keys.binary_search(key) {
                        Ok(pos) => pos + 1, // Key found, go to right child
                        Err(pos) => pos,    // Key not found, go to child at insertion point
                    };
                    node_id = internal.children[child_idx];
                }
                Node::Leaf(leaf) => {
                    // Search in leaf node
                    return leaf
                        .keys
                        .binary_search(key)
                        .ok()
                        .map(|pos| &leaf.values[pos]);
                }
            }
        }
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: K, value: V) {
        let (split_key, new_child) = self.insert_recursive(self.root, key, value);

        // If root was split, create new root
        if let Some((split_key, new_child)) = split_key.zip(new_child) {
            let mut new_root = InternalNode::new();
            new_root.keys.push(split_key);
            new_root.children.push(self.root);
            new_root.children.push(new_child);

            let new_root_id = self.alloc_node(Node::Internal(new_root));
            self.root = new_root_id;
        }
    }

    /// Insert into a subtree, returns (split_key, new_sibling_id) if node was split
    fn insert_recursive(
        &mut self,
        node_id: NodeId,
        key: K,
        value: V,
    ) -> (Option<K>, Option<NodeId>) {
        match &self.nodes[node_id] {
            Node::Internal(_) => self.insert_internal(node_id, key, value),
            Node::Leaf(_) => self.insert_leaf(node_id, key, value),
        }
    }

    /// Insert into an internal node
    fn insert_internal(
        &mut self,
        node_id: NodeId,
        key: K,
        value: V,
    ) -> (Option<K>, Option<NodeId>) {
        // Find child to insert into
        let child_idx = match &self.nodes[node_id] {
            Node::Internal(internal) => match internal.keys.binary_search(&key) {
                Ok(pos) => pos + 1, // Key found, go to right child
                Err(pos) => pos,    // Key not found, go to child at insertion point
            },
            _ => unreachable!(),
        };

        let child_id = match &self.nodes[node_id] {
            Node::Internal(internal) => internal.children[child_idx],
            _ => unreachable!(),
        };

        // Recursively insert into child
        let (split_key, new_child_id) = self.insert_recursive(child_id, key, value);

        // If child didn't split, we're done
        let (split_key, new_child_id) = match split_key.zip(new_child_id) {
            Some((k, id)) => (k, id),
            None => return (None, None),
        };

        // Child split - insert new key and child into this internal node
        match &mut self.nodes[node_id] {
            Node::Internal(internal) => {
                // Insert the split key at child_idx position
                internal.keys.insert(child_idx, split_key.clone());
                // Insert the new child right after the original child
                internal.children.insert(child_idx + 1, new_child_id);

                // Check if this internal node needs to split
                if internal.keys.len() > MAX_KEYS_INTERNAL {
                    return self.split_internal(node_id);
                }
            }
            _ => unreachable!(),
        }

        (None, None)
    }

    /// Insert into a leaf node
    fn insert_leaf(&mut self, node_id: NodeId, key: K, value: V) -> (Option<K>, Option<NodeId>) {
        match &mut self.nodes[node_id] {
            Node::Leaf(leaf) => {
                // Find insertion position
                match leaf.keys.binary_search(&key) {
                    Ok(pos) => {
                        // Key already exists - update value
                        leaf.values[pos] = value;
                        return (None, None);
                    }
                    Err(pos) => {
                        // Insert new key and value
                        leaf.keys.insert(pos, key);
                        leaf.values.insert(pos, value);
                    }
                }

                // Check if leaf needs to split
                if leaf.keys.len() > MAX_KEYS_LEAF {
                    return self.split_leaf(node_id);
                }
            }
            _ => unreachable!(),
        }

        (None, None)
    }

    /// Split an internal node, returns (split_key, new_sibling_id)
    fn split_internal(&mut self, node_id: NodeId) -> (Option<K>, Option<NodeId>) {
        let mid = MAX_KEYS_INTERNAL.div_ceil(2);

        let (split_key, right_keys, right_children) = match &mut self.nodes[node_id] {
            Node::Internal(internal) => {
                let split_key = internal.keys[mid].clone();
                let right_keys = internal.keys.split_off(mid + 1);
                let right_children = internal.children.split_off(mid + 1);
                internal.keys.pop(); // Remove the split key from left node

                (split_key, right_keys, right_children)
            }
            _ => unreachable!(),
        };

        let mut new_sibling = InternalNode::new();
        new_sibling.keys = right_keys;
        new_sibling.children = right_children;

        let new_sibling_id = self.alloc_node(Node::Internal(new_sibling));

        (Some(split_key), Some(new_sibling_id))
    }

    /// Split a leaf node, returns (split_key, new_sibling_id)
    fn split_leaf(&mut self, node_id: NodeId) -> (Option<K>, Option<NodeId>) {
        let mid = MAX_KEYS_LEAF.div_ceil(2);

        let (split_key, right_keys, right_values, old_next) = match &mut self.nodes[node_id] {
            Node::Leaf(leaf) => {
                let split_key = leaf.keys[mid].clone();
                let right_keys = leaf.keys.split_off(mid);
                let right_values = leaf.values.split_off(mid);
                let old_next = leaf.next;

                (split_key, right_keys, right_values, old_next)
            }
            _ => unreachable!(),
        };

        let mut new_sibling = LeafNode::new();
        new_sibling.keys = right_keys;
        new_sibling.values = right_values;
        new_sibling.next = old_next;

        let new_sibling_id = self.alloc_node(Node::Leaf(new_sibling));

        // Update the original leaf's next pointer
        match &mut self.nodes[node_id] {
            Node::Leaf(leaf) => {
                leaf.next = Some(new_sibling_id);
            }
            _ => unreachable!(),
        }

        (Some(split_key), Some(new_sibling_id))
    }

    /// Range scan iterator
    pub fn range_scan(&self, start: &K, end: &K) -> RangeScanIterator<'_, K, V> {
        // Find the starting leaf
        let mut node_id = self.root;

        while let Node::Internal(internal) = &self.nodes[node_id] {
            let child_idx = match internal.keys.binary_search(start) {
                Ok(pos) => pos + 1, // Key found, go to right child
                Err(pos) => pos,    // Key not found, go to child at insertion point
            };
            node_id = internal.children[child_idx];
        }

        RangeScanIterator {
            tree: self,
            current_leaf: Some(node_id),
            current_index: 0,
            end: end.clone(),
        }
    }
}

impl<K: Ord + Clone + Debug, V: Clone + Debug> Default for BPlusTree<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

/// Range scan iterator
pub struct RangeScanIterator<'a, K, V> {
    tree: &'a BPlusTree<K, V>,
    current_leaf: Option<NodeId>,
    current_index: usize,
    end: K,
}

impl<'a, K: Ord + Clone + Debug, V: Clone + Debug> Iterator for RangeScanIterator<'a, K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let leaf_id = self.current_leaf?;

        match &self.tree.nodes[leaf_id] {
            Node::Leaf(leaf) => {
                // Check if we've reached the end of current leaf
                if self.current_index >= leaf.keys.len() {
                    // Move to next leaf
                    self.current_leaf = leaf.next;
                    self.current_index = 0;
                    return self.next();
                }

                let key = &leaf.keys[self.current_index];

                // Check if we've passed the end key
                if key > &self.end {
                    return None;
                }

                let value = &leaf.values[self.current_index];
                self.current_index += 1;

                Some((key.clone(), value.clone()))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
