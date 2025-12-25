use crate::serialization::codec;
use crate::storage::{BufferPool, PageId, PageType};
use std::io::{self, Cursor};
use std::path::Path;

/// Maximum keys per node (order - 1)
const MAX_KEYS: usize = 10;

/// Page-based B+ Tree for disk persistence
///
/// Stores i64 keys and PageId values (for use as an index pointing to row locations)
pub struct BTreePageIndex {
    root_page_id: PageId,
    buffer_pool: BufferPool,
}

impl BTreePageIndex {
    /// Create a new B+ tree index
    pub fn create(path: impl AsRef<Path>, buffer_pool_size: usize) -> io::Result<Self> {
        let mut buffer_pool = BufferPool::new(buffer_pool_size, path)?;

        // Create root as a leaf page
        let root_page = buffer_pool.new_page(PageType::BTreeLeaf)?;
        let root_page_id = root_page.page_id();

        // Initialize as empty leaf
        let leaf_data = serialize_leaf_node(&[], &[], None)?;
        root_page.add_row(&leaf_data)?;

        buffer_pool.unpin_page(root_page_id, true);
        buffer_pool.flush_page(root_page_id)?;

        Ok(Self {
            root_page_id,
            buffer_pool,
        })
    }

    /// Open an existing B+ tree index
    pub fn open(
        path: impl AsRef<Path>,
        buffer_pool_size: usize,
        root_page_id: PageId,
    ) -> io::Result<Self> {
        let buffer_pool = BufferPool::new(buffer_pool_size, path)?;

        Ok(Self {
            root_page_id,
            buffer_pool,
        })
    }

    /// Get the root page ID (for persistence)
    pub fn root_page_id(&self) -> PageId {
        self.root_page_id
    }

    /// Insert a key-value pair
    pub fn insert(&mut self, key: i64, value: PageId) -> io::Result<()> {
        let (split_key, new_child_id) = self.insert_recursive(self.root_page_id, key, value)?;

        // If root was split, create new root
        if let Some((split_key, new_child_id)) = split_key.zip(new_child_id) {
            let new_root = self.buffer_pool.new_page(PageType::BTreeInternal)?;
            let new_root_id = new_root.page_id();

            let internal_data =
                serialize_internal_node(&[split_key], &[self.root_page_id, new_child_id])?;
            new_root.add_row(&internal_data)?;

            self.buffer_pool.unpin_page(new_root_id, true);

            self.root_page_id = new_root_id;
        }

        Ok(())
    }

    /// Insert into a subtree
    fn insert_recursive(
        &mut self,
        page_id: PageId,
        key: i64,
        value: PageId,
    ) -> io::Result<(Option<i64>, Option<PageId>)> {
        let page = self.buffer_pool.fetch_page(page_id)?;
        let page_type = page.page_type();
        self.buffer_pool.unpin_page(page_id, false);

        match page_type {
            PageType::BTreeInternal => self.insert_internal(page_id, key, value),
            PageType::BTreeLeaf => self.insert_leaf(page_id, key, value),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid page type for B+ tree",
            )),
        }
    }

    /// Insert into an internal node
    fn insert_internal(
        &mut self,
        page_id: PageId,
        key: i64,
        value: PageId,
    ) -> io::Result<(Option<i64>, Option<PageId>)> {
        // Read current node
        let (keys, mut children) = {
            let page = self.buffer_pool.fetch_page(page_id)?;
            let data = page
                .get_row(0)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Empty internal node"))?;
            let (keys, children) = deserialize_internal_node(data)?;
            self.buffer_pool.unpin_page(page_id, false);
            (keys, children)
        };

        // Find child to descend to
        let child_idx = match keys.binary_search(&key) {
            Ok(pos) => pos + 1,
            Err(pos) => pos,
        };
        let child_id = children[child_idx];

        // Recursively insert into child
        let (split_key, new_child_id) = self.insert_recursive(child_id, key, value)?;

        let (split_key, new_child_id) = match split_key.zip(new_child_id) {
            Some((k, id)) => (k, id),
            None => return Ok((None, None)),
        };

        // Child split - insert new key and child
        let mut keys = keys;
        keys.insert(child_idx, split_key);
        children.insert(child_idx + 1, new_child_id);

        // Check if this node needs to split
        if keys.len() > MAX_KEYS {
            return self.split_internal(page_id, keys, children);
        }

        // Write updated node
        let internal_data = serialize_internal_node(&keys, &children)?;
        self.write_internal(page_id, &internal_data)?;

        Ok((None, None))
    }

    /// Insert into a leaf node
    fn insert_leaf(
        &mut self,
        page_id: PageId,
        key: i64,
        value: PageId,
    ) -> io::Result<(Option<i64>, Option<PageId>)> {
        // Read current leaf
        let (mut keys, mut values, next) = {
            let page = self.buffer_pool.fetch_page(page_id)?;
            let data = page
                .get_row(0)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Empty leaf node"))?;
            let (keys, values, next) = deserialize_leaf_node(data)?;
            self.buffer_pool.unpin_page(page_id, false);
            (keys, values, next)
        };

        // Insert or update
        match keys.binary_search(&key) {
            Ok(pos) => {
                values[pos] = value;
            }
            Err(pos) => {
                keys.insert(pos, key);
                values.insert(pos, value);
            }
        }

        // Check if leaf needs to split
        if keys.len() > MAX_KEYS {
            return self.split_leaf(page_id, keys, values, next);
        }

        // Write updated leaf back
        self.write_leaf(page_id, &keys, &values, next)?;

        Ok((None, None))
    }

    /// Split an internal node
    fn split_internal(
        &mut self,
        page_id: PageId,
        mut keys: Vec<i64>,
        mut children: Vec<PageId>,
    ) -> io::Result<(Option<i64>, Option<PageId>)> {
        let mid = MAX_KEYS.div_ceil(2);

        let split_key = keys[mid];
        let right_keys = keys.split_off(mid + 1);
        let right_children = children.split_off(mid + 1);
        keys.pop(); // Remove split key from left

        // Write left node
        let left_data = serialize_internal_node(&keys, &children)?;
        self.write_internal(page_id, &left_data)?;

        // Create right node
        let right_page = self.buffer_pool.new_page(PageType::BTreeInternal)?;
        let right_page_id = right_page.page_id();
        let right_data = serialize_internal_node(&right_keys, &right_children)?;
        right_page.add_row(&right_data)?;
        self.buffer_pool.unpin_page(right_page_id, true);

        Ok((Some(split_key), Some(right_page_id)))
    }

    /// Split a leaf node
    fn split_leaf(
        &mut self,
        page_id: PageId,
        mut keys: Vec<i64>,
        mut values: Vec<PageId>,
        _next: Option<PageId>,
    ) -> io::Result<(Option<i64>, Option<PageId>)> {
        let mid = MAX_KEYS.div_ceil(2);

        let split_key = keys[mid];
        let right_keys = keys.split_off(mid);
        let right_values = values.split_off(mid);

        // Create right leaf first
        let right_data = serialize_leaf_node(&right_keys, &right_values, None)?;
        let right_page = self.buffer_pool.new_page(PageType::BTreeLeaf)?;
        let right_page_id = right_page.page_id();
        right_page.add_row(&right_data)?;
        self.buffer_pool.unpin_page(right_page_id, true);

        // Update left leaf to point to right
        self.write_leaf(page_id, &keys, &values, Some(right_page_id))?;

        Ok((Some(split_key), Some(right_page_id)))
    }

    /// Write leaf node to page
    fn write_leaf(
        &mut self,
        page_id: PageId,
        keys: &[i64],
        values: &[PageId],
        next: Option<PageId>,
    ) -> io::Result<()> {
        let data = serialize_leaf_node(keys, values, next)?;
        self.write_node_data(page_id, &data)
    }

    /// Write internal node to page
    fn write_internal(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        self.write_node_data(page_id, data)
    }

    /// Write node data to page (helper)
    fn write_node_data(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        let page = self.buffer_pool.fetch_page(page_id)?;

        // Update the existing row at slot 0 with the new data
        page.update_row(0, data)?;

        self.buffer_pool.unpin_page(page_id, true);

        Ok(())
    }

    /// Search for a value by key
    pub fn search(&mut self, key: i64) -> io::Result<Option<PageId>> {
        self.search_recursive(self.root_page_id, key)
    }

    /// Search recursively
    fn search_recursive(&mut self, page_id: PageId, key: i64) -> io::Result<Option<PageId>> {
        let page = self.buffer_pool.fetch_page(page_id)?;
        let page_type = page.page_type();
        let data = page
            .get_row(0)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Empty node"))?
            .to_vec();
        self.buffer_pool.unpin_page(page_id, false);

        match page_type {
            PageType::BTreeInternal => {
                let (keys, children) = deserialize_internal_node(&data)?;
                let child_idx = match keys.binary_search(&key) {
                    Ok(pos) => pos + 1,
                    Err(pos) => pos,
                };
                self.search_recursive(children[child_idx], key)
            }
            PageType::BTreeLeaf => {
                let (keys, values, _) = deserialize_leaf_node(&data)?;
                Ok(keys.binary_search(&key).ok().map(|pos| values[pos]))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid page type",
            )),
        }
    }

    /// Flush all pages to disk
    pub fn flush(&mut self) -> io::Result<()> {
        self.buffer_pool.flush_all()
    }
}

/// Serialize internal node (fixed-size format)
fn serialize_internal_node(keys: &[i64], children: &[PageId]) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();

    // Write actual number of keys
    codec::write_u16(&mut buf, keys.len() as u16)?;

    // Write keys, padding to MAX_KEYS
    for &key in keys {
        codec::write_i64(&mut buf, key)?;
    }
    for _ in keys.len()..MAX_KEYS {
        codec::write_i64(&mut buf, 0)?;
    }

    // Write children, padding to MAX_KEYS+1
    for &child in children {
        codec::write_u32(&mut buf, child)?;
    }
    for _ in children.len()..=MAX_KEYS {
        codec::write_u32(&mut buf, 0)?;
    }

    Ok(buf)
}

/// Deserialize internal node (fixed-size format)
fn deserialize_internal_node(data: &[u8]) -> io::Result<(Vec<i64>, Vec<PageId>)> {
    let mut cursor = Cursor::new(data);

    let num_keys = codec::read_u16(&mut cursor)? as usize;

    // Read all MAX_KEYS keys, but only keep num_keys
    let mut keys = Vec::with_capacity(num_keys);
    for i in 0..MAX_KEYS {
        let key = codec::read_i64(&mut cursor)?;
        if i < num_keys {
            keys.push(key);
        }
    }

    // Read all MAX_KEYS+1 children, but only keep num_keys+1
    let mut children = Vec::with_capacity(num_keys + 1);
    for i in 0..=MAX_KEYS {
        let child = codec::read_u32(&mut cursor)?;
        if i <= num_keys {
            children.push(child);
        }
    }

    Ok((keys, children))
}

/// Serialize leaf node (fixed-size format)
fn serialize_leaf_node(
    keys: &[i64],
    values: &[PageId],
    next: Option<PageId>,
) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();

    // Write actual number of keys
    codec::write_u16(&mut buf, keys.len() as u16)?;

    // Write keys, padding to MAX_KEYS
    for &key in keys {
        codec::write_i64(&mut buf, key)?;
    }
    for _ in keys.len()..MAX_KEYS {
        codec::write_i64(&mut buf, 0)?;
    }

    // Write values, padding to MAX_KEYS
    for &value in values {
        codec::write_u32(&mut buf, value)?;
    }
    for _ in values.len()..MAX_KEYS {
        codec::write_u32(&mut buf, 0)?;
    }

    // Write next pointer
    codec::write_u32(&mut buf, next.unwrap_or(0))?;

    Ok(buf)
}

/// Deserialize leaf node (fixed-size format)
fn deserialize_leaf_node(data: &[u8]) -> io::Result<(Vec<i64>, Vec<PageId>, Option<PageId>)> {
    let mut cursor = Cursor::new(data);

    let num_keys = codec::read_u16(&mut cursor)? as usize;

    // Read all MAX_KEYS keys, but only keep num_keys
    let mut keys = Vec::with_capacity(num_keys);
    for i in 0..MAX_KEYS {
        let key = codec::read_i64(&mut cursor)?;
        if i < num_keys {
            keys.push(key);
        }
    }

    // Read all MAX_KEYS values, but only keep num_keys
    let mut values = Vec::with_capacity(num_keys);
    for i in 0..MAX_KEYS {
        let value = codec::read_u32(&mut cursor)?;
        if i < num_keys {
            values.push(value);
        }
    }

    // Read next pointer
    let next_val = codec::read_u32(&mut cursor)?;
    let next = if next_val == 0 { None } else { Some(next_val) };

    Ok((keys, values, next))
}
