use std::collections::HashMap;
use std::hash::Hash;

/// Simple hash-based index for equality lookups.
///
/// Stores a mapping from key to one or more values since indexes can have
/// duplicate keys.
#[derive(Debug, Default)]
pub struct HashIndex<K, V> {
    buckets: HashMap<K, Vec<V>>,
}

impl<K: Eq + Hash, V> HashIndex<K, V> {
    /// Create an empty hash index.
    pub fn new() -> Self {
        Self {
            buckets: HashMap::new(),
        }
    }

    /// Insert a key/value pair into the index.
    pub fn insert(&mut self, key: K, value: V) {
        self.buckets.entry(key).or_default().push(value);
    }

    /// Return all values for a given key.
    pub fn get(&self, key: &K) -> impl Iterator<Item = &V> {
        self.buckets.get(key).into_iter().flatten()
    }
}
