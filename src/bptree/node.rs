use super::{IndexError, IndexKeyValue, StorageReference};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::RangeBounds;

/// The type of a B+ tree node
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum NodeType {
    /// A leaf node that contains actual data
    Leaf,
    /// An internal node that contains keys and pointers to other nodes
    Internal,
}

/// An entry in a B+ tree node
#[derive(Debug, Clone)]
pub struct IndexEntry<K, V> {
    /// The key-value pair
    pub kv: IndexKeyValue<K, V>,
    /// Pointer to a child node (for internal nodes)
    pub child: Option<Box<BPTreeNode<K, V>>>,
}

/// A B+ tree node
#[derive(Debug, Clone)]
pub struct BPTreeNode<K, V> {
    /// The type of this node (leaf or internal)
    pub node_type: NodeType,
    /// The entries in this node
    pub entries: Vec<IndexEntry<K, V>>,
    /// Pointer to the next leaf node (only for leaf nodes)
    pub next_leaf: Option<Box<BPTreeNode<K, V>>>,
    /// Maximum number of entries this node can hold
    pub max_entries: usize,
}

/// Represents the result of a node split operation: the split key and the new right node
pub type SplitResult<K, V> = Option<(K, Box<BPTreeNode<K, V>>)>;

impl<K: Clone + PartialOrd + Debug, V: Clone + Debug> BPTreeNode<K, V> {
    /// Create a new B+ tree node
    pub fn new(node_type: NodeType, max_entries: usize) -> Self {
        BPTreeNode {
            node_type,
            entries: Vec::with_capacity(max_entries + 1), // +1 for temporarily holding overflow
            next_leaf: None,
            max_entries,
        }
    }

    /// Find the position where a key should be inserted or exists
    pub fn find_position(&self, key: &K) -> usize {
        // Binary search for the key position
        match self.entries.binary_search_by(|entry| {
            if &entry.kv.key < key {
                Ordering::Less
            } else if &entry.kv.key > key {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }) {
            Ok(pos) => pos,  // Exact match
            Err(pos) => pos, // Position where key would be inserted
        }
    }

    /// Insert a key-value pair into this node
    pub fn insert(
        &mut self,
        key: K,
        value: Option<V>,
        storage_ref: Option<StorageReference>,
    ) -> Result<SplitResult<K, V>, IndexError> {
        let pos = self.find_position(&key);

        // Check if key already exists
        if pos < self.entries.len() && self.entries[pos].kv.key == key {
            // Update existing entry
            self.entries[pos].kv.value = value;
            self.entries[pos].kv.storage_ref = storage_ref;
            return Ok(None);
        }

        // Insert new entry
        let kv = IndexKeyValue {
            key,
            value,
            storage_ref,
        };
        let entry = IndexEntry { kv, child: None };
        self.entries.insert(pos, entry);

        // Split if necessary
        if self.entries.len() > self.max_entries {
            Ok(Some(self.split()))
        } else {
            Ok(None)
        }
    }

    /// Split this node into two and return the median key and right node
    pub fn split(&mut self) -> (K, Box<BPTreeNode<K, V>>) {
        let split_point = self.entries.len() / 2;
        let mut right_node = BPTreeNode::new(self.node_type, self.max_entries);

        // Move entries to the right node
        right_node.entries = self.entries.drain(split_point..).collect();

        let median_key = match self.node_type {
            NodeType::Internal => {
                // For internal nodes, the median becomes the parent key
                let median_entry = self.entries.pop().unwrap();
                let key = median_entry.kv.key.clone();

                // The right child of the median becomes the leftmost child of the right node
                if let Some(child) = median_entry.child {
                    right_node.entries.insert(
                        0,
                        IndexEntry {
                            kv: IndexKeyValue {
                                key: key.clone(),
                                value: None,
                                storage_ref: None,
                            },
                            child: Some(child),
                        },
                    );
                }

                key
            }
            NodeType::Leaf => {
                // For leaf nodes, the first key of the right node becomes the parent key
                // but we keep it in the leaf as well
                right_node.entries[0].kv.key.clone()
            }
        };

        // Link leaf nodes together
        if self.node_type == NodeType::Leaf {
            // The right node's next_leaf becomes the current node's next_leaf
            right_node.next_leaf = self.next_leaf.take();

            // The current node's next_leaf becomes the right node
            // We need to get ownership of this for returning, so we'll create it separately
            let boxed_right = Box::new(right_node);
            self.next_leaf = Some(Box::new(*boxed_right.clone()));

            return (median_key, boxed_right);
        }

        (median_key, Box::new(right_node))
    }

    /// Get a range of entries from this node
    pub fn range<R>(&self, range: R) -> Vec<&IndexEntry<K, V>>
    where
        R: RangeBounds<K>,
    {
        use std::ops::Bound;

        // Determine start position based on range start bound
        let start_pos = match range.start_bound() {
            Bound::Included(k) => self.find_position(k),
            Bound::Excluded(k) => {
                let pos = self.find_position(k);
                if pos < self.entries.len() && &self.entries[pos].kv.key == k {
                    pos + 1
                } else {
                    pos
                }
            }
            Bound::Unbounded => 0,
        };

        // Determine end position based on range end bound
        let end_pos = match range.end_bound() {
            Bound::Included(k) => {
                let pos = self.find_position(k);
                if pos < self.entries.len() && &self.entries[pos].kv.key <= k {
                    pos + 1
                } else {
                    pos
                }
            }
            Bound::Excluded(k) => {
                let pos = self.find_position(k);
                if pos < self.entries.len() && &self.entries[pos].kv.key < k {
                    pos + 1
                } else {
                    pos
                }
            }
            Bound::Unbounded => self.entries.len(),
        };

        // Return entries in range
        self.entries[start_pos..end_pos].iter().collect()
    }
}
