use super::{IndexError, IndexKeyValue, StorageReference};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::RangeBounds;

/// The type of a B+ tree node
///
/// A B+ tree has two types of nodes:
/// - Leaf nodes that contain the actual data
/// - Internal nodes that contain keys and pointers to child nodes
///
/// # Examples
///
/// ```
/// use lsmer::bptree::NodeType;
///
/// let leaf = NodeType::Leaf;
/// let internal = NodeType::Internal;
///
/// // Verify that we can distinguish between node types
/// assert!(matches!(leaf, NodeType::Leaf));
/// assert!(matches!(internal, NodeType::Internal));
/// ```
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum NodeType {
    /// A leaf node that contains actual data
    Leaf,
    /// An internal node that contains keys and pointers to other nodes
    Internal,
}

/// An entry in a B+ tree node
///
/// Each entry contains a key-value pair and optionally a pointer to a child node
/// (for internal nodes).
///
/// # Type Parameters
///
/// * `K` - The key type
/// * `V` - The value type
///
/// # Examples
///
/// ```
/// use lsmer::bptree::{IndexEntry, IndexKeyValue, BPTreeNode, NodeType};
///
/// let kv = IndexKeyValue {
///     key: 1,
///     value: Some("one".to_string()),
///     storage_ref: None,
/// };
///
/// let entry = IndexEntry {
///     kv,
///     child: Some(Box::new(BPTreeNode::new(NodeType::Leaf, 4))),
/// };
///
/// assert_eq!(entry.kv.key, 1);
/// assert!(entry.child.is_some());
/// ```
#[derive(Debug, Clone)]
pub struct IndexEntry<K, V> {
    /// The key-value pair
    pub kv: IndexKeyValue<K, V>,
    /// Pointer to a child node (for internal nodes)
    pub child: Option<Box<BPTreeNode<K, V>>>,
}

/// A B+ tree node
///
/// This structure represents a node in a B+ tree. It can be either a leaf node
/// containing actual data or an internal node containing keys and pointers to
/// child nodes.
///
/// # Type Parameters
///
/// * `K` - The key type, must implement `Clone + PartialOrd + Debug`
/// * `V` - The value type, must implement `Clone + Debug`
///
/// # Examples
///
/// ```
/// use lsmer::bptree::{BPTreeNode, NodeType, IndexKeyValue};
///
/// // Create a leaf node
/// let mut node = BPTreeNode::new(NodeType::Leaf, 4);
///
/// // Insert some values
/// node.insert(1, Some("one".to_string()), None)?;
/// node.insert(2, Some("two".to_string()), None)?;
///
/// // Find a value
/// let pos = node.find_position(&1);
/// assert_eq!(node.entries[pos].kv.key, 1);
///
/// // Get a range of values
/// let range = node.range(1..=2);
/// assert_eq!(range.len(), 2);
/// # Ok::<(), lsmer::bptree::IndexError>(())
/// ```
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
    ///
    /// # Arguments
    ///
    /// * `node_type` - The type of node to create (Leaf or Internal)
    /// * `max_entries` - Maximum number of entries this node can hold
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::{BPTreeNode, NodeType};
    ///
    /// // Create a leaf node
    /// let leaf = BPTreeNode::<i32, String>::new(NodeType::Leaf, 4);
    /// assert_eq!(leaf.node_type, NodeType::Leaf);
    /// assert_eq!(leaf.max_entries, 4);
    ///
    /// // Create an internal node
    /// let internal = BPTreeNode::<i32, String>::new(NodeType::Internal, 4);
    /// assert_eq!(internal.node_type, NodeType::Internal);
    /// ```
    pub fn new(node_type: NodeType, max_entries: usize) -> Self {
        BPTreeNode {
            node_type,
            entries: Vec::with_capacity(max_entries + 1), // +1 for temporarily holding overflow
            next_leaf: None,
            max_entries,
        }
    }

    /// Find the position where a key should be inserted or exists
    ///
    /// Uses binary search to efficiently find the position of a key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to find the position for
    ///
    /// # Returns
    ///
    /// The index where the key exists or should be inserted.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::{BPTreeNode, NodeType};
    ///
    /// let mut node = BPTreeNode::new(NodeType::Leaf, 4);
    /// node.insert(1, Some("one".to_string()), None)?;
    /// node.insert(3, Some("three".to_string()), None)?;
    ///
    /// // Find existing key
    /// assert_eq!(node.find_position(&1), 0);
    ///
    /// // Find insertion position for new key
    /// assert_eq!(node.find_position(&2), 1);
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
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
    ///
    /// If the node becomes too full after insertion, it will be split.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to insert
    /// * `storage_ref` - Optional reference to where the value is stored on disk
    ///
    /// # Returns
    ///
    /// * `Ok(None)` - If no split was necessary
    /// * `Ok(Some((K, Box<BPTreeNode>)))` - If the node was split, returns the median key and new right node
    /// * `Err(IndexError)` - If an error occurred during insertion
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::{BPTreeNode, NodeType};
    ///
    /// let mut node = BPTreeNode::new(NodeType::Leaf, 2);
    ///
    /// // First insertion - no split
    /// let result1 = node.insert(1, Some("one".to_string()), None)?;
    /// assert!(result1.is_none());
    ///
    /// // Second insertion - no split
    /// let result2 = node.insert(2, Some("two".to_string()), None)?;
    /// assert!(result2.is_none());
    ///
    /// // Third insertion - causes split
    /// let result3 = node.insert(3, Some("three".to_string()), None)?;
    /// assert!(result3.is_some());
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
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
    ///
    /// This method is called when a node becomes too full and needs to be split.
    /// For leaf nodes, the split point becomes a separator key in the parent node.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - The median key that will become a separator in the parent node
    /// - The new right node containing entries greater than or equal to the median key
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::{BPTreeNode, NodeType};
    ///
    /// // Create a node with max_entries = 2
    /// let mut node = BPTreeNode::new(NodeType::Leaf, 2);
    ///
    /// // Insert some keys
    /// node.insert(10, Some("ten".to_string()), None)?;
    /// node.insert(20, Some("twenty".to_string()), None)?;
    ///
    /// // Before split, verify we have 2 entries
    /// assert_eq!(node.entries.len(), 2);
    ///
    /// // Insert another key to force a split (when we insert 30, the node will be split)
    /// let result = node.insert(30, Some("thirty".to_string()), None)?;
    /// assert!(result.is_some());
    ///
    /// // Extract the split result directly (don't call split() again)
    /// let (median_key, right_node) = result.unwrap();
    ///
    /// // Split point is entries.len()/2 = 3/2 = 1
    /// // Left node (original) keeps [0..split_point] which is just [10]
    /// // Right node gets [split_point..] which is [20, 30]
    /// assert_eq!(node.entries.len(), 1);
    /// assert_eq!(right_node.entries.len(), 2);
    /// assert_eq!(node.entries[0].kv.key, 10);
    /// assert_eq!(right_node.entries[0].kv.key, 20);
    /// assert_eq!(right_node.entries[1].kv.key, 30);
    ///
    /// // For leaf nodes, median key is the first key in right node
    /// assert_eq!(median_key, 20);
    ///
    /// // Verify leaf node linking is set up
    /// assert!(node.next_leaf.is_some());
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
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
                // For leaf nodes, the median key is the first key in the right node
                right_node.entries[0].kv.key.clone()
            }
        };

        // Handle leaf node linking
        if let NodeType::Leaf = self.node_type {
            // Save the current node's next_leaf
            let next = self.next_leaf.take();

            // Box the right node
            let mut boxed_right = Box::new(right_node);

            // Set the right node's next_leaf to the saved next
            boxed_right.next_leaf = next;

            // Set the current node's next_leaf to point to the right node
            self.next_leaf = Some(boxed_right.clone());

            return (median_key, boxed_right);
        }

        (median_key, Box::new(right_node))
    }

    /// Get a range of entries from this node
    ///
    /// # Arguments
    ///
    /// * `range` - The range of keys to retrieve
    ///
    /// # Returns
    ///
    /// A vector of references to entries within the specified range.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bptree::{BPTreeNode, NodeType};
    ///
    /// let mut node = BPTreeNode::new(NodeType::Leaf, 4);
    /// node.insert(1, Some("one".to_string()), None)?;
    /// node.insert(2, Some("two".to_string()), None)?;
    /// node.insert(3, Some("three".to_string()), None)?;
    ///
    /// // Inclusive range
    /// let range1 = node.range(1..=2);
    /// assert_eq!(range1.len(), 2);
    ///
    /// // Exclusive range
    /// let range2 = node.range(1..3);
    /// assert_eq!(range2.len(), 2);
    ///
    /// // Unbounded range
    /// let range3 = node.range(2..);
    /// assert_eq!(range3.len(), 2);
    /// # Ok::<(), lsmer::bptree::IndexError>(())
    /// ```
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
