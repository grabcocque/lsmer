# B+ Tree Module

A high-performance B+ tree implementation optimized for disk-based storage and efficient key-value lookups.

## Overview

The B+ tree is a self-balancing tree data structure that maintains sorted data and allows efficient operations. It provides
searches, sequential access, insertions, and deletions in logarithmic time. This implementation is particularly well-suited
for disk-based storage systems like LSM trees.

## Features

- **Balanced Tree Structure**: Maintains O(log n) height for all operations
- **Disk-Optimized**: Designed for efficient disk access patterns
- **Range Queries**: Efficient sequential access and range scanning
- **Concurrent Access**: Thread-safe operations
- **Storage References**: Efficient handling of disk-based values

## Usage

```rust
use lsmer::{BPlusTree, TreeOps};

// Create a new B+ tree
let mut tree = BPlusTree::new();

// Insert key-value pairs
tree.insert("key1", "value1")?;
tree.insert("key2", "value2")?;

// Look up values
let value = tree.get("key1")?;

// Range scan
for (key, value) in tree.range("key1"..="key2") {
    println!("{}: {}", key, value);
}

// Delete keys
tree.delete("key1")?;
```

## Performance

- **Search**: O(log n)
- **Insert**: O(log n)
- **Delete**: O(log n)
- **Range Scan**: O(log n + k) where k is the number of elements in the range

## Implementation Details

The B+ tree implementation includes:

- Leaf node linking for efficient range scans
- Automatic rebalancing
- Optimized node splitting and merging
- Efficient disk page management
- Storage reference handling for large values

## Testing

The module includes comprehensive tests covering:

- Basic CRUD operations
- Range queries
- Edge cases and rebalancing
- Concurrent access patterns
- Disk-based operations

Run the tests with:

```bash
cargo test --package lsmer --lib bptree::tests
```
