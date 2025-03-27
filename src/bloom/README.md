# Bloom Filter Module

A high-performance, configurable Bloom filter implementation for probabilistic set membership testing.

## Overview

The Bloom filter is a space-efficient probabilistic data structure that tells you whether an element is definitely not in a set or possibly in a set. It's perfect for reducing disk I/O in LSM trees by quickly determining if a key might exist in an SSTable.

## Features

- **Configurable False Positive Rate**: Set your desired false positive rate during initialization
- **Merge Operation**: Combine multiple Bloom filters efficiently
- **Thread-Safe**: Built with async/await support
- **Memory Efficient**: Optimized for both space and time complexity
- **Generic Type Support**: Works with any hashable type

## Usage

```rust
use lsmer::BloomFilter;

// Create a new Bloom filter with expected 1000 elements and 1% false positive rate
let mut filter = BloomFilter::<String>::new(1000, 0.01);

// Insert elements
filter.insert(&"apple".to_string());
filter.insert(&"banana".to_string());

// Check for membership
assert!(filter.may_contain(&"apple".to_string()));
assert!(!filter.may_contain(&"grape".to_string()));

// Merge with another filter
let mut other_filter = BloomFilter::<String>::new(1000, 0.01);
other_filter.insert(&"cherry".to_string());
filter.merge(&other_filter).unwrap();

// Clear the filter
filter.clear();
```

## Performance

- Space complexity: O(n) where n is the number of expected elements
- Time complexity: O(k) for insertions and lookups, where k is the number of hash functions
- False positive rate is configurable and guaranteed to be within 2x of the target rate

## Implementation Details

The Bloom filter uses:

- Multiple hash functions for better distribution
- Bit array for compact storage
- CRC32 and SipHash for high-quality hashing
- Efficient bit operations for fast lookups

## Testing

The module includes comprehensive tests covering:

- Empty filter operations
- Insert and check operations
- False positive rate verification
- Merge operations
- Clear operations

Run the tests with:

```bash
cargo test --package lsmer --lib bloom::tests
```
