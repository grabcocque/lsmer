# Memtable Module

A high-performance in-memory buffer for the LSM tree implementation.

## Overview

The memtable serves as a write-optimized in-memory buffer that maintains sorted key-value pairs. When it reaches capacity,
it is flushed to disk as an SSTable.

## Features

- **Fast In-Memory Operations**: O(1) writes and O(log n) reads
- **Size-Based Flushing**: Automatic flushing when size threshold is reached
- **Concurrent Access**: Thread-safe operations with async support
- **Memory Management**: Efficient memory usage with configurable limits
- **String and Binary Support**: Handles both string and binary data

## Usage

```rust
use lsmer::{StringMemtable, Memtable};

// Create a new memtable with size limit
let mut memtable = StringMemtable::new(1024 * 1024); // 1MB limit

// Insert key-value pairs
memtable.put("key1", "value1")?;
memtable.put("key2", "value2")?;

// Get values
let value = memtable.get("key1")?;

// Delete keys
memtable.delete("key1")?;

// Check if memtable needs flushing
if memtable.should_flush() {
    // Flush to SSTable
    memtable.flush()?;
}
```

## Performance

- **Write Performance**: O(1) amortized
- **Read Performance**: O(log n)
- **Memory Usage**: Configurable size limits
- **Flush Performance**: O(n) where n is the number of entries

## Implementation Details

The memtable implementation includes:

- Skip list for efficient lookups
- Memory usage tracking
- Automatic size-based flushing
- Efficient serialization for disk writes
- Concurrent access patterns

## Testing

The module includes comprehensive tests covering:

- Basic CRUD operations
- Size limits and flushing
- Concurrent access patterns
- Memory management
- Serialization/deserialization

Run the tests with:

```bash
cargo test --package lsmer --lib memtable::tests
```
