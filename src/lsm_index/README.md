# LSM Index Module

The core module that orchestrates all LSM tree components and provides the main interface for key-value operations.

## Overview

The LSM Index module is the heart of the system, coordinating between the memtable, SSTables, WAL, and other components to provide a unified key-value interface with ACID guarantees.

## Features

- **Unified Interface**: Single entry point for all key-value operations
- **Component Coordination**: Manages memtable, SSTables, and WAL
- **Compaction Management**: Automatic background compaction
- **Concurrent Access**: Thread-safe operations
- **Configuration Options**: Flexible system tuning

## Usage

```rust
use lsmer::LsmIndex;

// Create a new LSM index
let mut index = LsmIndex::new("data_dir").await?;

// Basic operations
index.put("key", "value").await?;
let value = index.get("key").await?;
index.delete("key").await?;

// Batch operations
let batch = vec![
    ("key1", Some("value1")),
    ("key2", Some("value2")),
    ("key3", None), // Delete
];
index.write_batch(batch).await?;

// Range scan
for (key, value) in index.range("key1"..="key2").await? {
    println!("{}: {}", key, value);
}

// Configure compaction
index.set_compaction_threshold(0.7)?; // 70% full threshold
```

## Performance

- **Write Performance**: O(1) for in-memory operations
- **Read Performance**: O(log n) with Bloom filter optimization
- **Space Efficiency**: Automatic compaction
- **Durability**: Configurable through WAL

## Implementation Details

The LSM Index implementation includes:

- Memtable management
- SSTable coordination
- WAL integration
- Compaction scheduling
- Bloom filter usage
- Concurrent access patterns

## Component Interaction

```
[Client]
   ↓
[LSM Index]
   ↓
[Memtable] ←→ [WAL]
   ↓
[SSTables] ←→ [Bloom Filters]
   ↓
[Disk Storage]
```

## Testing

The module includes comprehensive tests covering:

- Basic CRUD operations
- Batch operations
- Range queries
- Compaction
- Crash recovery
- Concurrent access
- Configuration options

Run the tests with:

```bash
cargo test --package lsmer --lib lsm_index::tests
```
