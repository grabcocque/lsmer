# LSM Index Module

The core module that orchestrates all LSM tree components and provides the main interface for key-value operations.

## Overview

The LSM Index module is the heart of the system, coordinating between the memtable, SSTables, WAL, and other components to
provide a unified key-value interface with ACID guarantees. It uses a lock-free implementation based on crossbeam's SkipMap for high concurrency.

## Features

- **Unified Interface**: Single entry point for all key-value operations
- **Component Coordination**: Manages memtable, SSTables, and WAL
- **Compaction Management**: Automatic background compaction
- **Lock-Free Implementation**: Highly concurrent operations using crossbeam-skiplist
- **Configuration Options**: Flexible system tuning

## Implementation Highlights

- **Lock-Free Architecture**: Uses crossbeam's SkipMap for concurrent access without locks
- **High Scalability**: Eliminates contention points during high-throughput operations
- **Zero Deadlock Risk**: No locks means no deadlocks or livelocks
- **Optimized for Multi-Threading**: Better performance under concurrent workloads

## Usage

```rust
use lsmer::LsmIndex;

// Create a new LSM index with improved concurrency
let lsm = LsmIndex::new(
    1024 * 1024,        // Memtable capacity in bytes
    "data_dir",         // Base directory for data
    Some(3600),         // Optional compaction interval in seconds
    true,               // Use bloom filters
    0.01                // False positive rate for bloom filters
)?;

// Basic operations (all thread-safe and lock-free)
lsm.insert("key".to_string(), vec![1, 2, 3])?;
let value = lsm.get("key")?;
lsm.remove("key")?;

// Range queries
for (key, value) in lsm.range("a".to_string().."z".to_string())? {
    println!("{}: {:?}", key, value);
}

// Flush to disk
lsm.flush()?;
```

## Performance

- **Write Performance**: O(log n) for lock-free concurrent inserts
- **Read Performance**: O(log n) with Bloom filter optimization and no lock contention
- **Space Efficiency**: Automatic compaction
- **Durability**: Configurable through WAL
- **Concurrency**: Near-linear scaling with multiple threads

## Implementation Details

The LSM Index implementation includes:

- Lock-free SkipMap for the in-memory index
- Memtable management
- SSTable coordination
- WAL integration
- Compaction scheduling
- Bloom filter usage

## Component Interaction

```ascii
[Client Threads] ← Concurrent Access
       ↓
  [LSM Index]
       ↓
  [Lock-Free SkipMap] → [Memtable] ← [WAL]
       ↓
  [SSTables] ← [Bloom Filters]
       ↓
 [Disk Storage]
```

## Testing

The module includes comprehensive tests covering:

- Basic CRUD operations
- Range queries
- Compaction
- Crash recovery
- Concurrent access
- Configuration options

Run the tests with:

```bash
cargo test --test lsm_index_async_test
```

## Technical Implementation

The SkipMap implementation is built on crossbeam-skiplist, which provides:

- Wait-free reads: Readers never block, even during concurrent modifications
- Lock-free writes: Writers use atomic operations instead of locks
- Memory reclamation: Safe memory management through epoch-based reclamation
- High scalability: Near-linear scaling with the number of cores
