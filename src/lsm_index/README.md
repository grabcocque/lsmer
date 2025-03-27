# LSM Index Module

The core module that orchestrates all LSM tree components and provides the main interface for key-value operations.

## Overview

The LSM Index module is the heart of the system, coordinating between the memtable, SSTables, WAL, and other components to
provide a unified key-value interface with ACID guarantees. It uses a lock-free implementation based on crossbeam's
SkipMap for high concurrency and generational reference counting for safe concurrent access.

## Features

- **Unified Interface**: Single entry point for all key-value operations
- **Component Coordination**: Manages memtable, SSTables, and WAL
- **Compaction Management**: Automatic background compaction
- **Lock-Free Implementation**: Highly concurrent operations using crossbeam-skiplist
- **Generational Reference Counting**: Safe concurrent access with ABA problem prevention
- **Configuration Options**: Flexible system tuning

## Implementation Highlights

- **Lock-Free Architecture**: Uses crossbeam's SkipMap for concurrent access without locks
- **Generational References**: Prevents the ABA problem in lock-free data structures
- **Memory-Safe Concurrency**: Proper reference counting with generation tracking
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
- Generational reference counting for safe concurrent access
- Memtable management
- SSTable coordination
- WAL integration
- Compaction scheduling
- Bloom filter usage

### Generational Reference Counting

Our implementation uses a generational reference counting system to prevent the ABA problem in lock-free data structures:

- **GenRef\<T\>**: Core structure that tracks both reference counts and generation numbers
- **GenRefHandle\<T\>**: Safe handle to access the reference-counted data
- **GenIndexEntry**: Enhanced index entry with generational reference counting

This approach provides:

- Protection against the ABA problem (where a value changes from A to B and back to A between operations)
- Thread-safe memory management with proper reference counting
- Ability to detect stale references through generation numbers
- Improved concurrency without compromising safety

```rust
// Example of generational reference counting usage
use lsmer::lsm_index::{GenIndexEntry, make_gen_ref};

// Create a generationally reference-counted value
let handle = make_gen_ref(vec![1, 2, 3]);

// Multiple threads can safely access and clone this value
let value = handle.clone_data();  // Thread-safe cloning

// Check if the reference is stale (modified elsewhere)
if !handle.is_stale() {
    // Safe to use this reference
}
```

## Component Interaction

```ascii
[Client Threads] ← Concurrent Access
       ↓
  [LSM Index]
       ↓
  [Lock-Free SkipMap] → [Generational References] → [Memtable] ← [WAL]
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
- Lock-free specific scenarios
- Generational reference counting

Run the standard tests with:

```bash
cargo test --test lsm_index_async_test
```

Run the lock-free specific tests with:

```bash
cargo test --test lsm_index_lock_free_test
```

Run the generational reference counting tests with:

```bash
cargo test --test gen_ref_test
```

These tests specifically validate:

- Concurrent inserts from multiple threads
- Simultaneous reads and writes
- Range queries under heavy write load
- Memory safety with generational references
- Protection against the ABA problem
- Performance comparisons showing multi-threaded scaling

Recent performance tests show **1.8x throughput improvement** when using multiple threads with the lock-free implementation.

## Technical Implementation

The SkipMap implementation is built on crossbeam-skiplist, which provides:

- Wait-free reads: Readers never block, even during concurrent modifications
- Lock-free writes: Writers use atomic operations instead of locks
- Memory reclamation: Safe memory management through epoch-based reclamation
- High scalability: Near-linear scaling with the number of cores

Our generational reference counting system enhances this with:

- Generation tracking: Each modification increments a generation counter
- Stale detection: Handles can detect if they're referencing outdated data
- Safe memory management: Proper cleanup when the last reference is dropped
- Thread safety: All operations are atomic and thread-safe
