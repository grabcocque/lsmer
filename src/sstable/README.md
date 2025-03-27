# SSTable Module

A high-performance implementation of Sorted String Tables (SSTables) for disk-based storage in LSM trees.

## Overview

SSTables provide efficient, immutable disk storage for sorted key-value pairs. They are created when memtables are flushed
to disk and form the persistent storage layer of the LSM tree.

## Features

- **Immutable Storage**: Once written, never modified
- **Efficient Lookups**: Index-based access with Bloom filter optimization
- **Compression Support**: Optional data compression
- **Block-Based Storage**: Efficient disk access patterns
- **Metadata Management**: Comprehensive file and block metadata

## Usage

```rust
use lsmer::{SSTable, SSTableInfo};

// Create a new SSTable
let mut sstable = SSTable::new("path/to/sstable")?;

// Write data blocks
sstable.write_block(&key_value_pairs)?;

// Build index and Bloom filter
sstable.finalize()?;

// Read data
let value = sstable.get("key")?;

// Get SSTable metadata
let info = sstable.get_info()?;
println!("Number of entries: {}", info.num_entries);
println!("Data size: {} bytes", info.data_size);
```

## Performance

- **Read Performance**: O(log n) with Bloom filter optimization
- **Write Performance**: O(n) for initial creation
- **Space Efficiency**: Optional compression
- **Disk I/O**: Optimized block-based access

## Implementation Details

The SSTable implementation includes:

- Block-based storage format
- Index structure for quick lookups
- Bloom filter integration
- Compression support
- Metadata management
- Efficient disk I/O patterns

## File Format

```ascii
[Data Block 1]
[Data Block 2]
...
[Data Block N]
[Index Block]
[Bloom Filter]
[Footer]
```

## Testing

The module includes comprehensive tests covering:

- Block writing and reading
- Index construction and lookup
- Bloom filter integration
- Compression (if enabled)
- File format compatibility
- Error handling

Run the tests with:

```bash
cargo test --package lsmer --lib sstable::tests
```
