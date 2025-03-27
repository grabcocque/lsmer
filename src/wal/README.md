# Write-Ahead Log (WAL) Module

A high-performance Write-Ahead Logging implementation for ensuring data durability and crash recovery in LSM trees.

## Overview

The Write-Ahead Log ensures data durability by recording all operations before they are applied. This enables crash recovery
and provides ACID guarantees through log replay.

## Features

- **Durability Guarantees**: Configurable write durability levels
- **Crash Recovery**: Automatic recovery through log replay
- **Efficient Logging**: Batch writes and log rotation
- **Concurrent Access**: Thread-safe operations
- **Log Management**: Automatic cleanup and compaction

## Usage

```rust
use lsmer::{WriteAheadLog, DurabilityManager};

// Create a new WAL
let mut wal = WriteAheadLog::new("path/to/wal")?;

// Write with durability
wal.write_with_durability("key", "value", DurabilityLevel::Strong)?;

// Batch write operations
let operations = vec![
    Operation::Put("key1", "value1"),
    Operation::Delete("key2"),
    Operation::Put("key3", "value3"),
];
wal.write_batch(operations)?;

// Recover from crash
wal.recover()?;

// Clean up old log files
wal.cleanup()?;
```

## Performance

- **Write Performance**: O(1) for single operations, O(n) for batches
- **Recovery Performance**: O(n) where n is the number of log entries
- **Space Efficiency**: Automatic log rotation and cleanup
- **Durability Levels**: Configurable trade-offs between performance and durability

## Implementation Details

The WAL implementation includes:

- Log file format with checksums
- Batch write optimization
- Log rotation and cleanup
- Durability manager for different write guarantees
- Efficient log replay
- Concurrent access patterns

## File Format

```ascii
[Log Header]
[Record 1]
[Record 2]
...
[Record N]
[Checksum]
```

## Testing

The module includes comprehensive tests covering:

- Write operations
- Crash recovery
- Durability levels
- Log rotation
- Concurrent access
- Error handling

Run the tests with:

```bash
cargo test --package lsmer --lib wal::tests
```
