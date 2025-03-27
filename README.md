# LSmer - High-Performance LSM Tree Implementation

A blazing-fast, production-ready Log-Structured Merge (LSM) tree implementation in Rust.
Perfect for high-throughput key-value storage with ACID guarantees.

## 🚀 Features

- **Full LSM Tree Implementation**: Complete with memtable, SSTables, and compaction
- **Write-Ahead Logging (WAL)**: Ensures durability and crash recovery
- **B+ Tree Indexing**: Fast lookups with efficient disk access patterns
- **Bloom Filters**: Probabilistic set membership testing for reduced I/O
- **Async Support**: Built with Tokio for high concurrency
- **Configurable Durability**: Fine-grained control over write guarantees
- **Memory Efficient**: Optimized for both memory and disk usage
- **Lock-Free Architecture**: Concurrent access with crossbeam's SkipMap
- **Generational Reference Counting**: Safe concurrent access to shared data with ABA problem prevention

## 🏗️ Architecture

LSmer implements a complete LSM tree with the following components:

- **Memtable**: In-memory buffer for recent writes
- **SSTables**: Immutable sorted string tables on disk
- **B+ Tree Index**: Efficient key lookup structure
- **Bloom Filters**: Probabilistic membership testing
- **Write-Ahead Log**: Crash recovery and durability
- **Compaction**: Background merging of SSTables
- **Lock-Free Index**: Concurrent access through crossbeam's SkipMap
- **Generational References**: Memory-safe shared references with generation tracking to prevent the ABA problem

## 📚 Modules

Each component of LSmer is thoroughly documented:

- [Bloom Filter](src/bloom/README.md) - Probabilistic set membership testing
- [B+ Tree](src/bptree/README.md) - Efficient key lookup structure
- [Memtable](src/memtable/README.md) - In-memory write buffer
- [SSTable](src/sstable/README.md) - Immutable disk storage
- [Write-Ahead Log](src/wal/README.md) - Durability and crash recovery
- [LSM Index](src/lsm_index/README.md) - Core coordination and interface

## 🛠️ Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
lsmer = "0.1.0"
```

## 📖 Usage

```rust
use lsmer::{LsmIndex, StringMemtable, WriteAheadLog};

// Create a new LSM index
let mut index = LsmIndex::new("data_dir").await?;

// Insert a key-value pair
index.put("key", "value").await?;

// Retrieve a value
let value = index.get("key").await?;

// Delete a key
index.delete("key").await?;
```

## 🧪 Testing

Run the test suite:

```bash
cargo test
```

## 🔧 Performance

- **Write Performance**: O(1) for in-memory operations
- **Read Performance**: O(log n) for key lookups
- **Space Efficiency**: Automatic compaction and garbage collection
- **Durability**: Configurable write guarantees through WAL

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the EPL 2.0 - see the LICENSE file for details.
