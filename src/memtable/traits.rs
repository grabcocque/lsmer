use std::io;

/// Trait for calculating the size of an object in bytes
pub trait ByteSize {
    /// Returns the size of the object in bytes
    fn byte_size(&self) -> usize;
}

/// Trait for converting values to and from bytes
pub trait ToBytes {
    /// Converts the value to a byte representation
    fn to_bytes(&self) -> Vec<u8>;

    /// Reconstructs a value from its byte representation
    fn from_bytes(bytes: &[u8]) -> io::Result<Self>
    where
        Self: Sized;
}

/// Trait for writing contents to an SSTable
pub trait SSTableWriter {
    /// Flushes the contents to an SSTable file in the specified directory
    fn flush_to_sstable(&self, base_path: &str) -> io::Result<String>;
}

/// Trait defining the core memtable operations
pub trait Memtable<K, V> {
    /// Inserts a key-value pair into the memtable
    fn insert(&self, key: K, value: V) -> Result<Option<V>, super::error::MemtableError>;
    /// Retrieves a value from the memtable by key
    fn get(&self, key: &K) -> Result<Option<V>, super::error::MemtableError>;
    /// Removes a key-value pair from the memtable
    fn remove(&self, key: &K) -> Result<Option<V>, super::error::MemtableError>;
    /// Returns the number of entries in the memtable
    fn len(&self) -> Result<usize, super::error::MemtableError>;
    /// Returns true if the memtable is empty
    fn is_empty(&self) -> Result<bool, super::error::MemtableError>;
    /// Clears all entries from the memtable
    fn clear(&self) -> Result<(), super::error::MemtableError>;
    /// Returns the current size of the memtable in bytes
    fn size_bytes(&self) -> Result<usize, super::error::MemtableError>;
}

// Implement ByteSize for common types
impl ByteSize for String {
    fn byte_size(&self) -> usize {
        // Include the length of the string plus overhead for length field
        self.len() + std::mem::size_of::<usize>()
    }
}

impl ByteSize for Vec<u8> {
    fn byte_size(&self) -> usize {
        // Include the length of the vector plus overhead for length and capacity fields
        self.len() + std::mem::size_of::<usize>() * 2
    }
}

impl ByteSize for u8 {
    fn byte_size(&self) -> usize {
        1
    }
}

// Implement ToBytes for common types
impl ToBytes for String {
    fn to_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        String::from_utf8(bytes.to_vec()).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to convert bytes to String: {}", e),
            )
        })
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        Ok(bytes.to_vec())
    }
}
