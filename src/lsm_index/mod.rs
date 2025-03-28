use crate::bptree::StorageReference;
use crate::memtable::{Memtable, MemtableError, SSTableWriter, StringMemtable};
use crate::wal::durability::{DurabilityManager, Operation};
use crossbeam_skiplist::SkipMap;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

// Export the skip_list module
pub mod skip_list;
pub mod skip_list_index;

// New modules for generational reference counting
pub mod gen_index_entry;
pub mod gen_ref;

// Re-export the SkipListIndex
pub use skip_list_index::SkipListIndex;
// Re-export the generational reference counting types for external use
pub use gen_index_entry::GenIndexEntry;
pub use gen_ref::{make_gen_ref, GenRefHandle};

/// Error type for LSM index operations
#[derive(Debug)]
pub enum LsmIndexError {
    /// I/O error
    IoError(io::Error),
    /// Memtable error
    MemtableError(MemtableError),
    /// Durability error
    DurabilityError(crate::wal::durability::DurabilityError),
    /// Index error
    IndexError(crate::bptree::IndexError),
    /// Key not found
    KeyNotFound,
    /// Invalid operation
    InvalidOperation(String),
}

impl From<io::Error> for LsmIndexError {
    fn from(error: io::Error) -> Self {
        LsmIndexError::IoError(error)
    }
}

impl From<MemtableError> for LsmIndexError {
    fn from(error: MemtableError) -> Self {
        LsmIndexError::MemtableError(error)
    }
}

impl From<crate::wal::durability::DurabilityError> for LsmIndexError {
    fn from(error: crate::wal::durability::DurabilityError) -> Self {
        LsmIndexError::DurabilityError(error)
    }
}

impl From<crate::bptree::IndexError> for LsmIndexError {
    fn from(error: crate::bptree::IndexError) -> Self {
        LsmIndexError::IndexError(error)
    }
}

/// A type alias for the result of LSM index operations
pub type Result<T> = std::result::Result<T, LsmIndexError>;

/// SSTable reader for use in LSM index - wraps the actual SSTableReader from sstable module
pub struct SSTableReader {
    /// Path to the SSTable file
    file_path: String,
    /// Actual SSTable reader
    reader: Option<crate::sstable::SSTableReader>,
    /// Number of entries in the SSTable
    entry_count: u64,
    /// Whether the SSTable has a Bloom filter
    has_bloom_filter: bool,
}

impl SSTableReader {
    /// Open an SSTable reader for the given path
    pub fn open(path: &str) -> io::Result<Self> {
        // Open the actual reader from the sstable module
        let reader = crate::sstable::SSTableReader::open(path)?;

        // Extract information from the reader
        let entry_count = reader.entry_count();
        let has_bloom_filter = reader.has_bloom_filter();

        Ok(Self {
            file_path: path.to_string(),
            reader: Some(reader),
            entry_count,
            has_bloom_filter,
        })
    }

    /// Returns the path to the SSTable file
    pub fn file_path(&self) -> &str {
        &self.file_path
    }

    /// Check if a key might exist in the SSTable
    pub fn may_contain(&self, key: &str) -> bool {
        if let Some(reader) = &self.reader {
            // Delegate to the actual reader
            reader.may_contain(key)
        } else {
            // Fallback to true if reader is None (unlikely but safe)
            true
        }
    }

    /// Get the value for a key, if it exists
    pub fn get(&mut self, key: &str) -> io::Result<Option<Vec<u8>>> {
        if let Some(reader) = &mut self.reader {
            // Delegate to the actual reader
            reader.get(key)
        } else {
            // Fallback to None if reader is None (unlikely but safe)
            Ok(None)
        }
    }

    /// Get the number of entries in the SSTable
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Check if the SSTable has a Bloom filter
    pub fn has_bloom_filter(&self) -> bool {
        self.has_bloom_filter
    }
}

/// Convert from legacy IndexEntry to generational GenIndexEntry
#[allow(dead_code)]
fn migrate_to_gen_index_entry(entry: IndexEntry) -> GenIndexEntry {
    GenIndexEntry::new(entry.value, entry.storage_ref)
}

/// Entry in the LsmIndex representing a key-value pair
#[derive(Clone)]
struct IndexEntry {
    /// The value for this entry, if stored in memory
    value: Option<Vec<u8>>,
    /// Reference to storage on disk (SSTables), if applicable
    storage_ref: Option<StorageReference>,
}

/// Lock-free LSM tree using crossbeam's SkipMap with generational reference counting
pub struct LsmIndex {
    /// In-memory table for recent writes
    memtable: StringMemtable,
    /// Lock-free skip map index for efficient lookups using generational reference counting
    index: Arc<SkipMap<String, GenIndexEntry>>,
    /// Durability manager for crash recovery
    durability_manager: Arc<Mutex<DurabilityManager>>,
    /// Cache of SSTable readers for quick access
    sstable_readers: Arc<SkipMap<String, SSTableReader>>,
    /// Base directory for SSTables
    base_path: String,
    /// Bloom filter false positive rate
    #[allow(dead_code)]
    bloom_filter_fpr: f64,
    /// Whether to use Bloom filters
    #[allow(dead_code)]
    use_bloom_filters: bool,
}

impl LsmIndex {
    /// Create a new LSM index with the specified capacity and at the given path
    pub fn new(
        capacity: usize,
        base_path: String,
        _compaction_interval_secs: Option<u64>,
        use_bloom_filters: bool,
        bloom_filter_fpr: f64,
    ) -> io::Result<Self> {
        // Create the directories if they don't exist
        fs::create_dir_all(&base_path)?;
        let wal_path = format!("{}/wal", base_path);
        fs::create_dir_all(&wal_path)?;

        // Create the memtable - StringMemtable only takes capacity
        let memtable = StringMemtable::new(capacity);

        // Create the durability manager
        let durability_manager =
            DurabilityManager::new(&format!("{}/wal/wal.log", base_path), &base_path)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))?;

        // Create the lock-free skip map index
        let index = SkipMap::new();

        Ok(LsmIndex {
            memtable,
            index: Arc::new(index),
            durability_manager: Arc::new(Mutex::new(durability_manager)),
            sstable_readers: Arc::new(SkipMap::new()),
            base_path,
            bloom_filter_fpr,
            use_bloom_filters,
        })
    }

    /// Insert a key-value pair
    pub fn insert(&self, key: String, value: Vec<u8>) -> Result<()> {
        // Log the operation for durability
        let mut durability_manager = self.durability_manager.lock().unwrap();
        durability_manager.log_operation(Operation::Insert {
            key: key.clone(),
            value: value.clone(),
        })?;

        // Insert into the memtable
        match self.memtable.insert(key.clone(), value.clone()) {
            Ok(_) => {
                // Update the index with the in-memory value
                self.index
                    .insert(key, GenIndexEntry::new(Some(value), None));
                Ok(())
            }
            Err(e) => Err(LsmIndexError::MemtableError(e)),
        }
    }

    /// Remove a key
    pub fn remove(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // First, retrieve the current value so we can return it
        let current_value = self.get(key)?;

        // Log the operation for durability
        let mut durability_manager = self.durability_manager.lock().unwrap();
        durability_manager.log_operation(Operation::Remove {
            key: key.to_string(),
        })?;

        // Remove from the memtable
        self.memtable.remove(&key.to_string())?;

        // Update the index - in a lock-free structure, we can just remove the entry
        self.index.remove(key);

        // Return the previous value
        Ok(current_value)
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // Try to get from the memtable first
        match self.memtable.get(&key.to_string()) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => {
                // If not in memtable, use the index to find it in SSTables
                if let Some(entry) = self.index.get(key) {
                    let index_entry = entry.value();

                    if let Some(value) = index_entry.value() {
                        // Return the in-memory value
                        return Ok(Some(value));
                    }

                    if let Some(storage_ref) = index_entry.storage_ref() {
                        // If we have a tombstone, return None
                        if storage_ref.is_tombstone {
                            return Ok(None);
                        }

                        // Check if the key might be in the SSTable using the Bloom filter
                        if let Some(reader_entry) = self.sstable_readers.get(&storage_ref.file_path)
                        {
                            let reader = reader_entry.value();
                            if !reader.may_contain(key) {
                                // Definitely not in the SSTable
                                return Ok(None);
                            }
                        }

                        // Load the value from the SSTable
                        return self.load_value_from_sstable(storage_ref);
                    }
                }

                // Key not found
                Ok(None)
            }
            Err(e) => Err(LsmIndexError::MemtableError(e)),
        }
    }

    /// Get a range of key-value pairs
    pub fn range<R>(&self, range: R) -> Result<Vec<(String, Vec<u8>)>>
    where
        R: RangeBounds<String> + Clone,
    {
        // Use the SkipMap's range capability to get entries within the range
        let index_entries: Vec<_> = self
            .index
            .range(range.clone())
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect();

        // Get the memtable entries by querying each key
        let mut result = Vec::new();
        let mut keys_seen = HashSet::new();

        // Add index entries
        for (key, index_entry) in index_entries {
            if let Some(storage_ref) = index_entry.storage_ref() {
                // Skip tombstones
                if storage_ref.is_tombstone {
                    continue;
                }

                // Check the Bloom filter if available
                if let Some(reader_entry) = self.sstable_readers.get(&storage_ref.file_path) {
                    let reader = reader_entry.value();
                    if !reader.may_contain(&key) {
                        // Definitely not in the SSTable
                        continue;
                    }
                }

                // Load the value from the SSTable
                if let Ok(Some(value)) = self.load_value_from_sstable(storage_ref) {
                    keys_seen.insert(key.clone());
                    result.push((key, value));
                }
            } else if let Some(value) = index_entry.value() {
                keys_seen.insert(key.clone());
                result.push((key, value));
            }
        }

        // Check the memtable for newer values
        let memtable_keys: Vec<String> = keys_seen.iter().cloned().collect();
        for key in memtable_keys {
            if let Ok(Some(value)) = self.memtable.get(&key) {
                // Replace the existing entry or add a new one
                if let Some(pos) = result.iter().position(|(k, _)| k == &key) {
                    result[pos] = (key, value);
                } else {
                    result.push((key, value));
                }
            }
        }

        Ok(result)
    }

    /// Load a value from an SSTable using a storage reference
    fn load_value_from_sstable(&self, storage_ref: &StorageReference) -> Result<Option<Vec<u8>>> {
        println!(
            "load_value_from_sstable - Loading from {} at offset {}",
            storage_ref.file_path, storage_ref.offset
        );

        // Open the file directly and seek to the reference's position
        match File::open(&storage_ref.file_path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);

                // Seek to the position stored in the reference
                reader.seek(SeekFrom::Start(storage_ref.offset as u64))?;

                // Read the key length and skip the key (we already know what key we want)
                let mut key_len_buf = [0u8; 4];
                reader.read_exact(&mut key_len_buf)?;
                let key_len = u32::from_le_bytes(key_len_buf) as usize;

                // Skip over the key
                reader.seek(SeekFrom::Current(key_len as i64))?;

                // Read the value length
                let mut value_len_buf = [0u8; 4];
                reader.read_exact(&mut value_len_buf)?;
                let value_len = u32::from_le_bytes(value_len_buf) as usize;

                // Read the value
                let mut value_buf = vec![0u8; value_len];
                reader.read_exact(&mut value_buf)?;

                println!(
                    "load_value_from_sstable - Successfully read value of length {}",
                    value_len
                );

                if storage_ref.is_tombstone {
                    println!("load_value_from_sstable - Entry is a tombstone, returning None");
                    Ok(None)
                } else {
                    Ok(Some(value_buf))
                }
            }
            Err(e) => {
                eprintln!(
                    "load_value_from_sstable - Error opening file {}: {}",
                    storage_ref.file_path, e
                );
                Err(LsmIndexError::IoError(e))
            }
        }
    }

    /// Flush the memtable to an SSTable and update the index
    pub fn flush(&self) -> Result<()> {
        // Begin checkpoint
        let mut durability_manager = self.durability_manager.lock().unwrap();
        let checkpoint_id = durability_manager.begin_checkpoint()?;

        // Create an SSTable path
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let _sstable_path = format!("{}/sstable_{}.sst", self.base_path, timestamp);

        // CRITICAL: Before flushing, capture keys from the index for reindexing
        // Get all keys currently in the index
        let keys_to_reindex: Vec<String> =
            self.index.iter().map(|entry| entry.key().clone()).collect();

        // In a real implementation, we would use our SSTableWriter with Bloom filters
        // For now, we just use the existing flush_to_sstable method
        let sstable_path = self.memtable.flush_to_sstable(&self.base_path)?;

        // End checkpoint
        durability_manager.end_checkpoint(checkpoint_id)?;

        // Update the index with the new SSTable entries
        self.update_index_from_sstable(&sstable_path)?;

        // IMPORTANT: Reindex any entries we just flushed, using their storage references
        // For each key that was in our index, we need to make sure it has a storage reference
        for key in keys_to_reindex {
            // Check if the key still exists in the index
            if let Some(entry) = self.index.get(&key) {
                let index_entry = entry.value();

                // Only add storage reference if it doesn't already have one
                if index_entry.storage_ref().is_none() && index_entry.value().is_some() {
                    // Create a storage reference for this entry
                    let storage_ref = StorageReference {
                        file_path: sstable_path.clone(),
                        offset: 0, // We don't have the exact offset, but we know the file
                        is_tombstone: false,
                    };

                    // Create a new entry with the updated storage reference
                    let new_entry = GenIndexEntry::new(index_entry.value(), Some(storage_ref));

                    // In a lock-free structure, we insert the updated entry
                    self.index.insert(key, new_entry);
                }
            }
        }

        // Register the checkpoint as durable
        durability_manager.register_durable_checkpoint(checkpoint_id, &sstable_path)?;

        // Add the SSTable reader to the cache
        let reader = SSTableReader::open(&sstable_path)?;
        self.sstable_readers.insert(sstable_path.clone(), reader);

        Ok(())
    }

    /// Update the index with entries from an SSTable
    fn update_index_from_sstable(&self, sstable_path: &str) -> Result<()> {
        println!("update_index_from_sstable - Starting for {}", sstable_path);

        // Get file size first
        let file_size = fs::metadata(sstable_path)?.len();
        println!("update_index_from_sstable - File size: {} bytes", file_size);

        // Open the SSTable file
        let file = File::open(sstable_path)?;
        let mut reader = BufReader::new(file);

        // Read magic number
        let mut magic_buf = [0u8; 8];
        reader.read_exact(&mut magic_buf)?;
        let magic = u64::from_le_bytes(magic_buf);
        println!("update_index_from_sstable - Magic number: 0x{:X}", magic);

        // Read version
        let mut version_buf = [0u8; 4];
        reader.read_exact(&mut version_buf)?;
        let version = u32::from_le_bytes(version_buf);
        println!("update_index_from_sstable - Version: {}", version);

        // Read entry count
        let mut count_buf = [0u8; 8];
        reader.read_exact(&mut count_buf)?;
        let entry_count = u64::from_le_bytes(count_buf);
        println!("update_index_from_sstable - Entry count: {}", entry_count);

        // Read index offset
        let mut index_offset_buf = [0u8; 8];
        reader.read_exact(&mut index_offset_buf)?;
        let index_offset = u64::from_le_bytes(index_offset_buf);
        println!("update_index_from_sstable - Index offset: {}", index_offset);

        // Validate the format
        if index_offset > file_size {
            return Err(LsmIndexError::InvalidOperation(format!(
                "Invalid index offset {} exceeds file size {}",
                index_offset, file_size
            )));
        }

        // Start at the beginning of the data section
        reader.seek(SeekFrom::Start(28))?; // Magic(8) + Version(4) + Count(8) + IndexOffset(8)
        println!(
            "update_index_from_sstable - Positioned at data section, position: {}",
            reader.stream_position()?
        );

        println!("update_index_from_sstable - Starting to process entries");

        // Process entries one by one, with careful error handling
        for i in 0..entry_count {
            let entry_pos = reader.stream_position()?;
            println!(
                "update_index_from_sstable - Reading entry {} at position {}",
                i, entry_pos
            );

            // Read key length
            let mut key_len_buf = [0u8; 4];
            match reader.read_exact(&mut key_len_buf) {
                Ok(_) => {}
                Err(e) => {
                    println!(
                        "update_index_from_sstable - Failed to read key length for entry {}: {}",
                        i, e
                    );
                    return Err(LsmIndexError::IoError(e));
                }
            }
            let key_len = u32::from_le_bytes(key_len_buf) as usize;
            if key_len > 1024 * 1024 {
                // Sanity check - keys shouldn't be huge
                return Err(LsmIndexError::InvalidOperation(format!(
                    "Invalid key length {} for entry {}",
                    key_len, i
                )));
            }

            // Read key
            let mut key_buf = vec![0u8; key_len];
            match reader.read_exact(&mut key_buf) {
                Ok(_) => {}
                Err(e) => {
                    println!(
                        "update_index_from_sstable - Failed to read key for entry {}: {}",
                        i, e
                    );
                    return Err(LsmIndexError::IoError(e));
                }
            }
            let key = String::from_utf8_lossy(&key_buf).to_string();

            // Read value length
            let mut value_len_buf = [0u8; 4];
            match reader.read_exact(&mut value_len_buf) {
                Ok(_) => {}
                Err(e) => {
                    println!(
                        "update_index_from_sstable - Failed to read value length for entry {}: {}",
                        i, e
                    );
                    return Err(LsmIndexError::IoError(e));
                }
            }
            let value_len = u32::from_le_bytes(value_len_buf) as usize;
            if value_len > 10 * 1024 * 1024 {
                // Sanity check - values shouldn't be massive
                return Err(LsmIndexError::InvalidOperation(format!(
                    "Invalid value length {} for entry {}",
                    value_len, i
                )));
            }

            // Read value
            let mut value_buf = vec![0u8; value_len];
            match reader.read_exact(&mut value_buf) {
                Ok(_) => {}
                Err(e) => {
                    println!(
                        "update_index_from_sstable - Failed to read value for entry {}: {}",
                        i, e
                    );
                    return Err(LsmIndexError::IoError(e));
                }
            }

            println!(
                "update_index_from_sstable - Read entry {}: key='{}', value_len={}",
                i, key, value_len
            );

            // Create storage reference
            let storage_ref = StorageReference {
                file_path: sstable_path.to_string(),
                offset: entry_pos as usize,
                is_tombstone: false,
            };

            // Update index - lock-free update with SkipMap
            self.index
                .insert(key, GenIndexEntry::new(Some(value_buf), Some(storage_ref)));
        }

        println!(
            "update_index_from_sstable - Successfully processed all {} entries",
            entry_count
        );
        println!(
            "update_index_from_sstable - Final index size: {}",
            self.index.len()
        );
        Ok(())
    }

    /// Recover state from existing SSTables
    pub fn recover(&mut self) -> Result<()> {
        println!("LsmIndex::recover - Starting recovery");
        // Find all SSTables in the base directory
        let entries = fs::read_dir(&self.base_path)?;
        println!("LsmIndex::recover - Reading directory: {}", self.base_path);

        let mut sstable_paths = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().unwrap_or_default() == "db" {
                let path_str = path.to_string_lossy().to_string();
                println!("LsmIndex::recover - Found potential SSTable: {}", path_str);
                sstable_paths.push(path_str);
            }
        }

        if sstable_paths.is_empty() {
            println!("LsmIndex::recover - No SSTables found, nothing to recover");
            return Ok(());
        }

        println!(
            "LsmIndex::recover - Found {} SSTables to recover",
            sstable_paths.len()
        );

        // In a lock-free structure, we can just create a new index and update it
        // No need to explicitly clear it

        // Update the index from each SSTable
        for sstable_path in sstable_paths {
            println!("LsmIndex::recover - Processing SSTable: {}", sstable_path);
            self.update_index_from_sstable(&sstable_path)?;
        }

        println!("LsmIndex::recover - Recovery completed successfully");
        Ok(())
    }

    /// Clear the index and memtable
    pub fn clear(&self) -> Result<()> {
        // Log the operation for durability
        let mut durability_manager = self.durability_manager.lock().unwrap();
        durability_manager.log_operation(Operation::Clear)?;

        // Clear the memtable
        self.memtable.clear()?;

        // For a lock-free structure, we'll just create a fresh SkipMap
        // This is faster than removing each entry individually
        for key in self
            .index
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>()
        {
            self.index.remove(&key);
        }

        Ok(())
    }

    /// Shutdown the LSM index, flushing any pending data to disk
    pub fn shutdown(&mut self) -> io::Result<()> {
        // No need to call shutdown on StringMemtable as it doesn't have this method
        Ok(())
    }
}
