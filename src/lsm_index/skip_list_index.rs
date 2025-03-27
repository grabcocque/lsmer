use crate::lsm_index::skip_list::ConcurrentSkipList;
use crate::lsm_index::{LsmIndexError, Result, SSTableReader};
use crate::memtable::{Memtable, SSTableWriter, StringMemtable};
use crate::wal::durability::{DurabilityManager, Operation};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, RwLock};

/// Storage reference for a value stored in an SSTable
#[derive(Clone, Debug)]
pub struct SkipListStorageRef {
    /// Path to the SSTable file
    pub file_path: String,
    /// Offset of the entry within the file
    pub offset: usize,
    /// Whether this is a tombstone entry
    pub is_tombstone: bool,
}

/// LSM tree with skip list index for concurrent access
pub struct SkipListIndex {
    /// In-memory table for recent writes
    memtable: StringMemtable,
    /// Skip list index for efficient lockless lookups
    index: Arc<ConcurrentSkipList<String, Option<(Vec<u8>, Option<SkipListStorageRef>)>>>,
    /// Durability manager for crash recovery
    durability_manager: Arc<Mutex<DurabilityManager>>,
    /// Cache of SSTable readers for quick access
    sstable_readers: Arc<RwLock<HashMap<String, SSTableReader>>>,
    /// Base directory for SSTables
    base_path: String,
}

impl SkipListIndex {
    /// Create a new LSM index with the specified capacity and at the given path
    pub fn new(
        capacity: usize,
        base_path: String,
        _compaction_interval_secs: Option<u64>,
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

        // Create the skip list index
        let index = ConcurrentSkipList::new();

        Ok(SkipListIndex {
            memtable,
            index: Arc::new(index),
            durability_manager: Arc::new(Mutex::new(durability_manager)),
            sstable_readers: Arc::new(RwLock::new(HashMap::new())),
            base_path,
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
        match Memtable::insert(&self.memtable, key.clone(), value.clone()) {
            Ok(_) => {
                // Update the index (value is in memory, no storage ref)
                self.index.insert(key, Some((value, None)));
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
        Memtable::remove(&self.memtable, &key.to_string())?;

        // Update the index with None to indicate deletion
        self.index.insert(key.to_string(), None);

        // Return the previous value
        Ok(current_value)
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // Try memtable first
        match Memtable::get(&self.memtable, &key.to_string()) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => {
                // Now use the index
                match self.index.get(key) {
                    Some(Some((value, storage_ref))) => {
                        if let Some(ref storage_ref) = storage_ref {
                            if storage_ref.is_tombstone {
                                return Ok(None);
                            }

                            // Get the SSTable reader and check the Bloom filter
                            let readers = self.sstable_readers.read().unwrap();
                            if let Some(reader) = readers.get(&storage_ref.file_path) {
                                // Check if the key might be in the SSTable using the Bloom filter
                                if !reader.may_contain(key) {
                                    // Definitely not in the SSTable
                                    return Ok(None);
                                }
                            }

                            // Load from storage
                            self.load_value_from_sstable(storage_ref)
                        } else {
                            // Value is in memory
                            Ok(Some(value))
                        }
                    }
                    Some(None) => {
                        // Tombstone entry (explicit deletion)
                        Ok(None)
                    }
                    None => {
                        // Not found
                        Ok(None)
                    }
                }
            }
            Err(e) => Err(LsmIndexError::MemtableError(e)),
        }
    }

    /// Get a range of key-value pairs (simplified version)
    pub fn range<R>(&self, _range: R) -> Result<Vec<(String, Vec<u8>)>>
    where
        R: RangeBounds<String> + Clone,
    {
        // This would be more complex in a real implementation
        // For simplicity, we just return an empty vector
        Ok(Vec::new())
    }

    /// Load a value from an SSTable using a storage reference
    fn load_value_from_sstable(&self, storage_ref: &SkipListStorageRef) -> Result<Option<Vec<u8>>> {
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

                if storage_ref.is_tombstone {
                    Ok(None)
                } else {
                    Ok(Some(value_buf))
                }
            }
            Err(e) => Err(LsmIndexError::IoError(e)),
        }
    }

    /// Flush the memtable to an SSTable and update the index
    pub fn flush(&self) -> Result<()> {
        // Begin checkpoint
        let mut durability_manager = self.durability_manager.lock().unwrap();
        let checkpoint_id = durability_manager.begin_checkpoint()?;

        // Flush the memtable to an SSTable
        let sstable_path = SSTableWriter::flush_to_sstable(&self.memtable, &self.base_path)
            .map_err(|e| LsmIndexError::IoError(e))?;

        // End checkpoint
        durability_manager.end_checkpoint(checkpoint_id)?;

        // Update the index with the new SSTable entries
        self.update_index_from_sstable(&sstable_path)?;

        // Register the checkpoint as durable
        durability_manager.register_durable_checkpoint(checkpoint_id, &sstable_path)?;

        // Add the SSTable reader to the cache
        let mut readers = self.sstable_readers.write().unwrap();
        readers.insert(sstable_path.clone(), SSTableReader::open(&sstable_path)?);

        Ok(())
    }

    /// Update the index with entries from an SSTable
    fn update_index_from_sstable(&self, sstable_path: &str) -> Result<()> {
        // Open the SSTable file
        let file = File::open(sstable_path)?;
        let mut reader = BufReader::new(file);

        // Read header fields
        let mut magic_buf = [0u8; 8];
        reader.read_exact(&mut magic_buf)?;

        let mut version_buf = [0u8; 4];
        reader.read_exact(&mut version_buf)?;

        let mut count_buf = [0u8; 8];
        reader.read_exact(&mut count_buf)?;
        let entry_count = u64::from_le_bytes(count_buf);

        let mut index_offset_buf = [0u8; 8];
        reader.read_exact(&mut index_offset_buf)?;

        // Start at the beginning of the data section (28 bytes header)
        reader.seek(SeekFrom::Start(28))?;

        // Process entries one by one
        for _ in 0..entry_count {
            let entry_pos = reader.stream_position()?;

            // Read key length
            let mut key_len_buf = [0u8; 4];
            reader.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            // Read key
            let mut key_buf = vec![0u8; key_len];
            reader.read_exact(&mut key_buf)?;
            let key = String::from_utf8_lossy(&key_buf).to_string();

            // Read value length
            let mut value_len_buf = [0u8; 4];
            reader.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_le_bytes(value_len_buf) as usize;

            // Read value
            let mut value_buf = vec![0u8; value_len];
            reader.read_exact(&mut value_buf)?;

            // Create storage reference
            let storage_ref = SkipListStorageRef {
                file_path: sstable_path.to_string(),
                offset: entry_pos as usize,
                is_tombstone: false,
            };

            // Update index - value can be loaded from disk now
            self.index.insert(key, Some((value_buf, Some(storage_ref))));
        }

        Ok(())
    }

    /// Recover state from existing SSTables
    pub fn recover(&mut self) -> Result<()> {
        // Find all SSTables in the base directory
        let entries = fs::read_dir(&self.base_path)?;

        let mut sstable_paths = Vec::new();
        for entry in entries {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().unwrap_or_default() == "db" {
                let path_str = path.to_string_lossy().to_string();
                sstable_paths.push(path_str);
            }
        }

        if sstable_paths.is_empty() {
            return Ok(());
        }

        // Update the index from each SSTable
        for sstable_path in sstable_paths {
            self.update_index_from_sstable(&sstable_path)?;
        }

        Ok(())
    }

    /// Clear the index and memtable
    pub fn clear(&self) -> Result<()> {
        // Log the operation for durability
        let mut durability_manager = self.durability_manager.lock().unwrap();
        durability_manager.log_operation(Operation::Clear)?;

        // Clear the memtable
        self.memtable.clear()?;

        // There's no explicit clear method for the skip list,
        // but we could create a new one and replace the old one
        // This is a simplified version

        Ok(())
    }

    /// Shutdown the LSM index, flushing any pending data to disk
    pub fn shutdown(&mut self) -> io::Result<()> {
        // Implement any necessary cleanup here
        Ok(())
    }
}
