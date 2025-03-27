use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::error::MemtableError;
use super::traits::{ByteSize, Memtable, SSTableWriter};
use crate::sstable::{SSTableCompaction, SSTableInfo, MAGIC, VERSION};

/// A string-based memtable implementation
#[derive(Debug)]
pub struct StringMemtable {
    data: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
    max_size_bytes: usize,
    current_size_bytes: Arc<RwLock<usize>>,
}

impl StringMemtable {
    pub fn new(max_size_bytes: usize) -> Self {
        StringMemtable {
            data: Arc::new(RwLock::new(BTreeMap::new())),
            max_size_bytes,
            current_size_bytes: Arc::new(RwLock::new(0)),
        }
    }

    pub fn max_capacity(&self) -> usize {
        self.max_size_bytes
    }

    pub fn current_size(&self) -> Result<usize, MemtableError> {
        self.current_size_bytes
            .read()
            .map_err(|_| MemtableError::LockError)
            .map(|guard| *guard)
    }

    pub fn is_full(&self) -> Result<bool, MemtableError> {
        Ok(self.current_size()? >= self.max_size_bytes)
    }

    pub fn iter(&self) -> Result<Vec<(String, Vec<u8>)>, MemtableError> {
        let guard = self.data.read().map_err(|_| MemtableError::LockError)?;
        Ok(guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    pub fn range<R>(&self, range: R) -> Result<Vec<(String, Vec<u8>)>, MemtableError>
    where
        R: RangeBounds<String>,
    {
        let guard = self.data.read().map_err(|_| MemtableError::LockError)?;
        Ok(guard
            .range(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }

    fn generate_timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
    }
}

impl Memtable<String, Vec<u8>> for StringMemtable {
    fn insert(&self, key: String, value: Vec<u8>) -> Result<Option<Vec<u8>>, MemtableError> {
        let key_size = key.byte_size();
        let value_size = value.byte_size();
        let entry_size = key_size + value_size + std::mem::size_of::<usize>(); // Additional overhead for BTreeMap node

        let mut size_guard = self
            .current_size_bytes
            .write()
            .map_err(|_| MemtableError::LockError)?;

        // Check if adding this entry would exceed capacity
        if let Some(old_value) = self.get(&key)? {
            let old_size = key_size + old_value.byte_size() + std::mem::size_of::<usize>();
            if *size_guard - old_size + entry_size > self.max_size_bytes {
                return Err(MemtableError::CapacityExceeded);
            }
        } else if *size_guard + entry_size > self.max_size_bytes {
            return Err(MemtableError::CapacityExceeded);
        }

        let mut data_guard = self.data.write().map_err(|_| MemtableError::LockError)?;

        let old_value = data_guard.insert(key, value);
        if let Some(old_val) = &old_value {
            let old_size = key_size + old_val.byte_size() + std::mem::size_of::<usize>();
            *size_guard = *size_guard - old_size + entry_size;
        } else {
            *size_guard += entry_size;
        }

        Ok(old_value)
    }

    fn get(&self, key: &String) -> Result<Option<Vec<u8>>, MemtableError> {
        let guard = self.data.read().map_err(|_| MemtableError::LockError)?;
        Ok(guard.get(key).cloned())
    }

    fn remove(&self, key: &String) -> Result<Option<Vec<u8>>, MemtableError> {
        let mut data_guard = self.data.write().map_err(|_| MemtableError::LockError)?;
        let mut size_guard = self
            .current_size_bytes
            .write()
            .map_err(|_| MemtableError::LockError)?;

        let old_value = data_guard.remove(key);
        if let Some(old_val) = &old_value {
            *size_guard -= key.byte_size() + old_val.byte_size();
        }
        Ok(old_value)
    }

    fn len(&self) -> Result<usize, MemtableError> {
        let guard = self.data.read().map_err(|_| MemtableError::LockError)?;
        Ok(guard.len())
    }

    fn is_empty(&self) -> Result<bool, MemtableError> {
        let guard = self.data.read().map_err(|_| MemtableError::LockError)?;
        Ok(guard.is_empty())
    }

    fn clear(&self) -> Result<(), MemtableError> {
        let mut data_guard = self.data.write().map_err(|_| MemtableError::LockError)?;
        let mut size_guard = self
            .current_size_bytes
            .write()
            .map_err(|_| MemtableError::LockError)?;

        data_guard.clear();
        *size_guard = 0;
        Ok(())
    }

    fn size_bytes(&self) -> Result<usize, MemtableError> {
        self.current_size()
    }
}

impl SSTableWriter for StringMemtable {
    fn flush_to_sstable(&self, base_path: &str) -> io::Result<String> {
        println!("flush_to_sstable: Starting to flush memtable");

        // Clone the data while holding a read lock, and then release it immediately
        let data_clone: Vec<(String, Vec<u8>)>;
        {
            let guard = self.data.read().map_err(|_| {
                println!("flush_to_sstable: Failed to acquire read lock on data");
                io::Error::new(io::ErrorKind::Other, "Failed to acquire read lock on data")
            })?;
            println!(
                "flush_to_sstable: Acquired read lock, found {} items",
                guard.len()
            );

            // Clone data to avoid holding the lock during file I/O
            data_clone = guard.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        } // read lock is released here
        println!("flush_to_sstable: Released read lock after cloning");

        // Generate a unique filename for the SSTable
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let sstable_path = format!("{}/sstable_{}.db", base_path, timestamp);
        println!("flush_to_sstable: Generated SSTable path: {}", sstable_path);

        // Create the SSTable file
        println!("flush_to_sstable: Creating SSTable file");
        let mut file = match File::create(&sstable_path) {
            Ok(f) => f,
            Err(e) => {
                println!("flush_to_sstable: Failed to create file: {}", e);
                return Err(e);
            }
        };
        println!("flush_to_sstable: File created successfully");

        // Write header (we'll update the index offset later)
        let entry_count = data_clone.len() as u64;
        let mut index_offset: u64 = 0; // Placeholder, will update later

        // Write magic number and version
        file.write_all(&MAGIC.to_le_bytes())?;
        file.write_all(&VERSION.to_le_bytes())?;

        // Write entry count
        file.write_all(&entry_count.to_le_bytes())?;

        // Reserve space for index offset (we'll update it later)
        let index_offset_pos = file.stream_position()?;
        file.write_all(&index_offset.to_le_bytes())?;

        // Track data offsets for each key
        let mut key_offsets = Vec::with_capacity(data_clone.len());

        // Write data block
        let data_start_pos = file.stream_position()?;

        for (key, value) in &data_clone {
            // Record the offset of this value
            let value_offset = file.stream_position()? - data_start_pos;
            key_offsets.push((key.clone(), value_offset));

            // Write key length and key
            let key_len = key.len() as u32;
            file.write_all(&key_len.to_le_bytes())?;
            file.write_all(key.as_bytes())?;

            // Write value length and value
            let value_len = value.len() as u32;
            file.write_all(&value_len.to_le_bytes())?;
            file.write_all(value)?;
        }

        // Write index
        index_offset = file.stream_position()?;

        // Sort keys for better binary search later
        key_offsets.sort_by(|(a, _), (b, _)| a.cmp(b));

        // Write each key and its value offset
        for (key, offset) in key_offsets {
            // Write key length, key, and offset
            let key_len = key.len() as u32;
            file.write_all(&key_len.to_le_bytes())?;
            file.write_all(key.as_bytes())?;
            file.write_all(&offset.to_le_bytes())?;
        }

        // Update the index offset in the header
        file.seek(SeekFrom::Start(index_offset_pos))?;
        file.write_all(&index_offset.to_le_bytes())?;
        println!("flush_to_sstable: Updated index offset in header");

        // Clear the memtable after successful flush
        println!("flush_to_sstable: Clearing memtable");
        {
            let mut data_guard = self.data.write().map_err(|_| {
                println!("flush_to_sstable: Failed to acquire write lock on data");
                io::Error::new(io::ErrorKind::Other, "Failed to acquire write lock on data")
            })?;
            let mut size_guard = self.current_size_bytes.write().map_err(|_| {
                println!("flush_to_sstable: Failed to acquire write lock on size");
                io::Error::new(io::ErrorKind::Other, "Failed to acquire write lock on size")
            })?;
            data_guard.clear();
            *size_guard = 0;
        } // write locks are released here
        println!(
            "flush_to_sstable: Memtable cleared, returning path: {}",
            sstable_path
        );

        Ok(sstable_path)
    }
}

// Add SSTable compaction methods
impl StringMemtable {
    // ... existing methods ...

    pub fn identify_compaction_groups(
        sstables: &[SSTableInfo],
        size_ratio_threshold: f64,
        min_group_size: usize,
    ) -> Vec<Vec<usize>> {
        if sstables.len() < min_group_size {
            return Vec::new();
        }

        // Sort SSTables by size
        let mut sorted_indices: Vec<usize> = (0..sstables.len()).collect();
        sorted_indices.sort_by_key(|&i| sstables[i].size_bytes);

        let mut compaction_groups = Vec::new();
        let mut current_group = Vec::new();
        let mut smallest_size = sstables[sorted_indices[0]].size_bytes;

        for &idx in &sorted_indices {
            let current_size = sstables[idx].size_bytes;

            // If the size ratio exceeds our threshold, start a new group
            if current_size as f64 / smallest_size as f64 > size_ratio_threshold
                && !current_group.is_empty()
            {
                if current_group.len() >= min_group_size {
                    compaction_groups.push(current_group);
                }
                current_group = vec![idx];
                smallest_size = current_size;
            } else {
                current_group.push(idx);
            }
        }

        // Add the last group if it meets the minimum size requirement
        if current_group.len() >= min_group_size {
            compaction_groups.push(current_group);
        }

        compaction_groups
    }

    pub fn compact_sstables(
        &self,
        base_path: &str,
        sstables: &[SSTableInfo],
        delete_originals: bool,
    ) -> io::Result<String> {
        // Use the SSTableCompaction from our import
        SSTableCompaction::compact_sstables(
            &sstables
                .iter()
                .map(|info| info.path.clone())
                .collect::<Vec<_>>(),
            &format!("{}/merged_{}.sst", base_path, self.generate_timestamp()),
            delete_originals,
            true, // use bloom filter
            0.01, // false positive rate
        )
    }
}
