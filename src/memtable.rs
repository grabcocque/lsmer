use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug};
use std::io;
use std::mem;
use std::ops::RangeBounds;

/// Error types for Memtable operations
#[derive(Debug)]
pub enum MemtableError {
    /// Returned when trying to add an entry to a memtable that's at max capacity
    CapacityExceeded = 0,
    /// Returned when a key doesn't exist in the memtable
    KeyNotFound = 1,
}

impl fmt::Display for MemtableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemtableError::CapacityExceeded => write!(f, "Memtable capacity exceeded"),
            MemtableError::KeyNotFound => write!(f, "Key not found in memtable"),
        }
    }
}

impl Error for MemtableError {}

/// Trait for calculating the size of an object in bytes
pub trait ByteSize {
    /// Calculate the size of this object in bytes
    fn byte_size(&self) -> usize;
}

impl ByteSize for String {
    fn byte_size(&self) -> usize {
        // String size = struct size + length of string data
        mem::size_of::<String>() + self.len()
    }
}

impl ByteSize for u8 {
    fn byte_size(&self) -> usize {
        mem::size_of::<u8>()
    }
}

impl ByteSize for Vec<u8> {
    fn byte_size(&self) -> usize {
        // For Vec<u8>, we just need the struct size plus the bytes it contains
        mem::size_of::<Vec<u8>>() + self.len()
    }
}

/// An in-memory memtable implementation backed by a BTreeMap to keep keys sorted
///
/// The memtable has a maximum capacity in bytes, and attempting to add entries beyond
/// that capacity will result in an error.
pub struct Memtable<K, V>
where
    K: Ord + Clone + Debug + ByteSize,
    V: Clone + Debug + ByteSize,
{
    data: BTreeMap<K, V>,
    max_capacity_bytes: usize,
    current_size_bytes: usize,
}

impl<K, V> Memtable<K, V>
where
    K: Ord + Clone + Debug + ByteSize,
    V: Clone + Debug + ByteSize,
{
    /// Creates a new memtable with the specified maximum capacity in bytes
    pub fn new(max_capacity_bytes: usize) -> Self {
        Memtable {
            data: BTreeMap::new(),
            max_capacity_bytes,
            current_size_bytes: 0,
        }
    }

    /// Returns the current number of entries in the memtable
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the memtable contains no entries
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the maximum capacity of the memtable in bytes
    pub fn max_capacity(&self) -> usize {
        self.max_capacity_bytes
    }

    /// Returns the current size of the memtable in bytes
    pub fn current_size(&self) -> usize {
        self.current_size_bytes
    }

    /// Returns true if the memtable is at maximum capacity
    pub fn is_full(&self) -> bool {
        self.current_size_bytes >= self.max_capacity_bytes
    }

    /// Calculates the size of a key-value pair in bytes
    fn calculate_entry_size(key: &K, value: &V) -> usize {
        key.byte_size() + value.byte_size()
    }

    /// Inserts a key-value pair into the memtable
    ///
    /// Returns an error if the memtable would exceed its capacity in bytes.
    /// If the key already exists, the value is updated and the old value is returned.
    pub fn insert(&mut self, key: K, value: V) -> Result<Option<V>, MemtableError> {
        let new_entry_size = Self::calculate_entry_size(&key, &value);

        if let Some(existing_value) = self.data.get(&key) {
            // Update case: calculate size difference
            let existing_size = Self::calculate_entry_size(&key, existing_value);
            let size_difference = new_entry_size as isize - existing_size as isize;

            // Check if the update would exceed capacity
            if size_difference > 0
                && (self.current_size_bytes as isize + size_difference)
                    > self.max_capacity_bytes as isize
            {
                return Err(MemtableError::CapacityExceeded);
            }

            // Perform the update
            let old_value = self.data.insert(key, value);

            // Adjust the current size
            self.current_size_bytes = (self.current_size_bytes as isize + size_difference) as usize;

            Ok(old_value)
        } else {
            // New entry case: check if adding would exceed capacity
            if self.current_size_bytes + new_entry_size > self.max_capacity_bytes {
                return Err(MemtableError::CapacityExceeded);
            }

            // Insert the new entry
            let old_value = self.data.insert(key, value);

            // Update size counter
            self.current_size_bytes += new_entry_size;

            Ok(old_value)
        }
    }

    /// Retrieves a value by key
    pub fn get(&self, key: &K) -> Option<&V> {
        self.data.get(key)
    }

    /// Removes an entry by key
    ///
    /// Returns the value if the key existed, or None if it didn't
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(value) = self.data.get(key) {
            let size = Self::calculate_entry_size(key, value);
            let removed = self.data.remove(key);

            if removed.is_some() {
                self.current_size_bytes -= size;
            }

            removed
        } else {
            None
        }
    }

    /// Returns an iterator over the entries in the memtable, in sorted order by key
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.data.iter()
    }

    /// Returns an iterator over entries with keys in the given range
    ///
    /// The range is inclusive on the start and exclusive on the end.
    /// If start is None, the range starts from the first key.
    /// If end is None, the range ends at the last key.
    pub fn range<Q, R>(&self, range: R) -> impl Iterator<Item = (&K, &V)>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
        R: RangeBounds<Q>,
    {
        self.data.range(range)
    }

    /// Clears all entries from the memtable
    pub fn clear(&mut self) {
        self.data.clear();
        self.current_size_bytes = 0;
    }
}

/// SSTable file format:
///
/// ```
/// +----------------+
/// | HEADER         |
/// | - Magic (8B)   |
/// | - Version (4B) |
/// | - Entry Count  |
/// | - Index Offset |
/// +----------------+
/// | DATA BLOCK     |
/// | - Value 1      |
/// | - Value 2      |
/// | - ...          |
/// +----------------+
/// | INDEX          |
/// | - Key 1, Offset|
/// | - Key 2, Offset|
/// | - ...          |
/// +----------------+
/// ```
impl Memtable<String, Vec<u8>> {
    /// Flushes the memtable contents to an SSTable on disk
    /// Returns the path to the created SSTable file
    pub fn flush_to_sstable(&mut self, base_path: &str) -> Result<String, io::Error> {
        use std::fs::File;
        use std::io::{Seek, SeekFrom, Write};
        use std::time::{SystemTime, UNIX_EPOCH};

        if self.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot flush empty memtable",
            ));
        }

        // Generate a unique filename for this SSTable
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let sstable_path = format!("{}/sstable_{}.db", base_path, timestamp);

        // Write memtable contents to the SSTable file
        let mut file = File::create(&sstable_path)?;

        // Constants
        const MAGIC: u64 = 0x4C534D_5353544142; // "LSM-SSTAB" in hex
        const VERSION: u32 = 1;

        // Write header (we'll update the index offset later)
        let entry_count = self.len() as u64;
        let mut index_offset: u64 = 0; // Placeholder, will update later

        // Write magic number and version
        file.write_all(&MAGIC.to_le_bytes())?;
        file.write_all(&VERSION.to_le_bytes())?;

        // Write entry count
        file.write_all(&entry_count.to_le_bytes())?;

        // Reserve space for index offset (we'll update it later)
        let index_offset_pos = file.seek(SeekFrom::Current(0))?;
        file.write_all(&index_offset.to_le_bytes())?;

        // Track data offsets for each key
        let mut key_offsets = Vec::with_capacity(self.len());

        // Write data block
        let data_start_pos = file.seek(SeekFrom::Current(0))?;

        for (key, value) in self.iter() {
            // Record the offset of this value
            let value_offset = file.seek(SeekFrom::Current(0))? - data_start_pos;
            key_offsets.push((key.clone(), value_offset));

            // Write value length and value
            let value_len = value.len() as u32;
            file.write_all(&value_len.to_le_bytes())?;
            file.write_all(value)?;
        }

        // Write index
        index_offset = file.seek(SeekFrom::Current(0))?;

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

        // Clear the memtable after successful flush
        self.clear();

        Ok(sstable_path)
    }

    /// Identifies groups of SSTables that should be compacted together based on similar size
    ///
    /// # Arguments
    ///
    /// * `sstables` - List of SSTable metadata
    /// * `size_ratio_threshold` - Maximum size ratio between smallest and largest SSTable in a group
    /// * `min_group_size` - Minimum number of SSTables to consider for compaction
    ///
    /// # Returns
    ///
    /// A vector of groups, where each group is a vector of indices into the original `sstables` list
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

    /// Compacts multiple SSTables into a single SSTable
    ///
    /// # Arguments
    ///
    /// * `sstable_paths` - Paths to the SSTable files to compact
    /// * `output_path` - Directory where the compacted SSTable will be written
    /// * `delete_originals` - Whether to delete the original SSTable files after successful compaction
    ///
    /// # Returns
    ///
    /// The path to the compacted SSTable file, or an error if compaction failed
    pub fn compact_sstables(
        sstable_paths: &[String],
        output_path: &str,
        delete_originals: bool,
    ) -> Result<String, io::Error> {
        if sstable_paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No SSTables provided for compaction",
            ));
        }

        use std::fs::{self, File};
        use std::io::{BufReader, Read, Seek, SeekFrom, Write};
        use std::time::{SystemTime, UNIX_EPOCH};

        // Constants for SSTable format
        const MAGIC: u64 = 0x4C534D_5353544142; // "LSM-SSTAB" in hex
        const VERSION: u32 = 1;
        const HEADER_MAGIC_SIZE: usize = 8;
        const HEADER_VERSION_SIZE: usize = 4;
        const HEADER_ENTRY_COUNT_SIZE: usize = 8;
        const HEADER_INDEX_OFFSET_SIZE: usize = 8;
        const HEADER_SIZE: usize = HEADER_MAGIC_SIZE
            + HEADER_VERSION_SIZE
            + HEADER_ENTRY_COUNT_SIZE
            + HEADER_INDEX_OFFSET_SIZE;

        // Create a BTreeMap to merge and sort all key-value pairs
        let mut merged_data: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut _total_entries = 0;

        // Read and merge all SSTables
        for path in sstable_paths {
            let file = File::open(path)?;
            let mut reader = BufReader::new(file);

            // Read and validate header
            let mut magic_bytes = [0u8; HEADER_MAGIC_SIZE];
            reader.read_exact(&mut magic_bytes)?;
            let magic = u64::from_le_bytes(magic_bytes);

            if magic != MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid SSTable magic number: {:x}", magic),
                ));
            }

            let mut version_bytes = [0u8; HEADER_VERSION_SIZE];
            reader.read_exact(&mut version_bytes)?;
            let version = u32::from_le_bytes(version_bytes);

            if version != VERSION {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unsupported SSTable version: {}", version),
                ));
            }

            // Read entry count
            let mut entry_count_bytes = [0u8; HEADER_ENTRY_COUNT_SIZE];
            reader.read_exact(&mut entry_count_bytes)?;
            let entry_count = u64::from_le_bytes(entry_count_bytes);
            _total_entries += entry_count;

            // Read index offset
            let mut index_offset_bytes = [0u8; HEADER_INDEX_OFFSET_SIZE];
            reader.read_exact(&mut index_offset_bytes)?;
            let index_offset = u64::from_le_bytes(index_offset_bytes);

            // Read the index to get key-value mappings
            reader.seek(SeekFrom::Start(index_offset))?;

            for _ in 0..entry_count {
                // Read key length
                let mut key_len_bytes = [0u8; 4];
                reader.read_exact(&mut key_len_bytes)?;
                let key_len = u32::from_le_bytes(key_len_bytes) as usize;

                // Read key
                let mut key_bytes = vec![0u8; key_len];
                reader.read_exact(&mut key_bytes)?;
                let key = String::from_utf8(key_bytes).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid UTF-8 in key")
                })?;

                // Read value offset
                let mut value_offset_bytes = [0u8; 8];
                reader.read_exact(&mut value_offset_bytes)?;
                let value_offset = u64::from_le_bytes(value_offset_bytes);

                // Save current position to return to index
                let index_position = reader.seek(SeekFrom::Current(0))?;

                // Seek to value position (data_start + value_offset)
                reader.seek(SeekFrom::Start(HEADER_SIZE as u64 + value_offset))?;

                // Read value length
                let mut value_len_bytes = [0u8; 4];
                reader.read_exact(&mut value_len_bytes)?;
                let value_len = u32::from_le_bytes(value_len_bytes) as usize;

                // Read value
                let mut value = vec![0u8; value_len];
                reader.read_exact(&mut value)?;

                // Store in merged data (newer values overwrite older ones)
                merged_data.insert(key, value);

                // Return to index position
                reader.seek(SeekFrom::Start(index_position))?;
            }
        }

        // Generate a unique filename for the compacted SSTable
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let compacted_path = format!("{}/compacted_sstable_{}.db", output_path, timestamp);

        // Write the compacted SSTable
        let mut file = File::create(&compacted_path)?;

        // Write header (we'll update the index offset later)
        let entry_count = merged_data.len() as u64;
        let mut index_offset: u64 = 0; // Placeholder, will update later

        // Write magic number and version
        file.write_all(&MAGIC.to_le_bytes())?;
        file.write_all(&VERSION.to_le_bytes())?;

        // Write entry count
        file.write_all(&entry_count.to_le_bytes())?;

        // Reserve space for index offset (we'll update it later)
        let index_offset_pos = file.seek(SeekFrom::Current(0))?;
        file.write_all(&index_offset.to_le_bytes())?;

        // Track data offsets for each key
        let mut key_offsets = Vec::with_capacity(merged_data.len());

        // Write data block
        let data_start_pos = file.seek(SeekFrom::Current(0))?;

        for (key, value) in &merged_data {
            // Record the offset of this value
            let value_offset = file.seek(SeekFrom::Current(0))? - data_start_pos;
            key_offsets.push((key.clone(), value_offset));

            // Write value length and value
            let value_len = value.len() as u32;
            file.write_all(&value_len.to_le_bytes())?;
            file.write_all(value)?;
        }

        // Write index
        index_offset = file.seek(SeekFrom::Current(0))?;

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

        // Delete original SSTable files if requested
        if delete_originals {
            for path in sstable_paths {
                if let Err(e) = fs::remove_file(path) {
                    // Log the error but continue with other files
                    eprintln!(
                        "Warning: Failed to delete original SSTable file {}: {}",
                        path, e
                    );
                }
            }
        }

        Ok(compacted_path)
    }
}

/// A specialized memtable for string keys and binary values
pub type StringMemtable = Memtable<String, Vec<u8>>;

/// Represents metadata about an SSTable file
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    /// Path to the SSTable file
    pub path: String,
    /// Size of the SSTable file in bytes
    pub size_bytes: u64,
    /// Number of entries in the SSTable
    pub entry_count: u64,
}
