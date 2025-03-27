use crate::bloom::{BloomFilter, PartitionedBloomFilter};
use crc32fast;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};

/// Calculate a CRC32 checksum
fn calculate_checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// Represents metadata about an SSTable file
#[derive(Debug, Clone)]
pub struct SSTableInfo {
    /// Path to the SSTable file
    pub path: String,
    /// Size of the SSTable file in bytes
    pub size_bytes: u64,
    /// Number of entries in the SSTable
    pub entry_count: u64,
    /// Flag indicating if this SSTable has a Bloom filter
    pub has_bloom_filter: bool,
}

/// Constants for SSTable format
pub const MAGIC: u64 = 0x4C534D_5353544142; // "LSM-SSTAB" in hex
pub const VERSION: u32 = 3; // Updated to version 3 to support block checksums
pub const HEADER_MAGIC_SIZE: usize = 8;
pub const HEADER_VERSION_SIZE: usize = 4;
pub const HEADER_ENTRY_COUNT_SIZE: usize = 8;
pub const HEADER_INDEX_OFFSET_SIZE: usize = 8;
pub const HEADER_BLOOM_OFFSET_SIZE: usize = 8; // Offset to bloom filter
pub const HEADER_BLOOM_SIZE_SIZE: usize = 8; // Size of bloom filter in bytes
pub const HEADER_HAS_BLOOM_SIZE: usize = 1; // Flag indicating if bloom filter exists
pub const HEADER_CHECKSUM_SIZE: usize = 4; // File header checksum
pub const HEADER_SIZE: usize = HEADER_MAGIC_SIZE
    + HEADER_VERSION_SIZE
    + HEADER_ENTRY_COUNT_SIZE
    + HEADER_INDEX_OFFSET_SIZE
    + HEADER_BLOOM_OFFSET_SIZE
    + HEADER_BLOOM_SIZE_SIZE
    + HEADER_HAS_BLOOM_SIZE
    + HEADER_CHECKSUM_SIZE;

/// SSTable writer that supports both regular and partitioned Bloom filters
pub struct SSTableWriter {
    file: File,
    entry_count: u64,
    bloom_filter: Option<BloomFilter<String>>,
    partitioned_bloom_filter: Option<PartitionedBloomFilter<String>>,
    index_offset: u64,
    bloom_offset: u64,
    bloom_size: u64,
    has_bloom_filter: bool,
    #[allow(dead_code)] // For future optimistic concurrency implementation
    use_partitioned_bloom: bool,
    checksums: Vec<u32>, // Added checksums for data blocks
}

impl SSTableWriter {
    /// Create a new SSTable writer with optional Bloom filter
    pub fn new(
        path: &str,
        expected_entries: usize,
        use_bloom_filter: bool,
        false_positive_rate: f64,
    ) -> io::Result<Self> {
        Self::new_with_options(
            path,
            expected_entries,
            use_bloom_filter,
            false_positive_rate,
            false,
        )
    }

    /// Create a new SSTable writer with additional options for partitioned bloom filter
    pub fn new_with_options(
        path: &str,
        expected_entries: usize,
        use_bloom_filter: bool,
        false_positive_rate: f64,
        use_partitioned_bloom: bool,
    ) -> io::Result<Self> {
        let file = File::create(path)?;

        // Create appropriate bloom filter type if requested
        let (bloom_filter, partitioned_bloom_filter) = if use_bloom_filter {
            if use_partitioned_bloom {
                // Use partitioned bloom filter for parallel lookups
                let num_partitions = num_cpus::get();
                (
                    None,
                    Some(PartitionedBloomFilter::new(
                        expected_entries,
                        false_positive_rate,
                        num_partitions,
                    )),
                )
            } else {
                // Use regular bloom filter
                (
                    Some(BloomFilter::new(expected_entries, false_positive_rate)),
                    None,
                )
            }
        } else {
            (None, None)
        };

        let mut writer = SSTableWriter {
            file,
            entry_count: 0,
            bloom_filter,
            partitioned_bloom_filter,
            index_offset: 0,
            bloom_offset: 0,
            bloom_size: 0,
            has_bloom_filter: use_bloom_filter,
            #[allow(dead_code)] // For future optimistic concurrency implementation
            use_partitioned_bloom,
            checksums: Vec::new(),
        };

        // Write header with placeholders for values we'll fill in later
        writer.write_header()?;

        Ok(writer)
    }

    /// Write a key-value pair to the SSTable
    pub fn write_entry(&mut self, key: &str, value: &[u8]) -> io::Result<()> {
        // Write key length (4 bytes)
        let key_len = key.len() as u32;
        self.file.write_all(&key_len.to_le_bytes())?;

        // Write key
        self.file.write_all(key.as_bytes())?;

        // Write value length (4 bytes)
        let value_len = value.len() as u32;
        self.file.write_all(&value_len.to_le_bytes())?;

        // Write value
        self.file.write_all(value)?;

        // Calculate and store checksum for this entry
        let mut entry_data = Vec::new();
        entry_data.extend_from_slice(&key_len.to_le_bytes());
        entry_data.extend_from_slice(key.as_bytes());
        entry_data.extend_from_slice(&value_len.to_le_bytes());
        entry_data.extend_from_slice(value);

        let checksum = calculate_checksum(&entry_data);
        self.file.write_all(&checksum.to_le_bytes())?;
        self.checksums.push(checksum);

        // Add key to appropriate bloom filter if enabled
        if let Some(ref mut bloom) = self.bloom_filter {
            bloom.insert(&key.to_string());
        } else if let Some(ref mut bloom) = self.partitioned_bloom_filter {
            bloom.insert(&key.to_string());
        }

        // Update entry count
        self.entry_count += 1;

        Ok(())
    }

    /// Finalize the SSTable by writing the index and Bloom filter
    pub fn finalize(mut self) -> io::Result<()> {
        // Remember the current position - this is where the index starts
        self.index_offset = self.file.stream_position()?;

        // Write the index (empty for now as we're not using it yet)
        // This is a placeholder for future enhancements

        // Write bloom filter if enabled
        if self.has_bloom_filter {
            self.bloom_offset = self.file.stream_position()?;

            if let Some(ref bloom) = self.bloom_filter {
                // Write standard bloom filter metadata and data
                let bloom_size_bits = bloom.size_bits();
                let bloom_num_hashes = bloom.num_hashes();

                // First, write bloom filter type (0 = standard)
                println!("Writing standard bloom filter (type 0)");
                self.file.write_all(&[0u8])?;

                // Write metadata
                println!("Writing size_bits: {}", bloom_size_bits);
                self.file
                    .write_all(&(bloom_size_bits as u64).to_le_bytes())?;

                println!("Writing num_hashes: {}", bloom_num_hashes);
                self.file
                    .write_all(&(bloom_num_hashes as u32).to_le_bytes())?;

                // Write bloom filter data
                let bits = bloom.get_bits();
                println!("Writing {} bytes of bloom data", bits.len());
                for byte in bits {
                    self.file.write_all(&[*byte])?;
                }
            } else if let Some(ref bloom) = self.partitioned_bloom_filter {
                // For partitioned bloom filter, we'll serialize each partition individually

                // Get the number of partitions
                let num_partitions = bloom.num_partitions();

                // First write the filter type byte (1 = partitioned)
                println!("Writing partitioned bloom filter (type 1)");
                self.file.write_all(&[1u8])?;

                // Then write number of partitions
                println!("Writing num_partitions: {}", num_partitions);
                self.file
                    .write_all(&(num_partitions as u32).to_le_bytes())?;

                // Since we're serializing actual partitions, we need to get size_bits/num_hashes from the first partition
                // We'll just use these as metadata for compatibility - not actually used since each partition has its own
                let size_bits = if let Some(partition) = bloom.get_partition(0) {
                    partition.size_bits()
                } else {
                    100000 // Fallback value
                };

                let num_hashes = if let Some(partition) = bloom.get_partition(0) {
                    partition.num_hashes()
                } else {
                    7 // Fallback value
                };

                println!("Writing partition metadata size_bits: {}", size_bits);
                self.file.write_all(&(size_bits as u64).to_le_bytes())?;

                println!("Writing partition metadata num_hashes: {}", num_hashes);
                self.file.write_all(&(num_hashes as u32).to_le_bytes())?;

                // Now write each partition's data
                for i in 0..num_partitions {
                    if let Some(partition) = bloom.get_partition(i) {
                        // Get this partition's bits
                        let bits = partition.get_bits();

                        // Write size of this partition's bit array
                        let bits_len = bits.len() as u32;
                        println!("Writing partition {} bits length: {}", i, bits_len);
                        self.file.write_all(&bits_len.to_le_bytes())?;

                        // Write the partition's bits
                        println!("Writing partition {} data ({} bytes)", i, bits.len());
                        for byte in bits {
                            self.file.write_all(&[*byte])?;
                        }
                    } else {
                        // Write empty partition as fallback
                        println!("Writing empty partition {}", i);
                        self.file.write_all(&0u32.to_le_bytes())?; // 0 length
                    }
                }
            }

            // Calculate bloom filter size for header
            self.bloom_size = self.file.stream_position()? - self.bloom_offset;
        }

        // Write file checksums
        let _file_checksums_offset = self.file.stream_position()?;
        for checksum in &self.checksums {
            self.file.write_all(&checksum.to_le_bytes())?;
        }

        // Go back to the beginning and write the header
        self.file.seek(SeekFrom::Start(0))?;
        self.write_header()?;

        // Ensure all data is written to disk
        self.file.sync_all()?;

        Ok(())
    }

    /// Write the SSTable header
    fn write_header(&mut self) -> io::Result<()> {
        // Magic number (8 bytes)
        self.file.write_all(&MAGIC.to_le_bytes())?;

        // Version (4 bytes)
        self.file.write_all(&VERSION.to_le_bytes())?;

        // Entry count (8 bytes)
        self.file.write_all(&self.entry_count.to_le_bytes())?;

        // Index offset (8 bytes)
        self.file.write_all(&self.index_offset.to_le_bytes())?;

        // Bloom filter offset (8 bytes)
        self.file.write_all(&self.bloom_offset.to_le_bytes())?;

        // Bloom filter size (8 bytes)
        self.file.write_all(&self.bloom_size.to_le_bytes())?;

        // Has bloom filter flag (1 byte)
        self.file.write_all(&[self.has_bloom_filter as u8])?;

        // Calculate header checksum (excluding the checksum field itself)
        let mut header_data = Vec::new();
        header_data.extend_from_slice(&MAGIC.to_le_bytes());
        header_data.extend_from_slice(&VERSION.to_le_bytes());
        header_data.extend_from_slice(&self.entry_count.to_le_bytes());
        header_data.extend_from_slice(&self.index_offset.to_le_bytes());
        header_data.extend_from_slice(&self.bloom_offset.to_le_bytes());
        header_data.extend_from_slice(&self.bloom_size.to_le_bytes());
        header_data.push(self.has_bloom_filter as u8);

        let header_checksum = calculate_checksum(&header_data);
        self.file.write_all(&header_checksum.to_le_bytes())?;

        Ok(())
    }
}

/// SSTable reader that supports Bloom filters
#[derive(Debug)]
pub struct SSTableReader {
    file: BufReader<File>,
    entry_count: u64,
    index_offset: u64,
    bloom_offset: u64, // Add this field to store bloom filter offset
    bloom_filter: Option<BloomFilter<String>>,
    partitioned_bloom_filter: Option<PartitionedBloomFilter<String>>,
    has_bloom_filter: bool,
    #[allow(dead_code)] // Needed for future data integrity features
    block_checksums: Vec<u32>, // Added checksums for data blocks
    #[allow(dead_code)] // Needed for future data integrity features
    header_checksum: u32, // Header checksum for verification
}

impl SSTableReader {
    /// Open an SSTable for reading
    pub fn open(path: &str) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read header
        let mut magic_buf = [0u8; 8];
        reader.read_exact(&mut magic_buf)?;
        let magic = u64::from_le_bytes(magic_buf);
        println!("Header: Magic = {:X}", magic);
        if magic != MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid magic number - not an SSTable file",
            ));
        }

        let mut version_buf = [0u8; 4];
        reader.read_exact(&mut version_buf)?;
        let version = u32::from_le_bytes(version_buf);
        println!("Header: Version = {}", version);
        if version > VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unsupported SSTable version: {}", version),
            ));
        }

        let mut entry_count_buf = [0u8; 8];
        reader.read_exact(&mut entry_count_buf)?;
        let entry_count = u64::from_le_bytes(entry_count_buf);
        println!("Header: Entry count = {}", entry_count);

        let mut index_offset_buf = [0u8; 8];
        reader.read_exact(&mut index_offset_buf)?;
        let index_offset = u64::from_le_bytes(index_offset_buf);
        println!("Header: Index offset = {}", index_offset);

        let mut bloom_offset_buf = [0u8; 8];
        reader.read_exact(&mut bloom_offset_buf)?;
        let bloom_offset = u64::from_le_bytes(bloom_offset_buf);
        println!("Header: Bloom offset = {}", bloom_offset);

        let mut bloom_size_buf = [0u8; 8];
        reader.read_exact(&mut bloom_size_buf)?;
        let bloom_size = u64::from_le_bytes(bloom_size_buf);
        println!("Header: Bloom size = {}", bloom_size);

        let mut has_bloom_buf = [0u8; 1];
        reader.read_exact(&mut has_bloom_buf)?;
        let has_bloom_filter = has_bloom_buf[0] != 0;
        println!("Header: Has bloom filter = {}", has_bloom_filter);

        let mut header_checksum_buf = [0u8; 4];
        reader.read_exact(&mut header_checksum_buf)?;
        let header_checksum = u32::from_le_bytes(header_checksum_buf);
        println!("Header: Checksum = {}", header_checksum);

        // Create new reader instance
        let mut sstable_reader = SSTableReader {
            file: reader,
            entry_count,
            index_offset,
            bloom_offset, // Add this field to use the bloom offset value
            bloom_filter: None,
            partitioned_bloom_filter: None,
            has_bloom_filter,
            #[allow(dead_code)] // Needed for future data integrity features
            block_checksums: Vec::new(),
            #[allow(dead_code)] // Needed for future data integrity features
            header_checksum,
        };

        // Load the bloom filter if present
        if has_bloom_filter {
            sstable_reader.load_bloom_filter()?;
        }

        Ok(sstable_reader)
    }

    /// Load the Bloom filter from the SSTable file
    fn load_bloom_filter(&mut self) -> io::Result<()> {
        if !self.has_bloom_filter {
            return Ok(());
        }

        // Position the file at the bloom filter offset from the header
        let file_pos = self.file.stream_position()?;
        println!("Current file position: {}", file_pos);

        // Use the bloom_offset directly from the header
        println!("Seeking to bloom filter offset: {}", self.bloom_offset);
        self.file.seek(SeekFrom::Start(self.bloom_offset))?;

        // Dump a few bytes from this position to see what's in the file
        let mut preview_buf = [0u8; 16];
        let bytes_read = self.file.read(&mut preview_buf)?;
        println!(
            "Preview bytes at bloom filter offset (read {} bytes): {:?}",
            bytes_read, preview_buf
        );

        // Seek back to the start position
        self.file.seek(SeekFrom::Start(self.bloom_offset))?;

        // First, read the bloom filter type byte
        let mut bloom_type_buf = [0u8; 1];
        self.file.read_exact(&mut bloom_type_buf)?;
        let bloom_type = bloom_type_buf[0];
        println!("Bloom filter type: {}", bloom_type);

        // Process based on bloom filter type
        match bloom_type {
            0 => {
                // Standard bloom filter - read size and hash count
                let mut size_bits_buf = [0u8; 8];
                self.file.read_exact(&mut size_bits_buf)?;
                println!("Raw size_bits_buf: {:?}", size_bits_buf);
                let size_bits = u64::from_le_bytes(size_bits_buf) as usize;
                println!("Parsed size_bits: {}", size_bits);

                let mut num_hashes_buf = [0u8; 4];
                self.file.read_exact(&mut num_hashes_buf)?;
                let num_hashes = u32::from_le_bytes(num_hashes_buf) as usize;
                println!("Parsed num_hashes: {}", num_hashes);

                // Sanity check for bloom filter size
                const MAX_BLOOM_FILTER_BITS: usize = 100_000_000; // 100M bits (12.5MB) is reasonably large
                if size_bits > MAX_BLOOM_FILTER_BITS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Bloom filter bits too large: {} bits", size_bits),
                    ));
                }

                // Reasonable limit for number of hash functions
                const MAX_HASH_FUNCTIONS: usize = 20;
                if num_hashes > MAX_HASH_FUNCTIONS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unreasonable number of hash functions: {}", num_hashes),
                    ));
                }

                // Calculate the number of bytes needed for the bloom filter
                let size_bytes = match (size_bits + 7).checked_div(8) {
                    Some(bytes) => bytes,
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "Integer overflow calculating bloom filter size",
                        ));
                    }
                };

                // One more safety check on the byte size
                if size_bytes > MAX_BLOOM_FILTER_BITS / 8 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Bloom filter byte size too large: {} bytes", size_bytes),
                    ));
                }

                // Read bloom filter data
                let mut bits = vec![0u8; size_bytes];
                self.file.read_exact(&mut bits)?;

                // Create a new bloom filter with the loaded data
                let bloom_filter = BloomFilter::<String>::from_parts(bits, size_bits, num_hashes);
                self.bloom_filter = Some(bloom_filter);
            }
            1 => {
                // Partitioned bloom filter - read number of partitions first
                let mut num_partitions_buf = [0u8; 4];
                self.file.read_exact(&mut num_partitions_buf)?;
                let num_partitions = u32::from_le_bytes(num_partitions_buf) as usize;
                println!("Partitions: {}", num_partitions);

                // Safety check for number of partitions
                if num_partitions == 0 || num_partitions > 64 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid number of partitions: {}", num_partitions),
                    ));
                }

                // Read the metadata (size_bits and num_hashes)
                // These are used as overall metadata for the partitioned filter
                let mut size_bits_buf = [0u8; 8];
                self.file.read_exact(&mut size_bits_buf)?;
                let size_bits = u64::from_le_bytes(size_bits_buf) as usize;
                println!("Metadata size_bits: {}", size_bits);

                let mut num_hashes_buf = [0u8; 4];
                self.file.read_exact(&mut num_hashes_buf)?;
                let num_hashes = u32::from_le_bytes(num_hashes_buf) as usize;
                println!("Metadata num_hashes: {}", num_hashes);

                // Safety checks
                const MAX_BLOOM_FILTER_BITS: usize = 100_000_000;
                if size_bits > MAX_BLOOM_FILTER_BITS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Bloom filter bits too large: {} bits", size_bits),
                    ));
                }

                const MAX_HASH_FUNCTIONS: usize = 20;
                if num_hashes > MAX_HASH_FUNCTIONS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unreasonable number of hash functions: {}", num_hashes),
                    ));
                }

                // Create a new partitioned bloom filter with expected parameters
                // The actual parameters will be loaded from each partition
                let mut partitioned_filter = PartitionedBloomFilter::<String>::new(
                    10000, // Placeholder, will be adjusted based on read data
                    0.01,  // Placeholder
                    num_partitions,
                );

                // Load each partition
                let mut partitions = Vec::with_capacity(num_partitions);
                for i in 0..num_partitions {
                    // Read partition size
                    let mut bits_len_buf = [0u8; 4];
                    self.file.read_exact(&mut bits_len_buf)?;
                    let bits_len = u32::from_le_bytes(bits_len_buf) as usize;
                    println!("Partition {} bits length: {}", i, bits_len);

                    if bits_len > 0 {
                        // Read partition data
                        let mut bits = vec![0u8; bits_len];
                        self.file.read_exact(&mut bits)?;
                        println!("Read partition {} ({} bytes)", i, bits_len);

                        // Create a bloom filter from the data
                        let partition =
                            BloomFilter::<String>::from_parts(bits, size_bits, num_hashes);
                        partitions.push(partition);
                    } else {
                        // Empty partition - create an empty one
                        println!("Partition {} is empty", i);
                        partitions.push(BloomFilter::new(100, 0.01)); // Empty filter
                    }
                }

                // Replace the partitions in the filter with our loaded ones
                partitioned_filter.set_partitions(partitions);
                self.partitioned_bloom_filter = Some(partitioned_filter);
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unknown bloom filter type: {}", bloom_type),
                ));
            }
        }

        Ok(())
    }

    /// Check if a key might exist in the SSTable
    pub fn may_contain(&self, key: &str) -> bool {
        if let Some(bloom_filter) = &self.bloom_filter {
            bloom_filter.may_contain(&key.to_string())
        } else if let Some(partitioned_filter) = &self.partitioned_bloom_filter {
            partitioned_filter.may_contain(&key.to_string())
        } else {
            true // If no bloom filter, we have to assume the key might exist
        }
    }

    /// Check if multiple keys might exist in the SSTable (using parallel lookups if available)
    pub fn may_contain_batch(&self, keys: &[String]) -> Vec<bool> {
        if let Some(partitioned_filter) = &self.partitioned_bloom_filter {
            // Use parallel lookups for partitioned filter
            partitioned_filter.may_contain_parallel(keys)
        } else if let Some(bloom_filter) = &self.bloom_filter {
            // Fall back to sequential lookups for standard filter
            keys.iter()
                .map(|key| bloom_filter.may_contain(key))
                .collect()
        } else {
            // No filter, assume all keys might exist
            vec![true; keys.len()]
        }
    }

    /// Get the value for a key, if it exists
    pub fn get(&mut self, key: &str) -> io::Result<Option<Vec<u8>>> {
        // First check the bloom filter
        if !self.may_contain(key) {
            return Ok(None);
        }

        // Get the file size to help with validation
        let file_size = self.file.get_ref().metadata()?.len();

        // Reset file position to the start of data
        self.file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;

        // Scan the file for the key
        for _ in 0..self.entry_count {
            // Get current position for better error reporting
            let entry_start_pos = self.file.stream_position()?;

            // Read key length
            let mut key_len_buf = [0u8; 4];
            match self.file.read_exact(&mut key_len_buf) {
                Ok(_) => {}
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Failed to read key length at position {}: {}",
                            entry_start_pos, e
                        ),
                    ));
                }
            }

            let key_len = u32::from_le_bytes(key_len_buf);

            // Additional check - if key length would extend beyond file, it's corrupt
            if entry_start_pos + 4 + key_len as u64 > file_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Key length {} would extend beyond file size at position {}",
                        key_len, entry_start_pos
                    ),
                ));
            }

            // Sanity check for key length
            const MIN_KEY_SIZE: u32 = 1; // At least 1 byte
            const MAX_KEY_SIZE: u32 = 1024 * 1024; // 1MB max key size

            if key_len < MIN_KEY_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Key length too small: {}", key_len),
                ));
            }

            if key_len > MAX_KEY_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Key length too large: {}", key_len),
                ));
            }

            // Check for potential overflow when allocating buffer
            if key_len as usize > isize::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Key length exceeds maximum allocatable size: {}", key_len),
                ));
            }

            // Read key
            let mut key_buf = vec![0u8; key_len as usize];
            match self.file.read_exact(&mut key_buf) {
                Ok(_) => {}
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to read key data: {}", e),
                    ));
                }
            }

            // Check UTF-8 for key
            let current_key = match std::str::from_utf8(&key_buf) {
                Ok(s) => s,
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Key data is not valid UTF-8",
                    ));
                }
            };

            // Read value length
            let mut value_len_buf = [0u8; 4];
            match self.file.read_exact(&mut value_len_buf) {
                Ok(_) => {}
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to read value length: {}", e),
                    ));
                }
            }
            let value_len = u32::from_le_bytes(value_len_buf);

            // Sanity check for value length
            const MAX_VALUE_SIZE: u32 = 10 * 1024 * 1024; // 10MB max value size
            if value_len > MAX_VALUE_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Value length too large: {}", value_len),
                ));
            }

            // Check for potential overflow when allocating buffer
            if value_len as usize > isize::MAX as usize {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Value length exceeds maximum allocatable size: {}",
                        value_len
                    ),
                ));
            }

            // Additional check - if value length would extend beyond file, it's corrupt
            let current_pos = self.file.stream_position()?;
            if current_pos + value_len as u64 > file_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Value length {} would read past end of file", value_len),
                ));
            }

            // Read value
            let mut value = vec![0u8; value_len as usize];
            match self.file.read_exact(&mut value) {
                Ok(_) => {}
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to read value data: {}", e),
                    ));
                }
            }

            // Read checksum
            let mut checksum_buf = [0u8; 4];
            match self.file.read_exact(&mut checksum_buf) {
                Ok(_) => {}
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Failed to read checksum: {}", e),
                    ));
                }
            }

            let stored_checksum = u32::from_le_bytes(checksum_buf);

            // Verify checksum
            let mut entry_data = Vec::new();
            entry_data.extend_from_slice(&key_len_buf);
            entry_data.extend_from_slice(&key_buf);
            entry_data.extend_from_slice(&value_len_buf);
            entry_data.extend_from_slice(&value);

            let calculated_checksum = calculate_checksum(&entry_data);

            if calculated_checksum != stored_checksum {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "SSTable data block checksum verification failed",
                ));
            }

            if current_key == key {
                // Found the key, return the value
                return Ok(Some(value));
            }
            // No need to skip past the value, we've already read it
        }

        Ok(None)
    }

    /// Get the number of entries in the SSTable
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Check if the SSTable has a Bloom filter
    pub fn has_bloom_filter(&self) -> bool {
        self.has_bloom_filter
    }

    /// Load block checksums from the file
    #[allow(dead_code)] // Will be used in future data integrity features
    fn load_block_checksums(&mut self, file_size: u64) -> io::Result<()> {
        // If this is an older version file, no checksums to load
        if VERSION <= 2 {
            return Ok(());
        }

        // Safety check: Don't try to read if entry count is suspicious
        const MAX_REASONABLE_ENTRIES: u64 = 1_000_000; // Limit to prevent huge allocations
        if self.entry_count > MAX_REASONABLE_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unreasonable entry count: {}", self.entry_count),
            ));
        }

        // Calculate where checksums are stored (after data and bloom filter)
        // Use checked arithmetic to prevent integer overflow
        let checksums_start = if self.has_bloom_filter {
            match self
                .index_offset
                .checked_add(self.entry_count.checked_mul(4).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Integer overflow calculating checksum location",
                    )
                })?) {
                Some(start) => start,
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Integer overflow calculating checksum location",
                    ));
                }
            }
        } else {
            self.index_offset
        };

        // Only try to read checksums if the file is large enough
        // Use checked arithmetic to prevent overflow
        match checksums_start.checked_add(self.entry_count.checked_mul(4).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Integer overflow calculating checksum size",
            )
        })?) {
            Some(end_pos) if end_pos > file_size => return Ok(()),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Integer overflow calculating checksum size",
                ));
            }
            _ => {}
        };

        // Check that entry_count can be safely converted to usize
        if self.entry_count > usize::MAX as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Entry count too large for architecture: {}",
                    self.entry_count
                ),
            ));
        }

        // Seek to the checksums section
        self.file.seek(SeekFrom::Start(checksums_start))?;

        // Read checksums (4 bytes each)
        let capacity = self.entry_count as usize;
        self.block_checksums = Vec::with_capacity(capacity);
        for _ in 0..self.entry_count {
            let mut checksum_buf = [0u8; 4];
            self.file.read_exact(&mut checksum_buf)?;
            self.block_checksums.push(u32::from_le_bytes(checksum_buf));
        }

        Ok(())
    }
}

/// SSTable compaction utilities
pub struct SSTableCompaction;

impl SSTableCompaction {
    /// Identifies groups of SSTables that should be compacted together based on similar size
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

            // If the current SSTable's size is within the threshold of the smallest one in the group
            if current_group.is_empty()
                || (current_size as f64 / smallest_size as f64) <= size_ratio_threshold
            {
                current_group.push(idx);
            } else {
                // Otherwise, start a new group if the current one has enough members
                if current_group.len() >= min_group_size {
                    compaction_groups.push(current_group);
                }
                current_group = vec![idx];
                smallest_size = current_size;
            }
        }

        // Don't forget to add the last group if it has enough members
        if current_group.len() >= min_group_size {
            compaction_groups.push(current_group);
        }

        compaction_groups
    }

    /// Compacts multiple SSTables into a single one, with a Bloom filter
    pub fn compact_sstables(
        sstable_paths: &[String],
        output_path: &str,
        delete_originals: bool,
        use_bloom_filter: bool,
        false_positive_rate: f64,
    ) -> io::Result<String> {
        // First count total entries
        let mut total_entries = 0;
        for path in sstable_paths {
            let reader = SSTableReader::open(path)?;
            total_entries += reader.entry_count();
        }

        // Create a new SSTable writer with a Bloom filter
        let mut writer = SSTableWriter::new(
            output_path,
            total_entries as usize,
            use_bloom_filter,
            false_positive_rate,
        )?;

        // Map to accumulate all key-value pairs
        let mut map = BTreeMap::new();

        // Read all SSTables and merge them
        for path in sstable_paths {
            let mut reader = SSTableReader::open(path)?;
            reader.file.seek(SeekFrom::Start(HEADER_SIZE as u64))?;

            for _ in 0..reader.entry_count() {
                // Read key length
                let mut key_len_buf = [0u8; 4];
                reader.file.read_exact(&mut key_len_buf)?;
                let key_len = u32::from_le_bytes(key_len_buf) as usize;

                // Read key
                let mut key_buf = vec![0u8; key_len];
                reader.file.read_exact(&mut key_buf)?;
                let key = String::from_utf8_lossy(&key_buf).to_string();

                // Read value length
                let mut value_len_buf = [0u8; 4];
                reader.file.read_exact(&mut value_len_buf)?;
                let value_len = u32::from_le_bytes(value_len_buf) as usize;

                // Read value
                let mut value = vec![0u8; value_len];
                reader.file.read_exact(&mut value)?;

                // Store in map, overwriting any previous value for this key
                map.insert(key, value);
            }
        }

        // Write all entries to the new SSTable
        for (key, value) in map {
            writer.write_entry(&key, &value)?;
        }

        // Finalize the SSTable
        writer.finalize()?;

        // Delete original files if requested
        if delete_originals {
            for path in sstable_paths {
                fs::remove_file(path)?;
            }
        }

        Ok(output_path.to_string())
    }
}

// Tests moved to tests/sstable_checksum_test.rs
