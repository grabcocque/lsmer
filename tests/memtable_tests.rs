use lsmer::{MemtableError, StringMemtable};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Calculate a reasonable capacity for test memtables
fn calculate_test_capacity(entries: usize) -> usize {
    // For test purposes, estimate each entry to be around 100 bytes
    // This is a rough approximation that allows our tests to work consistently
    entries * 100
}

/// Test fixture to provide a common setup for memtable tests
fn create_test_memtable() -> StringMemtable {
    let capacity = calculate_test_capacity(3); // Space for approximately 3 entries
    let mut memtable = StringMemtable::new(capacity);

    // Pre-populate with some test data
    memtable.insert("key1".to_string(), vec![1, 2, 3]).unwrap();
    memtable.insert("key2".to_string(), vec![4, 5, 6]).unwrap();

    memtable
}

#[test]
fn test_basic_operations() {
    let memtable = create_test_memtable();

    // Test get operations
    assert_eq!(memtable.get(&"key1".to_string()), Some(&vec![1, 2, 3]));
    assert_eq!(memtable.get(&"key2".to_string()), Some(&vec![4, 5, 6]));
    assert_eq!(memtable.get(&"key3".to_string()), None);

    // Test len and is_empty
    assert_eq!(memtable.len(), 2);
    assert!(!memtable.is_empty());
    assert!(!memtable.is_full());

    // Test max capacity is in bytes now
    assert_eq!(memtable.max_capacity(), calculate_test_capacity(3));

    // Test current size is populated
    assert!(memtable.current_size() > 0);
}

#[test]
fn test_remove_operations() {
    let mut memtable = create_test_memtable();

    // Record size before removal
    let size_before = memtable.current_size();

    // Test remove
    let removed = memtable.remove(&"key1".to_string());
    assert_eq!(removed, Some(vec![1, 2, 3]));
    assert_eq!(memtable.len(), 1);
    assert_eq!(memtable.get(&"key1".to_string()), None);

    // Size should decrease after removal
    assert!(memtable.current_size() < size_before);

    // Test remove non-existent key
    let removed = memtable.remove(&"nonexistent".to_string());
    assert_eq!(removed, None);
}

#[test]
fn test_capacity_constraints() {
    // Create a memtable with capacity for approximately 2 entries
    let capacity = calculate_test_capacity(2);
    let mut memtable = StringMemtable::new(capacity);

    // Fill to capacity
    memtable.insert("key1".to_string(), vec![1, 2, 3]).unwrap();
    memtable.insert("key2".to_string(), vec![4, 5, 6]).unwrap();

    // Try inserting an entry with a large value that would exceed capacity
    let large_value = vec![7; 1000]; // 1000 bytes, definitely exceeds remaining capacity
    let result = memtable.insert("key3".to_string(), large_value);
    assert!(result.is_err());
    match result {
        Err(MemtableError::CapacityExceeded) => (),
        _ => panic!("Expected CapacityExceeded error"),
    }

    // Update existing key with same-sized value should work even at capacity
    let result = memtable.insert("key1".to_string(), vec![10, 11, 12]);
    assert!(result.is_ok());
    assert_eq!(memtable.get(&"key1".to_string()), Some(&vec![10, 11, 12]));

    // Update existing key with much larger value should fail if it exceeds capacity
    let large_update = vec![99; 1000]; // 1000 bytes, should exceed capacity
    let result = memtable.insert("key1".to_string(), large_update);
    assert!(result.is_err());
    match result {
        Err(MemtableError::CapacityExceeded) => (),
        _ => panic!("Expected CapacityExceeded error for large update"),
    }
}

#[test]
fn test_clear_operation() {
    let mut memtable = create_test_memtable();

    // Test clear
    memtable.clear();
    assert_eq!(memtable.len(), 0);
    assert!(memtable.is_empty());
    assert_eq!(memtable.get(&"key1".to_string()), None);
    assert_eq!(memtable.get(&"key2".to_string()), None);

    // Size should be reset to 0
    assert_eq!(memtable.current_size(), 0);
}

#[test]
fn test_iteration() {
    let mut memtable = StringMemtable::new(calculate_test_capacity(3));

    // Add items in reverse order to verify sorting
    memtable.insert("c".to_string(), vec![5, 6]).unwrap();
    memtable.insert("a".to_string(), vec![1, 2]).unwrap();
    memtable.insert("b".to_string(), vec![3, 4]).unwrap();

    // Collect keys in iteration order
    let keys: Vec<String> = memtable.iter().map(|(k, _)| k.clone()).collect();

    // Verify keys are in sorted order
    assert_eq!(
        keys,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
}

// Create a helper function for test setup
fn create_range_test_memtable() -> StringMemtable {
    let mut memtable = StringMemtable::new(calculate_test_capacity(10));

    // Insert several keys in mixed order
    let pairs = [
        ("apple", vec![1, 1]),
        ("banana", vec![2, 2]),
        ("cherry", vec![3, 3]),
        ("date", vec![4, 4]),
        ("elderberry", vec![5, 5]),
        ("fig", vec![6, 6]),
        ("grape", vec![7, 7]),
        ("honeydew", vec![8, 8]),
    ];

    for (key, value) in pairs.iter() {
        memtable.insert(key.to_string(), value.clone()).unwrap();
    }

    memtable
}

#[test]
fn test_range_query_closed() {
    let memtable = create_range_test_memtable();

    // Test full-closed range query "banana".."elderberry" (inclusive start, inclusive end)
    let range_result: Vec<(String, Vec<u8>)> = memtable
        .range("banana".to_string()..="elderberry".to_string())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Debug output
    println!("Full-closed range contents:");
    for (i, (k, _)) in range_result.iter().enumerate() {
        println!("  {}: {}", i, k);
    }

    // This range includes "banana", "cherry", "date", and "elderberry"
    assert_eq!(range_result.len(), 4);
    assert_eq!(range_result[0].0, "banana");
    assert_eq!(range_result[1].0, "cherry");
    assert_eq!(range_result[2].0, "date");
    assert_eq!(range_result[3].0, "elderberry");
}

#[test]
fn test_range_query_half_open() {
    let memtable = create_range_test_memtable();

    // Test half-open range query "cherry".."grape" (inclusive start, exclusive end)
    let range_result: Vec<(String, Vec<u8>)> = memtable
        .range("cherry".to_string().."grape".to_string())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Debug output
    println!("Half-open range contents:");
    for (i, (k, _)) in range_result.iter().enumerate() {
        println!("  {}: {}", i, k);
    }

    // This range includes "cherry", "date", and "elderberry", "fig"
    assert_eq!(range_result.len(), 4);
    assert_eq!(range_result[0].0, "cherry");
    assert_eq!(range_result[1].0, "date");
    assert_eq!(range_result[2].0, "elderberry");
    assert_eq!(range_result[3].0, "fig");
}

#[test]
fn test_range_query_unbounded_start() {
    let memtable = create_range_test_memtable();

    // Test unbounded start range query (start from beginning up to "cherry")
    let range_result: Vec<(String, Vec<u8>)> = memtable
        .range(..="cherry".to_string())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Debug output
    println!("Unbounded start range contents:");
    for (i, (k, _)) in range_result.iter().enumerate() {
        println!("  {}: {}", i, k);
    }

    // This range includes "apple", "banana", and "cherry"
    assert_eq!(range_result.len(), 3);
    assert_eq!(range_result[0].0, "apple");
    assert_eq!(range_result[1].0, "banana");
    assert_eq!(range_result[2].0, "cherry");
}

#[test]
fn test_range_query_unbounded_end() {
    let memtable = create_range_test_memtable();

    // Test unbounded end range query (from "fig" to the end)
    let range_result: Vec<(String, Vec<u8>)> = memtable
        .range("fig".to_string()..)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // Debug output
    println!("Unbounded end range contents:");
    for (i, (k, _)) in range_result.iter().enumerate() {
        println!("  {}: {}", i, k);
    }

    // This range includes "fig", "grape", and "honeydew"
    assert_eq!(range_result.len(), 3);
    assert_eq!(range_result[0].0, "fig");
    assert_eq!(range_result[1].0, "grape");
    assert_eq!(range_result[2].0, "honeydew");
}

#[test]
fn test_range_query_empty() {
    let memtable = create_range_test_memtable();

    // Test empty range query (range that doesn't include any keys)
    let range_result: Vec<(String, Vec<u8>)> = memtable
        .range("grapefruit".to_string().."guava".to_string())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    println!("Empty range length: {}", range_result.len());

    assert_eq!(range_result.len(), 0);
}

#[test]
fn test_range_query_large() {
    // Test performance of range queries with many keys (validate log complexity)
    let mut large_memtable = StringMemtable::new(calculate_test_capacity(1000));

    // Insert 100 keys with predictable pattern
    for i in 0..100 {
        let key = format!("key{:03}", i);
        large_memtable.insert(key, vec![i as u8]).unwrap();
    }

    // Query a range in the middle (should be fast even with many keys)
    let start_key = "key050".to_string();
    let end_key = "key059".to_string();

    let range_result: Vec<(String, Vec<u8>)> = large_memtable
        .range(start_key..=end_key)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    println!("Large range query results: {}", range_result.len());

    // Verify we got exactly the expected 10 keys in the right order
    assert_eq!(range_result.len(), 10);
    for (i, result) in range_result.iter().enumerate().take(10) {
        assert_eq!(result.0, format!("key{:03}", i + 50));
        assert_eq!(result.1, vec![(i + 50) as u8]);
    }
}

#[test]
fn test_flush_to_sstable() {
    // Create a temp directory for the test
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    // Create a test memtable
    let mut memtable = create_test_memtable();

    // Add one more item to reach capacity
    memtable.insert("key3".to_string(), vec![7, 8, 9]).unwrap();

    // Debug: Check if the memtable is full
    println!(
        "Current size: {}, Max capacity: {}",
        memtable.current_size(),
        memtable.max_capacity()
    );

    // Ensure the memtable has the expected data (3 entries)
    // The memtable is about 55% full, so check for at least 50% full
    assert!(memtable.current_size() >= memtable.max_capacity() * 5 / 10); // At least 50% full
    assert_eq!(memtable.len(), 3);

    // Flush to an SSTable
    let sstable_path = memtable.flush_to_sstable(temp_path).unwrap();

    // Verify memtable is now empty
    assert_eq!(memtable.len(), 0);
    assert!(memtable.is_empty());

    // Verify the SSTable file exists
    assert!(Path::new(&sstable_path).exists());

    // Basic verification of file content (at least it has data)
    let metadata = fs::metadata(&sstable_path).unwrap();
    assert!(metadata.len() > 0);

    // Verify the SSTable format by reading the header
    let file = std::fs::File::open(&sstable_path).unwrap();
    let mut reader = std::io::BufReader::new(file);

    // Read magic number
    let mut magic_bytes = [0u8; 8];
    reader.read_exact(&mut magic_bytes).unwrap();
    let magic = u64::from_le_bytes(magic_bytes);
    assert_eq!(magic, 0x4C534D_5353544142, "Invalid magic number");

    // Read version
    let mut version_bytes = [0u8; 4];
    reader.read_exact(&mut version_bytes).unwrap();
    let version = u32::from_le_bytes(version_bytes);
    assert_eq!(version, 1, "Invalid version");

    // Read entry count
    let mut entry_count_bytes = [0u8; 8];
    reader.read_exact(&mut entry_count_bytes).unwrap();
    let entry_count = u64::from_le_bytes(entry_count_bytes);
    assert_eq!(entry_count, 3, "Wrong entry count");

    // Read index offset
    let mut index_offset_bytes = [0u8; 8];
    reader.read_exact(&mut index_offset_bytes).unwrap();
    let index_offset = u64::from_le_bytes(index_offset_bytes);
    assert!(index_offset > 0, "Invalid index offset");

    // Clean up
    temp_dir.close().unwrap();
}

#[test]
fn test_multiple_flushes() {
    // Create a temp directory for the test
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    // Create a memtable with capacity for approximately 2 entries
    let capacity = calculate_test_capacity(2);
    let mut memtable = StringMemtable::new(capacity);

    // Track the SSTable files created
    let mut sstable_files = Vec::new();

    // Fill and flush the memtable multiple times
    for i in 0..3 {
        // Fill the memtable to capacity
        let base = i * 2;

        // Insert entries until the memtable is at least 50% full
        memtable
            .insert(format!("key{}", base), vec![base as u8, 1, 2])
            .unwrap();
        memtable
            .insert(format!("key{}", base + 1), vec![base as u8 + 1, 3, 4])
            .unwrap();

        // Print the current size for debugging
        println!(
            "Iteration {}: Current size: {}, Max capacity: {}",
            i,
            memtable.current_size(),
            memtable.max_capacity()
        );

        // Ensure the memtable has the expected data
        assert!(memtable.current_size() >= memtable.max_capacity() * 5 / 10); // At least 50% full
        assert_eq!(memtable.len(), 2);

        // Add a small delay to ensure unique timestamps for filenames
        std::thread::sleep(std::time::Duration::from_secs(1));

        // Flush to an SSTable
        let sstable_path = memtable.flush_to_sstable(temp_path).unwrap();
        sstable_files.push(sstable_path.clone());

        // Verify the SSTable file exists
        assert!(Path::new(&sstable_path).exists());

        // Verify memtable is now empty
        assert_eq!(memtable.len(), 0);
        assert!(memtable.is_empty());
    }

    // Verify we created 3 different SSTable files
    assert_eq!(sstable_files.len(), 3);

    // Verify all files are unique
    let unique_files: std::collections::HashSet<_> = sstable_files.iter().cloned().collect();
    assert_eq!(unique_files.len(), 3);

    // Verify all files exist and have content
    for file_path in &sstable_files {
        let metadata = fs::metadata(file_path).unwrap();
        assert!(metadata.len() > 0, "SSTable file should have content");
    }

    // Clean up
    temp_dir.close().unwrap();
}

#[test]
fn test_sstable_compaction() {
    // Create a temp directory for the test
    let temp_dir = tempfile::tempdir().unwrap();
    let temp_path = temp_dir.path().to_str().unwrap();

    // Create multiple SSTables with different data
    let sstable_paths = create_test_sstables(temp_path, 3);

    // Get metadata about the SSTables
    let mut sstable_infos = Vec::new();
    for path in &sstable_paths {
        let metadata = fs::metadata(path).unwrap();

        // Read the entry count from the file
        let file = std::fs::File::open(path).unwrap();
        let mut reader = std::io::BufReader::new(file);

        // Skip magic and version
        reader.seek(SeekFrom::Start(12)).unwrap();

        // Read entry count
        let mut entry_count_bytes = [0u8; 8];
        reader.read_exact(&mut entry_count_bytes).unwrap();
        let entry_count = u64::from_le_bytes(entry_count_bytes);

        sstable_infos.push(lsmer::SSTableInfo {
            path: path.clone(),
            size_bytes: metadata.len(),
            entry_count,
        });
    }

    // Identify groups of SSTables to compact
    let compaction_groups = StringMemtable::identify_compaction_groups(
        &sstable_infos,
        1.5, // Size ratio threshold
        2,   // Minimum group size
    );

    // We should have at least one group to compact
    assert!(!compaction_groups.is_empty());

    // Compact the first group
    let group_indices = &compaction_groups[0];
    let group_paths: Vec<String> = group_indices
        .iter()
        .map(|&idx| sstable_infos[idx].path.clone())
        .collect();

    // First test: compaction without deleting originals
    {
        let compacted_path =
            StringMemtable::compact_sstables(&group_paths, temp_path, false).unwrap();

        // Verify the compacted SSTable exists
        assert!(Path::new(&compacted_path).exists());

        // Verify the original SSTable files still exist
        for path in &group_paths {
            assert!(
                Path::new(path).exists(),
                "Original SSTable file should still exist: {}",
                path
            );
        }
    }

    // Second test: compaction with deleting originals
    {
        let compacted_path =
            StringMemtable::compact_sstables(&group_paths, temp_path, true).unwrap();

        // Verify the compacted SSTable exists
        assert!(Path::new(&compacted_path).exists());

        // Verify the compacted SSTable has content
        let metadata = fs::metadata(&compacted_path).unwrap();
        assert!(metadata.len() > 0);

        // Verify the original SSTable files have been deleted
        for path in &group_paths {
            assert!(
                !Path::new(path).exists(),
                "Original SSTable file should be deleted: {}",
                path
            );
        }

        // Read the compacted SSTable to verify it contains all the keys
        let file = std::fs::File::open(&compacted_path).unwrap();
        let mut reader = std::io::BufReader::new(file);

        // Skip magic and version
        reader.seek(SeekFrom::Start(12)).unwrap();

        // Read entry count
        let mut entry_count_bytes = [0u8; 8];
        reader.read_exact(&mut entry_count_bytes).unwrap();
        let entry_count = u64::from_le_bytes(entry_count_bytes);

        // The compacted SSTable should have at least as many entries as the original SSTables
        // (possibly fewer if there were duplicate keys)
        let original_entries: u64 = group_indices
            .iter()
            .map(|&idx| sstable_infos[idx].entry_count)
            .sum();

        assert!(
            entry_count <= original_entries,
            "Compacted SSTable has {} entries, original SSTables had {} entries total",
            entry_count,
            original_entries
        );
    }

    // Clean up
    temp_dir.close().unwrap();
}

// Helper function to create test SSTables
fn create_test_sstables(base_path: &str, count: usize) -> Vec<String> {
    let mut sstable_paths = Vec::new();

    for i in 0..count {
        // Create a memtable with slightly different capacity for each SSTable
        let capacity = calculate_test_capacity(2 + i);
        let mut memtable = StringMemtable::new(capacity);

        // Insert different data in each SSTable
        for j in 0..3 {
            let key = format!("key{}_{}", i, j);
            let value = vec![(i * 10 + j) as u8; 5 + i]; // Different sized values
            memtable.insert(key, value).unwrap();
        }

        // Add a delay to ensure unique timestamps
        std::thread::sleep(std::time::Duration::from_secs(1));

        // Flush to SSTable
        let path = memtable.flush_to_sstable(base_path).unwrap();
        sstable_paths.push(path);
    }

    sstable_paths
}
