use lsmer::sstable::{SSTableReader, SSTableWriter};
use std::time::Instant;
use tempfile::tempdir;

#[test]
fn test_sstable_with_partitioned_bloom_filter() {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    // Create test data
    let test_data: Vec<(String, Vec<u8>)> = (0..1000)
        .map(|i| (format!("key-{}", i), format!("value-{}", i).into_bytes()))
        .collect();

    // Create SSTable writer with partitioned bloom filter
    let sstable_path = format!("{}/test_sstable_partitioned", temp_path);
    println!(
        "Creating partitioned bloom filter SSTable at: {}",
        sstable_path
    );

    // Create writer with partitioned bloom
    let writer_result = SSTableWriter::new_with_options(
        &sstable_path,
        test_data.len(), // Expected entries
        true,            // Use bloom filter
        0.01,            // 1% false positive rate
        true,            // Use partitioned bloom
    );

    assert!(writer_result.is_ok(), "Failed to create SSTable writer");
    let mut writer = writer_result.unwrap();

    // Write test data
    for (key, value) in &test_data {
        writer.write_entry(key, value).unwrap();
    }

    // Finalize the SSTable
    writer.finalize().unwrap();

    // Open the SSTable for reading
    println!("Opening SSTable for reading from: {}", sstable_path);
    let reader_result = SSTableReader::open(&sstable_path);
    assert!(
        reader_result.is_ok(),
        "Failed to open SSTable reader: {:?}",
        reader_result
    );
    let reader = reader_result.unwrap();

    // Check if keys exist
    for (key, _) in &test_data {
        assert!(
            reader.may_contain(key),
            "Key '{}' should be in the filter",
            key
        );
    }

    // Check some keys that don't exist
    for i in 2000..2010 {
        let key = format!("key-{}", i);
        // Some false positives are possible, but most should return false
        if reader.may_contain(&key) {
            println!("False positive for key: {}", key);
        }
    }

    // Test batch lookups
    let existing_keys: Vec<String> = test_data.iter().take(100).map(|(k, _)| k.clone()).collect();

    let non_existing_keys: Vec<String> = (2000..2100).map(|i| format!("key-{}", i)).collect();

    // Combine them to measure false positive rate
    let mixed_keys: Vec<String> = existing_keys
        .iter()
        .chain(non_existing_keys.iter())
        .cloned()
        .collect();

    // Get results for batch lookup
    let results = reader.may_contain_batch(&mixed_keys);

    // First 100 should be true
    for (i, &result) in results.iter().take(100).enumerate() {
        assert!(result, "Key at index {} should exist", i);
    }

    // Count false positives in remaining results
    let false_positives = results[100..].iter().filter(|&&b| b).count();
    println!(
        "False positive rate: {}/{} = {:.2}%",
        false_positives,
        non_existing_keys.len(),
        (false_positives as f64 / non_existing_keys.len() as f64) * 100.0
    );

    // False positive rate should be reasonable (under 5%)
    assert!(
        false_positives < 5,
        "Too many false positives: {}",
        false_positives
    );
}

#[test]
fn test_performance_comparison() {
    // Create a temporary directory
    let temp_dir = tempdir().unwrap();
    let temp_path = temp_dir.path().to_string_lossy().to_string();

    // Create test data
    let test_data: Vec<(String, Vec<u8>)> = (0..5000)
        .map(|i| (format!("key-{}", i), format!("value-{}", i).into_bytes()))
        .collect();

    // Create lookup batch
    let lookup_keys: Vec<String> = (0..1000).map(|i| format!("key-{}", i)).collect();

    // 1. Create standard bloom filter SSTable
    let standard_path = format!("{}/standard_bloom", temp_path);
    let mut standard_writer = SSTableWriter::new(
        &standard_path,
        test_data.len(),
        true, // Use bloom filter
        0.01, // 1% false positive rate
    )
    .unwrap();

    // 2. Create partitioned bloom filter SSTable
    let partitioned_path = format!("{}/partitioned_bloom", temp_path);
    let mut partitioned_writer = SSTableWriter::new_with_options(
        &partitioned_path,
        test_data.len(),
        true, // Use bloom filter
        0.01, // 1% false positive rate
        true, // Use partitioned bloom
    )
    .unwrap();

    // Write test data to both
    for (key, value) in &test_data {
        standard_writer.write_entry(key, value).unwrap();
        partitioned_writer.write_entry(key, value).unwrap();
    }

    // Finalize both
    standard_writer.finalize().unwrap();
    partitioned_writer.finalize().unwrap();

    // Open both for reading
    let standard_reader = SSTableReader::open(&standard_path).unwrap();
    let partitioned_reader = SSTableReader::open(&partitioned_path).unwrap();

    // Time standard bloom filter lookups
    let start = Instant::now();
    let _standard_results: Vec<bool> = lookup_keys
        .iter()
        .map(|key| standard_reader.may_contain(key))
        .collect();
    let standard_duration = start.elapsed();

    // Time partitioned bloom filter batch lookups
    let start = Instant::now();
    let _partitioned_results = partitioned_reader.may_contain_batch(&lookup_keys);
    let partitioned_duration = start.elapsed();

    println!("Standard bloom lookup: {:?}", standard_duration);
    println!("Partitioned bloom lookup: {:?}", partitioned_duration);

    // We don't assert which is faster since it depends on the test environment,
    // but in a multi-core system, the partitioned bloom should be faster for batch lookups
}
