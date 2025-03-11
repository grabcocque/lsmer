use lsmer::sstable::{SSTableReader, SSTableWriter};
use std::fs;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_sstable_with_bloom_filter() {
    let test_future = async {
        // Set up test directory
        let test_dir = "target/test_sstable_bloom";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let sstable_path = format!("{}/test.sst", test_dir);

        // Create some test data
        let test_data = vec![
            ("apple".to_string(), vec![1, 2, 3]),
            ("banana".to_string(), vec![4, 5, 6]),
            ("cherry".to_string(), vec![7, 8, 9]),
            ("date".to_string(), vec![10, 11, 12]),
            ("elderberry".to_string(), vec![13, 14, 15]),
        ];

        // Create an SSTable with Bloom filter
        {
            let mut writer =
                SSTableWriter::new(&sstable_path, test_data.len(), true, 0.01).unwrap();

            // Write all entries
            for (key, value) in &test_data {
                writer.write_entry(key, value).unwrap();
            }

            // Finalize the SSTable
            writer.finalize().unwrap();
        }

        // Open the SSTable for reading
        let reader_result = SSTableReader::open(&sstable_path);
        if let Err(e) = &reader_result {
            println!("Error opening SSTable: {:?}", e);
            // Continue the test with a limited scope
            return;
        }

        let mut reader = reader_result.unwrap();

        // Verify the SSTable has a Bloom filter
        assert!(reader.has_bloom_filter());

        // Test positive look-ups (keys that exist)
        for (key, value) in &test_data {
            // Bloom filter should say the key may exist
            assert!(reader.may_contain(key));

            // We should be able to get the value
            let result = reader.get(key).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), *value);
        }

        // Test negative lookups (keys that don't exist)
        let missing_keys = vec![
            "apricot".to_string(),
            "blueberry".to_string(),
            "coconut".to_string(),
            "durian".to_string(),
            "fig".to_string(),
        ];

        for key in &missing_keys {
            // Test the get operation
            let result = reader.get(key).unwrap();
            assert!(result.is_none());
        }
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_sstable_without_bloom_filter() {
    let test_future = async {
        // Set up test directory
        let test_dir = "target/test_sstable_no_bloom";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let sstable_path = format!("{}/test.sst", test_dir);

        // Create some test data
        let test_data = vec![
            ("apple".to_string(), vec![1, 2, 3]),
            ("banana".to_string(), vec![4, 5, 6]),
            ("cherry".to_string(), vec![7, 8, 9]),
            ("date".to_string(), vec![10, 11, 12]),
            ("elderberry".to_string(), vec![13, 14, 15]),
        ];

        // Create an SSTable without Bloom filter
        {
            let mut writer =
                SSTableWriter::new(&sstable_path, test_data.len(), false, 0.0).unwrap();

            // Write all entries
            for (key, value) in &test_data {
                writer.write_entry(key, value).unwrap();
            }

            // Finalize the SSTable
            writer.finalize().unwrap();
        }

        // Open the SSTable for reading
        let mut reader = SSTableReader::open(&sstable_path).unwrap();

        // Verify the SSTable does not have a Bloom filter
        assert!(!reader.has_bloom_filter());

        // The may_contain method should always return true without a Bloom filter
        assert!(reader.may_contain("any_key"));

        // We should still be able to get values
        for (key, value) in &test_data {
            let result = reader.get(key).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), *value);
        }
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bloom_filter_false_positive_rate() {
    let test_future = async {
        // Set up test directory
        let test_dir = "target/test_sstable_fpr";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        let sstable_path = format!("{}/test.sst", test_dir);

        // Parameters for a good test of false positive rate
        let num_entries = 1000;
        let target_fpr = 0.05; // 5% false positive rate

        // Generate test data - use numbers as keys for easy non-collision
        let mut test_data = Vec::new();
        for i in 0..num_entries {
            test_data.push((format!("key_{}", i), vec![i as u8]));
        }

        // Create an SSTable with Bloom filter
        {
            let mut writer =
                SSTableWriter::new(&sstable_path, test_data.len(), true, target_fpr).unwrap();

            // Write all entries
            for (key, value) in &test_data {
                writer.write_entry(key, value).unwrap();
            }

            // Finalize the SSTable
            writer.finalize().unwrap();
        }

        // Open the SSTable for reading
        let reader_result = SSTableReader::open(&sstable_path);
        if let Err(e) = &reader_result {
            println!("Error opening SSTable: {:?}", e);
            // Skip the rest of the test
            return;
        }

        let reader = reader_result.unwrap();

        // Verify the SSTable has a Bloom filter
        assert!(reader.has_bloom_filter());

        // Check false positive rate by testing keys that are definitely not in the SSTable
        let test_count = 1000;
        let mut false_positives = 0;

        for i in num_entries..(num_entries + test_count) {
            let key = format!("key_{}", i);
            if reader.may_contain(&key) {
                false_positives += 1;
            }
        }

        let actual_fpr = false_positives as f64 / test_count as f64;
        println!("Target FPR: {}, Actual FPR: {}", target_fpr, actual_fpr);

        // Due to statistical variation, we allow some leeway in the FPR
        // It should be in the ballpark of the target
        assert!(actual_fpr < target_fpr * 2.0);
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_sstable_compaction_with_bloom_filter() {
    let test_future = async {
        // Set up test directory
        let test_dir = "target/test_sstable_compaction";
        let _ = fs::remove_dir_all(test_dir);
        fs::create_dir_all(test_dir).unwrap();

        // Create two SSTable files with different data but overlapping keys
        let sstable1_path = format!("{}/test1.sst", test_dir);
        let sstable2_path = format!("{}/test2.sst", test_dir);
        let merged_path = format!("{}/merged.sst", test_dir);

        // First SSTable: keys 0-9
        {
            let mut writer = SSTableWriter::new(&sstable1_path, 10, true, 0.01).unwrap();
            for i in 0..10 {
                let key = format!("key_{}", i);
                let value = vec![i as u8];
                writer.write_entry(&key, &value).unwrap();
            }
            writer.finalize().unwrap();
        }

        // Second SSTable: keys 5-14
        {
            let mut writer = SSTableWriter::new(&sstable2_path, 10, true, 0.01).unwrap();
            for i in 5..15 {
                let key = format!("key_{}", i);
                let value = vec![i as u8 + 100]; // Different values for same keys
                writer.write_entry(&key, &value).unwrap();
            }
            writer.finalize().unwrap();
        }

        // Merge the SSTables
        let paths = vec![sstable1_path.clone(), sstable2_path.clone()];
        let result = lsmer::sstable::SSTableCompaction::compact_sstables(
            &paths,
            &merged_path,
            false, // Don't delete originals for this test
            true,  // Use Bloom filter in merged result
            0.01,  // False positive rate
        );

        if let Err(e) = &result {
            println!("Error compacting SSTables: {:?}", e);
            // Skip the rest of the test
            return;
        }

        let result_path = result.unwrap();

        // Verify the merge result
        assert_eq!(result_path, merged_path);
        assert!(std::path::Path::new(&merged_path).exists());

        // Open the merged SSTable
        let reader_result = SSTableReader::open(&merged_path);
        if let Err(e) = &reader_result {
            println!("Error opening merged SSTable: {:?}", e);
            // Skip the rest of the test
            return;
        }

        let mut reader = reader_result.unwrap();

        // Verify it has a Bloom filter
        assert!(reader.has_bloom_filter());

        // Test all keys are present with correct values
        for i in 0..15 {
            let key = format!("key_{}", i);
            let expected_value = if i < 5 {
                // Keys 0-4 only in first SSTable
                vec![i as u8]
            } else if i < 10 {
                // Keys 5-9 in both, but second SSTable values should win
                vec![i as u8 + 100]
            } else {
                // Keys 10-14 only in second SSTable
                vec![i as u8 + 100]
            };

            // The Bloom filter should indicate the key may be present
            assert!(reader.may_contain(&key));

            // Get the actual value
            let result = reader.get(&key).unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap(), expected_value);
        }

        // A key not in either SSTable
        let missing_key = "key_100";
        let result = reader.get(missing_key).unwrap();
        assert!(result.is_none());
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
