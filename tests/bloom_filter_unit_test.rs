use lsmer::BloomFilter;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_bloom_filter_empty() {
    let test_future = async {
        let filter = BloomFilter::<String>::new(100, 0.01);
        assert!(!filter.may_contain(&"test".to_string()));
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bloom_filter_insert_and_check() {
    let test_future = async {
        let mut filter = BloomFilter::<String>::new(100, 0.01);

        // Insert elements
        filter.insert(&"apple".to_string());
        filter.insert(&"banana".to_string());
        filter.insert(&"cherry".to_string());

        // Check inserted elements
        assert!(filter.may_contain(&"apple".to_string()));
        assert!(filter.may_contain(&"banana".to_string()));
        assert!(filter.may_contain(&"cherry".to_string()));

        // Check non-inserted element
        assert!(!filter.may_contain(&"grape".to_string()));
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
        // Create a filter with a controlled false positive rate
        let expected_elements = 1000;
        let target_fpr = 0.05; // 5% false positive rate
        let mut filter = BloomFilter::<usize>::new(expected_elements, target_fpr);

        // Insert elements
        for i in 0..expected_elements {
            filter.insert(&i);
        }

        // Check that all inserted elements are recognized
        for i in 0..expected_elements {
            assert!(filter.may_contain(&i));
        }

        // Check false positive rate by testing elements that were definitely not inserted
        let test_range = expected_elements * 2; // Test twice as many elements as inserted
        let mut false_positives = 0;

        for i in expected_elements..test_range {
            if filter.may_contain(&i) {
                false_positives += 1;
            }
        }

        let actual_fpr = false_positives as f64 / (test_range - expected_elements) as f64;
        println!("Target FPR: {}, Actual FPR: {}", target_fpr, actual_fpr);

        // The actual FPR should be in the ballpark of the target
        // It might not be exact due to statistical variations
        assert!(actual_fpr < target_fpr * 2.0); // Allow some leeway
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bloom_filter_merge() {
    let test_future = async {
        let mut filter1 = BloomFilter::<String>::new(100, 0.01);
        let mut filter2 = BloomFilter::<String>::new(100, 0.01);

        // Insert different elements into each filter
        filter1.insert(&"apple".to_string());
        filter1.insert(&"banana".to_string());

        filter2.insert(&"cherry".to_string());
        filter2.insert(&"date".to_string());

        // Merge the filters
        filter1.merge(&filter2).unwrap();

        // Check that the merged filter contains all elements
        assert!(filter1.may_contain(&"apple".to_string()));
        assert!(filter1.may_contain(&"banana".to_string()));
        assert!(filter1.may_contain(&"cherry".to_string()));
        assert!(filter1.may_contain(&"date".to_string()));
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bloom_filter_clear() {
    let test_future = async {
        let mut filter = BloomFilter::<String>::new(100, 0.01);

        // Insert elements
        filter.insert(&"apple".to_string());
        filter.insert(&"banana".to_string());

        // Check inserted elements
        assert!(filter.may_contain(&"apple".to_string()));
        assert!(filter.may_contain(&"banana".to_string()));

        // Clear the filter
        filter.clear();

        // Elements should no longer be recognized
        assert!(!filter.may_contain(&"apple".to_string()));
        assert!(!filter.may_contain(&"banana".to_string()));
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
