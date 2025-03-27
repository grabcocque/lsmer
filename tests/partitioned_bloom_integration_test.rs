use lsmer::bloom::PartitionedBloomFilter;
use rayon::prelude::*;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const NUM_ELEMENTS: usize = 10_000;
const FPR: f64 = 0.01;

#[test]
fn test_basic_functionality() {
    let mut filter = PartitionedBloomFilter::<String>::new(NUM_ELEMENTS, FPR, 4);

    // Insert some elements
    filter.insert(&"apple".to_string());
    filter.insert(&"banana".to_string());
    filter.insert(&"cherry".to_string());

    // Check positive matches
    assert!(filter.may_contain(&"apple".to_string()));
    assert!(filter.may_contain(&"banana".to_string()));
    assert!(filter.may_contain(&"cherry".to_string()));

    // Check negative match
    assert!(!filter.may_contain(&"grape".to_string()));
}

#[test]
fn test_parallel_lookups() {
    // Create filter with multiple partitions
    let mut filter = PartitionedBloomFilter::<String>::new(NUM_ELEMENTS, FPR, 4);

    // Create test data
    let inserted_items: Vec<String> = (0..NUM_ELEMENTS).map(|i| format!("item-{}", i)).collect();

    // Insert items
    for item in &inserted_items {
        filter.insert(item);
    }

    // Create some items that weren't inserted
    let non_inserted_items: Vec<String> = (NUM_ELEMENTS..NUM_ELEMENTS + 100)
        .map(|i| format!("item-{}", i))
        .collect();

    // Test parallel lookup
    let results = filter.may_contain_parallel(&inserted_items);

    // All inserted items should return true
    assert!(results.iter().all(|&r| r));

    // All non-inserted items should likely return false
    let non_inserted_results = filter.may_contain_parallel(&non_inserted_items);
    let false_positives = non_inserted_results.iter().filter(|&&r| r).count();

    // Allow some false positives according to our FPR
    let max_expected_false_positives = (non_inserted_items.len() as f64 * FPR * 2.0) as usize;
    assert!(false_positives <= max_expected_false_positives);
}

#[test]
fn test_concurrent_inserts() {
    // Create a shared filter
    let filter = Arc::new(Mutex::new(PartitionedBloomFilter::<String>::new(
        NUM_ELEMENTS,
        FPR,
        8,
    )));

    // Create batches for parallel insertion
    let batches: Vec<Vec<String>> = (0..8)
        .map(|batch_idx| {
            (0..NUM_ELEMENTS / 8)
                .map(|i| format!("batch-{}-item-{}", batch_idx, i))
                .collect()
        })
        .collect();

    // Insert in parallel
    batches.par_iter().for_each(|batch| {
        let mut guard = filter.lock().unwrap();
        for item in batch {
            guard.insert(item);
        }
    });

    // Verify all items were inserted
    let filter = filter.lock().unwrap();
    for batch in &batches {
        for item in batch {
            assert!(filter.may_contain(item));
        }
    }
}

#[test]
fn test_performance_comparison() {
    // Create datasets
    let dataset: Vec<String> = (0..NUM_ELEMENTS).map(|i| format!("item-{}", i)).collect();

    let lookup_set: Vec<String> = (0..1000)
        .map(|i| format!("item-{}", i % (NUM_ELEMENTS * 2)))
        .collect();

    // 1. Standard bloom filter
    let mut standard_filter = lsmer::bloom::BloomFilter::new(NUM_ELEMENTS, FPR);
    for item in &dataset {
        standard_filter.insert(item);
    }

    // 2. Partitioned bloom filter
    let mut partitioned_filter = PartitionedBloomFilter::new(NUM_ELEMENTS, FPR, num_cpus::get());
    for item in &dataset {
        partitioned_filter.insert(item);
    }

    // Time sequential lookups on standard filter
    let start = Instant::now();
    let _results: Vec<bool> = lookup_set
        .iter()
        .map(|item| standard_filter.may_contain(item))
        .collect();
    let standard_time = start.elapsed();

    // Time parallel lookups on partitioned filter
    let start = Instant::now();
    let _results = partitioned_filter.may_contain_parallel(&lookup_set);
    let partitioned_time = start.elapsed();

    println!(
        "Standard filter: {:?}, Partitioned filter: {:?}",
        standard_time, partitioned_time
    );

    // Partitioned should be faster on multi-core systems for batch lookups
    // But we don't assert this since test systems could vary widely
}

#[test]
fn test_bulk_insert() {
    let mut filter = PartitionedBloomFilter::<String>::new(NUM_ELEMENTS, FPR, 4);

    // Create bulk data
    let bulk_data: Vec<String> = (0..1000).map(|i| format!("bulk-{}", i)).collect();

    // Insert in bulk
    filter.insert_bulk(&bulk_data);

    // Verify all items were inserted
    for item in &bulk_data {
        assert!(filter.may_contain(item));
    }
}

#[test]
fn test_may_contain_any_all() {
    let mut filter = PartitionedBloomFilter::<String>::new(NUM_ELEMENTS, FPR, 4);

    // Insert some elements
    for i in 0..100 {
        filter.insert(&format!("present-{}", i));
    }

    // Test may_contain_any
    let any_present = ["present-1", "present-50", "absent-1"];
    let all_present = ["present-1", "present-10", "present-50"];
    let none_present = ["absent-1", "absent-2", "absent-3"];

    // Convert to strings
    let any_present: Vec<String> = any_present.iter().map(|s| s.to_string()).collect();
    let all_present: Vec<String> = all_present.iter().map(|s| s.to_string()).collect();
    let none_present: Vec<String> = none_present.iter().map(|s| s.to_string()).collect();

    // Test results
    assert!(filter.may_contain_any_parallel(&any_present));
    assert!(filter.may_contain_all_parallel(&all_present));
    assert!(!filter.may_contain_all_parallel(&any_present));
    assert!(!filter.may_contain_any_parallel(&none_present));
}
