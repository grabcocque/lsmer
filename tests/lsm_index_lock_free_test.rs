use lsmer::lsm_index::LsmIndex;
use std::fs;
use std::io;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// Set up a clean test directory
fn setup_test_dir(dir: &str) -> io::Result<()> {
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir)
}

#[test]
fn test_concurrent_inserts() {
    let test_dir = "target/test_concurrent_inserts";
    setup_test_dir(test_dir).unwrap();

    // Create a shared LSM index
    let lsm =
        Arc::new(LsmIndex::new(1024 * 1024, test_dir.to_string(), None, false, 0.01).unwrap());

    // Number of threads
    let thread_count = 8;
    // Operations per thread
    let ops_per_thread = 1000;

    // Create a barrier to synchronize thread start
    let barrier = Arc::new(Barrier::new(thread_count));

    // Create threads
    let mut handles = vec![];
    for thread_id in 0..thread_count {
        let lsm_clone = Arc::clone(&lsm);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            barrier_clone.wait();

            // Each thread inserts its own range of keys
            for i in 0..ops_per_thread {
                let key = format!("key_{}_{}", thread_id, i);
                let value = vec![thread_id as u8, i as u8];
                lsm_clone.insert(key, value).unwrap();
            }
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all keys were inserted correctly
    for thread_id in 0..thread_count {
        for i in 0..ops_per_thread {
            let key = format!("key_{}_{}", thread_id, i);
            let expected_value = vec![thread_id as u8, i as u8];
            let actual_value = lsm.get(&key).unwrap().unwrap();
            assert_eq!(
                actual_value, expected_value,
                "Value mismatch for key {}",
                key
            );
        }
    }
}

#[test]
fn test_concurrent_reads_and_writes() {
    let test_dir = "target/test_concurrent_reads_writes";
    setup_test_dir(test_dir).unwrap();

    // Create a shared LSM index
    let lsm =
        Arc::new(LsmIndex::new(1024 * 1024, test_dir.to_string(), None, false, 0.01).unwrap());

    // Insert some initial data
    for i in 0..1000 {
        let key = format!("initial_key_{}", i);
        let value = vec![i as u8];
        lsm.insert(key, value).unwrap();
    }

    // Flag to track if the writer threads are active
    let writers_active = Arc::new(std::sync::atomic::AtomicBool::new(true));

    // Number of threads for each operation type
    let read_threads = 6;
    let write_threads = 2;
    let total_threads = read_threads + write_threads;

    // Create a barrier to synchronize thread start
    let barrier = Arc::new(Barrier::new(total_threads));

    // Create reader threads
    let mut handles = vec![];
    for _ in 0..read_threads {
        let lsm_clone = Arc::clone(&lsm);
        let barrier_clone = Arc::clone(&barrier);
        let writers_active_clone = Arc::clone(&writers_active);

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            barrier_clone.wait();

            // Each thread reads all initial keys many times
            for _ in 0..100 {
                for i in 0..1000 {
                    let key = format!("initial_key_{}", i);
                    let value = lsm_clone.get(&key).unwrap();

                    // Just assert that the value exists, since it might have been updated
                    assert!(value.is_some(), "Key {} should exist", key);

                    // If writers are still running, we can't assert specific values
                    if !writers_active_clone.load(std::sync::atomic::Ordering::Relaxed) {
                        // Only check specific values after writers have stopped
                        // Original value is [i as u8] or updated value from writers [0xFF, thread_id, i]
                        let val = value.unwrap();
                        if val.len() == 1 {
                            assert_eq!(val[0], i as u8);
                        } else if val.len() == 3 {
                            assert_eq!(val[0], 0xFF);
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Create writer threads
    for thread_id in 0..write_threads {
        let lsm_clone = Arc::clone(&lsm);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            // Wait for all threads to be ready
            barrier_clone.wait();

            // Each thread inserts its own range of keys
            for i in 0..200 {
                let key = format!("writer_{}_{}", thread_id, i);
                let value = vec![thread_id as u8, i as u8];
                lsm_clone.insert(key, value).unwrap();

                // Also update some initial keys
                let initial_key = format!("initial_key_{}", (i * thread_id) % 1000);
                let new_value = vec![0xFF, thread_id as u8, i as u8];
                lsm_clone.insert(initial_key, new_value).unwrap();
            }
        });

        handles.push(handle);
    }

    // Wait for writer threads to complete
    let writer_threads = handles.split_off(read_threads);
    for handle in writer_threads {
        handle.join().unwrap();
    }

    // Signal that writers are done
    writers_active.store(false, std::sync::atomic::Ordering::Relaxed);

    // Wait for reader threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // After all threads complete, verify that all keys exist
    // Writer thread keys
    for thread_id in 0..write_threads {
        for i in 0..200 {
            let key = format!("writer_{}_{}", thread_id, i);
            let expected_value = vec![thread_id as u8, i as u8];
            let actual_value = lsm.get(&key).unwrap().unwrap();
            assert_eq!(
                actual_value, expected_value,
                "Value mismatch for writer key {}",
                key
            );
        }
    }
}

#[test]
fn test_performance_comparison() {
    let test_dir = "target/test_performance_comparison";
    setup_test_dir(test_dir).unwrap();

    // Create a shared LSM index
    let lsm =
        Arc::new(LsmIndex::new(1024 * 1024, test_dir.to_string(), None, false, 0.01).unwrap());

    // Insert some initial data
    for i in 0..10000 {
        let key = format!("key_{}", i);
        let value = vec![i as u8];
        lsm.insert(key, value).unwrap();
    }

    // Test single-threaded performance
    let start = Instant::now();
    for i in 0..1000 {
        let key = format!("key_{}", i);
        let _ = lsm.get(&key).unwrap();
    }
    let single_thread_duration = start.elapsed();

    // Test multi-threaded performance
    let thread_count = 4;
    let barrier = Arc::new(Barrier::new(thread_count));
    let mut handles = vec![];
    let total_duration = Arc::new(std::sync::Mutex::new(Duration::from_secs(0)));

    for _ in 0..thread_count {
        let lsm_clone = Arc::clone(&lsm);
        let barrier_clone = Arc::clone(&barrier);
        let total_duration_clone = Arc::clone(&total_duration);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            let start = Instant::now();
            for i in 0..1000 {
                let key = format!("key_{}", i);
                let _ = lsm_clone.get(&key).unwrap();
            }
            let thread_duration = start.elapsed();

            // Add this thread's duration to the total
            let mut total = total_duration_clone.lock().unwrap();
            *total += thread_duration;
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }

    // Calculate average thread duration
    let avg_thread_duration = {
        let total = total_duration.lock().unwrap();
        *total / thread_count as u32
    };

    println!("Single thread duration: {:?}", single_thread_duration);
    println!(
        "Average thread duration (across {} threads): {:?}",
        thread_count, avg_thread_duration
    );

    // Assert that per-thread performance is reasonable - this is a very basic
    // check and might need adjustment based on the actual hardware
    // We're not making a strict assertion because performance can vary widely across systems
    println!(
        "Performance ratio: {:.2}x",
        single_thread_duration.as_secs_f64() / avg_thread_duration.as_secs_f64()
    );
}

#[test]
fn test_range_query_under_concurrent_writes() {
    let test_dir = "target/test_range_query_concurrent";
    setup_test_dir(test_dir).unwrap();

    // Create a shared LSM index
    let lsm =
        Arc::new(LsmIndex::new(1024 * 1024, test_dir.to_string(), None, false, 0.01).unwrap());

    // Insert some initial data with keys that will sort in a predictable order
    for i in 0..1000 {
        let key = format!("key_{:04}", i); // Pad with zeros for consistent ordering
        let value = vec![i as u8];
        lsm.insert(key, value).unwrap();
    }

    // Spawn a thread that continuously does writes
    let lsm_clone = Arc::clone(&lsm);
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let running_clone = Arc::clone(&running);

    let writer_handle = thread::spawn(move || {
        let mut counter = 0;
        while running_clone.load(std::sync::atomic::Ordering::Relaxed) {
            // Mix of inserts and updates
            let key = format!("key_{:04}", counter % 2000);
            let value = vec![counter as u8];
            lsm_clone.insert(key, value).unwrap();
            counter += 1;

            // Small sleep to avoid completely overwhelming the system
            thread::sleep(Duration::from_micros(10));
        }
    });

    // In the main thread, do range queries
    for _ in 0..10 {
        // Different range each time
        for start in (0..900).step_by(100) {
            let end = start + 100;
            let range_start = format!("key_{:04}", start);
            let range_end = format!("key_{:04}", end);

            let results = lsm.range(range_start..range_end).unwrap();

            // We can't assert exact counts because the writer might add/update keys
            // but we can check that results make sense
            assert!(!results.is_empty(), "Range query should return results");

            // Check that keys are in order
            let mut prev_key = String::new();
            for (key, _) in &results {
                if !prev_key.is_empty() {
                    assert!(key > &prev_key, "Keys should be in order");
                }
                prev_key = key.clone();
            }
        }
    }

    // Stop the writer thread
    running.store(false, std::sync::atomic::Ordering::Relaxed);
    writer_handle.join().unwrap();
}
