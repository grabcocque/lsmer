use lsmer::bptree::StorageReference;
use lsmer::lsm_index::{make_gen_ref, GenIndexEntry};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_gen_ref_basic() {
    // Create a basic generational reference
    let handle = make_gen_ref(vec![1, 2, 3]);

    // Check value access
    assert_eq!(*handle.get(), vec![1, 2, 3]);

    // Check generation
    assert_eq!(handle.generation(), 0);
    assert!(!handle.is_stale());

    // Clone the handle and check values
    let handle2 = handle.clone();
    assert_eq!(*handle2.get(), vec![1, 2, 3]);
    assert_eq!(handle2.generation(), 0);
}

#[test]
fn test_gen_index_entry() {
    // Create a storage reference
    let storage_ref = StorageReference {
        file_path: "test.db".to_string(),
        offset: 42,
        is_tombstone: false,
    };

    // Create a GenIndexEntry with a value and storage reference
    let entry = GenIndexEntry::new(Some(vec![1, 2, 3]), Some(storage_ref));

    // Check value access
    assert_eq!(entry.value(), Some(vec![1, 2, 3]));

    // Check storage reference access
    let sr = entry.storage_ref().unwrap();
    assert_eq!(sr.file_path, "test.db");
    assert_eq!(sr.offset, 42);
    assert!(!sr.is_tombstone);

    // Check tombstone status
    assert!(!entry.is_tombstone());

    // Create a tombstone entry
    let tombstone_ref = StorageReference {
        file_path: "test.db".to_string(),
        offset: 42,
        is_tombstone: true,
    };
    let tombstone_entry = GenIndexEntry::new(None, Some(tombstone_ref));

    // Check tombstone status
    assert!(tombstone_entry.is_tombstone());
}

#[test]
fn test_concurrent_access() {
    // Create a shared reference
    let handle = make_gen_ref(vec![1, 2, 3]);
    let handle = Arc::new(handle);

    // Set up concurrent access from multiple threads
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = Vec::new();

    for i in 0..10 {
        let handle_clone = handle.clone();
        let barrier_clone = barrier.clone();

        let thread_handle = thread::spawn(move || {
            // Wait for all threads to be ready
            barrier_clone.wait();

            // Read the value
            let value = handle_clone.clone_data();
            assert_eq!(value, vec![1, 2, 3]);

            // Check generation
            assert_eq!(handle_clone.generation(), 0);

            // Return thread ID and value
            (i, value)
        });

        handles.push(thread_handle);
    }

    // Collect results from all threads
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // Verify all threads got the correct value
    for (_thread_id, value) in results {
        assert_eq!(value, vec![1, 2, 3]);
    }
}

#[test]
fn test_update_with_stale_detection() {
    // Create a new GenIndexEntry
    let entry1 = GenIndexEntry::new(Some(vec![1, 2, 3]), None);

    // Clone the entry
    let entry2 = entry1.clone();

    // Update entry1 with new value
    let entry1_updated = entry1.with_value(vec![4, 5, 6]);

    // Check values
    assert_eq!(entry1_updated.value(), Some(vec![4, 5, 6]));

    // The original entry2 should still have the original value
    assert_eq!(entry2.value(), Some(vec![1, 2, 3]));
}
