use lsmer::bptree::{BPTreeNode, NodeType};
use lsmer::bptree::{BPlusTree, StorageReference};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::time::timeout;

static SETUP: OnceCell<()> = OnceCell::const_new();

async fn setup() {
    SETUP
        .get_or_init(|| async {
            // Setup code if needed
        })
        .await;
}

#[tokio::test]
async fn test_bptree_range_query() {
    let test_future = async {
        let mut tree = BPlusTree::new(4);

        // Insert some test data
        for i in 0..10 {
            tree.insert(i, vec![i as u8], None).unwrap();
        }

        // Test inclusive range
        let result = tree.range(2..=5).unwrap();
        assert_eq!(result.len(), 4);
        for (i, kv) in result.iter().enumerate() {
            assert_eq!(kv.key, i as i32 + 2);
            assert_eq!(kv.value, Some(vec![(i + 2) as u8]));
        }

        // Test exclusive range
        let result = tree.range(2..5).unwrap();
        assert_eq!(result.len(), 3);
        for (i, kv) in result.iter().enumerate() {
            assert_eq!(kv.key, i as i32 + 2);
            assert_eq!(kv.value, Some(vec![(i + 2) as u8]));
        }

        // Test full range
        let result = tree.range(..).unwrap();
        assert_eq!(result.len(), 10);
        for (i, kv) in result.iter().enumerate() {
            assert_eq!(kv.key, i as i32);
            assert_eq!(kv.value, Some(vec![i as u8]));
        }
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bptree_storage_ref() {
    let test_future = async {
        let mut tree = BPlusTree::new(4);
        let storage_ref = Some(StorageReference {
            file_path: "test.sst".to_string(),
            offset: 0,
            is_tombstone: false,
        });

        // Insert with storage reference
        for i in 0..5 {
            tree.insert(i, vec![i as u8], storage_ref.clone()).unwrap();
        }

        let result = tree.range(1..=3).unwrap();
        assert_eq!(result.len(), 3);
        for kv in result.iter() {
            assert_eq!(kv.storage_ref, storage_ref);
            assert!(kv.key >= 1 && kv.key <= 3);
            assert_eq!(kv.value, Some(vec![kv.key as u8]));
        }
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bptree_leaf_traversal() {
    let test_future = async {
        let mut tree = BPlusTree::new(4);

        // Insert values with gaps
        for i in (0..20).step_by(2) {
            tree.insert(i, vec![i as u8], None).unwrap();
        }

        // Test range that includes gaps
        let result = tree.range(5..=10).unwrap();
        assert_eq!(result.len(), 3);
        for kv in result.iter() {
            assert!(kv.key >= 5 && kv.key <= 10);
            assert_eq!(kv.value, Some(vec![kv.key as u8]));
            assert_eq!(kv.key % 2, 0); // Should only have even numbers
        }

        // Test range with sequential values
        let result = tree.range(2..=7).unwrap();
        assert_eq!(result.len(), 3);
        for kv in result.iter() {
            assert!(kv.key >= 2 && kv.key <= 7);
            assert_eq!(kv.value, Some(vec![kv.key as u8]));
        }

        // Test empty range
        let result = tree.range(20..=30).unwrap();
        assert_eq!(result.len(), 0);
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bptree_edge_cases() {
    let test_future = async {
        let mut tree = BPlusTree::new(4);

        // Test empty tree
        let result = tree.range(..).unwrap();
        assert_eq!(result.len(), 0);

        // Test single value
        tree.insert(5, vec![5], None).unwrap();

        // Test exact range
        let result = tree.range(5..=5).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, 5);
        assert_eq!(result[0].value, Some(vec![5]));

        // Test range after value
        let result = tree.range(6..=10).unwrap();
        assert_eq!(result.len(), 0);

        // Test range before value
        let result = tree.range(0..=5).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].key, 5);
    };

    // Run with a 10-second timeout
    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_bptree_large_dataset() {
    setup().await;

    // Create a new B+ tree with order 4 (max 3 keys per node)
    let mut tree = BPlusTree::new(4);

    // Insert 1000 items
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let value = vec![i as u8];
        tree.insert(key, value, None).unwrap();
    }

    // Verify all items exist
    for i in 0..1000 {
        let key = format!("key_{:04}", i);
        let result = tree.find(&key).unwrap();
        assert!(result.is_some());
        if let Some(kv) = result {
            assert_eq!(kv.value, Some(vec![i as u8]));
        }
    }

    // Test range query
    let range_result = tree
        .range("key_0100".to_string().."key_0200".to_string())
        .unwrap();
    assert_eq!(range_result.len(), 100);
}

#[tokio::test]
async fn test_bptree_node_operations() {
    // Test direct BPTreeNode operations

    // Create a leaf node with max 3 entries
    let mut leaf_node: BPTreeNode<String, Vec<u8>> = BPTreeNode::new(NodeType::Leaf, 3);
    assert_eq!(leaf_node.node_type, NodeType::Leaf);
    assert_eq!(leaf_node.entries.len(), 0);
    assert_eq!(leaf_node.max_entries, 3);

    // Insert keys
    leaf_node
        .insert("key1".to_string(), Some(vec![1]), None)
        .unwrap();
    assert_eq!(leaf_node.entries.len(), 1);

    leaf_node
        .insert("key3".to_string(), Some(vec![3]), None)
        .unwrap();
    assert_eq!(leaf_node.entries.len(), 2);

    leaf_node
        .insert("key2".to_string(), Some(vec![2]), None)
        .unwrap();
    assert_eq!(leaf_node.entries.len(), 3);

    // Verify the order of keys after insertion
    assert_eq!(leaf_node.entries[0].kv.key, "key1");
    assert_eq!(leaf_node.entries[1].kv.key, "key2");
    assert_eq!(leaf_node.entries[2].kv.key, "key3");

    // Test finding positions
    let pos1 = leaf_node.find_position(&"key1".to_string());
    assert_eq!(pos1, 0);

    let pos3 = leaf_node.find_position(&"key3".to_string());
    assert_eq!(pos3, 2);

    let non_existent = leaf_node.find_position(&"key5".to_string());
    assert_eq!(non_existent, 3); // Should be at the end

    // Test splitting when max entries is reached
    let split_result = leaf_node
        .insert("key4".to_string(), Some(vec![4]), None)
        .unwrap();

    // Split should have occurred and returned a result
    assert!(split_result.is_some());
    let (median_key, right_node) = split_result.unwrap();

    // For a leaf node with max_entries=3, when inserting a 4th entry:
    // - Split point is at index 2 (4 entries / 2)
    // - Left node keeps entries [0..2]
    // - Right node gets entries [2..4]
    // - Median key is the first key in right node ("key3")
    assert_eq!(median_key, "key3");

    // Original node should have 2 entries, right node should have 2 entries
    assert_eq!(leaf_node.entries.len(), 2);
    assert_eq!(right_node.entries.len(), 2);

    // Verify right node has key3 and key4
    assert_eq!(right_node.entries[0].kv.key, "key3");
    assert_eq!(right_node.entries[1].kv.key, "key4");

    // Test range query on a node
    let range_entries = leaf_node.range("key1".to_string().."key3".to_string());
    assert_eq!(range_entries.len(), 2); // Should include key1 and key2

    // Create internal node and test operations
    let mut internal_node: BPTreeNode<String, Vec<u8>> = BPTreeNode::new(NodeType::Internal, 3);
    assert_eq!(internal_node.node_type, NodeType::Internal);

    // Internal nodes should function differently than leaf nodes when splitting
    internal_node
        .insert("key1".to_string(), None, None)
        .unwrap();
    internal_node
        .insert("key2".to_string(), None, None)
        .unwrap();
    internal_node
        .insert("key3".to_string(), None, None)
        .unwrap();

    // Adding a 4th entry should cause a split
    let split_result = internal_node
        .insert("key4".to_string(), None, None)
        .unwrap();
    assert!(split_result.is_some());
}

// Modified storage ref test
#[tokio::test]
async fn test_bptree_storage_ref_check() {
    setup().await;

    // Create a B+ tree with order 4
    let mut tree = BPlusTree::new(4);

    // Create a storage reference
    let storage_ref = StorageReference {
        file_path: "test.db".to_string(),
        offset: 123,
        is_tombstone: false,
    };

    // Insert with storage ref
    tree.insert("key1".to_string(), vec![1, 2, 3], Some(storage_ref.clone()))
        .unwrap();

    // Check that the storage ref is preserved
    let result = tree.find(&"key1".to_string()).unwrap();
    assert!(result.is_some());
    if let Some(kv) = result {
        // Use as_ref to avoid moving storage_ref
        if let Some(ref sr) = kv.storage_ref {
            assert_eq!(sr.file_path, "test.db");
            assert_eq!(sr.offset, 123);
            assert!(!sr.is_tombstone);
        } else {
            panic!("Storage reference is missing");
        }
    }
}
