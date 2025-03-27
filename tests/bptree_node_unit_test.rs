use lsmer::bptree::{BPTreeNode, IndexEntry, IndexKeyValue, NodeType, StorageReference};
use std::ops::Bound;
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_node_creation_and_properties() {
    let test_future = async {
        // Test leaf node creation
        let leaf_node: BPTreeNode<i32, String> = BPTreeNode::new(NodeType::Leaf, 4);
        assert_eq!(leaf_node.node_type, NodeType::Leaf);
        assert_eq!(leaf_node.max_entries, 4);
        assert_eq!(leaf_node.entries.len(), 0);
        assert!(leaf_node.next_leaf.is_none());

        // Test internal node creation
        let internal_node: BPTreeNode<i32, String> = BPTreeNode::new(NodeType::Internal, 5);
        assert_eq!(internal_node.node_type, NodeType::Internal);
        assert_eq!(internal_node.max_entries, 5);
        assert_eq!(internal_node.entries.len(), 0);
        assert!(internal_node.next_leaf.is_none());
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_find_position() {
    let test_future = async {
        let mut node: BPTreeNode<i32, String> = BPTreeNode::new(NodeType::Leaf, 5);

        // Empty node
        assert_eq!(node.find_position(&10), 0);

        // Insert some entries
        node.insert(10, Some("ten".to_string()), None).unwrap();
        node.insert(30, Some("thirty".to_string()), None).unwrap();
        node.insert(50, Some("fifty".to_string()), None).unwrap();

        // Find exact positions
        assert_eq!(node.find_position(&10), 0);
        assert_eq!(node.find_position(&30), 1);
        assert_eq!(node.find_position(&50), 2);

        // Find positions for keys not in the node
        assert_eq!(node.find_position(&5), 0); // Before first
        assert_eq!(node.find_position(&20), 1); // Between 10 and 30
        assert_eq!(node.find_position(&40), 2); // Between 30 and 50
        assert_eq!(node.find_position(&60), 3); // After last
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_insert_and_update() {
    let test_future = async {
        let mut node: BPTreeNode<i32, String> = BPTreeNode::new(NodeType::Leaf, 4);

        // Basic insert
        let result = node.insert(10, Some("ten".to_string()), None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // No split yet
        assert_eq!(node.entries.len(), 1);
        assert_eq!(node.entries[0].kv.key, 10);
        assert_eq!(node.entries[0].kv.value.as_ref().unwrap(), "ten");

        // Insert with storage reference
        let storage_ref = StorageReference {
            file_path: "test.sst".to_string(),
            offset: 100,
            is_tombstone: false,
        };
        let result = node.insert(20, Some("twenty".to_string()), Some(storage_ref.clone()));
        assert!(result.is_ok());
        assert!(result.unwrap().is_none()); // No split yet
        assert_eq!(node.entries.len(), 2);
        assert_eq!(node.entries[1].kv.key, 20);
        assert_eq!(node.entries[1].kv.value.as_ref().unwrap(), "twenty");
        assert_eq!(
            node.entries[1].kv.storage_ref.as_ref().unwrap(),
            &storage_ref
        );

        // Update existing key
        let result = node.insert(10, Some("TEN".to_string()), None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        assert_eq!(node.entries.len(), 2); // Still 2 entries
        assert_eq!(node.entries[0].kv.key, 10);
        assert_eq!(node.entries[0].kv.value.as_ref().unwrap(), "TEN"); // Updated value

        // Insert with null value
        let result = node.insert(30, None, None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        assert_eq!(node.entries.len(), 3);
        assert_eq!(node.entries[2].kv.key, 30);
        assert!(node.entries[2].kv.value.is_none()); // Null value
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_leaf_node_split() {
    let test_future = async {
        let mut leaf_node: BPTreeNode<i32, String> = BPTreeNode::new(NodeType::Leaf, 3);

        // Insert until we need to split
        leaf_node.insert(10, Some("ten".to_string()), None).unwrap();
        leaf_node
            .insert(20, Some("twenty".to_string()), None)
            .unwrap();
        leaf_node
            .insert(30, Some("thirty".to_string()), None)
            .unwrap();

        // This insert should cause a split
        let result = leaf_node
            .insert(40, Some("forty".to_string()), None)
            .unwrap();
        assert!(result.is_some());

        let (median_key, right_node) = result.unwrap();

        // For leaf nodes, median key should be the first key of the right node (30)
        assert_eq!(median_key, 30);

        // Left node should have [10, 20]
        assert_eq!(leaf_node.entries.len(), 2);
        assert_eq!(leaf_node.entries[0].kv.key, 10);
        assert_eq!(leaf_node.entries[1].kv.key, 20);

        // Right node should have [30, 40]
        assert_eq!(right_node.entries.len(), 2);
        assert_eq!(right_node.entries[0].kv.key, 30);
        assert_eq!(right_node.entries[1].kv.key, 40);

        // Leaf nodes should be linked
        assert!(leaf_node.next_leaf.is_some());
        let next_leaf = leaf_node.next_leaf.as_ref().unwrap();
        assert_eq!(next_leaf.entries[0].kv.key, 30);
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_internal_node_split() {
    let test_future = async {
        let mut internal_node: BPTreeNode<i32, String> = BPTreeNode::new(NodeType::Internal, 3);

        // Create some child nodes
        let child1 = Box::new(BPTreeNode::<i32, String>::new(NodeType::Leaf, 3));
        let child2 = Box::new(BPTreeNode::<i32, String>::new(NodeType::Leaf, 3));
        let child3 = Box::new(BPTreeNode::<i32, String>::new(NodeType::Leaf, 3));
        let child4 = Box::new(BPTreeNode::<i32, String>::new(NodeType::Leaf, 3));

        // Insert entries with children
        internal_node.entries.push(IndexEntry {
            kv: IndexKeyValue {
                key: 10,
                value: None,
                storage_ref: None,
            },
            child: Some(child1),
        });

        internal_node.entries.push(IndexEntry {
            kv: IndexKeyValue {
                key: 20,
                value: None,
                storage_ref: None,
            },
            child: Some(child2),
        });

        internal_node.entries.push(IndexEntry {
            kv: IndexKeyValue {
                key: 30,
                value: None,
                storage_ref: None,
            },
            child: Some(child3),
        });

        // Add one more to force a split
        internal_node.entries.push(IndexEntry {
            kv: IndexKeyValue {
                key: 40,
                value: None,
                storage_ref: None,
            },
            child: Some(child4),
        });

        // Split the internal node
        let (median_key, _right_node) = internal_node.split();

        // For internal nodes, median key should be the middle key (20 or 30)
        // With 4 entries, it depends on the implementation, but should be 20 or 30
        assert!(median_key == 20 || median_key == 30);

        // Internal nodes should not be linked
        assert!(internal_node.next_leaf.is_none());
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}

#[tokio::test]
async fn test_range_queries() {
    let test_future = async {
        let mut node: BPTreeNode<i32, String> = BPTreeNode::new(NodeType::Leaf, 5);

        // Insert some entries in sorted order
        node.insert(10, Some("ten".to_string()), None).unwrap();
        node.insert(20, Some("twenty".to_string()), None).unwrap();
        node.insert(30, Some("thirty".to_string()), None).unwrap();
        node.insert(40, Some("forty".to_string()), None).unwrap();
        node.insert(50, Some("fifty".to_string()), None).unwrap();

        // Test various range queries

        // Inclusive range
        let range1 = node.range(20..=40);
        assert_eq!(range1.len(), 3);
        assert_eq!(range1[0].kv.key, 20);
        assert_eq!(range1[1].kv.key, 30);
        assert_eq!(range1[2].kv.key, 40);

        // Exclusive range
        let range2 = node.range(20..40);
        assert_eq!(range2.len(), 2);
        assert_eq!(range2[0].kv.key, 20);
        assert_eq!(range2[1].kv.key, 30);

        // From bound
        let range3 = node.range(30..);
        assert_eq!(range3.len(), 3);
        assert_eq!(range3[0].kv.key, 30);
        assert_eq!(range3[1].kv.key, 40);
        assert_eq!(range3[2].kv.key, 50);

        // To bound
        let range4 = node.range(..30);
        assert_eq!(range4.len(), 2);
        assert_eq!(range4[0].kv.key, 10);
        assert_eq!(range4[1].kv.key, 20);

        // Full range
        let range5 = node.range(..);
        assert_eq!(range5.len(), 5);

        // Empty range
        let range6 = node.range(100..200);
        assert_eq!(range6.len(), 0);

        // Single element
        let range7 = node.range(20..=20);
        assert_eq!(range7.len(), 1);
        assert_eq!(range7[0].kv.key, 20);

        // Test with explicit bounds
        let range8 = node.range((Bound::Excluded(20), Bound::Excluded(40)));
        assert_eq!(range8.len(), 1);
        assert_eq!(range8[0].kv.key, 30);
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(_) => (),
        Err(_) => panic!("Test timed out after 10 seconds"),
    }
}
