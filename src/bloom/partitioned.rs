use rayon::prelude::*;
use siphasher::sip::SipHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::Arc;

use super::BloomFilter;

/// A partitioned Bloom filter that enables parallel lookups
///
/// This implementation divides a single logical bloom filter into multiple
/// independent partitions that can be queried in parallel, improving
/// lookup performance on multi-core systems.
#[derive(Debug, Clone)]
pub struct PartitionedBloomFilter<T> {
    /// Number of partitions
    num_partitions: usize,
    /// Individual BloomFilter partitions
    partitions: Vec<BloomFilter<T>>,
    /// Marker for phantom data
    _marker: PhantomData<T>,
    /// Expected number of elements
    #[allow(dead_code)] // Kept for future optimizations
    expected_elements: usize,
    /// Target false positive rate
    #[allow(dead_code)] // Kept for future optimizations
    false_positive_rate: f64,
}

impl<T: Hash + Send + Sync> PartitionedBloomFilter<T> {
    /// Creates a new partitioned Bloom filter with the given number of partitions
    ///
    /// # Arguments
    ///
    /// * `expected_elements` - Total expected elements to be inserted across all partitions
    /// * `false_positive_rate` - Target false positive rate
    /// * `num_partitions` - Number of partitions (typically matches available CPU cores)
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bloom::PartitionedBloomFilter;
    ///
    /// // Create a filter with 4 partitions for 1000 elements with 1% FPR
    /// let filter: PartitionedBloomFilter<&str> = PartitionedBloomFilter::new(1000, 0.01, 4);
    /// ```
    pub fn new(expected_elements: usize, false_positive_rate: f64, num_partitions: usize) -> Self {
        // Use at least 1 partition, default to # of CPUs if 0
        let num_partitions = if num_partitions == 0 {
            num_cpus::get()
        } else {
            num_partitions
        };

        // Cap number of partitions to a reasonable max
        let num_partitions = std::cmp::min(num_partitions, 64);

        // Calculate elements per partition
        let elements_per_partition = expected_elements.div_ceil(num_partitions);

        // Create individual bloom filters for each partition
        let partitions = (0..num_partitions)
            .map(|_| BloomFilter::new(elements_per_partition, false_positive_rate))
            .collect();

        Self {
            partitions,
            num_partitions,
            expected_elements,
            false_positive_rate,
            _marker: PhantomData,
        }
    }

    /// Determines which partition an item belongs to
    fn get_partition_index(&self, item: &T) -> usize {
        let mut hasher = SipHasher::new_with_keys(0xDEADBEEF, 0xCAFEBABE);
        item.hash(&mut hasher);
        let hash = hasher.finish();

        // Use modulo to determine partition
        (hash as usize) % self.num_partitions
    }

    /// Inserts an item into the appropriate partition
    ///
    /// # Arguments
    ///
    /// * `item` - The item to insert
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::<&str>::new(1000, 0.01, 4);
    /// filter.insert(&"test");
    /// assert!(filter.may_contain(&"test"));
    /// ```
    pub fn insert(&mut self, item: &T) {
        let idx = self.get_partition_index(item);
        self.partitions[idx].insert(item);
    }

    /// Inserts items in bulk, potentially using parallelism
    ///
    /// # Arguments
    ///
    /// * `items` - Slice of items to insert
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::<&str>::new(1000, 0.01, 4);
    /// filter.insert_bulk(&["apple", "banana", "cherry"]);
    /// ```
    pub fn insert_bulk(&mut self, items: &[T]) {
        // Group items by partition to minimize lock contention
        let mut partition_items: Vec<Vec<&T>> = vec![Vec::new(); self.num_partitions];

        for item in items {
            let idx = self.get_partition_index(item);
            partition_items[idx].push(item);
        }

        // Insert items into their respective partitions
        for (idx, items) in partition_items.into_iter().enumerate() {
            for item in items {
                self.partitions[idx].insert(item);
            }
        }
    }

    /// Checks if an item might be in the filter
    ///
    /// # Arguments
    ///
    /// * `item` - The item to check
    ///
    /// # Returns
    ///
    /// * `bool` - True if the item might be in the set, false if definitely not
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::<&str>::new(1000, 0.01, 4);
    /// filter.insert(&"apple");
    /// assert!(filter.may_contain(&"apple"));
    /// assert!(!filter.may_contain(&"banana"));
    /// ```
    pub fn may_contain(&self, item: &T) -> bool {
        let idx = self.get_partition_index(item);
        self.partitions[idx].may_contain(item)
    }

    /// Checks if multiple items might be in the filter using parallel execution
    ///
    /// # Arguments
    ///
    /// * `items` - Slice of items to check
    ///
    /// # Returns
    ///
    /// * `Vec<bool>` - Vector of results (true if might contain, false if definitely not)
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::<&str>::new(1000, 0.01, 4);
    /// filter.insert(&"apple");
    /// filter.insert(&"cherry");
    ///
    /// let results = filter.may_contain_parallel(&["apple", "banana", "cherry"]);
    /// assert_eq!(results, vec![true, false, true]);
    /// ```
    pub fn may_contain_parallel(&self, items: &[T]) -> Vec<bool> {
        let filter = Arc::new(self);

        items
            .par_iter()
            .map(|item| {
                let idx = filter.get_partition_index(item);
                filter.partitions[idx].may_contain(item)
            })
            .collect()
    }

    /// Checks if any of the items might be in the filter (parallel execution)
    ///
    /// # Arguments
    ///
    /// * `items` - Slice of items to check
    ///
    /// # Returns
    ///
    /// * `bool` - True if any item might be in the set, false if all definitely not
    pub fn may_contain_any_parallel(&self, items: &[T]) -> bool {
        let filter = Arc::new(self);

        items.par_iter().any(|item| {
            let idx = filter.get_partition_index(item);
            filter.partitions[idx].may_contain(item)
        })
    }

    /// Checks if all items might be in the filter (parallel execution)
    ///
    /// # Arguments
    ///
    /// * `items` - Slice of items to check
    ///
    /// # Returns
    ///
    /// * `bool` - True if all items might be in the set, false otherwise
    pub fn may_contain_all_parallel(&self, items: &[T]) -> bool {
        let filter = Arc::new(self);

        items.par_iter().all(|item| {
            let idx = filter.get_partition_index(item);
            filter.partitions[idx].may_contain(item)
        })
    }

    /// Clears all partitions
    ///
    /// # Examples
    ///
    /// ```
    /// use lsmer::bloom::PartitionedBloomFilter;
    ///
    /// let mut filter = PartitionedBloomFilter::<&str>::new(1000, 0.01, 4);
    /// filter.insert(&"test");
    /// filter.clear();
    /// assert!(!filter.may_contain(&"test"));
    /// ```
    pub fn clear(&mut self) {
        for partition in &mut self.partitions {
            partition.clear();
        }
    }

    /// Gets the number of partitions
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Gets a reference to a specific partition
    ///
    /// # Arguments
    ///
    /// * `index` - Index of the partition to get
    ///
    /// # Returns
    ///
    /// * `Option<&BloomFilter<T>>` - Reference to the partition if index is valid, None otherwise
    pub fn get_partition(&self, index: usize) -> Option<&BloomFilter<T>> {
        self.partitions.get(index)
    }

    /// Sets the partitions (used for deserialization)
    ///
    /// # Arguments
    ///
    /// * `partitions` - Vector of bloom filters to use as partitions
    pub fn set_partitions(&mut self, partitions: Vec<BloomFilter<T>>) {
        self.num_partitions = partitions.len();
        self.partitions = partitions;
    }

    /// Calculates the estimated false positive rate based on current occupancy
    ///
    /// # Arguments
    ///
    /// * `num_elements` - Current number of elements in the filter
    pub fn false_positive_rate(&self, num_elements: usize) -> f64 {
        // Each partition handles approximately the same number of elements
        let elements_per_partition = num_elements / self.num_partitions;

        // Average FPR across all partitions (they should be similar)
        self.partitions
            .iter()
            .map(|p| p.false_positive_rate(elements_per_partition))
            .sum::<f64>()
            / self.num_partitions as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_query() {
        let mut filter = PartitionedBloomFilter::<String>::new(1000, 0.01, 4);

        // Insert some elements
        filter.insert(&"apple".to_string());
        filter.insert(&"banana".to_string());
        filter.insert(&"cherry".to_string());

        // Test single lookups
        assert!(filter.may_contain(&"apple".to_string()));
        assert!(filter.may_contain(&"banana".to_string()));
        assert!(filter.may_contain(&"cherry".to_string()));
        assert!(!filter.may_contain(&"grape".to_string()));
    }

    #[test]
    fn test_parallel_queries() {
        let mut filter = PartitionedBloomFilter::<&str>::new(1000, 0.01, 4);

        // Insert some elements
        let items = ["apple", "banana", "cherry", "date", "elderberry"];
        for item in &items {
            filter.insert(item);
        }

        // Test parallel lookups
        let queries = ["apple", "banana", "grape", "date", "fig"];
        let results = filter.may_contain_parallel(&queries);

        assert_eq!(results, vec![true, true, false, true, false]);
        assert!(filter.may_contain_any_parallel(&queries));
        assert!(!filter.may_contain_all_parallel(&queries));
    }

    #[test]
    fn test_bulk_insert() {
        let mut filter = PartitionedBloomFilter::<&str>::new(1000, 0.01, 4);

        // Bulk insert
        let items = ["apple", "banana", "cherry", "date", "elderberry"];
        filter.insert_bulk(&items);

        // Verify all items were inserted
        for item in &items {
            assert!(filter.may_contain(item));
        }

        // Verify a non-inserted item isn't found
        assert!(!filter.may_contain(&"fig"));
    }

    #[test]
    fn test_clear() {
        let mut filter = PartitionedBloomFilter::<&str>::new(1000, 0.01, 4);

        // Insert and verify
        filter.insert(&"test");
        assert!(filter.may_contain(&"test"));

        // Clear and verify
        filter.clear();
        assert!(!filter.may_contain(&"test"));
    }
}
