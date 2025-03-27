use siphasher::sip::SipHasher;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

/// A Bloom filter implementation using double hashing technique
/// to reduce the number of required hash functions.
#[derive(Debug, Clone)]
pub struct BloomFilter<T> {
    /// Bit array to store the filter data
    bits: Vec<u8>,
    /// Number of hash functions to use
    num_hashes: usize,
    /// Size of the bit array in bits
    size_bits: usize,
    /// Phantom data for type T
    _marker: PhantomData<T>,
}

impl<T: Hash> BloomFilter<T> {
    /// Create a new Bloom filter with optimal parameters for the expected number of elements
    /// and desired false positive rate.
    pub fn new(expected_elements: usize, false_positive_rate: f64) -> Self {
        // Safety check for expected elements and false positive rate
        let expected_elements = if expected_elements == 0 {
            1 // Avoid division by zero
        } else if expected_elements > 10_000_000 {
            10_000_000 // Cap at 10 million elements for safety
        } else {
            expected_elements
        };

        let false_positive_rate = if false_positive_rate <= 0.0 || false_positive_rate >= 1.0 {
            0.01 // Default to 1% if out of range
        } else {
            false_positive_rate
        };

        // Calculate optimal size in bits
        // m = -n * ln(p) / (ln(2)^2)
        let ln2_squared = std::f64::consts::LN_2.powi(2);
        let mut size_bits = (-1.0 * (expected_elements as f64) * false_positive_rate.ln()
            / ln2_squared)
            .ceil() as usize;

        // Safety cap on maximum bit size
        const MAX_BLOOM_FILTER_BITS: usize = 100_000_000; // 100 million bits (12.5MB)
        if size_bits > MAX_BLOOM_FILTER_BITS {
            size_bits = MAX_BLOOM_FILTER_BITS;
        }

        // Calculate optimal number of hash functions
        // k = (m/n) * ln(2)
        let mut num_hashes = ((size_bits as f64 / expected_elements as f64)
            * std::f64::consts::LN_2)
            .ceil() as usize;

        // Limit number of hash functions for performance
        const MAX_HASH_FUNCTIONS: usize = 20;
        num_hashes = num_hashes.clamp(1, MAX_HASH_FUNCTIONS);

        // Size in bytes (rounded up)
        let size_bytes = (size_bits + 7) / 8;

        BloomFilter {
            bits: vec![0; size_bytes],
            num_hashes,
            size_bits,
            _marker: PhantomData,
        }
    }

    /// Insert an element into the Bloom filter
    pub fn insert(&mut self, item: &T) {
        let (h1, h2) = self.get_hash_values(item);

        for i in 0..self.num_hashes {
            let index = self.get_bit_index(h1, h2, i);
            self.set_bit(index);
        }
    }

    /// Check if an element might be in the Bloom filter
    pub fn may_contain(&self, item: &T) -> bool {
        let (h1, h2) = self.get_hash_values(item);

        for i in 0..self.num_hashes {
            let index = self.get_bit_index(h1, h2, i);
            if !self.get_bit(index) {
                return false; // Definitely not in the set
            }
        }

        true // Possibly in the set
    }

    /// Compute two different hash values for the item, to be used with the double hashing technique
    fn get_hash_values(&self, item: &T) -> (u64, u64) {
        // Use SipHasher with different keys for the two hash functions
        // SipHasher takes two u64 values as keys (k0 and k1)
        let mut hasher1 = SipHasher::new_with_keys(0x0123456789ABCDEF, 0xFEDCBA9876543210);
        let mut hasher2 = SipHasher::new_with_keys(0xABCDEF0123456789, 0x0123456789ABCDEF);

        // Hash the item with each hasher
        item.hash(&mut hasher1);
        let h1 = hasher1.finish();

        item.hash(&mut hasher2);
        let h2 = hasher2.finish();

        // Ensure h2 is odd to ensure we hit all positions when using double hashing
        let h2 = if h2 % 2 == 0 { h2 + 1 } else { h2 };

        (h1, h2)
    }

    /// Calculate bit index using double hashing formula: (h1 + i * h2) % size
    fn get_bit_index(&self, h1: u64, h2: u64, i: usize) -> usize {
        ((h1.wrapping_add((i as u64).wrapping_mul(h2))) % self.size_bits as u64) as usize
    }

    /// Set a bit in the filter
    fn set_bit(&mut self, index: usize) {
        let byte_index = index / 8;
        let bit_offset = index % 8;
        self.bits[byte_index] |= 1 << bit_offset;
    }

    /// Get a bit from the filter
    fn get_bit(&self, index: usize) -> bool {
        let byte_index = index / 8;
        let bit_offset = index % 8;
        (self.bits[byte_index] & (1 << bit_offset)) != 0
    }

    /// Get the false positive rate of the filter
    pub fn false_positive_rate(&self, num_elements: usize) -> f64 {
        // p = (1 - e^(-kn/m))^k
        let k = self.num_hashes as f64;
        let m = self.size_bits as f64;
        let n = num_elements as f64;

        (1.0 - std::f64::consts::E.powf(-k * n / m)).powf(k)
    }

    /// Get the number of bits in the filter
    pub fn size_bits(&self) -> usize {
        self.size_bits
    }

    /// Get the number of hash functions used
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Merge another Bloom filter into this one
    pub fn merge(&mut self, other: &Self) -> Result<(), &'static str> {
        if self.size_bits != other.size_bits || self.num_hashes != other.num_hashes {
            return Err("Cannot merge Bloom filters of different sizes or hash counts");
        }

        for (i, byte) in other.bits.iter().enumerate() {
            self.bits[i] |= *byte;
        }

        Ok(())
    }

    /// Clear the Bloom filter
    pub fn clear(&mut self) {
        for byte in &mut self.bits {
            *byte = 0;
        }
    }

    /// Get a reference to the internal bit array for serialization
    pub fn get_bits(&self) -> &[u8] {
        &self.bits
    }

    /// Set the internal bit array for deserialization
    pub fn set_bits(&mut self, bits: Vec<u8>) {
        self.bits = bits;
    }

    /// Set parameters for a deserialized Bloom filter
    pub fn set_parameters(&mut self, size_bits: usize, num_hashes: usize) {
        self.size_bits = size_bits;
        self.num_hashes = num_hashes;
    }

    /// Create a Bloom filter from existing parts
    pub fn from_parts(bits: Vec<u8>, size_bits: usize, num_hashes: usize) -> Self {
        // Safety checks
        let size_bits = if size_bits == 0 {
            bits.len() * 8 // Use actual bit array size if size_bits is invalid
        } else {
            std::cmp::min(size_bits, 100_000_000) // Cap at 100 million bits
        };

        let num_hashes = num_hashes.clamp(1, 20); // 1-20 hash functions

        BloomFilter {
            bits,
            num_hashes,
            size_bits,
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_empty() {
        let filter = BloomFilter::<String>::new(100, 0.01);
        assert!(!filter.may_contain(&"test".to_string()));
    }

    #[test]
    fn test_bloom_filter_insert_and_check() {
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
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
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
    }

    #[test]
    fn test_bloom_filter_merge() {
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
    }

    #[test]
    fn test_bloom_filter_clear() {
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
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut filter = BloomFilter::<String>::new(100, 0.01);

        // Insert elements
        filter.insert(&"apple".to_string());
        filter.insert(&"banana".to_string());

        // Serialize
        let bits = filter.get_bits().to_vec();
        let size_bits = filter.size_bits();
        let num_hashes = filter.num_hashes();

        // Deserialize
        let deserialized = BloomFilter::from_parts(bits, size_bits, num_hashes);

        // Check serialized filter behaves the same
        assert!(deserialized.may_contain(&"apple".to_string()));
        assert!(deserialized.may_contain(&"banana".to_string()));
        assert!(!deserialized.may_contain(&"grape".to_string()));
    }
}
