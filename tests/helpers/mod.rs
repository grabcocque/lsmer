use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Creates a unique directory path for each test to ensure test isolation when running in parallel
pub fn unique_test_dir(test_name: &str) -> PathBuf {
    let test_id = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
    let pid = std::process::id();
    let dir = format!("target/test_tmp/{}_{}_pid{}", test_name, test_id, pid);

    // Create the directory
    let path = PathBuf::from(&dir);
    let _ = fs::remove_dir_all(&path); // Clean up from previous runs
    fs::create_dir_all(&path).unwrap();

    path
}

/// Helper for setting up temporary test files with automatic cleanup
pub struct TestDir {
    pub path: PathBuf,
}

impl TestDir {
    /// Creates a new temporary test directory
    pub fn new(test_name: &str) -> Self {
        let path = unique_test_dir(test_name);
        TestDir { path }
    }

    /// Get the path as a string
    #[allow(dead_code)]
    pub fn as_str(&self) -> String {
        self.path.to_string_lossy().to_string()
    }

    /// Creates subdirectories relative to the test directory
    pub fn create_subdir(&self, subdir: &str) -> PathBuf {
        let subdir_path = self.path.join(subdir);
        fs::create_dir_all(&subdir_path).unwrap();
        subdir_path
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        // Clean up the directory when the test is done
        let _ = fs::remove_dir_all(&self.path);
    }
}
