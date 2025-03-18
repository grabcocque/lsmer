# Parallel Testing Guide

This project is configured for both parallel test execution and randomized test order by default to speed up the test suite and catch ordering dependencies between tests.

## Running Tests

The project is configured to automatically run tests in parallel using all available CPU cores with randomized test ordering. Simply run:

```bash
cargo test
```

Or use our included script for better feedback and timing information:

```bash
./run_tests.sh
```

## Advanced Options

The `run_tests.sh` script supports several options:

```bash
# Run tests with default settings (parallel execution and randomized order)
./run_tests.sh

# Run tests sequentially (single thread)
./run_tests.sh --sequential

# Explicitly randomize tests (useful when specifying a seed)
./run_tests.sh --random

# Disable randomization (override the default)
./run_tests.sh --no-random

# Run tests with a specific random seed (for reproducibility)
./run_tests.sh --random --seed 12345

# Combine with specific test filtering
./run_tests.sh --test mytest
```

## Randomized Testing

Running tests in random order helps identify tests that have unintentional dependencies on each other.
If a test passes only when run after another specific test, it likely has hidden dependencies.

When using our scripts, a seed is generated and displayed:

```
Using default randomization from .cargo/config.toml
To reproduce this test order, use: --random --seed 12345
```

If you encounter a failure with randomized tests, you can reproduce the exact same test order by
running with the same seed:

```bash
./run_tests.sh --random --seed 12345
```

## How It Works

1. The `.cargo/config.toml` file contains settings that enable both parallel test execution and randomized ordering:

   ```toml
   [test]
   threads = "num-cpus"

   [env]
   RUST_TEST_SHUFFLE = "1"
   ```

2. The `Cargo.toml` file includes optimized test profiles to make tests run faster:

   ```toml
   [profile.test]
   opt-level = 2
   debug = true
   codegen-units = 16
   debug-assertions = true
   ```

## Writing Parallel-Safe Tests

To make your tests parallel-safe, they must not interfere with each other. Here are some guidelines:

1. **Use Unique Paths**: Each test should use unique file paths. Use the `TestDir` helper from `tests/helpers/mod.rs`:

   ```rust
   use helpers::TestDir;
   
   #[tokio::test]
   async fn my_test() {
       // Create a unique test directory
       let test_dir = TestDir::new("my_test_name");
       
       // Use the paths in your test
       let base_dir = test_dir.as_str();
       let data_path = format!("{}/data.db", base_dir);
       
       // The directory will be automatically cleaned up when test_dir goes out of scope
   }
   ```

2. **Avoid Global State**: Don't rely on global variables or state that could be modified by other tests.

3. **Use Timeouts**: All tests should have timeouts to prevent hanging.

4. **Don't Depend on Test Order**: Tests should pass regardless of the order they run in.

## Example

See `tests/parallel_test_example.rs` for a complete example of parallel-safe tests.

## Debugging

If you encounter test failures when running in parallel or with randomization, you can isolate the issue:

```bash
# Run sequentially
./run_tests.sh --sequential

# Run without randomization
./run_tests.sh --no-random

# Run both sequentially and without randomization
./run_tests.sh --sequential --no-random
```

If a test passes when run alone but fails in parallel, it likely has a dependency on global state or non-unique resources.

If a test passes in a fixed order but fails with randomization, try to find which test it depends on:

```bash
./run_tests.sh --random --seed <seed_from_failure>
```
