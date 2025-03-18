#!/bin/bash

# Script to run tests with timing, parallelism, and randomized order
# Usage: ./run_tests.sh [options]
#   --random           Explicitly enable randomized test execution order
#   --seed <number>    Use specific seed for randomization
#   --sequential       Run tests sequentially (1 thread)
#   --no-random        Disable randomization (overrides the default)
#   Other arguments are passed directly to cargo test

# Parse arguments
RANDOM_ORDER=true # Default to true since we've enabled it in .cargo/config.toml
EXPLICIT_RANDOM=false
NO_RANDOM=false
SEED=""
THREADS=$(nproc) # Default to using all cores
EXTRA_ARGS=""

while [[ $# -gt 0 ]]; do
  case $1 in
  --random)
    EXPLICIT_RANDOM=true
    RANDOM_ORDER=true
    shift
    ;;
  --no-random)
    NO_RANDOM=true
    RANDOM_ORDER=false
    shift
    ;;
  --seed)
    SEED="$2"
    shift 2
    ;;
  --sequential)
    THREADS=1
    shift
    ;;
  *)
    EXTRA_ARGS="$EXTRA_ARGS $1"
    shift
    ;;
  esac
done

# Set up test randomization if requested
if [ "$RANDOM_ORDER" = true ] && [ "$NO_RANDOM" = false ]; then
  if [ -z "$SEED" ]; then
    # Generate a random seed between 1 and 999999
    SEED=$((RANDOM * 1000 + RANDOM))
  fi

  if [ "$EXPLICIT_RANDOM" = true ]; then
    # Only set these if explicit randomization was requested
    # (otherwise .cargo/config.toml settings take effect)
    export RUST_TEST_SHUFFLE=1
    export RUST_TEST_SHUFFLE_SEED=$SEED
    echo "Explicitly randomizing test order with seed: $SEED"
  else
    echo "Using default randomization from .cargo/config.toml"
  fi

  echo "To reproduce this test order, use: --random --seed $SEED"
else
  # Disable randomization if requested
  export RUST_TEST_SHUFFLE=0
  echo "Test randomization is disabled"
fi

# Set thread count for parallelism
export RUST_TEST_THREADS=$THREADS

echo "Running tests with $THREADS thread(s)..."
echo "==============================================="

# Start timer
start_time=$(date +%s)

# Run tests with color output and backtrace
RUST_BACKTRACE=1 cargo test "$EXTRA_ARGS"

# End timer and calculate duration
end_time=$(date +%s)
duration=$((end_time - start_time))

# Print results
echo "==============================================="
echo "All tests completed in $duration seconds"
echo "Tests ran using $THREADS thread(s)"
if [ "$RANDOM_ORDER" = true ] && [ "$NO_RANDOM" = false ]; then
  echo "Tests were randomized with seed: $SEED"
fi
