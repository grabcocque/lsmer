#!/bin/bash

# Script to update all test files to use tokio::test with a 10-second timeout

# Check if backup directory exists, if not create it
if [ ! -d "backup" ]; then
  mkdir backup
fi

# Get date for backup directory naming
DATE=$(date +"%Y%m%d_%H%M%S")
BACKUP_DIR="backup/tests_${DATE}"
mkdir -p "$BACKUP_DIR"

# Create a function to update a single file
update_file() {
  local file=$1
  local filename=$(basename "$file")
  
  echo "Processing $filename..."

  # Create a backup of the original file
  cp "$file" "$BACKUP_DIR/$filename"
  
  # First, check if we need to fix any imports
  if grep -q "use std::time::Duration" "$file" && grep -q "use tokio::time::Duration" "$file"; then
    echo "  Fixing duplicate Duration imports..."
    sed -i '/use tokio::time::Duration/d' "$file"
  fi
  
  if grep -q "use tokio::time::timeout" "$file" && grep -q "use tokio::time::{.*timeout" "$file"; then
    echo "  Fixing duplicate timeout imports..."
    sed -i 's/use tokio::time::{.*timeout.*}/use tokio::time::{}/g' "$file"
    sed -i '/use tokio::time:{}/d' "$file"
  fi
  
  # Check if file already uses tokio::test
  if grep -q "#\[tokio::test\]" "$file"; then
    echo "  File already uses tokio::test, checking if it has timeouts..."
    
    # Check if it has timeouts
    if grep -q "timeout(" "$file" && grep -q "Duration::from_secs(10)" "$file"; then
      echo "  File already has timeouts, skipping."
      return
    fi
    
    # If it has tokio::test but no timeouts, add timeouts
    echo "  File has tokio::test but no timeouts, adding timeouts."
    
    # First, ensure imports exist
    if ! grep -q "use std::time::Duration" "$file"; then
      sed -i '1s/^/use std::time::Duration;\n/' "$file"
    fi
    
    if ! grep -q "use tokio::time::timeout" "$file"; then
      sed -i '1s/^/use tokio::time::timeout;\n/' "$file"
    fi
    
    # Fix any function calls that need awaiting
    sed -i 's/\([a-zA-Z0-9_]*\)(\([^)]*\))?/\1(\2).await?/g' "$file"
    
    # Add timeout blocks around each test function
    awk '
      # Add imports if not already present
      BEGIN { in_test = 0; indent = 0; }
      
      # Process each async function
      /^#\[tokio::test\]/ { in_test = 1; print; next; }
      /^async fn/ && in_test { 
        print $0; 
        print "    let test_future = async {"; 
        indent = 1; 
        next; 
      }
      
      # Add closing timeout block at end of function
      /^}/ && in_test && indent { 
        print "    };"; 
        print ""; 
        print "    match timeout(Duration::from_secs(10), test_future).await {"; 
        print "        Ok(_) => (),"; 
        print "        Err(_) => panic!(\"Test timed out after 10 seconds\"),"; 
        print "    }"; 
        print $0; 
        in_test = 0; 
        indent = 0; 
        next; 
      }
      
      # Indent existing code inside the function
      in_test && indent { print "    " $0; next; }
      
      # Print everything else as is
      { print; }
    ' "$file" > "$file.tmp"
    mv "$file.tmp" "$file"
    
  else
    # Full conversion from #[test] to #[tokio::test] with timeouts
    echo "  Converting to tokio::test with timeouts..."
    
    # Check if imports already exist before adding
    if ! grep -q "use std::time::Duration" "$file"; then
      sed -i '1s/^/use std::time::Duration;\n/' "$file"
    fi
    
    if ! grep -q "use tokio::time::timeout" "$file"; then
      sed -i '1s/^/use tokio::time::timeout;\n/' "$file"
    fi
    
    # Replace #[test] with #[tokio::test]
    sed -i 's/#\[test\]/#\[tokio::test\]/g' "$file"
    
    # Add async to function signature and wrap body in timeout
    sed -i 's/\(fn [a-zA-Z0-9_]*\)(/async \1(/g' "$file"
    
    # Fix any function calls that need awaiting
    sed -i 's/\([a-zA-Z0-9_]*\)(\([^)]*\))?/\1(\2).await?/g' "$file"
    
    # Use awk to wrap the function body
    awk '
      # Find each test function
      BEGIN { in_test = 0; indent = 0; }
      /^#\[tokio::test\]/ { in_test = 1; print; next; }
      /^async fn/ && in_test { 
        print $0; 
        print "    let test_future = async {"; 
        indent = 1; 
        next; 
      }
      
      # Add closing timeout block at end of function
      /^}/ && in_test && indent { 
        print "    };"; 
        print ""; 
        print "    match timeout(Duration::from_secs(10), test_future).await {"; 
        print "        Ok(_) => (),"; 
        print "        Err(_) => panic!(\"Test timed out after 10 seconds\"),"; 
        print "    }"; 
        print $0; 
        in_test = 0; 
        indent = 0; 
        next; 
      }
      
      # Indent existing code inside the function
      in_test && indent { print "    " $0; next; }
      
      # Print everything else as is
      { print; }
    ' "$file" > "$file.tmp"
    mv "$file.tmp" "$file"
  fi
  
  # Final cleanup for any remaining issues
  # Fix duplicate imports and remove unused ones
  if grep -q -c "use std::time::Duration" "$file" | grep -q "2"; then
    sed -i '0,/use std::time::Duration;/{//d;}' "$file"
  fi
  
  if grep -q -c "use tokio::time::timeout" "$file" | grep -q "2"; then
    sed -i '0,/use tokio::time::timeout;/{//d;}' "$file"
  fi
}

# Restore from backup directory before running again
if [ -d "$BACKUP_DIR" ]; then
  echo "Restoring from backup before applying new changes..."
  for file in "$BACKUP_DIR"/*.rs; do
    if [ -f "$file" ]; then
      cp "$file" "tests/$(basename "$file")"
    fi
  done
fi

# Update all test files
echo "Updating all test files to use tokio::test with a 10-second timeout..."

for file in tests/*.rs; do
  # Skip the already updated bptree_basic_unit_test.rs
  if [ "$(basename "$file")" != "bptree_basic_unit_test.rs" ]; then
    update_file "$file"
  fi
done

echo "All files processed. Backups are in $BACKUP_DIR"
echo "Please run 'cargo test' to verify that all tests still pass." 