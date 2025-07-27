#!/bin/bash

# Memory-Efficient Fact Table Creation Script
# This script runs the fact table creation with different configurations

set -e  # Exit on any error

echo "📊 Memory-Efficient Fact Table Creation"
echo "======================================"

# Function to run the fact table creation
run_fact_table() {
    local test_mode=$1
    local sample_size=$2
    local description=$3
    local no_upload=${4:-false}
    
    echo ""
    echo "🔄 Running: $description"
    echo "----------------------------------------"
    
    local upload_flag=""
    if [ "$no_upload" = "true" ]; then
        upload_flag="--no-upload"
    fi
    
    if [ "$test_mode" = "true" ]; then
        python scripts/create_memory_efficient_fact_table.py \
            --test \
            --sample-size "$sample_size" \
            --s3-bucket "commercial-rates" \
            --s3-prefix "tic-mrf/test" \
            $upload_flag
    else
        python scripts/create_memory_efficient_fact_table.py \
            --s3-bucket "commercial-rates" \
            --s3-prefix "tic-mrf/test" \
            $upload_flag
    fi
    
    echo "✅ Completed: $description"
}

# Function to test S3 connection first
test_connection() {
    echo "🧪 Testing S3 connection and file structure..."
    python scripts/test_fact_table_s3.py
    
    if [ $? -eq 0 ]; then
        echo "✅ S3 connection test passed"
        return 0
    else
        echo "❌ S3 connection test failed"
        return 1
    fi
}

# Main script logic
case "${1:-help}" in
    "test")
        echo "Running S3 connection test only..."
        test_connection
        ;;
    
    "small")
        echo "Running small test (100 records)..."
        if test_connection; then
            run_fact_table "true" 100 "Small test with 100 records"
        fi
        ;;
    
    "medium")
        echo "Running medium test (1000 records)..."
        if test_connection; then
            run_fact_table "true" 1000 "Medium test with 1000 records"
        fi
        ;;
    
    "large")
        echo "Running large test (10000 records)..."
        if test_connection; then
            run_fact_table "true" 10000 "Large test with 10000 records"
        fi
        ;;
    
    "full")
        echo "Running full fact table creation (all records)..."
        if test_connection; then
            echo "🔄 Running full fact table creation..."
            python scripts/create_memory_efficient_fact_table.py \
                --s3-bucket "commercial-rates" \
                --s3-prefix "tic-mrf/test"
            echo "✅ Completed full fact table creation"
        fi
        ;;
    
    "custom")
        if [ -z "$2" ]; then
            echo "❌ Please provide a sample size for custom run"
            echo "Usage: $0 custom <sample_size>"
            exit 1
        fi
        echo "Running custom test with $2 records..."
        if test_connection; then
            run_fact_table "true" "$2" "Custom test with $2 records"
        fi
        ;;
    
    "local")
        echo "Running with local files (not S3)..."
        echo "🔄 Running fact table creation with local files..."
        python scripts/create_memory_efficient_fact_table.py \
            --test \
            --sample-size 1000 \
            --local
        echo "✅ Completed local fact table creation"
        ;;
    
    "local-only")
        echo "Running with local files and no S3 upload..."
        if test_connection; then
            run_fact_table "true" 1000 "Local processing with no S3 upload" "true"
        fi
        ;;
    
    "help"|*)
        echo "Usage: $0 {test|small|medium|large|full|custom <size>|local|local-only}"
        echo ""
        echo "Options:"
        echo "  test     - Test S3 connection and file structure"
        echo "  small    - Run with 100 records (quick test)"
        echo "  medium   - Run with 1000 records (medium test)"
        echo "  large    - Run with 10000 records (large test)"
        echo "  full     - Run with all records (full creation)"
        echo "  custom N - Run with N records (custom sample size)"
        echo "  local    - Run with local files instead of S3"
        echo "  local-only - Use S3 for input but save locally only (no upload)"
        echo ""
        echo "Examples:"
        echo "  $0 test          # Test S3 connection"
        echo "  $0 small         # Quick test with 100 records"
        echo "  $0 custom 5000   # Custom test with 5000 records"
        echo "  $0 full          # Full fact table creation"
        echo "  $0 local         # Use local files"
        ;;
esac 