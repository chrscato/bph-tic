#!/bin/bash

# NPPES Provider Information Backfill Script
# This script runs the NPPES backfill with different configurations

set -e  # Exit on any error

echo "🏥 NPPES Provider Information Backfill"
echo "======================================"

# Function to run the backfill
run_backfill() {
    local limit=$1
    local description=$2
    
    echo ""
    echo "🔄 Running: $description"
    echo "----------------------------------------"
    
    python scripts/backfill_provider_info.py \
        --limit "$limit" \
        --s3-bucket "commercial-rates" \
        --s3-prefix "tic-mrf/test"
    
    echo "✅ Completed: $description"
}

# Function to test S3 connection first
test_connection() {
    echo "🧪 Testing S3 connection..."
    python scripts/test_s3_connection.py
    
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
        echo "Running small test (50 NPIs)..."
        if test_connection; then
            run_backfill 50 "Small test with 50 NPIs"
        fi
        ;;
    
    "medium")
        echo "Running medium test (500 NPIs)..."
        if test_connection; then
            run_backfill 500 "Medium test with 500 NPIs"
        fi
        ;;
    
    "large")
        echo "Running large test (1000 NPIs)..."
        if test_connection; then
            run_backfill 1000 "Large test with 1000 NPIs"
        fi
        ;;
    
    "full")
        echo "Running full backfill (all NPIs)..."
        if test_connection; then
            echo "🔄 Running full NPPES backfill..."
            python scripts/backfill_provider_info.py \
                --s3-bucket "commercial-rates" \
                --s3-prefix "tic-mrf/test"
            echo "✅ Completed full backfill"
        fi
        ;;
    
    "custom")
        if [ -z "$2" ]; then
            echo "❌ Please provide a limit for custom run"
            echo "Usage: $0 custom <limit>"
            exit 1
        fi
        echo "Running custom test with $2 NPIs..."
        if test_connection; then
            run_backfill "$2" "Custom test with $2 NPIs"
        fi
        ;;
    
    "help"|*)
        echo "Usage: $0 {test|small|medium|large|full|custom <limit>}"
        echo ""
        echo "Options:"
        echo "  test     - Test S3 connection only"
        echo "  small    - Run with 50 NPIs (quick test)"
        echo "  medium   - Run with 500 NPIs (medium test)"
        echo "  large    - Run with 1000 NPIs (large test)"
        echo "  full     - Run with all NPIs (full backfill)"
        echo "  custom N - Run with N NPIs (custom limit)"
        echo ""
        echo "Examples:"
        echo "  $0 test          # Test S3 connection"
        echo "  $0 small         # Quick test with 50 NPIs"
        echo "  $0 custom 250    # Custom test with 250 NPIs"
        echo "  $0 full          # Full backfill"
        ;;
esac 