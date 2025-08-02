#!/bin/bash

# Local test script for small amount of data on a specific payer endpoint

echo "=== TiC ETL Local Test Script ==="
echo ""

# Check if payer name is provided
if [ $# -eq 0 ]; then
    echo "Usage: ./test_local.sh <payer_name> [max_files] [max_records]"
    echo ""
    echo "Examples:"
    echo "  ./test_local.sh bcbs_fl"
    echo "  ./test_local.sh bcbs_fl 1 500"
    echo ""
    echo "Available payers:"
    python test_local_small.py
    exit 1
fi

PAYER_NAME=$1
MAX_FILES=${2:-2}
MAX_RECORDS=${3:-1000}

echo "Testing payer: $PAYER_NAME"
echo "Max files: $MAX_FILES"
echo "Max records per file: $MAX_RECORDS"
echo ""

# Run the test
python test_local_small.py "$PAYER_NAME" --max-files "$MAX_FILES" --max-records "$MAX_RECORDS"

echo ""
echo "Test completed!" 