#!/bin/bash

# S3 Batch Consolidation Runner Script
# This script runs the S3 batch consolidation with configurable options

set -e

# Default configuration
S3_BUCKET=${S3_BUCKET:-"commercial-rates"}
S3_PREFIX=${S3_PREFIX:-"tic-mrf"}
CONSOLIDATED_PREFIX=${CONSOLIDATED_PREFIX:-"tic-mrf/consolidated"}
MAX_WORKERS=${MAX_WORKERS:-4}
CHUNK_SIZE=${CHUNK_SIZE:-1000}
CLEANUP_ORIGINAL_FILES=${CLEANUP_ORIGINAL_FILES:-false}
DRY_RUN=${DRY_RUN:-false}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -b, --bucket BUCKET        S3 bucket name (default: commercial-rates)"
    echo "  -p, --prefix PREFIX        S3 source prefix (default: commercial-rates/tic-mrf)"
    echo "  -c, --consolidated PREFIX  Consolidated output prefix (default: commercial-rates/tic-mrf/consolidated)"
    echo "  -w, --workers NUM          Max workers (default: 4)"
    echo "  -s, --chunk-size NUM       Chunk size for processing (default: 1000)"
    echo "  --cleanup                  Clean up original batch files after consolidation"
    echo "  --dry-run                  Only list files without processing"
    echo "  -h, --help                 Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  S3_BUCKET                  S3 bucket name"
    echo "  S3_PREFIX                  S3 source prefix"
    echo "  CONSOLIDATED_PREFIX        Consolidated output prefix"
    echo "  MAX_WORKERS                Max workers"
    echo "  CHUNK_SIZE                 Chunk size for processing"
    echo "  CLEANUP_ORIGINAL_FILES     Clean up original files (true/false)"
    echo "  DRY_RUN                    Dry run mode (true/false)"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Run with defaults"
    echo "  $0 --dry-run                         # List files without processing"
    echo "  $0 --cleanup                         # Consolidate and cleanup original files"
    echo "  $0 -b my-bucket -p my/prefix        # Custom bucket and prefix"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -b|--bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        -p|--prefix)
            S3_PREFIX="$2"
            shift 2
            ;;
        -c|--consolidated)
            CONSOLIDATED_PREFIX="$2"
            shift 2
            ;;
        -w|--workers)
            MAX_WORKERS="$2"
            shift 2
            ;;
        -s|--chunk-size)
            CHUNK_SIZE="$2"
            shift 2
            ;;
        --cleanup)
            CLEANUP_ORIGINAL_FILES=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Export environment variables
export S3_BUCKET
export S3_PREFIX
export CONSOLIDATED_PREFIX
export MAX_WORKERS
export CHUNK_SIZE
export CLEANUP_ORIGINAL_FILES
export DRY_RUN

# Print configuration
echo "S3 Batch Consolidation Configuration:"
echo "  S3 Bucket: $S3_BUCKET"
echo "  S3 Prefix: $S3_PREFIX"
echo "  Consolidated Prefix: $CONSOLIDATED_PREFIX"
echo "  Max Workers: $MAX_WORKERS"
echo "  Chunk Size: $CHUNK_SIZE"
echo "  Cleanup Original Files: $CLEANUP_ORIGINAL_FILES"
echo "  Dry Run: $DRY_RUN"
echo ""

# Check if we're in dry run mode
if [[ "$DRY_RUN" == "true" ]]; then
    echo "DRY RUN MODE - Files will be listed but not processed"
    echo ""
fi

# Run the consolidation script
echo "Starting S3 batch consolidation..."
python scripts/consolidate_s3_batches.py

echo "Consolidation completed!" 