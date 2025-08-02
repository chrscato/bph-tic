#!/usr/bin/env python3
"""Test script for backfill_provider_info.py configuration."""

import os
import sys
from pathlib import Path

# Add the scripts directory to the path
sys.path.insert(0, str(Path(__file__).parent / "scripts"))

from backfill_provider_info import create_nppes_config, NPPESDataManager

def test_config():
    """Test the configuration setup."""
    print("Testing NPPES Configuration...")
    
    # Test environment variables
    print(f"S3_BUCKET: {os.getenv('S3_BUCKET', 'Not set')}")
    print(f"S3_PREFIX: {os.getenv('S3_PREFIX', 'Not set')}")
    print(f"LOCAL_DATA_DIR: {os.getenv('LOCAL_DATA_DIR', 'Not set')}")
    
    # Test configuration creation
    try:
        config = create_nppes_config(limit=10)
        print(f"\nConfiguration created successfully:")
        print(f"  S3 Bucket: {config.s3_bucket}")
        print(f"  S3 Prefix: {config.s3_prefix}")
        print(f"  Local Data Dir: {config.local_data_dir}")
        print(f"  Local Base Pattern: {config.local_base_pattern}")
        print(f"  Limit: {config.limit}")
        
        # Test NPPES manager initialization
        nppes_manager = NPPESDataManager(config)
        print(f"\nNPPES Manager initialized successfully")
        print(f"  NPPES file: {nppes_manager.nppes_file}")
        
        return True
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_config()
    sys.exit(0 if success else 1) 