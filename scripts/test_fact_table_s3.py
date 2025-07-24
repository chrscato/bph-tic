#!/usr/bin/env python3
"""Test script to verify S3 fact table creation with chunked parquet files."""

import boto3
import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_s3_fact_table_files(bucket_name="commercial-rates", prefix="tic-mrf/test"):
    """Test S3 connection and analyze files needed for fact table creation."""
    try:
        s3_client = boto3.client('s3')
        
        # Test bucket access
        logger.info(f"Testing connection to S3 bucket: {bucket_name}")
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info("✅ S3 bucket access successful")
        
        # List files
        logger.info(f"Listing files in {bucket_name}/{prefix}")
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        all_files = []
        for page in pages:
            if 'Contents' in page:
                all_files.extend([obj['Key'] for obj in page['Contents']])
        
        if not all_files:
            logger.error(f"No files found in {bucket_name}/{prefix}")
            return False
        
        logger.info(f"Found {len(all_files)} total files")
        
        # Categorize files
        rates_files = [f for f in all_files if 'rates' in f and f.endswith('.parquet')]
        org_files = [f for f in all_files if 'orgs' in f and f.endswith('.parquet')]
        provider_files = [f for f in all_files if 'providers' in f and f.endswith('.parquet')]
        
        logger.info(f"📊 File Summary:")
        logger.info(f"   Rates: {len(rates_files)} files")
        logger.info(f"   Organizations: {len(org_files)} files")
        logger.info(f"   Providers: {len(provider_files)} files")
        
        # Test reading a rates file
        if rates_files:
            test_file = rates_files[0]
            logger.info(f"Testing read of rates file: {test_file}")
            
            # Create temp directory
            temp_dir = Path("temp_s3_test")
            temp_dir.mkdir(exist_ok=True)
            temp_file = temp_dir / "test_rates.parquet"
            
            try:
                s3_client.download_file(bucket_name, test_file, str(temp_file))
                df = pd.read_parquet(temp_file)
                
                logger.info(f"✅ Successfully read rates file")
                logger.info(f"   Shape: {df.shape}")
                logger.info(f"   Columns: {list(df.columns)}")
                
                # Check for required columns for fact table
                required_columns = ['rate_uuid', 'organization_uuid', 'provider_network', 'negotiated_rate', 'service_code']
                missing_columns = [col for col in required_columns if col not in df.columns]
                
                if missing_columns:
                    logger.warning(f"   Missing required columns: {missing_columns}")
                else:
                    logger.info(f"   ✅ All required columns present")
                
                # Check provider_network structure
                if 'provider_network' in df.columns:
                    sample_network = df['provider_network'].dropna().iloc[0] if len(df['provider_network'].dropna()) > 0 else None
                    if sample_network:
                        logger.info(f"   Sample provider_network: {type(sample_network)}")
                        if isinstance(sample_network, dict):
                            logger.info(f"   Provider network keys: {list(sample_network.keys())}")
                            if 'npi_list' in sample_network:
                                npi_list = sample_network['npi_list']
                                logger.info(f"   NPI list type: {type(npi_list)}, length: {len(npi_list) if hasattr(npi_list, '__len__') else 'N/A'}")
                
            finally:
                if temp_file.exists():
                    temp_file.unlink()
                if temp_dir.exists():
                    temp_dir.rmdir()
        
        # Test reading an organizations file
        if org_files:
            test_file = org_files[0]
            logger.info(f"Testing read of organizations file: {test_file}")
            
            temp_dir = Path("temp_s3_test")
            temp_dir.mkdir(exist_ok=True)
            temp_file = temp_dir / "test_orgs.parquet"
            
            try:
                s3_client.download_file(bucket_name, test_file, str(temp_file))
                df = pd.read_parquet(temp_file)
                
                logger.info(f"✅ Successfully read organizations file")
                logger.info(f"   Shape: {df.shape}")
                logger.info(f"   Columns: {list(df.columns)}")
                
                # Check for organization_uuid column
                if 'organization_uuid' in df.columns:
                    logger.info(f"   ✅ organization_uuid column present")
                else:
                    logger.warning(f"   ❌ organization_uuid column missing")
                
            finally:
                if temp_file.exists():
                    temp_file.unlink()
                if temp_dir.exists():
                    temp_dir.rmdir()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ S3 fact table test failed: {str(e)}")
        return False

def test_fact_table_creation():
    """Test the fact table creation process with a small sample."""
    logger.info("🧪 Testing fact table creation process...")
    
    try:
        # Import the fact table builder
        from create_memory_efficient_fact_table import MemoryEfficientFactTableBuilder
        
        # Create a test builder with small sample
        builder = MemoryEfficientFactTableBuilder(
            s3_bucket="commercial-rates",
            s3_prefix="tic-mrf/test",
            test_mode=True,
            sample_size=100,  # Very small test
            chunk_size=50,
            use_s3=True
        )
        
        # Test file discovery
        rates_files = builder.get_rates_files()
        logger.info(f"Found {len(rates_files)} rates files for processing")
        
        if not rates_files:
            logger.error("❌ No rates files found")
            return False
        
        # Test loading reference data
        builder.load_reference_data()
        
        logger.info("✅ Fact table creation test setup successful")
        return True
        
    except Exception as e:
        logger.error(f"❌ Fact table creation test failed: {str(e)}")
        return False

def main():
    """Main test function."""
    logger.info("🧪 Testing S3 fact table creation with chunked parquet files")
    
    # Test 1: S3 connection and file discovery
    success1 = test_s3_fact_table_files()
    
    if success1:
        logger.info("✅ S3 connection and file discovery test passed")
        
        # Test 2: Fact table creation setup
        success2 = test_fact_table_creation()
        
        if success2:
            logger.info("✅ Fact table creation test passed")
            logger.info("🎉 All tests passed! You can now run the fact table creation:")
            logger.info("  python scripts/create_memory_efficient_fact_table.py --test --sample-size 1000")
        else:
            logger.error("❌ Fact table creation test failed")
    else:
        logger.error("❌ S3 connection test failed")

if __name__ == "__main__":
    main() 