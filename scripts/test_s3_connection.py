#!/usr/bin/env python3
"""Test script to verify S3 connection and file discovery for NPPES backfill."""

import boto3
import pandas as pd
from pathlib import Path
import logging
from tqdm import tqdm

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_s3_connection(bucket_name="commercial-rates", prefix="tic-mrf/test"):
    """Test S3 connection and list files."""
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
        
        # Test reading a provider file
        if provider_files:
            test_file = provider_files[0]
            logger.info(f"Testing read of provider file: {test_file}")
            
            # Create temp directory
            temp_dir = Path("nppes_data")
            temp_dir.mkdir(exist_ok=True)
            temp_file = temp_dir / "test_provider.parquet"
            
            try:
                s3_client.download_file(bucket_name, test_file, str(temp_file))
                df = pd.read_parquet(temp_file)
                
                logger.info(f"✅ Successfully read provider file")
                logger.info(f"   Shape: {df.shape}")
                logger.info(f"   Columns: {list(df.columns)}")
                
                # Look for NPI column
                npi_columns = [col for col in df.columns if 'npi' in col.lower()]
                if npi_columns:
                    npi_col = npi_columns[0]
                    npis = df[npi_col].dropna().astype(str).tolist()
                    logger.info(f"   Found {len(npis)} NPIs in test file")
                    logger.info(f"   Sample NPIs: {npis[:5]}")
                else:
                    logger.warning(f"   No NPI column found. Available columns: {list(df.columns)}")
                
            finally:
                if temp_file.exists():
                    temp_file.unlink()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ S3 test failed: {str(e)}")
        return False

def main():
    """Main test function."""
    logger.info("🧪 Testing S3 connection for NPPES backfill script")
    
    success = test_s3_connection()
    
    if success:
        logger.info("✅ All tests passed! S3 connection is working correctly.")
        logger.info("You can now run the backfill script:")
        logger.info("  python scripts/backfill_provider_info.py --limit 100")
    else:
        logger.error("❌ Tests failed. Please check your S3 configuration and permissions.")

if __name__ == "__main__":
    main() 