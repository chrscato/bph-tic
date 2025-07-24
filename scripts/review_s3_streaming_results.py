#!/usr/bin/env python3
"""
Review S3 streaming fact table results.
Analyzes the streaming chunks and provides insights on the data.
"""

import pandas as pd
import boto3
import logging
from pathlib import Path
import json
from datetime import datetime
import io

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def review_s3_streaming_results():
    """Review the S3 streaming fact table results."""
    
    # S3 configuration
    s3_bucket = "commercial-rates"
    s3_prefix = "tic-mrf/test"
    chunk_key = "tic-mrf/test/fact_tables/streaming_chunks/chunk_0000_20250724_091055.parquet"
    summary_key = "tic-mrf/test/s3_streaming_fact_table_summary.json"
    
    # Initialize S3 client
    try:
        s3_client = boto3.client('s3')
        logger.info("✅ S3 client initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize S3 client: {e}")
        return
    
    # Load the streaming chunk
    logger.info(f"Loading streaming chunk from s3://{s3_bucket}/{chunk_key}")
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=chunk_key)
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))
        logger.info(f"✅ Loaded {len(df):,} records from streaming chunk")
    except Exception as e:
        logger.error(f"❌ Failed to load streaming chunk: {e}")
        return
    
    # Basic statistics
    print("\n" + "="*60)
    print("📊 S3 STREAMING FACT TABLE ANALYSIS")
    print("="*60)
    
    print(f"\n📈 RECORD COUNT: {len(df):,} records")
    print(f"📁 FILE SIZE: {response['ContentLength'] / 1024 / 1024:.1f} MB")
    
    # Column analysis
    print(f"\n📋 COLUMNS ({len(df.columns)} total):")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i:2d}. {col}")
    
    # Sample data
    print(f"\n🔍 SAMPLE DATA (first 3 records):")
    print(df.head(3).to_string())
    
    # NPPES enrichment analysis
    if 'npi' in df.columns:
        npi_count = df['npi'].notna().sum()
        print(f"\n🏥 NPPES ENRICHMENT:")
        print(f"  Records with NPI: {npi_count:,} ({npi_count/len(df)*100:.1f}%)")
        print(f"  Records without NPI: {len(df) - npi_count:,} ({(len(df) - npi_count)/len(df)*100:.1f}%)")
    
    # Service code analysis
    if 'service_code' in df.columns:
        service_counts = df['service_code'].value_counts().head(10)
        print(f"\n🏥 TOP 10 SERVICE CODES:")
        for code, count in service_counts.items():
            print(f"  {code}: {count:,} records")
    
    # Service category analysis
    if 'service_category' in df.columns:
        category_counts = df['service_category'].value_counts()
        print(f"\n📊 SERVICE CATEGORIES:")
        for category, count in category_counts.items():
            print(f"  {category}: {count:,} records ({count/len(df)*100:.1f}%)")
    
    # Organization analysis
    if 'organization_name' in df.columns:
        org_counts = df['organization_name'].value_counts().head(5)
        print(f"\n🏢 TOP 5 ORGANIZATIONS:")
        for org, count in org_counts.items():
            print(f"  {org}: {count:,} records")
    
    # Rate analysis
    if 'negotiated_rate' in df.columns:
        rates = df['negotiated_rate'].dropna()
        if len(rates) > 0:
            print(f"\n💰 RATE ANALYSIS:")
            print(f"  Min rate: ${rates.min():,.2f}")
            print(f"  Max rate: ${rates.max():,.2f}")
            print(f"  Mean rate: ${rates.mean():,.2f}")
            print(f"  Median rate: ${rates.median():,.2f}")
    
    # NPI analysis
    if 'npi' in df.columns:
        unique_npis = df['npi'].nunique()
        print(f"\n👨‍⚕️ PROVIDER ANALYSIS:")
        print(f"  Unique NPIs: {unique_npis:,}")
        print(f"  Average records per NPI: {len(df)/unique_npis:.1f}")
    
    # Load and display summary
    try:
        summary_response = s3_client.get_object(Bucket=s3_bucket, Key=summary_key)
        summary_data = json.loads(summary_response['Body'].read().decode('utf-8'))
        print(f"\n📋 S3 SUMMARY METADATA:")
        for key, value in summary_data.items():
            print(f"  {key}: {value}")
    except Exception as e:
        logger.warning(f"Could not load summary: {e}")
    
    # Data quality checks
    print(f"\n🔍 DATA QUALITY CHECKS:")
    
    # Check for missing values
    missing_counts = df.isnull().sum()
    columns_with_missing = missing_counts[missing_counts > 0]
    if len(columns_with_missing) > 0:
        print(f"  Columns with missing values:")
        for col, count in columns_with_missing.items():
            print(f"    {col}: {count:,} missing ({count/len(df)*100:.1f}%)")
    else:
        print("  ✅ No missing values found")
    
    # Check for duplicates (handle unhashable types)
    try:
        duplicates = df.duplicated().sum()
        print(f"  Duplicate records: {duplicates:,} ({duplicates/len(df)*100:.1f}%)")
    except Exception as e:
        print(f"  Duplicate check failed: {e}")
        print("  (Some columns contain unhashable data types)")
    
    print("\n" + "="*60)
    print("✅ S3 STREAMING REVIEW COMPLETE")
    print("="*60)

def compare_with_original():
    """Compare streaming results with original fact table."""
    
    print("\n" + "="*60)
    print("🔄 COMPARISON WITH ORIGINAL FACT TABLE")
    print("="*60)
    
    # Load original fact table
    original_file = Path("dashboard_data/memory_efficient_fact_table.parquet")
    if original_file.exists():
        original_df = pd.read_parquet(original_file)
        print(f"\n📊 ORIGINAL FACT TABLE:")
        print(f"  Records: {len(original_df):,}")
        print(f"  File size: {original_file.stat().st_size / 1024 / 1024:.1f} MB")
        print(f"  Columns: {len(original_df.columns)}")
        
        # Compare record counts
        streaming_count = 17067  # From the logs
        print(f"\n📈 COMPARISON:")
        print(f"  Original records: {len(original_df):,}")
        print(f"  Streaming records: {streaming_count:,}")
        difference = len(original_df) - streaming_count
        percentage = (difference / len(original_df)) * 100
        print(f"  Difference: {difference:,} ({percentage:.1f}%)")
        
        # Check if streaming is subset
        if streaming_count < len(original_df):
            print(f"  ⚠️  Streaming processed fewer records (test mode limited to 2 files)")
        else:
            print(f"  ✅ Streaming processed more records")
    else:
        print("❌ Original fact table not found")

if __name__ == "__main__":
    print("🚀 Starting S3 streaming results review...")
    review_s3_streaming_results()
    compare_with_original() 