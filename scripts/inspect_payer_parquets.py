#!/usr/bin/env python3
"""Simple script to inspect payer parquet files in S3."""

import boto3
import pandas as pd
from pathlib import Path
import tempfile

def inspect_payer_parquets(bucket="commercial-rates", prefix="tic-mrf/test"):
    """Inspect payer parquet files in S3."""
    s3_client = boto3.client('s3')
    
    # List payer files
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    payer_files = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if 'payers' in obj['Key'] and obj['Key'].endswith('.parquet'):
                    payer_files.append(obj['Key'])
    
    print(f"Found {len(payer_files)} payer files:")
    for i, file_key in enumerate(payer_files, 1):
        print(f"  {i}. {file_key}")
    
    # Inspect first file in detail
    if payer_files:
        print(f"\nInspecting first file: {payer_files[0]}")
        
        # Download and read
        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
            s3_client.download_file(bucket, payer_files[0], tmp_file.name)
            df = pd.read_parquet(tmp_file.name)
        
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"\nSample data:")
        print(df.head(3).to_string())
        
        # Check payer_uuid mapping
        if 'payer_uuid' in df.columns:
            print(f"\nUnique payers: {df['payer_uuid'].nunique()}")
            print(f"Sample UUIDs: {df['payer_uuid'].head().tolist()}")
        
        # Check payer names
        if 'payer_name' in df.columns:
            print(f"\nPayer names: {df['payer_name'].unique()}")

if __name__ == "__main__":
    inspect_payer_parquets() 