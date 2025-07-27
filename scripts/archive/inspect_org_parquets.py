#!/usr/bin/env python3
"""Simple script to inspect organization parquet files in S3."""

import boto3
import pandas as pd
from pathlib import Path
import tempfile

def inspect_org_parquets(bucket="commercial-rates", prefix="tic-mrf/test"):
    """Inspect organization parquet files in S3."""
    s3_client = boto3.client('s3')
    
    # List organization files
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    org_files = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if 'orgs' in obj['Key'] and obj['Key'].endswith('.parquet'):
                    org_files.append(obj['Key'])
    
    print(f"Found {len(org_files)} organization files:")
    for i, file_key in enumerate(org_files, 1):
        print(f"  {i}. {file_key}")
    
    # Inspect first file in detail
    if org_files:
        print(f"\nInspecting first file: {org_files[0]}")
        
        # Download and read
        with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp_file:
            s3_client.download_file(bucket, org_files[0], tmp_file.name)
            df = pd.read_parquet(tmp_file.name)
        
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"\nSample data:")
        print(df.head(3).to_string())
        
        # Check organization_uuid mapping
        if 'organization_uuid' in df.columns:
            print(f"\nUnique organizations: {df['organization_uuid'].nunique()}")
            print(f"Sample UUIDs: {df['organization_uuid'].head().tolist()}")

if __name__ == "__main__":
    inspect_org_parquets() 