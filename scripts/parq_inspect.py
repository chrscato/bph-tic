#!/usr/bin/env python3
"""Inspect Parquet files from TiC MRF processing."""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import numpy as np

def inspect_parquet_file(file_path):
    """Comprehensive inspection of a Parquet file."""
    
    print(f"🔍 Inspecting: {file_path}")
    print("=" * 60)
    
    # Basic file info
    file_size = Path(file_path).stat().st_size
    print(f"File size: {file_size / 1024 / 1024:.2f} MB")
    
    # Read with PyArrow for schema info
    parquet_file = pq.ParquetFile(file_path)
    print(f"Schema:")
    print(parquet_file.schema)
    print()
    
    # Read with Pandas for analysis
    df = pd.read_parquet(file_path)
    
    # Basic stats
    print(f"📊 Basic Statistics:")
    print(f"Total records: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    print()
    
    # Column info
    print(f"📋 Column Details:")
    for col in df.columns:
        dtype = df[col].dtype
        null_count = df[col].isnull().sum()
        
        # Handle list/array columns safely
        try:
            # Check if column contains lists/arrays
            sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
            if isinstance(sample_val, (list, tuple)) or (hasattr(sample_val, 'dtype') and 'int' in str(sample_val.dtype)):
                unique_count = "list/array"
            else:
                unique_count = df[col].nunique() if len(df) < 10000 else "many"
        except:
            unique_count = "complex"
            
        print(f"  {col:20} | {str(dtype):15} | {null_count:6} nulls | {unique_count} unique")
    print()
    
    # Service code analysis
    if 'service_code' in df.columns:
        print(f"🏥 Service Code Analysis:")
        code_counts = df['service_code'].value_counts()
        print(f"Unique service codes: {len(code_counts)}")
        print("Top codes by frequency:")
        for code, count in code_counts.head(10).items():
            print(f"  {code}: {count:,} records")
        print()
    
    # Rate analysis
    if 'negotiated_rate' in df.columns:
        rates = df['negotiated_rate']
        print(f"💰 Rate Analysis:")
        print(f"Rate range: ${rates.min():.2f} - ${rates.max():.2f}")
        print(f"Average rate: ${rates.mean():.2f}")
        print(f"Median rate: ${rates.median():.2f}")
        print(f"Standard deviation: ${rates.std():.2f}")
        
        # Rate percentiles
        print(f"Rate percentiles:")
        for p in [10, 25, 50, 75, 90, 95, 99]:
            print(f"  {p:2d}th percentile: ${rates.quantile(p/100):.2f}")
        print()
    
    # Provider analysis
    if 'provider_tin' in df.columns:
        tin_counts = df['provider_tin'].value_counts()
        print(f"🏢 Provider Analysis:")
        print(f"Unique TINs: {len(tin_counts)}")
        print("Top TINs by record count:")
        for tin, count in tin_counts.head(5).items():
            print(f"  {tin}: {count:,} records")
        print()
    
    # Payer analysis
    if 'payer' in df.columns:
        payer_counts = df['payer'].value_counts()
        print(f"🏥 Payer Analysis:")
        for payer, count in payer_counts.items():
            print(f"  {payer}: {count:,} records")
        print()
    
    # Sample records
    print(f"📄 Sample Records (first 5):")
    # Show key columns only for readability
    key_cols = ['service_code', 'negotiated_rate', 'billing_code_type', 'provider_tin', 'payer']
    available_cols = [col for col in key_cols if col in df.columns]
    print(df[available_cols].head())
    print()
    
    # Rate distribution by service code
    if 'service_code' in df.columns and 'negotiated_rate' in df.columns:
        print(f"💲 Rate Distribution by Service Code:")
        rate_stats = df.groupby('service_code')['negotiated_rate'].agg(['count', 'min', 'max', 'mean', 'std']).round(2)
        print(rate_stats)
        print()
    
    return df

def inspect_all_parquet_files(directory="output"):
    """Inspect all Parquet files in a directory."""
    
    parquet_files = list(Path(directory).glob("*.parquet"))
    
    if not parquet_files:
        print(f"No Parquet files found in {directory}/")
        return
    
    print(f"Found {len(parquet_files)} Parquet file(s) in {directory}/")
    print()
    
    for file_path in parquet_files:
        inspect_parquet_file(file_path)
        print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    # Inspect all files in output directory
    inspect_all_parquet_files("output")
    
    # Or inspect a specific file
    # inspect_parquet_file("output/centene_fidelis_rates.parquet")