#!/usr/bin/env python3
"""Check NPPES data and compare with production data."""

import pandas as pd
from pathlib import Path

def check_nppes_data():
    """Check what's actually in the NPPES data."""
    
    # Load NPPES data
    nppes_file = Path("nppes_data/nppes_providers.parquet")
    if not nppes_file.exists():
        print("❌ NPPES file not found!")
        return
    
    nppes_df = pd.read_parquet(nppes_file)
    print(f"📊 NPPES Data:")
    print(f"   Total records: {len(nppes_df):,}")
    print(f"   Unique NPIs: {nppes_df['npi'].nunique():,}")
    print(f"   Columns: {list(nppes_df.columns)}")
    
    # Load production providers data
    prod_providers_file = Path("production_data/providers/providers_final.parquet")
    if prod_providers_file.exists():
        prod_df = pd.read_parquet(prod_providers_file)
        print(f"\n📊 Production Providers Data:")
        print(f"   Total records: {len(prod_df):,}")
        print(f"   Unique NPIs: {prod_df['npi'].nunique():,}")
        
        # Check overlap
        nppes_npis = set(nppes_df['npi'].astype(str))
        prod_npis = set(prod_df['npi'].dropna().astype(str))
        overlap = nppes_npis.intersection(prod_npis)
        
        print(f"\n🔗 Overlap Analysis:")
        print(f"   NPPES NPIs: {len(nppes_npis):,}")
        print(f"   Production NPIs: {len(prod_npis):,}")
        print(f"   Overlap: {len(overlap):,}")
        print(f"   Match rate: {len(overlap)/len(prod_npis)*100:.1f}%")
        
        # Show some sample NPIs
        print(f"\n📋 Sample NPIs from NPPES:")
        print(f"   {list(nppes_df['npi'].head(5))}")
        
        print(f"\n📋 Sample NPIs from Production:")
        print(f"   {list(prod_df['npi'].dropna().head(5))}")
        
        # Check if there are duplicates
        nppes_duplicates = nppes_df['npi'].duplicated().sum()
        prod_duplicates = prod_df['npi'].duplicated().sum()
        
        print(f"\n⚠️  Duplicate Analysis:")
        print(f"   NPPES duplicates: {nppes_duplicates}")
        print(f"   Production duplicates: {prod_duplicates}")

if __name__ == "__main__":
    check_nppes_data() 