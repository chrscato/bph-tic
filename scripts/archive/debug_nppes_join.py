#!/usr/bin/env python3
"""
Debug script to check NPPES data structure and join issues.
"""

import pandas as pd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def debug_nppes_join():
    """Debug NPPES join issues."""
    
    # Check if NPPES file exists
    nppes_path = Path("nppes_data/nppes_providers.parquet")
    if not nppes_path.exists():
        logger.error(f"NPPES file not found: {nppes_path}")
        return
    
    # Load NPPES data
    logger.info("Loading NPPES data...")
    nppes_df = pd.read_parquet(nppes_path)
    logger.info(f"NPPES data shape: {nppes_df.shape}")
    logger.info(f"NPPES columns: {list(nppes_df.columns)}")
    
    # Check NPPES data sample
    logger.info("\nNPPES data sample:")
    print(nppes_df.head().to_string())
    
    # Check for NPIs
    logger.info(f"\nNPPES NPIs: {nppes_df['npi'].nunique():,} unique NPIs")
    logger.info(f"NPPES NPIs sample: {nppes_df['npi'].head(10).tolist()}")
    
    # Check for null NPIs
    null_npis = nppes_df['npi'].isnull().sum()
    logger.info(f"Null NPIs in NPPES: {null_npis}")
    
    # Load providers data
    providers_path = Path("ortho_radiology_data/providers/providers_final.parquet")
    if providers_path.exists():
        logger.info("\nLoading providers data...")
        providers_df = pd.read_parquet(providers_path)
        logger.info(f"Providers data shape: {providers_df.shape}")
        
        # Check provider NPIs
        logger.info(f"Provider NPIs: {providers_df['npi'].nunique():,} unique NPIs")
        logger.info(f"Provider NPIs sample: {providers_df['npi'].dropna().head(10).tolist()}")
        
        # Check for null NPIs in providers
        null_provider_npis = providers_df['npi'].isnull().sum()
        logger.info(f"Null NPIs in providers: {null_provider_npis}")
        
        # Check overlap
        nppes_npis = set(nppes_df['npi'].dropna().unique())
        provider_npis = set(providers_df['npi'].dropna().unique())
        overlap = nppes_npis.intersection(provider_npis)
        
        logger.info(f"\nNPI Overlap Analysis:")
        logger.info(f"NPPES NPIs: {len(nppes_npis):,}")
        logger.info(f"Provider NPIs: {len(provider_npis):,}")
        logger.info(f"Overlapping NPIs: {len(overlap):,}")
        logger.info(f"Overlap percentage: {len(overlap)/len(provider_npis)*100:.1f}%")
        
        # Test the join
        logger.info("\nTesting NPPES join...")
        test_join = providers_df.merge(
            nppes_df[['npi', 'provider_type', 'primary_specialty']],
            on='npi',
            how='inner'
        )
        logger.info(f"After inner join: {len(test_join):,} records")
        
        if len(test_join) > 0:
            logger.info("Sample joined data:")
            logger.info(f"Joined columns: {list(test_join.columns)}")
            # Show available columns
            available_cols = ['npi']
            if 'provider_type' in test_join.columns:
                available_cols.append('provider_type')
            if 'primary_specialty' in test_join.columns:
                available_cols.append('primary_specialty')
            
            print(test_join[available_cols].head().to_string())
        else:
            logger.warning("No records after inner join!")
            
            # Check data types
            logger.info(f"\nData type comparison:")
            logger.info(f"NPPES NPI dtype: {nppes_df['npi'].dtype}")
            logger.info(f"Provider NPI dtype: {providers_df['npi'].dtype}")
            
            # Check sample values
            logger.info(f"\nSample NPI values:")
            logger.info(f"NPPES NPIs: {nppes_df['npi'].dropna().head(5).tolist()}")
            logger.info(f"Provider NPIs: {providers_df['npi'].dropna().head(5).tolist()}")
    
    # Check NPPES file structure
    logger.info(f"\nNPPES file info:")
    logger.info(f"File size: {nppes_path.stat().st_size / 1024 / 1024:.1f} MB")
    
    # Check for specific columns
    expected_cols = ['provider_type', 'primary_specialty', 'addresses']
    for col in expected_cols:
        if col in nppes_df.columns:
            non_null_count = nppes_df[col].notna().sum()
            logger.info(f"{col}: {non_null_count:,} non-null values out of {len(nppes_df):,}")
        else:
            logger.warning(f"Column '{col}' not found in NPPES data")

if __name__ == "__main__":
    debug_nppes_join() 