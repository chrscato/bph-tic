#!/usr/bin/env python3
"""
Comprehensive analysis of NPPES data structure and content.
"""

import pandas as pd
import json
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_nppes_data():
    """Comprehensive analysis of NPPES data."""
    
    # Load NPPES data
    nppes_path = Path("nppes_data/nppes_providers.parquet")
    if not nppes_path.exists():
        logger.error(f"NPPES file not found: {nppes_path}")
        return
    
    logger.info("Loading NPPES data...")
    df = pd.read_parquet(nppes_path)
    
    # Basic information
    logger.info(f"NPPES Dataset Overview:")
    logger.info(f"Shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")
    logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
    
    # Data types
    logger.info(f"\nData Types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"  {col}: {dtype}")
    
    # Sample data
    logger.info(f"\nSample Data (first 3 rows):")
    print(df.head(3).to_string())
    
    # Column-by-column analysis
    logger.info(f"\nColumn-by-Column Analysis:")
    
    for col in df.columns:
        logger.info(f"\n--- {col} ---")
        
        # Basic stats
        non_null = df[col].notna().sum()
        null_count = df[col].isnull().sum()
        
        logger.info(f"Non-null: {non_null:,} ({non_null/len(df)*100:.1f}%)")
        logger.info(f"Null: {null_count:,} ({null_count/len(df)*100:.1f}%)")
        
        # Handle unique count for complex types
        try:
            unique_count = df[col].nunique()
            logger.info(f"Unique values: {unique_count:,}")
        except TypeError:
            logger.info(f"Unique values: Cannot compute (complex data type)")
        
        # Sample values
        if non_null > 0:
            sample_values = df[col].dropna().head(5).tolist()
            logger.info(f"Sample values: {sample_values}")
        
        # Special analysis for complex columns
        if col == 'addresses':
            analyze_addresses_column(df[col])
        elif col == 'provider_name':
            analyze_provider_name_column(df[col])
        elif col == 'primary_specialty':
            analyze_primary_specialty_column(df[col])
        elif col == 'secondary_specialties':
            analyze_secondary_specialties_column(df[col])
        elif col == 'credentials':
            analyze_credentials_column(df[col])
    
    # NPI analysis
    logger.info(f"\n--- NPI Analysis ---")
    logger.info(f"NPIs: {df['npi'].nunique():,} unique")
    logger.info(f"NPI format check:")
    sample_npis = df['npi'].head(10).tolist()
    for npi in sample_npis:
        logger.info(f"  {npi} (length: {len(str(npi))})")
    
    # Gender analysis
    if 'gender' in df.columns:
        logger.info(f"\n--- Gender Analysis ---")
        gender_counts = df['gender'].value_counts()
        print(gender_counts.to_string())
    
    # Provider type analysis
    if 'provider_type' in df.columns:
        logger.info(f"\n--- Provider Type Analysis ---")
        provider_type_counts = df['provider_type'].value_counts()
        print(provider_type_counts.to_string())
    
    # Date analysis
    if 'enumeration_date' in df.columns:
        logger.info(f"\n--- Enumeration Date Analysis ---")
        logger.info(f"Date range: {df['enumeration_date'].min()} to {df['enumeration_date'].max()}")
        logger.info(f"Most recent: {df['enumeration_date'].max()}")
    
    if 'last_updated' in df.columns:
        logger.info(f"\n--- Last Updated Analysis ---")
        logger.info(f"Date range: {df['last_updated'].min()} to {df['last_updated'].max()}")
        logger.info(f"Most recent: {df['last_updated'].max()}")

def analyze_addresses_column(addresses_series):
    """Analyze the addresses column structure."""
    logger.info("Addresses column analysis:")
    
    # Count non-null addresses
    non_null_addresses = addresses_series.dropna()
    logger.info(f"Non-null addresses: {len(non_null_addresses):,}")
    
    if len(non_null_addresses) > 0:
        # Sample address structure
        sample_address = non_null_addresses.iloc[0]
        logger.info(f"Sample address structure: {sample_address}")
        
        # Analyze address fields
        if isinstance(sample_address, list) and len(sample_address) > 0:
            first_addr = sample_address[0]
            if isinstance(first_addr, dict):
                logger.info(f"Address fields: {list(first_addr.keys())}")
                
                # Check for common fields
                common_fields = ['city', 'state', 'zip', 'country', 'street', 'phone', 'fax']
                for field in common_fields:
                    if field in first_addr:
                        logger.info(f"  {field}: {first_addr[field]}")
                    else:
                        logger.info(f"  {field}: NOT FOUND")

def analyze_provider_name_column(name_series):
    """Analyze the provider_name column structure."""
    logger.info("Provider name column analysis:")
    
    non_null_names = name_series.dropna()
    logger.info(f"Non-null names: {len(non_null_names):,}")
    
    if len(non_null_names) > 0:
        sample_name = non_null_names.iloc[0]
        logger.info(f"Sample name structure: {sample_name}")
        
        if isinstance(sample_name, dict):
            logger.info(f"Name fields: {list(sample_name.keys())}")
            for key, value in sample_name.items():
                logger.info(f"  {key}: {value}")

def analyze_primary_specialty_column(specialty_series):
    """Analyze the primary_specialty column."""
    logger.info("Primary specialty column analysis:")
    
    non_null_specialties = specialty_series.dropna()
    logger.info(f"Non-null specialties: {len(non_null_specialties):,}")
    
    if len(non_null_specialties) > 0:
        # Show unique specialties
        unique_specialties = non_null_specialties.unique()
        logger.info(f"Unique specialties: {len(unique_specialties):,}")
        
        # Show top specialties
        top_specialties = non_null_specialties.value_counts().head(10)
        logger.info("Top 10 specialties:")
        print(top_specialties.to_string())

def analyze_secondary_specialties_column(specialty_series):
    """Analyze the secondary_specialties column."""
    logger.info("Secondary specialties column analysis:")
    
    non_null_specialties = specialty_series.dropna()
    logger.info(f"Non-null secondary specialties: {len(non_null_specialties):,}")
    
    if len(non_null_specialties) > 0:
        sample_specialty = non_null_specialties.iloc[0]
        logger.info(f"Sample secondary specialty structure: {sample_specialty}")
        
        # Count non-empty lists
        non_empty_lists = non_null_specialties[non_null_specialties.apply(lambda x: isinstance(x, list) and len(x) > 0)]
        logger.info(f"Non-empty secondary specialty lists: {len(non_empty_lists):,}")

def analyze_credentials_column(credentials_series):
    """Analyze the credentials column."""
    logger.info("Credentials column analysis:")
    
    non_null_credentials = credentials_series.dropna()
    logger.info(f"Non-null credentials: {len(non_null_credentials):,}")
    
    if len(non_null_credentials) > 0:
        sample_credential = non_null_credentials.iloc[0]
        logger.info(f"Sample credential structure: {sample_credential}")
        
        if isinstance(sample_credential, list):
            logger.info(f"Credentials are stored as lists")
            # Count non-empty lists
            non_empty_lists = non_null_credentials[non_null_credentials.apply(lambda x: isinstance(x, list) and len(x) > 0)]
            logger.info(f"Non-empty credential lists: {len(non_empty_lists):,}")

if __name__ == "__main__":
    analyze_nppes_data() 