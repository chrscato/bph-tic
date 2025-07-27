#!/usr/bin/env python3
"""
Show exactly how NPPES data maps to the fact table fields.
"""

import pandas as pd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def show_nppes_fact_table_mapping():
    """Show how NPPES data maps to fact table fields."""
    
    # Load NPPES data
    nppes_path = Path("nppes_data/nppes_providers.parquet")
    if not nppes_path.exists():
        logger.error(f"NPPES file not found: {nppes_path}")
        return
    
    logger.info("Loading NPPES data...")
    nppes_df = pd.read_parquet(nppes_path)
    
    logger.info("NPPES to Fact Table Field Mapping:")
    logger.info("=" * 50)
    
    # Show the mapping
    mappings = {
        'npi': 'npi (direct copy)',
        'provider_type': 'nppes_provider_type (renamed)',
        'primary_specialty': 'nppes_primary_specialty (renamed)',
        'gender': 'nppes_gender (renamed)',
        'addresses': 'nppes_addresses (renamed, then extracted to nppes_zip, nppes_state, etc.)',
        'credentials': 'nppes_credentials (renamed)',
        'provider_name': 'nppes_provider_name (renamed)',
        'enumeration_date': 'nppes_enumeration_date (renamed)',
        'last_updated': 'nppes_last_updated (renamed)',
        'secondary_specialties': 'nppes_secondary_specialties (renamed)',
        'metadata': 'nppes_metadata (renamed)'
    }
    
    for nppes_col, fact_table_field in mappings.items():
        if nppes_col in nppes_df.columns:
            non_null = nppes_df[nppes_col].notna().sum()
            logger.info(f"✅ {nppes_col} -> {fact_table_field}")
            logger.info(f"   Coverage: {non_null:,}/{len(nppes_df):,} ({non_null/len(nppes_df)*100:.1f}%)")
        else:
            logger.info(f"❌ {nppes_col} -> {fact_table_field} (COLUMN NOT FOUND)")
    
    logger.info("\n" + "=" * 50)
    logger.info("EXTRACTED LOCATION FIELDS:")
    
    # Show what location fields will be extracted
    location_extractions = {
        'nppes_zip': 'Extracted from addresses[0].zip',
        'nppes_state': 'Extracted from addresses[0].state', 
        'nppes_city': 'Extracted from addresses[0].city',
        'nppes_country': 'Extracted from addresses[0].country',
        'nppes_street': 'Extracted from addresses[0].street',
        'nppes_phone': 'Extracted from addresses[0].phone',
        'nppes_fax': 'Extracted from addresses[0].fax'
    }
    
    for field, description in location_extractions.items():
        logger.info(f"📍 {field}: {description}")
    
    logger.info("\n" + "=" * 50)
    logger.info("SAMPLE NPPES RECORD ANALYSIS:")
    
    # Show a sample record
    sample_record = nppes_df.iloc[0]
    logger.info(f"Sample NPI: {sample_record['npi']}")
    
    # Show provider name
    if 'provider_name' in sample_record:
        name = sample_record['provider_name']
        if isinstance(name, dict):
            full_name = f"{name.get('first', '')} {name.get('middle', '')} {name.get('last', '')} {name.get('suffix', '')}".strip()
            logger.info(f"Provider Name: {full_name}")
    
    # Show primary specialty
    if 'primary_specialty' in sample_record:
        logger.info(f"Primary Specialty: {sample_record['primary_specialty']}")
    
    # Show provider type
    if 'provider_type' in sample_record:
        logger.info(f"Provider Type: {sample_record['provider_type']}")
    
    # Show address extraction
    if 'addresses' in sample_record:
        addresses = sample_record['addresses']
        if isinstance(addresses, list) and len(addresses) > 0:
            addr = addresses[0]
            logger.info(f"Address: {addr.get('street', '')}, {addr.get('city', '')}, {addr.get('state', '')} {addr.get('zip', '')}")
    
    logger.info("\n" + "=" * 50)
    logger.info("FACT TABLE FIELD AVAILABILITY:")
    
    # Show what will be available in fact table
    fact_table_fields = [
        'npi',
        'nppes_provider_type', 
        'nppes_primary_specialty',
        'nppes_gender',
        'nppes_zip',
        'nppes_state', 
        'nppes_city',
        'nppes_country',
        'nppes_credentials',
        'nppes_provider_name',
        'nppes_enumeration_date',
        'nppes_last_updated',
        'nppes_secondary_specialties',
        'nppes_metadata'
    ]
    
    for field in fact_table_fields:
        logger.info(f"📋 {field}")
    
    logger.info("\n" + "=" * 50)
    logger.info("JOIN STRATEGY:")
    
    # Show join strategy
    logger.info("1. Load NPPES data (65,236 records)")
    logger.info("2. Rename columns to avoid conflicts")
    logger.info("3. Inner join with providers on NPI")
    logger.info("4. Extract location fields from addresses")
    logger.info("5. Create provider combinations (NPI × specialty × zip)")
    logger.info("6. Join with rates data")
    
    # Show expected results
    logger.info("\nExpected Results with Inner Join:")
    logger.info("- Only providers with NPPES data will be included")
    logger.info("- All NPPES fields will be populated")
    logger.info("- Location data will be extracted and available")
    logger.info("- Provider specialties will be enriched")

if __name__ == "__main__":
    show_nppes_fact_table_mapping() 