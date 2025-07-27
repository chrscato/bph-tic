#!/usr/bin/env python3
"""
Inspect the 3 core parquet files to understand structure and join relationships.
Focus on getting down to NPI/rate level for fact table creation.
"""

import pandas as pd
from pathlib import Path
import logging
from IPython.display import display

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def inspect_core_parquets():
    """Inspect the 3 core parquet files and their relationships."""
    
    logger.info("Inspecting 3 core parquet files for fact table creation...")
    logger.info("=" * 60)
    
    # 1. INSPECT RATES DATA
    logger.info("\n1. RATES DATA INSPECTION")
    logger.info("-" * 30)
    
    rates_path = Path("ortho_radiology_data/rates/rates_final.parquet")
    if rates_path.exists():
        rates_df = pd.read_parquet(rates_path)
        logger.info(f"✅ Rates file found: {rates_path}")
        logger.info(f"   Records: {len(rates_df):,}")
        logger.info(f"   Columns: {len(rates_df.columns)}")
        logger.info(f"   Memory: {rates_df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
        
        logger.info(f"\n   Columns: {list(rates_df.columns)}")
        
        # Show sample data
        logger.info(f"\n   Sample data (first 3 rows):")
        for i, row in rates_df.head(3).iterrows():
            logger.info(f"   Row {i}: {dict(row)}")
        
        # Key fields analysis
        logger.info(f"\n   Key fields analysis:")
        logger.info(f"   - rate_uuid: {rates_df['rate_uuid'].nunique():,} unique values")
        logger.info(f"   - organization_uuid: {rates_df['organization_uuid'].nunique():,} unique values")
        logger.info(f"   - service_code: {rates_df['service_code'].nunique():,} unique values")
        logger.info(f"   - negotiated_rate: ${rates_df['negotiated_rate'].min():.2f} to ${rates_df['negotiated_rate'].max():.2f}")
        
        # Check for NPI in rates
        if 'npi' in rates_df.columns:
            logger.info(f"   - npi: {rates_df['npi'].nunique():,} unique values")
            logger.info(f"   - npi null count: {rates_df['npi'].isnull().sum():,}")
        else:
            logger.info(f"   - npi: NOT PRESENT in rates data")
        
    else:
        logger.error(f"❌ Rates file not found: {rates_path}")
        return
    
    # 2. INSPECT ORGANIZATIONS DATA
    logger.info("\n\n2. ORGANIZATIONS DATA INSPECTION")
    logger.info("-" * 30)
    
    orgs_path = Path("ortho_radiology_data/organizations/organizations_final.parquet")
    if orgs_path.exists():
        orgs_df = pd.read_parquet(orgs_path)
        logger.info(f"✅ Organizations file found: {orgs_path}")
        logger.info(f"   Records: {len(orgs_df):,}")
        logger.info(f"   Columns: {len(orgs_df.columns)}")
        logger.info(f"   Memory: {orgs_df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
        
        logger.info(f"\n   Columns: {list(orgs_df.columns)}")
        
        # Show sample data
        logger.info(f"\n   Sample data (first 3 rows):")
        for i, row in orgs_df.head(3).iterrows():
            logger.info(f"   Row {i}: {dict(row)}")
        
        # Key fields analysis
        logger.info(f"\n   Key fields analysis:")
        logger.info(f"   - organization_uuid: {orgs_df['organization_uuid'].nunique():,} unique values")
        
        # Check overlap with rates
        rates_orgs = set(rates_df['organization_uuid'].unique())
        orgs_orgs = set(orgs_df['organization_uuid'].unique())
        overlap = rates_orgs.intersection(orgs_orgs)
        logger.info(f"   - Organizations in rates: {len(rates_orgs):,}")
        logger.info(f"   - Organizations in orgs file: {len(orgs_orgs):,}")
        logger.info(f"   - Overlap: {len(overlap):,} ({len(overlap)/len(rates_orgs)*100:.1f}%)")
        
    else:
        logger.error(f"❌ Organizations file not found: {orgs_path}")
    
    # 3. INSPECT PROVIDERS DATA
    logger.info("\n\n3. PROVIDERS DATA INSPECTION")
    logger.info("-" * 30)
    
    providers_path = Path("ortho_radiology_data/providers/providers_final.parquet")
    if providers_path.exists():
        providers_df = pd.read_parquet(providers_path)
        logger.info(f"✅ Providers file found: {providers_path}")
        logger.info(f"   Records: {len(providers_df):,}")
        logger.info(f"   Columns: {len(providers_df.columns)}")
        logger.info(f"   Memory: {providers_df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
        
        logger.info(f"\n   Columns: {list(providers_df.columns)}")
        
        # Show sample data
        logger.info(f"\n   Sample data (first 3 rows):")
        for i, row in providers_df.head(3).iterrows():
            logger.info(f"   Row {i}: {dict(row)}")
        
        # Key fields analysis
        logger.info(f"\n   Key fields analysis:")
        logger.info(f"   - organization_uuid: {providers_df['organization_uuid'].nunique():,} unique values")
        logger.info(f"   - npi: {providers_df['npi'].nunique():,} unique values")
        logger.info(f"   - npi null count: {providers_df['npi'].isnull().sum():,}")
        
        # Check overlap with rates
        providers_orgs = set(providers_df['organization_uuid'].unique())
        overlap = rates_orgs.intersection(providers_orgs)
        logger.info(f"   - Organizations in rates: {len(rates_orgs):,}")
        logger.info(f"   - Organizations in providers: {len(providers_orgs):,}")
        logger.info(f"   - Overlap: {len(overlap):,} ({len(overlap)/len(rates_orgs)*100:.1f}%)")
        
        # Check NPI distribution
        if 'npi' in providers_df.columns:
            npi_counts = providers_df['npi'].value_counts()
            logger.info(f"   - NPIs per organization (sample):")
            logger.info(f"     {npi_counts.head(10).to_dict()}")
        
    else:
        logger.error(f"❌ Providers file not found: {providers_path}")
    
    # 4. JOIN ANALYSIS
    logger.info("\n\n4. JOIN ANALYSIS")
    logger.info("-" * 30)
    
    # Test the join path
    logger.info("Testing join path: rates -> organizations -> providers")
    
    # Join rates with organizations
    test_join = rates_df.merge(orgs_df, on='organization_uuid', how='left', suffixes=('', '_org'))
    logger.info(f"   Rates + Organizations: {len(test_join):,} records")
    
    # Join with providers
    test_join = test_join.merge(providers_df, on='organization_uuid', how='left', suffixes=('', '_provider'))
    logger.info(f"   Rates + Organizations + Providers: {len(test_join):,} records")
    
    # Check NPI availability
    if 'npi' in test_join.columns:
        npi_coverage = test_join['npi'].notna().sum()
        logger.info(f"   Records with NPI: {npi_coverage:,} ({npi_coverage/len(test_join)*100:.1f}%)")
        logger.info(f"   Records without NPI: {len(test_join) - npi_coverage:,}")
        
        # Show sample of records with NPI
        logger.info(f"\n   Sample records with NPI (first 3):")
        npi_records = test_join[test_join['npi'].notna()].head(3)
        for i, row in npi_records.iterrows():
            logger.info(f"   Row {i}: rate_uuid={row['rate_uuid']}, npi={row['npi']}, org={row['organization_uuid']}")
    
    # 5. NPI/RATE LEVEL ANALYSIS
    logger.info("\n\n5. NPI/RATE LEVEL ANALYSIS")
    logger.info("-" * 30)
    
    if 'npi' in test_join.columns:
        # Unique combinations
        npi_rate_combinations = test_join[['rate_uuid', 'npi']].dropna().drop_duplicates()
        logger.info(f"   Unique rate_uuid + npi combinations: {len(npi_rate_combinations):,}")
        
        # Check for multiple NPIs per rate
        npi_per_rate = test_join.groupby('rate_uuid')['npi'].nunique()
        logger.info(f"   NPIs per rate statistics:")
        logger.info(f"     - Min: {npi_per_rate.min()}")
        logger.info(f"     - Max: {npi_per_rate.max()}")
        logger.info(f"     - Mean: {npi_per_rate.mean():.2f}")
        logger.info(f"     - Median: {npi_per_rate.median()}")
        
        # Show distribution
        npi_dist = npi_per_rate.value_counts().sort_index()
        logger.info(f"   NPIs per rate distribution:")
        for npis, count in npi_dist.items():
            logger.info(f"     {npis} NPI(s): {count:,} rates")
        
        # Check for rates without NPI
        rates_without_npi = test_join[test_join['npi'].isna()]['rate_uuid'].nunique()
        logger.info(f"   Rates without any NPI: {rates_without_npi:,}")
    
    # 6. RECOMMENDATIONS
    logger.info("\n\n6. FACT TABLE JOIN RECOMMENDATIONS")
    logger.info("-" * 30)
    
    logger.info("Based on the analysis, here's the recommended join strategy:")
    logger.info("1. Start with rates (core fact table)")
    logger.info("2. LEFT JOIN with organizations on organization_uuid")
    logger.info("3. LEFT JOIN with providers on organization_uuid")
    logger.info("4. LEFT JOIN with NPPES on npi")
    logger.info("")
    logger.info("This will give you:")
    logger.info("- One row per rate (no multiplication)")
    logger.info("- Organization context for each rate")
    logger.info("- Provider context (NPI) for each rate")
    logger.info("- NPPES enrichment for providers with NPPES data")
    logger.info("")
    logger.info("Expected result: rates_df.shape[0] rows with all context joined in")

def display_sample_data():
    """Display sample data for notebook environments."""
    try:
        from IPython.display import display
        
        logger.info("\n\nSAMPLE DATA DISPLAY")
        logger.info("=" * 30)
        
        # Load and display sample data
        rates_path = Path("ortho_radiology_data/rates/rates_final.parquet")
        if rates_path.exists():
            rates_df = pd.read_parquet(rates_path)
            logger.info("Rates sample:")
            display(rates_df.head())
        
        orgs_path = Path("ortho_radiology_data/organizations/organizations_final.parquet")
        if orgs_path.exists():
            orgs_df = pd.read_parquet(orgs_path)
            logger.info("Organizations sample:")
            display(orgs_df.head())
        
        providers_path = Path("ortho_radiology_data/providers/providers_final.parquet")
        if providers_path.exists():
            providers_df = pd.read_parquet(providers_path)
            logger.info("Providers sample:")
            display(providers_df.head())
            
    except ImportError:
        logger.info("IPython not available - skipping display output")

if __name__ == "__main__":
    inspect_core_parquets()
    display_sample_data() 