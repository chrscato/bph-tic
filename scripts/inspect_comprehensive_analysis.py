#!/usr/bin/env python3
"""
Inspect the comprehensive analysis parquet file.
"""

import pandas as pd
import json
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def inspect_comprehensive_analysis():
    """Inspect the comprehensive analysis parquet file."""
    
    # Load the parquet file
    parquet_path = Path("dashboard_data/comprehensive_analysis.parquet")
    
    if not parquet_path.exists():
        logger.error(f"Parquet file not found: {parquet_path}")
        return
    
    logger.info(f"Loading parquet file: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    
    # Basic information
    logger.info(f"Dataset shape: {df.shape}")
    logger.info(f"Columns: {list(df.columns)}")
    
    # Data types
    logger.info("\nData types:")
    for col, dtype in df.dtypes.items():
        logger.info(f"  {col}: {dtype}")
    
    # Sample data
    logger.info("\nSample data (first 5 rows):")
    print(df.head().to_string())
    
    # Save sample data to CSV
    sample_csv_path = Path("dashboard_data/comprehensive_analysis_sample.csv")
    sample_df = df.head(100)  # First 100 rows for CSV
    sample_df.to_csv(sample_csv_path, index=False)
    logger.info(f"\nSample data saved to CSV: {sample_csv_path}")
    logger.info(f"CSV contains {len(sample_df)} rows with {len(sample_df.columns)} columns")
    
    # Save full dataset sample to CSV (all 26 columns, 200 rows)
    full_csv_path = Path("dashboard_data/comprehensive_analysis_full_sample.csv")
    logger.info(f"\nSaving full dataset sample to CSV: {full_csv_path}")
    
    # Take 200 rows with all 26 columns
    full_sample_df = df.head(200)
    full_sample_df.to_csv(full_csv_path, index=False)
    
    logger.info(f"Full dataset sample saved to CSV: {full_csv_path}")
    logger.info(f"CSV contains {len(full_sample_df):,} rows with {len(full_sample_df.columns)} columns")
    logger.info(f"File size: {full_csv_path.stat().st_size / 1024:.1f} KB")
    
    # Display output for better visualization
    try:
        from IPython.display import display
        logger.info("\nDisplay output (first 10 rows):")
        display(df.head(10))
    except ImportError:
        logger.info("IPython display not available - using standard print")
        print("\nFirst 10 rows:")
        print(df.head(10).to_string())
    
    # Key statistics
    logger.info("\nKey statistics:")
    
    # Rate statistics
    if 'negotiated_rate' in df.columns:
        rate_stats = df['negotiated_rate'].describe()
        logger.info(f"\nRate statistics:")
        print(rate_stats.to_string())
    
    # Count statistics
    logger.info(f"\nRecord counts:")
    logger.info(f"  Total records: {len(df):,}")
    logger.info(f"  Unique service codes: {df['service_code'].nunique():,}")
    logger.info(f"  Unique organizations: {df['organization_uuid'].nunique():,}")
    logger.info(f"  Unique zip codes: {df['primary_zip'].nunique():,}")
    logger.info(f"  Unique states: {df['primary_state'].nunique():,}")
    
    # Rate categories
    if 'rate_category' in df.columns:
        logger.info(f"\nRate category distribution:")
        rate_cat_counts = df['rate_category'].value_counts()
        print(rate_cat_counts.to_string())
        
        try:
            from IPython.display import display
            display(rate_cat_counts)
        except ImportError:
            pass
    
    # Service categories
    if 'service_category' in df.columns:
        logger.info(f"\nService category distribution:")
        service_cat_counts = df['service_category'].value_counts()
        print(service_cat_counts.to_string())
        
        try:
            from IPython.display import display
            display(service_cat_counts)
        except ImportError:
            pass
    
    # Provider count statistics
    if 'provider_count' in df.columns:
        logger.info(f"\nProvider count statistics:")
        provider_stats = df['provider_count'].describe()
        print(provider_stats.to_string())
    
    # Sample of organizations with highest provider counts
    if 'provider_count' in df.columns and 'organization_name' in df.columns:
        logger.info(f"\nTop 10 organizations by provider count:")
        top_orgs = df.groupby(['organization_uuid', 'organization_name'])['provider_count'].first().nlargest(10)
        print(top_orgs.to_string())
        
        try:
            from IPython.display import display
            display(top_orgs)
        except ImportError:
            pass
    
    # Sample of highest rates
    if 'negotiated_rate' in df.columns:
        logger.info(f"\nTop 10 highest rates:")
        top_rates = df.nlargest(10, 'negotiated_rate')[['service_code', 'service_description', 'negotiated_rate', 'organization_name']]
        print(top_rates.to_string())
        
        try:
            from IPython.display import display
            display(top_rates)
        except ImportError:
            pass
    
    # Geographic distribution
    if 'primary_state' in df.columns:
        logger.info(f"\nTop 10 states by record count:")
        state_counts = df['primary_state'].value_counts().head(10)
        print(state_counts.to_string())
    
    # Check for any missing data
    logger.info(f"\nMissing data summary:")
    missing_data = df.isnull().sum()
    missing_data = missing_data[missing_data > 0]
    if len(missing_data) > 0:
        print(missing_data.to_string())
    else:
        logger.info("  No missing data found")
    
    # Save additional CSV files for analysis
    logger.info(f"\nSaving additional CSV files for analysis:")
    
    # Save highest rates to CSV
    if 'negotiated_rate' in df.columns:
        top_rates_csv = Path("dashboard_data/highest_rates_sample.csv")
        top_rates_df = df.nlargest(50, 'negotiated_rate')[['service_code', 'service_description', 'negotiated_rate', 'organization_name', 'provider_count', 'rate_category', 'service_category']]
        top_rates_df.to_csv(top_rates_csv, index=False)
        logger.info(f"  Highest rates saved to: {top_rates_csv}")
    
    # Save rate category summary to CSV
    if 'rate_category' in df.columns:
        rate_summary_csv = Path("dashboard_data/rate_category_summary.csv")
        rate_summary = df.groupby('rate_category').agg({
            'negotiated_rate': ['count', 'mean', 'median', 'min', 'max'],
            'organization_uuid': 'nunique',
            'service_code': 'nunique'
        }).round(2)
        rate_summary.columns = ['record_count', 'avg_rate', 'median_rate', 'min_rate', 'max_rate', 'unique_organizations', 'unique_service_codes']
        rate_summary.to_csv(rate_summary_csv)
        logger.info(f"  Rate category summary saved to: {rate_summary_csv}")
    
    # Save service category summary to CSV
    if 'service_category' in df.columns:
        service_summary_csv = Path("dashboard_data/service_category_summary.csv")
        service_summary = df.groupby('service_category').agg({
            'negotiated_rate': ['count', 'mean', 'median', 'min', 'max'],
            'organization_uuid': 'nunique',
            'service_code': 'nunique'
        }).round(2)
        service_summary.columns = ['record_count', 'avg_rate', 'median_rate', 'min_rate', 'max_rate', 'unique_organizations', 'unique_service_codes']
        service_summary.to_csv(service_summary_csv)
        logger.info(f"  Service category summary saved to: {service_summary_csv}")
    
    # Save top organizations to CSV
    if 'provider_count' in df.columns and 'organization_name' in df.columns:
        orgs_csv = Path("dashboard_data/top_organizations.csv")
        top_orgs_df = df.groupby(['organization_uuid', 'organization_name']).agg({
            'provider_count': 'first',
            'negotiated_rate': ['count', 'mean', 'median'],
            'service_code': 'nunique'
        }).round(2)
        top_orgs_df.columns = ['provider_count', 'rate_count', 'avg_rate', 'median_rate', 'unique_service_codes']
        top_orgs_df = top_orgs_df.sort_values('provider_count', ascending=False).head(100)
        top_orgs_df.to_csv(orgs_csv)
        logger.info(f"  Top organizations saved to: {orgs_csv}")
    
    # Check list columns
    list_columns = []
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if it's a list column
            sample_values = df[col].dropna().head(10)
            if len(sample_values) > 0 and isinstance(sample_values.iloc[0], list):
                list_columns.append(col)
    
    if list_columns:
        logger.info(f"\nList columns found: {list_columns}")
        for col in list_columns:
            logger.info(f"\nSample values for {col}:")
            sample_values = df[col].dropna().head(5)
            for i, val in enumerate(sample_values):
                logger.info(f"  {i+1}: {val}")
    
    # Final summary display
    try:
        from IPython.display import display, HTML
        logger.info(f"\nFinal Summary Display:")
        
        # Create summary DataFrame
        summary_data = {
            'Metric': [
                'Total Records',
                'Unique Service Codes', 
                'Unique Organizations',
                'Unique Zip Codes',
                'Unique States',
                'Mean Rate',
                'Median Rate',
                'Max Rate',
                'Min Rate'
            ],
            'Value': [
                f"{len(df):,}",
                f"{df['service_code'].nunique():,}",
                f"{df['organization_uuid'].nunique():,}",
                f"{df['primary_zip'].nunique():,}",
                f"{df['primary_state'].nunique():,}",
                f"${df['negotiated_rate'].mean():.2f}",
                f"${df['negotiated_rate'].median():.2f}",
                f"${df['negotiated_rate'].max():.2f}",
                f"${df['negotiated_rate'].min():.2f}"
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        display(HTML("<h3>Dataset Summary</h3>"))
        display(summary_df)
        
        # Show sample of key columns
        display(HTML("<h3>Sample Data (Key Columns)</h3>"))
        key_cols = ['service_code', 'service_description', 'negotiated_rate', 'organization_name', 'provider_count', 'rate_category', 'service_category']
        available_cols = [col for col in key_cols if col in df.columns]
        display(df[available_cols].head(10))
        
    except ImportError:
        logger.info("IPython display not available for summary")

if __name__ == "__main__":
    inspect_comprehensive_analysis() 