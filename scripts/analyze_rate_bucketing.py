#!/usr/bin/env python3
"""
Analyze rate bucketing patterns in the comprehensive analysis data.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from collections import Counter

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_rate_bucketing():
    """Analyze rate bucketing patterns in the data."""
    
    # Load the parquet file
    parquet_path = Path("dashboard_data/comprehensive_analysis.parquet")
    
    if not parquet_path.exists():
        logger.error(f"Parquet file not found: {parquet_path}")
        return
    
    logger.info(f"Loading parquet file: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    
    logger.info(f"Analyzing rate bucketing patterns...")
    
    # Basic rate statistics
    rates = df['negotiated_rate'].dropna()
    logger.info(f"Total rates: {len(rates):,}")
    logger.info(f"Unique rates: {rates.nunique():,}")
    logger.info(f"Rate coverage: {rates.nunique() / len(rates) * 100:.2f}%")
    
    # Find most common rates
    rate_counts = rates.value_counts()
    logger.info(f"\nTop 20 most common rates:")
    print(rate_counts.head(20).to_string())
    
    # Analyze rate clustering
    logger.info(f"\nRate clustering analysis:")
    
    # Check for exact duplicates
    exact_duplicates = rate_counts[rate_counts > 1]
    logger.info(f"Rates with duplicates: {len(exact_duplicates):,}")
    logger.info(f"Total duplicate records: {exact_duplicates.sum():,}")
    logger.info(f"Duplicate percentage: {exact_duplicates.sum() / len(rates) * 100:.2f}%")
    
    # Analyze rate ranges
    logger.info(f"\nRate range analysis:")
    logger.info(f"Min rate: ${rates.min():.2f}")
    logger.info(f"Max rate: ${rates.max():.2f}")
    logger.info(f"Mean rate: ${rates.mean():.2f}")
    logger.info(f"Median rate: ${rates.median():.2f}")
    
    # Check for common rate patterns
    logger.info(f"\nCommon rate patterns:")
    
    # Round to nearest dollar
    rounded_rates = rates.round(0)
    rounded_counts = rounded_rates.value_counts()
    logger.info(f"Top 10 rounded rates:")
    print(rounded_counts.head(10).to_string())
    
    # Check for multiples of common values
    logger.info(f"\nChecking for common rate multiples:")
    
    # Check multiples of $10
    multiples_10 = rates[rates % 10 == 0]
    logger.info(f"Rates that are multiples of $10: {len(multiples_10):,} ({len(multiples_10)/len(rates)*100:.1f}%)")
    
    # Check multiples of $5
    multiples_5 = rates[rates % 5 == 0]
    logger.info(f"Rates that are multiples of $5: {len(multiples_5):,} ({len(multiples_5)/len(rates)*100:.1f}%)")
    
    # Check multiples of $1
    multiples_1 = rates[rates % 1 == 0]
    logger.info(f"Rates that are whole dollars: {len(multiples_1):,} ({len(multiples_1)/len(rates)*100:.1f}%)")
    
    # Analyze by service code
    logger.info(f"\nRate bucketing by service code:")
    service_rate_analysis = df.groupby('service_code').agg({
        'negotiated_rate': ['count', 'nunique', 'mean', 'std']
    }).round(2)
    service_rate_analysis.columns = ['total_rates', 'unique_rates', 'mean_rate', 'std_rate']
    service_rate_analysis['coverage'] = (service_rate_analysis['unique_rates'] / service_rate_analysis['total_rates'] * 100).round(2)
    
    # Show service codes with lowest rate variety
    low_variety = service_rate_analysis.sort_values('coverage').head(10)
    logger.info(f"Service codes with lowest rate variety (most bucketed):")
    print(low_variety.to_string())
    
    # Show service codes with highest rate variety
    high_variety = service_rate_analysis.sort_values('coverage', ascending=False).head(10)
    logger.info(f"Service codes with highest rate variety (least bucketed):")
    print(high_variety.to_string())
    
    # Analyze by organization
    logger.info(f"\nRate bucketing by organization:")
    org_rate_analysis = df.groupby('organization_uuid').agg({
        'negotiated_rate': ['count', 'nunique', 'mean', 'std']
    }).round(2)
    org_rate_analysis.columns = ['total_rates', 'unique_rates', 'mean_rate', 'std_rate']
    org_rate_analysis['coverage'] = (org_rate_analysis['unique_rates'] / org_rate_analysis['total_rates'] * 100).round(2)
    
    # Show organizations with lowest rate variety
    org_low_variety = org_rate_analysis.sort_values('coverage').head(10)
    logger.info(f"Organizations with lowest rate variety (most bucketed):")
    print(org_low_variety.to_string())
    
    # Save analysis to CSV
    logger.info(f"\nSaving rate bucketing analysis to CSV...")
    
    # Save most common rates
    common_rates_csv = Path("dashboard_data/rate_bucketing_analysis.csv")
    rate_counts_df = rate_counts.reset_index()
    rate_counts_df.columns = ['rate', 'count']
    rate_counts_df['percentage'] = (rate_counts_df['count'] / len(rates) * 100).round(2)
    rate_counts_df.to_csv(common_rates_csv, index=False)
    logger.info(f"Rate bucketing analysis saved to: {common_rates_csv}")
    
    # Save service code analysis
    service_analysis_csv = Path("dashboard_data/service_rate_variety.csv")
    service_rate_analysis.to_csv(service_analysis_csv)
    logger.info(f"Service code rate variety saved to: {service_analysis_csv}")
    
    # Save organization analysis
    org_analysis_csv = Path("dashboard_data/organization_rate_variety.csv")
    org_rate_analysis.to_csv(org_analysis_csv)
    logger.info(f"Organization rate variety saved to: {org_analysis_csv}")
    
    # Summary
    logger.info(f"\nRate Bucketing Summary:")
    logger.info(f"- {exact_duplicates.sum():,} out of {len(rates):,} rates are duplicates")
    logger.info(f"- {len(exact_duplicates.sum())/len(rates)*100:.1f}% of rates are bucketed")
    logger.info(f"- Most common rate: ${rate_counts.index[0]:.2f} (appears {rate_counts.iloc[0]:,} times)")
    logger.info(f"- Top 10 rates account for {rate_counts.head(10).sum()/len(rates)*100:.1f}% of all rates")

if __name__ == "__main__":
    analyze_rate_bucketing() 