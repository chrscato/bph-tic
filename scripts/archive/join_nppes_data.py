#!/usr/bin/env python3
"""Utility script to join NPPES data with existing provider data."""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NPPESJoiner:
    """Utility class for joining NPPES data with provider data."""
    
    def __init__(self, nppes_data_dir: str = "nppes_data"):
        self.nppes_data_dir = Path(nppes_data_dir)
        self.nppes_file = self.nppes_data_dir / "nppes_providers.parquet"
        
        if not self.nppes_file.exists():
            raise FileNotFoundError(f"NPPES data file not found: {self.nppes_file}")
        
        # Load NPPES data
        self.nppes_df = pd.read_parquet(self.nppes_file)
        logger.info(f"Loaded NPPES data: {len(self.nppes_df)} providers")
    
    def join_with_provider_data(self, provider_df: pd.DataFrame, 
                               join_columns: Optional[list] = None,
                               enrich_columns: Optional[list] = None) -> pd.DataFrame:
        """
        Join NPPES data with provider data.
        
        Args:
            provider_df: DataFrame containing provider data with NPI column
            join_columns: List of NPPES columns to include in the join (None = all)
            enrich_columns: List of specific columns to add (None = all available)
        
        Returns:
            DataFrame with provider data enriched with NPPES information
        """
        if 'npi' not in provider_df.columns:
            raise ValueError("Provider DataFrame must contain 'npi' column")
        
        # Prepare NPPES data for joining
        if join_columns:
            nppes_join_df = self.nppes_df[['npi'] + join_columns].copy()
        else:
            nppes_join_df = self.nppes_df.copy()
        
        # Perform the join
        enriched_df = provider_df.merge(
            nppes_join_df,
            on='npi',
            how='left',
            suffixes=('', '_nppes')
        )
        
        # Filter columns if requested
        if enrich_columns:
            # Keep all original columns plus specified NPPES columns
            original_columns = [col for col in provider_df.columns if not col.endswith('_nppes')]
            nppes_columns = [col for col in enrich_columns if col in nppes_join_df.columns]
            final_columns = original_columns + nppes_columns
            enriched_df = enriched_df[final_columns]
        
        # Calculate join statistics
        matched_count = enriched_df['provider_type'].notna().sum()
        match_rate = matched_count / len(enriched_df) * 100
        
        logger.info(f"Join completed: {matched_count:,}/{len(enriched_df):,} providers matched ({match_rate:.1f}%)")
        
        return enriched_df
    
    def get_available_columns(self) -> list:
        """Get list of available NPPES columns for joining."""
        return list(self.nppes_df.columns)
    
    def get_nppes_stats(self) -> Dict[str, Any]:
        """Get statistics about the NPPES dataset."""
        stats = {
            'total_providers': len(self.nppes_df),
            'individual_providers': len(self.nppes_df[self.nppes_df['provider_type'] == 'Individual']),
            'organization_providers': len(self.nppes_df[self.nppes_df['provider_type'] == 'Organization']),
            'providers_with_addresses': self.nppes_df['addresses'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum(),
            'providers_with_specialties': self.nppes_df['primary_specialty'].apply(lambda x: isinstance(x, str) and bool(x.strip())).sum(),
            'providers_with_credentials': self.nppes_df['credentials'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum(),
            'unique_states': len(set([addr.get('state') for addresses in self.nppes_df['addresses'] if isinstance(addresses, list) for addr in addresses if isinstance(addr, dict) and addr.get('state')])),
            'unique_primary_specialties': self.nppes_df['primary_specialty'].apply(lambda x: x if isinstance(x, str) and x.strip() else None).dropna().nunique()
        }
        return stats

def main():
    """Example usage of the NPPES joiner."""
    try:
        # Initialize joiner
        joiner = NPPESJoiner()
        
        # Show available columns
        print("Available NPPES columns:")
        print(joiner.get_available_columns())
        
        # Show NPPES statistics
        print("\nNPPES Dataset Statistics:")
        stats = joiner.get_nppes_stats()
        for key, value in stats.items():
            print(f"  {key}: {value:,}")
        
        # Example: Load provider data and join
        provider_file = Path("production_data/providers/providers_final.parquet")
        if provider_file.exists():
            print(f"\nLoading provider data from {provider_file}")
            provider_df = pd.read_parquet(provider_file)
            
            # Join with key NPPES columns
            key_columns = ['provider_type', 'primary_specialty', 'gender', 'addresses']
            enriched_df = joiner.join_with_provider_data(
                provider_df, 
                join_columns=key_columns
            )
            
            print(f"Enriched provider data shape: {enriched_df.shape}")
            print(f"Sample of enriched data:")
            print(enriched_df[['npi', 'provider_type', 'primary_specialty']].head())
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 