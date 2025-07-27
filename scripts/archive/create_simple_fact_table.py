#!/usr/bin/env python3
"""
Create a simple fact table with basic joins - no explosion or complex aggregation.
Just rates + organizations + providers + NPPES data joined together.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, timezone
import json

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimpleFactTableBuilder:
    """Build a simple fact table with basic joins - no explosion."""
    
    def __init__(self, test_mode=False, sample_size=1000, nppes_inner_join=False):
        self.rates_df = None
        self.providers_df = None
        self.organizations_df = None
        self.nppes_df = None
        self.output_path = Path("dashboard_data")
        self.output_path.mkdir(exist_ok=True)
        self.test_mode = test_mode
        self.sample_size = sample_size
        self.nppes_inner_join = nppes_inner_join
        
        if test_mode:
            logger.info(f"Running in TEST MODE with sample size: {sample_size:,}")
            self.output_path = Path("dashboard_data/test")
            self.output_path.mkdir(exist_ok=True)
        
        if nppes_inner_join:
            logger.info("Using INNER JOIN for NPPES data - only keeping records with NPPES enrichment")
        else:
            logger.info("Using LEFT JOIN for NPPES data - keeping all records")
    
    def load_data(self):
        """Load all source datasets."""
        logger.info("Loading datasets...")
        
        # Load rates data (this is our core fact table)
        rates_path = Path("ortho_radiology_data/rates/rates_final.parquet")
        if rates_path.exists():
            # Load with memory-efficient approach
            if self.test_mode:
                # For test mode, sample first then load
                logger.info("Loading rates in test mode with sampling...")
                # Read just the columns we need for sampling
                sample_df = pd.read_parquet(rates_path, columns=['rate_uuid', 'organization_uuid'])
                sample_indices = sample_df.sample(n=min(self.sample_size, len(sample_df)), random_state=42).index
                del sample_df  # Free memory
                
                # Now load only the sampled rows
                self.rates_df = pd.read_parquet(rates_path).iloc[sample_indices]
                logger.info(f"Sampled rates for test mode: {len(self.rates_df):,} records")
            else:
                # For full mode, load in chunks if needed
                logger.info("Loading rates in full mode...")
                try:
                    self.rates_df = pd.read_parquet(rates_path)
                except MemoryError:
                    logger.warning("Memory error loading rates, trying chunked approach...")
                    # Load in chunks and combine
                    chunk_size = 100000
                    chunks = []
                    for chunk in pd.read_parquet(rates_path, chunksize=chunk_size):
                        chunks.append(chunk)
                    self.rates_df = pd.concat(chunks, ignore_index=True)
                    del chunks  # Free memory
                
                logger.info(f"Loaded rates: {len(self.rates_df):,} records")
        else:
            logger.error(f"Rates file not found: {rates_path}")
            return False
        
        # Load organizations data
        organizations_path = Path("ortho_radiology_data/organizations/organizations_final.parquet")
        if organizations_path.exists():
            self.organizations_df = pd.read_parquet(organizations_path)
            logger.info(f"Loaded organizations: {len(self.organizations_df):,} records")
        else:
            logger.warning(f"Organizations file not found: {organizations_path}")
        
        # Load providers data
        providers_path = Path("ortho_radiology_data/providers/providers_final.parquet")
        if providers_path.exists():
            self.providers_df = pd.read_parquet(providers_path)
            logger.info(f"Loaded providers: {len(self.providers_df):,} records")
        else:
            logger.warning(f"Providers file not found: {providers_path}")
        
        # Load NPPES data
        nppes_path = Path("nppes_data/nppes_providers.parquet")
        if nppes_path.exists():
            self.nppes_df = pd.read_parquet(nppes_path)
            logger.info(f"Loaded NPPES: {len(self.nppes_df):,} records")
        else:
            logger.warning(f"NPPES file not found: {nppes_path}")
        
        return True
    
    def extract_npis_from_provider_network(self, provider_network_data):
        """Extract NPIs from provider_network.npi_list field."""
        if isinstance(provider_network_data, dict) and 'npi_list' in provider_network_data:
            npi_list = provider_network_data['npi_list']
            if isinstance(npi_list, (list, np.ndarray)):
                return list(npi_list)
        return []
    
    def extract_nppes_address_fields(self, addresses_data):
        """Extract address fields from NPPES addresses data."""
        if isinstance(addresses_data, list) and len(addresses_data) > 0:
            # Get the first address (primary location)
            addr = addresses_data[0]
            if isinstance(addr, dict):
                return {
                    'nppes_city': addr.get('city', ''),
                    'nppes_state': addr.get('state', ''),
                    'nppes_zip': addr.get('zip', ''),
                    'nppes_country': addr.get('country', ''),
                    'nppes_street': addr.get('street', ''),
                    'nppes_phone': addr.get('phone', ''),
                    'nppes_fax': addr.get('fax', ''),
                    'nppes_address_type': addr.get('type', ''),
                    'nppes_address_purpose': addr.get('purpose', '')
                }
        
        # Return empty values if no address data
        return {
            'nppes_city': '',
            'nppes_state': '',
            'nppes_zip': '',
            'nppes_country': '',
            'nppes_street': '',
            'nppes_phone': '',
            'nppes_fax': '',
            'nppes_address_type': '',
            'nppes_address_purpose': ''
        }
    
    def create_simple_fact_table(self):
        """Create the fact table with simple joins - no explosion."""
        logger.info("Creating simple fact table with basic joins...")
        
        if self.rates_df is None:
            logger.error("No rates data available")
            return pd.DataFrame()
        
        # Start with rates data (this is our core fact)
        fact_table = self.rates_df.copy()
        logger.info(f"Starting with {len(fact_table):,} rate records")
        
        # Extract NPIs from provider_network.npi_list
        logger.info("Extracting NPIs from provider_network.npi_list...")
        fact_table['rate_npis'] = fact_table['provider_network'].apply(self.extract_npis_from_provider_network)
        
        # Count NPIs per rate
        npi_counts = fact_table['rate_npis'].apply(len)
        logger.info(f"NPI distribution per rate:")
        logger.info(f"  - Min NPIs per rate: {npi_counts.min()}")
        logger.info(f"  - Max NPIs per rate: {npi_counts.max()}")
        logger.info(f"  - Mean NPIs per rate: {npi_counts.mean():.2f}")
        logger.info(f"  - Total NPIs across all rates: {npi_counts.sum():,}")
        
        # Show distribution
        npi_dist = npi_counts.value_counts().sort_index()
        logger.info(f"  - NPI count distribution:")
        for count, num_rates in npi_dist.head(10).items():
            logger.info(f"    {count} NPI(s): {num_rates:,} rates")
        
        # Show sample of rates with multiple NPIs
        multi_npi_rates = fact_table[npi_counts > 1].head(3)
        logger.info(f"  - Sample rates with multiple NPIs:")
        for _, row in multi_npi_rates.iterrows():
            logger.info(f"    Rate {row['rate_uuid'][:8]}...: {len(row['rate_npis'])} NPIs = {row['rate_npis']}")
        
        # Join with organizations
        if self.organizations_df is not None:
            logger.info("Joining with organizations...")
            fact_table = fact_table.merge(
                self.organizations_df,
                on='organization_uuid',
                how='left',
                suffixes=('', '_org')
            )
            logger.info(f"After organization join: {len(fact_table):,} records")
        
        # Explode rates by NPI to create one row per rate/NPI combination
        logger.info("Exploding rates by NPI to create one row per rate/NPI combination...")
        
        # Create exploded fact table
        exploded_rows = []
        for _, row in fact_table.iterrows():
            rate_npis = row['rate_npis']
            if rate_npis:  # Only process rows with NPIs
                for npi in rate_npis:
                    new_row = row.copy()
                    new_row['npi'] = npi  # Set the specific NPI for this row
                    exploded_rows.append(new_row)
            else:
                # Keep rows without NPIs as-is
                new_row = row.copy()
                new_row['npi'] = None
                exploded_rows.append(new_row)
        
        # Convert back to DataFrame
        fact_table = pd.DataFrame(exploded_rows)
        logger.info(f"After exploding by NPI: {len(fact_table):,} records")
        
        # Join with NPPES data using the exploded NPI
        if self.nppes_df is not None:
            logger.info("Joining with NPPES data using exploded NPIs...")
            
            # Prepare NPPES columns for joining
            nppes_join_cols = ['provider_type', 'primary_specialty', 'gender', 'addresses', 'credentials', 'provider_name', 'enumeration_date', 'last_updated', 'secondary_specialties', 'metadata']
            available_nppes_cols = [col for col in nppes_join_cols if col in self.nppes_df.columns]
            logger.info(f"Available NPPES columns for join: {available_nppes_cols}")
            
            nppes_join_df = self.nppes_df[['npi'] + available_nppes_cols].copy()
            
            # Rename NPPES columns to avoid conflicts
            rename_map = {
                'provider_type': 'nppes_provider_type',
                'primary_specialty': 'nppes_primary_specialty',
                'gender': 'nppes_gender',
                'addresses': 'nppes_addresses',
                'credentials': 'nppes_credentials',
                'provider_name': 'nppes_provider_name',
                'enumeration_date': 'nppes_enumeration_date',
                'last_updated': 'nppes_last_updated',
                'secondary_specialties': 'nppes_secondary_specialties',
                'metadata': 'nppes_metadata'
            }
            nppes_join_df = nppes_join_df.rename(columns=rename_map)
            
            # Join with fact table using the exploded NPI
            join_type = 'inner' if self.nppes_inner_join else 'left'
            fact_table = fact_table.merge(
                nppes_join_df,
                on='npi',
                how=join_type,
                suffixes=('', '_nppes')
            )
            
            if self.nppes_inner_join:
                logger.info(f"After NPPES inner join: {len(fact_table):,} records with NPPES data")
            else:
                logger.info(f"After NPPES left join: {len(fact_table):,} records")
            
            # Extract NPPES address fields into individual columns
            logger.info("Extracting NPPES address fields...")
            address_fields = fact_table['nppes_addresses'].apply(self.extract_nppes_address_fields)
            
            # Add extracted address fields to fact table
            for field_name in ['nppes_city', 'nppes_state', 'nppes_zip', 'nppes_country', 
                              'nppes_street', 'nppes_phone', 'nppes_fax', 
                              'nppes_address_type', 'nppes_address_purpose']:
                fact_table[field_name] = address_fields.apply(lambda x: x.get(field_name, ''))
            
            logger.info(f"Added {len(['nppes_city', 'nppes_state', 'nppes_zip', 'nppes_country', 
                              'nppes_street', 'nppes_phone', 'nppes_fax', 
                              'nppes_address_type', 'nppes_address_purpose'])} address fields")
        
        # Add simple derived columns
        logger.info("Adding derived columns...")
        
        # Rate categories
        fact_table['rate_category'] = pd.cut(
            fact_table['negotiated_rate'],
            bins=[0, 100, 500, 1000, 5000, 10000, float('inf')],
            labels=['$0-100', '$100-500', '$500-1K', '$1K-5K', '$5K-10K', '$10K+']
        )
        
        # Service code categories
        fact_table['service_category'] = fact_table['service_code'].apply(self.categorize_service_code)
        
        # Create unique identifier for each rate/NPI combination
        fact_table['fact_key'] = fact_table['rate_uuid'] + '_' + fact_table['npi'].astype(str)
        
        logger.info(f"Final fact table: {len(fact_table):,} records with {len(fact_table.columns)} columns")
        
        return fact_table
    
    def categorize_service_code(self, service_code):
        """Categorize service codes into meaningful groups."""
        if pd.isna(service_code):
            return 'Unknown'
        
        code = str(service_code)
        
        # CPT code categories
        if code.startswith('992'):  # E&M codes
            return 'Evaluation & Management'
        elif code.startswith('7'):  # Radiology
            return 'Radiology'
        elif code.startswith('2'):  # Surgery
            return 'Surgery'
        elif code.startswith('9'):  # Medicine
            return 'Medicine'
        elif code.startswith('0'):  # Anesthesia
            return 'Anesthesia'
        elif code.startswith('1'):  # Pathology/Lab
            return 'Pathology/Laboratory'
        elif code.startswith('3'):  # Radiology
            return 'Radiology'
        elif code.startswith('4'):  # Medicine
            return 'Medicine'
        elif code.startswith('5'):  # Medicine
            return 'Medicine'
        elif code.startswith('6'):  # Medicine
            return 'Medicine'
        else:
            return 'Other'
    
    def save_fact_table(self, fact_table):
        """Save the fact table to parquet."""
        if fact_table.empty:
            logger.error("No fact table data to save")
            return None
        
        # Save fact table
        output_file = self.output_path / "simple_fact_table.parquet"
        fact_table.to_parquet(output_file, index=False, compression='snappy')
        
        logger.info(f"Saved fact table to: {output_file}")
        logger.info(f"File size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")
        
        # Create summary statistics
        summary = {
            'total_records': len(fact_table),
            'unique_rates': fact_table['rate_uuid'].nunique(),
            'unique_service_codes': fact_table['service_code'].nunique(),
            'unique_organizations': fact_table['organization_uuid'].nunique(),
            'unique_providers': fact_table['npi'].nunique(),
            'rate_range': {
                'min': float(fact_table['negotiated_rate'].min()),
                'max': float(fact_table['negotiated_rate'].max()),
                'mean': float(fact_table['negotiated_rate'].mean()),
                'median': float(fact_table['negotiated_rate'].median())
            },
            'service_categories': fact_table['service_category'].value_counts().to_dict(),
            'rate_categories': fact_table['rate_category'].value_counts().to_dict(),
            'columns': list(fact_table.columns),
            'generated_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Save summary
        summary_file = self.output_path / "simple_fact_table_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Saved fact table summary to: {summary_file}")
        
        return output_file
    
    def run_fact_table_creation(self):
        """Run the complete fact table creation process."""
        logger.info("Starting simple fact table creation...")
        
        # Load data
        if not self.load_data():
            return None
        
        # Create fact table
        fact_table = self.create_simple_fact_table()
        
        if fact_table.empty:
            logger.error("Failed to create fact table")
            return None
        
        # Save fact table
        output_file = self.save_fact_table(fact_table)
        
        logger.info("Simple fact table creation completed successfully!")
        return output_file

def main():
    """Main function to create the simple fact table."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Create a simple fact table parquet file')
    parser.add_argument('--test', action='store_true', help='Run in test mode with small sample')
    parser.add_argument('--sample-size', type=int, default=1000, help='Sample size for test mode (default: 1000)')
    parser.add_argument('--nppes-inner-join', action='store_true', help='Use inner join for NPPES data (only keep records with NPPES enrichment)')
    
    args = parser.parse_args()
    
    if args.test:
        logger.info(f"Starting simple fact table creation in TEST MODE with sample size: {args.sample_size:,}")
        builder = SimpleFactTableBuilder(test_mode=True, sample_size=args.sample_size, nppes_inner_join=args.nppes_inner_join)
    else:
        logger.info("Starting simple fact table creation in FULL MODE")
        builder = SimpleFactTableBuilder(test_mode=False, nppes_inner_join=args.nppes_inner_join)
    
    output_file = builder.run_fact_table_creation()
    
    if output_file:
        logger.info(f"Simple fact table created successfully: {output_file}")
        if args.test:
            logger.info("Test mode completed - check dashboard_data/test/ for output files")
        else:
            logger.info("Full mode completed - check dashboard_data/ for output files")
    else:
        logger.error("Simple fact table creation failed")

if __name__ == "__main__":
    main() 