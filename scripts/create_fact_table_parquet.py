#!/usr/bin/env python3
"""
Create a low-level fact table parquet with individual rate records joined to related entities.
No aggregation - just individual records with all available context.
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

class FactTableBuilder:
    """Build a low-level fact table from rates, providers, organizations, and NPPES data."""
    
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
        
        # Load rates data
        rates_path = Path("ortho_radiology_data/rates/rates_final.parquet")
        if rates_path.exists():
            self.rates_df = pd.read_parquet(rates_path)
            logger.info(f"Loaded rates: {len(self.rates_df):,} records")
            
            # Apply sampling in test mode
            if self.test_mode:
                self.rates_df = self.rates_df.sample(n=min(self.sample_size, len(self.rates_df)), random_state=42)
                logger.info(f"Sampled rates for test mode: {len(self.rates_df):,} records")
        else:
            logger.error(f"Rates file not found: {rates_path}")
            return False
        
        # Load providers data
        providers_path = Path("ortho_radiology_data/providers/providers_final.parquet")
        if providers_path.exists():
            self.providers_df = pd.read_parquet(providers_path)
            logger.info(f"Loaded providers: {len(self.providers_df):,} records")
            
            # In test mode, only keep providers from organizations that have rates
            if self.test_mode and self.rates_df is not None:
                orgs_with_rates = self.rates_df['organization_uuid'].unique()
                self.providers_df = self.providers_df[self.providers_df['organization_uuid'].isin(orgs_with_rates)]
                logger.info(f"Filtered providers for test mode: {len(self.providers_df):,} records")
        else:
            logger.warning(f"Providers file not found: {providers_path}")
        
        # Load organizations data
        organizations_path = Path("ortho_radiology_data/organizations/organizations_final.parquet")
        if organizations_path.exists():
            self.organizations_df = pd.read_parquet(organizations_path)
            logger.info(f"Loaded organizations: {len(self.organizations_df):,} records")
            
            # In test mode, only keep organizations that have rates
            if self.test_mode and self.rates_df is not None:
                orgs_with_rates = self.rates_df['organization_uuid'].unique()
                self.organizations_df = self.organizations_df[self.organizations_df['organization_uuid'].isin(orgs_with_rates)]
                logger.info(f"Filtered organizations for test mode: {len(self.organizations_df):,} records")
        else:
            logger.warning(f"Organizations file not found: {organizations_path}")
        
        # Load NPPES data
        nppes_path = Path("nppes_data/nppes_providers.parquet")
        if nppes_path.exists():
            self.nppes_df = pd.read_parquet(nppes_path)
            logger.info(f"Loaded NPPES: {len(self.nppes_df):,} records")
            
            # In test mode, only keep NPPES data for providers we have
            if self.test_mode and self.providers_df is not None:
                npis_in_providers = self.providers_df['npi'].dropna().unique()
                self.nppes_df = self.nppes_df[self.nppes_df['npi'].isin(npis_in_providers)]
                logger.info(f"Filtered NPPES for test mode: {len(self.nppes_df):,} records")
        else:
            logger.warning(f"NPPES file not found: {nppes_path}")
        
        return True
    
    def extract_zip_codes(self, addresses_data) -> list:
        """Extract zip codes from addresses data."""
        zip_codes = []
        
        if isinstance(addresses_data, list):
            for addr in addresses_data:
                if isinstance(addr, dict) and 'zip' in addr:
                    zip_code = addr['zip']
                    if zip_code and str(zip_code).strip():
                        zip_codes.append(str(zip_code).strip())
        
        return zip_codes
    
    def extract_state_codes(self, addresses_data) -> list:
        """Extract state codes from addresses data."""
        state_codes = []
        
        if isinstance(addresses_data, list):
            for addr in addresses_data:
                if isinstance(addr, dict) and 'state' in addr:
                    state = addr['state']
                    if state and str(state).strip():
                        state_codes.append(str(state).strip())
        
        return state_codes
    
    def extract_city_codes(self, addresses_data) -> list:
        """Extract city codes from addresses data."""
        city_codes = []
        
        if isinstance(addresses_data, list):
            for addr in addresses_data:
                if isinstance(addr, dict) and 'city' in addr:
                    city = addr['city']
                    if city and str(city).strip():
                        city_codes.append(str(city).strip())
        
        return city_codes
    
    def extract_country_codes(self, addresses_data) -> list:
        """Extract country codes from addresses data."""
        country_codes = []
        
        if isinstance(addresses_data, list):
            for addr in addresses_data:
                if isinstance(addr, dict) and 'country' in addr:
                    country = addr['country']
                    if country and str(country).strip():
                        country_codes.append(str(country).strip())
        
        return country_codes
    
    def prepare_providers_with_location(self):
        """Prepare providers data with extracted location information."""
        if self.providers_df is None:
            return pd.DataFrame()
        
        logger.info("Preparing providers with location data...")
        providers_enhanced = self.providers_df.copy()
        
        # Extract location info from addresses
        providers_enhanced['provider_zip_codes'] = providers_enhanced['addresses'].apply(self.extract_zip_codes)
        providers_enhanced['provider_state_codes'] = providers_enhanced['addresses'].apply(self.extract_state_codes)
        providers_enhanced['provider_city_codes'] = providers_enhanced['addresses'].apply(self.extract_city_codes)
        providers_enhanced['provider_country_codes'] = providers_enhanced['addresses'].apply(self.extract_country_codes)
        
        # Create primary location fields (first address)
        providers_enhanced['provider_zip'] = providers_enhanced['provider_zip_codes'].apply(
            lambda x: x[0] if x else None
        )
        providers_enhanced['provider_state'] = providers_enhanced['provider_state_codes'].apply(
            lambda x: x[0] if x else None
        )
        providers_enhanced['provider_city'] = providers_enhanced['provider_city_codes'].apply(
            lambda x: x[0] if x else None
        )
        providers_enhanced['provider_country'] = providers_enhanced['provider_country_codes'].apply(
            lambda x: x[0] if x else None
        )
        
        # Join with NPPES data if available
        if self.nppes_df is not None:
            logger.info("Joining providers with NPPES data...")
            
            # Prepare NPPES columns for joining
            nppes_join_cols = ['provider_type', 'primary_specialty', 'gender', 'addresses', 'credentials']
            available_nppes_cols = [col for col in nppes_join_cols if col in self.nppes_df.columns]
            logger.info(f"Available NPPES columns for join: {available_nppes_cols}")
            
            nppes_join_df = self.nppes_df[['npi'] + available_nppes_cols].copy()
            logger.info(f"NPPES join dataframe columns: {list(nppes_join_df.columns)}")
            
            # Rename NPPES columns to avoid conflicts
            nppes_join_df = nppes_join_df.rename(columns={
                'provider_type': 'nppes_provider_type',
                'primary_specialty': 'nppes_primary_specialty',
                'gender': 'nppes_gender',
                'addresses': 'nppes_addresses',
                'credentials': 'nppes_credentials'
            })
            
            # Join with providers
            join_type = 'inner' if self.nppes_inner_join else 'left'
            providers_enhanced = providers_enhanced.merge(
                nppes_join_df,
                on='npi',
                how=join_type
            )
            
            if self.nppes_inner_join:
                logger.info(f"After NPPES inner join: {len(providers_enhanced):,} provider records with NPPES data")
            else:
                logger.info(f"After NPPES left join: {len(providers_enhanced):,} provider records")
            
            # Debug: show available columns after join
            nppes_cols = [col for col in providers_enhanced.columns if col.startswith('nppes_')]
            logger.info(f"Available NPPES columns after join: {nppes_cols}")
            
            # Extract NPPES location info
            providers_enhanced['nppes_zip_codes'] = providers_enhanced['nppes_addresses'].apply(self.extract_zip_codes)
            providers_enhanced['nppes_state_codes'] = providers_enhanced['nppes_addresses'].apply(self.extract_state_codes)
            providers_enhanced['nppes_city_codes'] = providers_enhanced['nppes_addresses'].apply(self.extract_city_codes)
            providers_enhanced['nppes_country_codes'] = providers_enhanced['nppes_addresses'].apply(self.extract_country_codes)
            
            # Create primary NPPES location fields
            providers_enhanced['nppes_zip'] = providers_enhanced['nppes_zip_codes'].apply(
                lambda x: x[0] if x else None
            )
            providers_enhanced['nppes_state'] = providers_enhanced['nppes_state_codes'].apply(
                lambda x: x[0] if x else None
            )
            providers_enhanced['nppes_city'] = providers_enhanced['nppes_city_codes'].apply(
                lambda x: x[0] if x else None
            )
            providers_enhanced['nppes_country'] = providers_enhanced['nppes_country_codes'].apply(
                lambda x: x[0] if x else None
            )
        
        return providers_enhanced
    
    def create_fact_table(self):
        """Create the low-level fact table with individual records exploded by NPI/specialty/zip."""
        logger.info("Creating exploded fact table...")
        
        if self.rates_df is None:
            logger.error("No rates data available")
            return pd.DataFrame()
        
        # Start with rates data
        fact_table = self.rates_df.copy()
        logger.info(f"Starting with {len(fact_table):,} rate records")
        
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
        
        # Prepare providers with location and explode by NPI/specialty/zip combinations
        if self.providers_df is not None:
            logger.info("Preparing providers and exploding by NPI/specialty/zip...")
            providers_enhanced = self.prepare_providers_with_location()
            
            # Create NPI/specialty/zip combinations for each provider
            provider_combinations = []
            
            for _, provider in providers_enhanced.iterrows():
                npi = provider['npi']
                organization_uuid = provider['organization_uuid']
                
                # Get provider specialties (both from provider and NPPES)
                provider_specialties = []
                if pd.notna(provider.get('primary_specialty')):
                    provider_specialties.append(provider['primary_specialty'])
                if pd.notna(provider.get('nppes_primary_specialty')):
                    provider_specialties.append(provider['nppes_primary_specialty'])
                
                # Remove duplicates and None values
                provider_specialties = list(set([s for s in provider_specialties if pd.notna(s)]))
                if not provider_specialties:
                    provider_specialties = ['Unknown']
                
                # Get provider zip codes (both from provider and NPPES)
                provider_zips = []
                if pd.notna(provider.get('provider_zip')):
                    provider_zips.append(provider['provider_zip'])
                if pd.notna(provider.get('nppes_zip')):
                    provider_zips.append(provider['nppes_zip'])
                
                # Remove duplicates and None values
                provider_zips = list(set([z for z in provider_zips if pd.notna(z)]))
                if not provider_zips:
                    provider_zips = ['Unknown']
                
                # Create combinations
                for specialty in provider_specialties:
                    for zip_code in provider_zips:
                        combination = {
                            'organization_uuid': organization_uuid,
                            'npi': npi,
                            'specialty': specialty,
                            'zip_code': zip_code,
                            'provider_type': provider.get('provider_type'),
                            'nppes_provider_type': provider.get('nppes_provider_type'),
                            'provider_city': provider.get('provider_city'),
                            'provider_state': provider.get('provider_state'),
                            'provider_country': provider.get('provider_country'),
                            'nppes_city': provider.get('nppes_city'),
                            'nppes_state': provider.get('nppes_state'),
                            'nppes_country': provider.get('nppes_country'),
                            'nppes_gender': provider.get('nppes_gender'),
                            'nppes_credentials': provider.get('nppes_credentials'),
                            'nppes_primary_specialty': provider.get('nppes_primary_specialty')
                        }
                        provider_combinations.append(combination)
            
            # Convert to DataFrame
            combinations_df = pd.DataFrame(provider_combinations)
            logger.info(f"Created {len(combinations_df):,} NPI/specialty/zip combinations")
            
            # Join rates with provider combinations
            fact_table = fact_table.merge(
                combinations_df,
                on='organization_uuid',
                how='left'
            )
            logger.info(f"After exploding by NPI/specialty/zip: {len(fact_table):,} records")
        
        # Add derived columns for analysis
        logger.info("Adding derived columns...")
        
        # Rate categories
        fact_table['rate_category'] = pd.cut(
            fact_table['negotiated_rate'],
            bins=[0, 100, 500, 1000, 5000, 10000, float('inf')],
            labels=['$0-100', '$100-500', '$500-1K', '$1K-5K', '$5K-10K', '$10K+']
        )
        
        # Service code categories
        fact_table['service_category'] = fact_table['service_code'].apply(self.categorize_service_code)
        
        # Location priority (use NPPES location if available, otherwise provider location)
        fact_table['primary_city'] = fact_table['nppes_city'].fillna(fact_table['provider_city'])
        fact_table['primary_state'] = fact_table['nppes_state'].fillna(fact_table['provider_state'])
        fact_table['primary_zip'] = fact_table['zip_code']  # Use the exploded zip_code
        fact_table['primary_country'] = fact_table['nppes_country'].fillna(fact_table['provider_country'])
        
        # Specialty priority (use exploded specialty, fallback to NPPES, then provider)
        fact_table['primary_specialty'] = fact_table['specialty']  # Use the exploded specialty
        fact_table['primary_provider_type'] = fact_table['nppes_provider_type'].fillna(fact_table['provider_type'])
        
        # Create unique identifier for each rate/NPI/specialty/zip combination
        fact_table['fact_key'] = fact_table['rate_uuid'] + '_' + fact_table['npi'].astype(str) + '_' + fact_table['specialty'] + '_' + fact_table['zip_code']
        
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
        output_file = self.output_path / "fact_table.parquet"
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
            'unique_zip_codes': fact_table['primary_zip'].nunique(),
            'unique_states': fact_table['primary_state'].nunique(),
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
        summary_file = self.output_path / "fact_table_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Saved fact table summary to: {summary_file}")
        
        return output_file
    
    def run_fact_table_creation(self):
        """Run the complete fact table creation process."""
        logger.info("Starting fact table creation...")
        
        # Load data
        if not self.load_data():
            return None
        
        # Create fact table
        fact_table = self.create_fact_table()
        
        if fact_table.empty:
            logger.error("Failed to create fact table")
            return None
        
        # Save fact table
        output_file = self.save_fact_table(fact_table)
        
        logger.info("Fact table creation completed successfully!")
        return output_file

def main():
    """Main function to create the fact table."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Create a low-level fact table parquet file')
    parser.add_argument('--test', action='store_true', help='Run in test mode with small sample')
    parser.add_argument('--sample-size', type=int, default=1000, help='Sample size for test mode (default: 1000)')
    parser.add_argument('--nppes-inner-join', action='store_true', help='Use inner join for NPPES data (only keep records with NPPES enrichment)')
    
    args = parser.parse_args()
    
    if args.test:
        logger.info(f"Starting fact table creation in TEST MODE with sample size: {args.sample_size:,}")
        builder = FactTableBuilder(test_mode=True, sample_size=args.sample_size, nppes_inner_join=args.nppes_inner_join)
    else:
        logger.info("Starting fact table creation in FULL MODE")
        builder = FactTableBuilder(test_mode=False, nppes_inner_join=args.nppes_inner_join)
    
    output_file = builder.run_fact_table_creation()
    
    if output_file:
        logger.info(f"Fact table created successfully: {output_file}")
        if args.test:
            logger.info("Test mode completed - check dashboard_data/test/ for output files")
        else:
            logger.info("Full mode completed - check dashboard_data/ for output files")
    else:
        logger.error("Fact table creation failed")

if __name__ == "__main__":
    main() 