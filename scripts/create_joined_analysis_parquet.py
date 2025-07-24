#!/usr/bin/env python3
"""Create comprehensive joined analysis parquet with key dimensions."""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JoinedAnalysisBuilder:
    """Build comprehensive joined analysis parquet with key dimensions."""
    
    def __init__(self):
        self.ortho_data_path = Path("ortho_radiology_data")
        self.nppes_data_path = Path("nppes_data")
        self.output_path = Path("dashboard_data")
        
        # Create output directory
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Data storage
        self.rates_df = None
        self.providers_df = None
        self.organizations_df = None
        self.nppes_df = None
        self.joined_df = None
        
    def load_data(self):
        """Load all required datasets."""
        logger.info("Loading datasets...")
        
        # Load rates data
        rates_file = self.ortho_data_path / "rates" / "rates_final.parquet"
        if rates_file.exists():
            self.rates_df = pd.read_parquet(rates_file)
            logger.info(f"Loaded rates: {len(self.rates_df):,} records")
        
        # Load providers data
        providers_file = self.ortho_data_path / "providers" / "providers_final.parquet"
        if providers_file.exists():
            self.providers_df = pd.read_parquet(providers_file)
            logger.info(f"Loaded providers: {len(self.providers_df):,} records")
        
        # Load organizations data
        organizations_file = self.ortho_data_path / "organizations" / "organizations_final.parquet"
        if organizations_file.exists():
            self.organizations_df = pd.read_parquet(organizations_file)
            logger.info(f"Loaded organizations: {len(self.organizations_df):,} records")
        
        # Load NPPES data
        nppes_file = self.nppes_data_path / "nppes_providers.parquet"
        if nppes_file.exists():
            self.nppes_df = pd.read_parquet(nppes_file)
            logger.info(f"Loaded NPPES: {len(self.nppes_df):,} records")
    
    def extract_zip_codes(self, addresses_data) -> List[str]:
        """Extract zip codes from addresses data."""
        zip_codes = []
        
        if isinstance(addresses_data, list):
            for addr in addresses_data:
                if isinstance(addr, dict) and 'zip' in addr:
                    zip_code = addr['zip']
                    if zip_code and str(zip_code).strip():
                        zip_codes.append(str(zip_code).strip())
        
        return zip_codes
    
    def extract_state_codes(self, addresses_data) -> List[str]:
        """Extract state codes from addresses data."""
        state_codes = []
        
        if isinstance(addresses_data, list):
            for addr in addresses_data:
                if isinstance(addr, dict) and 'state' in addr:
                    state = addr['state']
                    if state and str(state).strip():
                        state_codes.append(str(state).strip())
        
        return state_codes
    
    def prepare_providers_with_location(self) -> pd.DataFrame:
        """Prepare providers data with extracted location information."""
        logger.info("Preparing providers with location data...")
        
        if self.providers_df is None:
            return pd.DataFrame()
        
        # Create a copy for processing
        providers_enhanced = self.providers_df.copy()
        
        # Extract zip codes and states from addresses
        providers_enhanced['zip_codes'] = providers_enhanced['addresses'].apply(self.extract_zip_codes)
        providers_enhanced['state_codes'] = providers_enhanced['addresses'].apply(self.extract_state_codes)
        
        # Create primary location fields (first address)
        providers_enhanced['primary_zip'] = providers_enhanced['zip_codes'].apply(
            lambda x: x[0] if x else None
        )
        providers_enhanced['primary_state'] = providers_enhanced['state_codes'].apply(
            lambda x: x[0] if x else None
        )
        
        # Extract NPPES data if available
        if self.nppes_df is not None:
            # Prepare NPPES data for joining
            nppes_join_cols = ['provider_type', 'primary_specialty', 'gender', 'addresses', 'credentials']
            available_nppes_cols = [col for col in nppes_join_cols if col in self.nppes_df.columns]
            nppes_join_df = self.nppes_df[['npi'] + available_nppes_cols].copy()
            
            # Rename NPPES columns
            nppes_join_df = nppes_join_df.rename(columns={
                'provider_type': 'nppes_provider_type',
                'primary_specialty': 'nppes_primary_specialty',
                'gender': 'nppes_gender',
                'addresses': 'nppes_addresses',
                'credentials': 'nppes_credentials'
            })
            
            # Join with providers
            providers_enhanced = providers_enhanced.merge(
                nppes_join_df,
                on='npi',
                how='left'
            )
            
            # Extract NPPES location data
            providers_enhanced['nppes_zip_codes'] = providers_enhanced['nppes_addresses'].apply(self.extract_zip_codes)
            providers_enhanced['nppes_state_codes'] = providers_enhanced['nppes_addresses'].apply(self.extract_state_codes)
            providers_enhanced['nppes_primary_zip'] = providers_enhanced['nppes_zip_codes'].apply(
                lambda x: x[0] if x else None
            )
            providers_enhanced['nppes_primary_state'] = providers_enhanced['nppes_state_codes'].apply(
                lambda x: x[0] if x else None
            )
        
        return providers_enhanced
    
    def create_rates_with_provider_info(self) -> pd.DataFrame:
        """Create rates data joined with provider and organization information."""
        logger.info("Creating rates with provider and organization info...")
        
        if self.rates_df is None:
            return pd.DataFrame()
        
        # Start with rates data
        rates_enhanced = self.rates_df.copy()
        
        # Join with organizations
        if self.organizations_df is not None:
            rates_enhanced = rates_enhanced.merge(
                self.organizations_df,
                on='organization_uuid',
                how='left',
                suffixes=('', '_org')
            )
        
        # Join with providers (via organization_uuid)
        if self.providers_df is not None:
            # Prepare enhanced providers data with NPPES info
            providers_enhanced = self.prepare_providers_with_location()
            
            # Get provider info by organization
            provider_org_summary = providers_enhanced.groupby('organization_uuid').agg({
                'npi': 'count',
                'primary_specialty': lambda x: list(set([str(val) for val in x.dropna() if pd.notna(val)])),
                'provider_type': lambda x: list(set([str(val) for val in x.dropna() if pd.notna(val)])),
                'primary_zip': lambda x: list(set([str(val) for val in x.dropna() if pd.notna(val)])),
                'primary_state': lambda x: list(set([str(val) for val in x.dropna() if pd.notna(val)])),
                'nppes_primary_specialty': lambda x: list(set([str(val) for val in x.dropna() if pd.notna(val)])),
                'nppes_provider_type': lambda x: list(set([str(val) for val in x.dropna() if pd.notna(val)]))
            }).reset_index()
            
            provider_org_summary.columns = [
                'organization_uuid', 'provider_count', 'specialties', 'provider_types',
                'zip_codes', 'state_codes', 'nppes_specialties', 'nppes_provider_types'
            ]
            
            rates_enhanced = rates_enhanced.merge(
                provider_org_summary,
                on='organization_uuid',
                how='left'
            )
        
        return rates_enhanced
    
    def create_comprehensive_analysis_table(self) -> pd.DataFrame:
        """Create the comprehensive analysis table with all key dimensions."""
        logger.info("Creating comprehensive analysis table...")
        
        # Get enhanced rates data
        rates_enhanced = self.create_rates_with_provider_info()
        
        if rates_enhanced.empty:
            logger.error("No rates data available")
            return pd.DataFrame()
        
        # Create analysis records
        analysis_records = []
        
        for _, row in rates_enhanced.iterrows():
            record = {
                # Rate information
                'rate_uuid': row['rate_uuid'],
                'service_code': row['service_code'],
                'service_description': row['service_description'],
                'negotiated_rate': row['negotiated_rate'],
                'billing_code_type': row['billing_code_type'],
                'billing_class': row['billing_class'],
                'rate_type': row['rate_type'],
                
                # Organization information
                'organization_uuid': row['organization_uuid'],
                'organization_name': row.get('organization_name', ''),
                'organization_type': row.get('organization_type', ''),
                'tin': row.get('tin', ''),
                
                # Provider information (aggregated)
                'provider_count': row.get('provider_count', 0),
                'specialties': row.get('specialties', []),
                'provider_types': row.get('provider_types', []),
                'zip_codes': row.get('zip_codes', []),
                'state_codes': row.get('state_codes', []),
                'nppes_specialties': row.get('nppes_specialties', []),
                'nppes_provider_types': row.get('nppes_provider_types', []),
                
                # Geographic information
                'primary_zip': row.get('zip_codes', [None])[0] if isinstance(row.get('zip_codes'), list) and row.get('zip_codes') else None,
                'primary_state': row.get('state_codes', [None])[0] if isinstance(row.get('state_codes'), list) and row.get('state_codes') else None,
                
                # Specialty information
                'primary_specialty': row.get('specialties', [None])[0] if isinstance(row.get('specialties'), list) and row.get('specialties') else None,
                'primary_nppes_specialty': row.get('nppes_specialties', [None])[0] if isinstance(row.get('nppes_specialties'), list) and row.get('nppes_specialties') else None,
                
                # Metadata
                'created_at': row.get('created_at'),
                'updated_at': row.get('updated_at')
            }
            
            analysis_records.append(record)
        
        # Convert to DataFrame
        analysis_df = pd.DataFrame(analysis_records)
        
        # Add derived columns
        analysis_df['rate_category'] = pd.cut(
            analysis_df['negotiated_rate'],
            bins=[0, 100, 500, 1000, 5000, 10000, float('inf')],
            labels=['$0-100', '$100-500', '$500-1K', '$1K-5K', '$5K-10K', '$10K+']
        )
        
        # Add service code categories
        analysis_df['service_category'] = analysis_df['service_code'].apply(self.categorize_service_code)
        
        logger.info(f"Created analysis table with {len(analysis_df):,} records")
        
        return analysis_df
    
    def categorize_service_code(self, service_code: str) -> str:
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
    
    def save_analysis_table(self, analysis_df: pd.DataFrame):
        """Save the analysis table to parquet."""
        if analysis_df.empty:
            logger.error("No analysis data to save")
            return
        
        # Save comprehensive analysis table
        output_file = self.output_path / "comprehensive_analysis.parquet"
        analysis_df.to_parquet(output_file, index=False, compression='snappy')
        
        logger.info(f"Saved comprehensive analysis to: {output_file}")
        logger.info(f"File size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")
        
        # Create summary statistics
        summary = {
            'total_records': len(analysis_df),
            'unique_service_codes': analysis_df['service_code'].nunique(),
            'unique_organizations': analysis_df['organization_uuid'].nunique(),
            'unique_zip_codes': analysis_df['primary_zip'].nunique(),
            'unique_states': analysis_df['primary_state'].nunique(),
            'rate_range': {
                'min': float(analysis_df['negotiated_rate'].min()),
                'max': float(analysis_df['negotiated_rate'].max()),
                'mean': float(analysis_df['negotiated_rate'].mean()),
                'median': float(analysis_df['negotiated_rate'].median())
            },
            'service_categories': analysis_df['service_category'].value_counts().to_dict(),
            'rate_categories': analysis_df['rate_category'].value_counts().to_dict(),
            'generated_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Save summary
        summary_file = self.output_path / "analysis_summary.json"
        import json
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Saved analysis summary to: {summary_file}")
        
        return output_file
    
    def run_full_analysis(self):
        """Run the complete analysis and create joined parquet."""
        logger.info("Starting comprehensive analysis table creation...")
        
        # Load data
        self.load_data()
        
        # Create comprehensive analysis table
        analysis_df = self.create_comprehensive_analysis_table()
        
        # Save results
        output_file = self.save_analysis_table(analysis_df)
        
        logger.info("Comprehensive analysis table creation completed!")
        
        return {
            'output_file': str(output_file),
            'record_count': len(analysis_df),
            'summary_file': str(self.output_path / "analysis_summary.json")
        }

def main():
    """Main entry point."""
    builder = JoinedAnalysisBuilder()
    results = builder.run_full_analysis()
    
    print("\n" + "="*60)
    print("COMPREHENSIVE ANALYSIS TABLE CREATED")
    print("="*60)
    
    print(f"\n📊 RESULTS:")
    print(f"   Output File: {results['output_file']}")
    print(f"   Record Count: {results['record_count']:,}")
    print(f"   Summary File: {results['summary_file']}")
    
    print(f"\n🎯 KEY DIMENSIONS INCLUDED:")
    print(f"   • Service Code & Description")
    print(f"   • Negotiated Rate & Rate Categories")
    print(f"   • Organization Information")
    print(f"   • Provider Counts & Specialties")
    print(f"   • Geographic Data (Zip Codes, States)")
    print(f"   • NPPES Enrichment Data")
    print(f"   • Service Code Categories")
    
    print(f"\n📈 DASHBOARD READY:")
    print(f"   • Filter by specialty, zip code, service code")
    print(f"   • Analyze rate distributions")
    print(f"   • Geographic analysis")
    print(f"   • Organization comparisons")

if __name__ == "__main__":
    main() 