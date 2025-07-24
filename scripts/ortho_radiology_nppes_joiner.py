#!/usr/bin/env python3
"""Orthopedic Radiology Data Joiner with NPPES Integration and Dashboard Summary."""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
import logging
from dataclasses import dataclass, asdict
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class JoinerConfig:
    """Configuration for the ortho radiology data joiner."""
    # Input paths
    ortho_data_dir: str = "ortho_radiology_data"
    nppes_data_dir: str = "nppes_data"
    
    # Output paths
    output_dir: str = "dashboard_data"
    summary_file: str = "dashboard_summary.json"
    
    # Processing options
    include_analytics: bool = True
    include_geographic_analysis: bool = True
    include_specialty_analysis: bool = True
    include_rate_analysis: bool = True
    
    # Quality thresholds
    min_rate_threshold: float = 0.01
    max_rate_threshold: float = 100000.00
    min_provider_count: int = 10

class OrthoRadiologyNPPESJoiner:
    """Joins ortho radiology data with NPPES data and generates dashboard summaries."""
    
    def __init__(self, config: JoinerConfig):
        self.config = config
        self.ortho_data_path = Path(config.ortho_data_dir)
        self.nppes_data_path = Path(config.nppes_data_dir)
        self.output_path = Path(config.output_dir)
        
        # Create output directory
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Data storage
        self.rates_df = None
        self.providers_df = None
        self.organizations_df = None
        self.payers_df = None
        self.analytics_df = None
        self.nppes_df = None
        self.joined_data = None
        
        # Summary statistics
        self.summary_stats = {}
        
    def load_ortho_radiology_data(self) -> Dict[str, pd.DataFrame]:
        """Load all ortho radiology data files."""
        logger.info("Loading ortho radiology data...")
        
        data_files = {
            'rates': self.ortho_data_path / "rates" / "rates_final.parquet",
            'providers': self.ortho_data_path / "providers" / "providers_final.parquet",
            'organizations': self.ortho_data_path / "organizations" / "organizations_final.parquet",
            'payers': self.ortho_data_path / "payers" / "payers_final.parquet",
            'analytics': self.ortho_data_path / "analytics" / "analytics_final.parquet"
        }
        
        loaded_data = {}
        
        for data_type, file_path in data_files.items():
            if file_path.exists():
                logger.info(f"Loading {data_type} data from {file_path}")
                df = pd.read_parquet(file_path)
                loaded_data[data_type] = df
                setattr(self, f"{data_type}_df", df)
                logger.info(f"Loaded {len(df):,} {data_type} records")
            else:
                logger.warning(f"File not found: {file_path}")
                loaded_data[data_type] = pd.DataFrame()
        
        return loaded_data
    
    def load_nppes_data(self) -> pd.DataFrame:
        """Load NPPES provider data."""
        logger.info("Loading NPPES data...")
        
        nppes_file = self.nppes_data_path / "nppes_providers.parquet"
        if nppes_file.exists():
            self.nppes_df = pd.read_parquet(nppes_file)
            logger.info(f"Loaded {len(self.nppes_df):,} NPPES provider records")
            return self.nppes_df
        else:
            logger.warning(f"NPPES file not found: {nppes_file}")
            self.nppes_df = pd.DataFrame()
            return self.nppes_df
    
    def join_data(self) -> pd.DataFrame:
        """Join ortho radiology data with NPPES data."""
        logger.info("Joining ortho radiology data with NPPES data...")
        
        if self.providers_df is None or self.providers_df.empty:
            logger.error("No provider data available for joining")
            return pd.DataFrame()
        
        if self.nppes_df is None or self.nppes_df.empty:
            logger.warning("No NPPES data available, using provider data as-is")
            self.joined_data = self.providers_df.copy()
            return self.joined_data
        
        # Check for duplicate NPIs and handle them
        logger.info("Checking for duplicate NPIs...")
        
        # Check providers_df for duplicates
        if 'npi' in self.providers_df.columns:
            provider_duplicates = self.providers_df['npi'].duplicated().sum()
            if provider_duplicates > 0:
                logger.warning(f"Found {provider_duplicates} duplicate NPIs in providers data, keeping first occurrence")
                self.providers_df = self.providers_df.drop_duplicates(subset=['npi'], keep='first')
        
        # Check nppes_df for duplicates
        if 'npi' in self.nppes_df.columns:
            nppes_duplicates = self.nppes_df['npi'].duplicated().sum()
            if nppes_duplicates > 0:
                logger.warning(f"Found {nppes_duplicates} duplicate NPIs in NPPES data, keeping first occurrence")
                self.nppes_df = self.nppes_df.drop_duplicates(subset=['npi'], keep='first')
        
        # Prepare NPPES data for joining - be more careful about column selection
        nppes_join_cols = ['provider_type', 'primary_specialty', 'gender', 'addresses', 'credentials']
        available_nppes_cols = [col for col in nppes_join_cols if col in self.nppes_df.columns]
        
        # Create a clean NPPES DataFrame with only the columns we need
        nppes_join_df = self.nppes_df[['npi'] + available_nppes_cols].copy()
        
        # Rename NPPES columns to avoid conflicts
        nppes_join_df = nppes_join_df.rename(columns={
            'provider_type': 'nppes_provider_type',
            'primary_specialty': 'nppes_primary_specialty',
            'gender': 'nppes_gender',
            'addresses': 'nppes_addresses',
            'credentials': 'nppes_credentials'
        })
        
        logger.info(f"Joining {len(self.providers_df):,} providers with {len(nppes_join_df):,} NPPES records")
        logger.info(f"NPPES columns: {list(nppes_join_df.columns)}")
        
        # Join providers with NPPES data
        self.joined_data = self.providers_df.merge(
            nppes_join_df,
            on='npi',
            how='left'
        )
        
        # Calculate join statistics
        matched_count = self.joined_data['nppes_provider_type'].notna().sum()
        match_rate = matched_count / len(self.joined_data) * 100
        
        logger.info(f"Join completed: {matched_count:,}/{len(self.joined_data):,} providers matched ({match_rate:.1f}%)")
        
        return self.joined_data
    
    def generate_basic_statistics(self) -> Dict[str, Any]:
        """Generate basic statistics for all datasets."""
        logger.info("Generating basic statistics...")
        
        stats = {
            'data_load_timestamp': datetime.now(timezone.utc).isoformat(),
            'datasets': {},
            'join_statistics': {},
            'data_quality': {}
        }
        
        # Dataset statistics
        for dataset_name in ['rates', 'providers', 'organizations', 'payers', 'analytics']:
            df = getattr(self, f"{dataset_name}_df")
            if df is not None and not df.empty:
                stats['datasets'][dataset_name] = {
                    'record_count': len(df),
                    'columns': list(df.columns),
                    'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
                    'null_counts': df.isnull().sum().to_dict()
                }
        
        # Join statistics
        if self.joined_data is not None and not self.joined_data.empty:
            stats['join_statistics'] = {
                'total_providers': len(self.joined_data),
                'nppes_matched': self.joined_data['nppes_provider_type'].notna().sum(),
                'match_rate_percent': (self.joined_data['nppes_provider_type'].notna().sum() / len(self.joined_data)) * 100,
                'providers_with_specialties': self.joined_data['nppes_primary_specialty'].notna().sum(),
                'providers_with_addresses': self.joined_data['nppes_addresses'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum()
            }
        
        # Data quality metrics
        if self.rates_df is not None and not self.rates_df.empty:
            rates = self.rates_df['negotiated_rate'].dropna()
            stats['data_quality']['rates'] = {
                'total_rates': len(rates),
                'zero_rates': (rates == 0).sum(),
                'negative_rates': (rates < 0).sum(),
                'outlier_rates': ((rates < self.config.min_rate_threshold) | (rates > self.config.max_rate_threshold)).sum(),
                'rate_statistics': {
                    'mean': float(rates.mean()),
                    'median': float(rates.median()),
                    'std': float(rates.std()),
                    'min': float(rates.min()),
                    'max': float(rates.max()),
                    'percentiles': {
                        'p10': float(rates.quantile(0.10)),
                        'p25': float(rates.quantile(0.25)),
                        'p75': float(rates.quantile(0.75)),
                        'p90': float(rates.quantile(0.90)),
                        'p95': float(rates.quantile(0.95))
                    }
                }
            }
        
        self.summary_stats['basic'] = stats
        return stats
    
    def generate_specialty_analysis(self) -> Dict[str, Any]:
        """Generate specialty-focused analysis."""
        logger.info("Generating specialty analysis...")
        
        if self.joined_data is None or self.joined_data.empty:
            return {}
        
        specialty_stats = {
            'specialty_distribution': {},
            'top_specialties': [],
            'specialty_rate_analysis': {},
            'orthopedic_focus': {}
        }
        
        # Specialty distribution
        if 'nppes_primary_specialty' in self.joined_data.columns:
            specialty_counts = self.joined_data['nppes_primary_specialty'].value_counts()
            specialty_stats['specialty_distribution'] = specialty_counts.head(20).to_dict()
            specialty_stats['top_specialties'] = specialty_counts.head(10).index.tolist()
        
        # Specialty rate analysis
        if self.rates_df is not None and not self.rates_df.empty and self.joined_data is not None:
            # Check what columns are available for joining
            logger.info(f"Rates columns: {list(self.rates_df.columns)}")
            logger.info(f"Joined data columns: {list(self.joined_data.columns)}")
            
            # Find the correct join column - could be 'provider_uuid', 'npi', or something else
            possible_join_cols = ['provider_uuid', 'npi', 'provider_id']
            join_col = None
            for col in possible_join_cols:
                if col in self.rates_df.columns and col in self.joined_data.columns:
                    join_col = col
                    break
            
            if join_col:
                logger.info(f"Using '{join_col}' for joining rates with specialties")
                # Join rates with provider specialties
                rates_with_specialty = self.rates_df.merge(
                    self.joined_data[[join_col, 'nppes_primary_specialty']].dropna(),
                    on=join_col,
                    how='inner'
                )
                
                if not rates_with_specialty.empty:
                    specialty_rates = rates_with_specialty.groupby('nppes_primary_specialty')['negotiated_rate'].agg([
                        'count', 'mean', 'median', 'std', 'min', 'max'
                    ]).round(2)
                    
                    specialty_stats['specialty_rate_analysis'] = specialty_rates.to_dict('index')
                else:
                    logger.warning("No rates found after joining with specialties")
            else:
                logger.warning("No suitable join column found between rates and provider data")
                specialty_stats['specialty_rate_analysis'] = {}
        
        # Orthopedic focus analysis
        orthopedic_keywords = ['orthopedic', 'orthopaedic', 'sports', 'spine', 'joint', 'musculoskeletal']
        if 'nppes_primary_specialty' in self.joined_data.columns:
            orthopedic_mask = self.joined_data['nppes_primary_specialty'].str.contains(
                '|'.join(orthopedic_keywords), case=False, na=False
            )
            orthopedic_providers = self.joined_data[orthopedic_mask]
            
            specialty_stats['orthopedic_focus'] = {
                'orthopedic_provider_count': len(orthopedic_providers),
                'orthopedic_percentage': (len(orthopedic_providers) / len(self.joined_data)) * 100,
                'orthopedic_specialties': orthopedic_providers['nppes_primary_specialty'].value_counts().to_dict()
            }
        
        self.summary_stats['specialty'] = specialty_stats
        return specialty_stats
    
    def generate_geographic_analysis(self) -> Dict[str, Any]:
        """Generate geographic analysis."""
        logger.info("Generating geographic analysis...")
        
        if self.joined_data is None or self.joined_data.empty:
            return {}
        
        geo_stats = {
            'state_distribution': {},
            'geographic_coverage': {},
            'regional_analysis': {}
        }
        
        # Extract state information from addresses
        if 'nppes_addresses' in self.joined_data.columns:
            states = []
            for addresses in self.joined_data['nppes_addresses']:
                if isinstance(addresses, list) and len(addresses) > 0:
                    for addr in addresses:
                        if isinstance(addr, dict) and 'state' in addr:
                            states.append(addr['state'])
            
            if states:
                state_counts = pd.Series(states).value_counts()
                geo_stats['state_distribution'] = state_counts.to_dict()
                geo_stats['geographic_coverage'] = {
                    'total_states': len(state_counts),
                    'top_states': state_counts.head(10).to_dict(),
                    'providers_per_state_avg': len(states) / len(state_counts)
                }
        
        # Regional analysis (if rates data available)
        if self.rates_df is not None and not self.rates_df.empty:
            # This would require more complex geographic joining
            geo_stats['regional_analysis'] = {
                'note': 'Regional rate analysis requires additional geographic data processing'
            }
        
        self.summary_stats['geographic'] = geo_stats
        return geo_stats
    
    def generate_rate_analysis(self) -> Dict[str, Any]:
        """Generate comprehensive rate analysis."""
        logger.info("Generating rate analysis...")
        
        if self.rates_df is None or self.rates_df.empty:
            return {}
        
        rate_stats = {
            'overall_rate_statistics': {},
            'service_code_analysis': {},
            'payer_analysis': {},
            'rate_distribution': {},
            'outlier_analysis': {}
        }
        
        # Overall rate statistics
        rates = self.rates_df['negotiated_rate'].dropna()
        rate_stats['overall_rate_statistics'] = {
            'total_rates': len(rates),
            'mean_rate': float(rates.mean()),
            'median_rate': float(rates.median()),
            'std_dev': float(rates.std()),
            'rate_range': {
                'min': float(rates.min()),
                'max': float(rates.max())
            },
            'percentiles': {
                'p10': float(rates.quantile(0.10)),
                'p25': float(rates.quantile(0.25)),
                'p75': float(rates.quantile(0.75)),
                'p90': float(rates.quantile(0.90)),
                'p95': float(rates.quantile(0.95))
            }
        }
        
        # Service code analysis
        if 'service_code' in self.rates_df.columns:
            service_code_stats = self.rates_df.groupby('service_code')['negotiated_rate'].agg([
                'count', 'mean', 'median', 'std', 'min', 'max'
            ]).round(2)
            
            rate_stats['service_code_analysis'] = {
                'total_service_codes': len(service_code_stats),
                'top_service_codes': service_code_stats.nlargest(20, 'count').to_dict('index'),
                'high_cost_procedures': service_code_stats.nlargest(10, 'mean').to_dict('index')
            }
        
        # Payer analysis
        if 'payer_uuid' in self.rates_df.columns:
            payer_stats = self.rates_df.groupby('payer_uuid')['negotiated_rate'].agg([
                'count', 'mean', 'median', 'std'
            ]).round(2)
            
            rate_stats['payer_analysis'] = {
                'total_payers': len(payer_stats),
                'payer_statistics': payer_stats.to_dict('index')
            }
        
        # Rate distribution analysis
        rate_bins = pd.cut(rates, bins=10)
        # Convert interval objects to strings for JSON serialization
        rate_distribution = {}
        for interval, count in rate_bins.value_counts().items():
            rate_distribution[str(interval)] = int(count)
        rate_stats['rate_distribution'] = rate_distribution
        
        # Outlier analysis
        q1 = rates.quantile(0.25)
        q3 = rates.quantile(0.75)
        iqr = q3 - q1
        outlier_mask = (rates < (q1 - 1.5 * iqr)) | (rates > (q3 + 1.5 * iqr))
        
        rate_stats['outlier_analysis'] = {
            'outlier_count': outlier_mask.sum(),
            'outlier_percentage': (outlier_mask.sum() / len(rates)) * 100,
            'iqr_range': {
                'q1': float(q1),
                'q3': float(q3),
                'iqr': float(iqr)
            }
        }
        
        self.summary_stats['rates'] = rate_stats
        return rate_stats
    
    def generate_dashboard_summary(self) -> Dict[str, Any]:
        """Generate comprehensive dashboard summary."""
        logger.info("Generating dashboard summary...")
        
        # Generate all analyses
        basic_stats = self.generate_basic_statistics()
        specialty_stats = self.generate_specialty_analysis()
        geo_stats = self.generate_geographic_analysis()
        rate_stats = self.generate_rate_analysis()
        
        # Combine all statistics
        dashboard_summary = {
            'metadata': {
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'data_sources': {
                    'ortho_radiology_data': str(self.ortho_data_path),
                    'nppes_data': str(self.nppes_data_path)
                },
                'processing_config': asdict(self.config)
            },
            'summary': {
                'total_providers': len(self.joined_data) if self.joined_data is not None else 0,
                'total_rates': len(self.rates_df) if self.rates_df is not None else 0,
                'total_organizations': len(self.organizations_df) if self.organizations_df is not None else 0,
                'total_payers': len(self.payers_df) if self.payers_df is not None else 0,
                'nppes_match_rate': basic_stats.get('join_statistics', {}).get('match_rate_percent', 0)
            },
            'analyses': {
                'basic': basic_stats,
                'specialty': specialty_stats,
                'geographic': geo_stats,
                'rates': rate_stats
            }
        }
        
        # Save summary to file
        summary_file = self.output_path / self.config.summary_file
        with open(summary_file, 'w') as f:
            json.dump(dashboard_summary, f, indent=2, default=str)
        
        logger.info(f"Dashboard summary saved to: {summary_file}")
        
        return dashboard_summary
    
    def create_visualization_data(self) -> Dict[str, Any]:
        """Create data structures optimized for dashboard visualizations."""
        logger.info("Creating visualization data...")
        
        viz_data = {
            'charts': {},
            'tables': {},
            'metrics': {}
        }
        
        # Key metrics
        viz_data['metrics'] = {
            'total_providers': len(self.joined_data) if self.joined_data is not None else 0,
            'total_rates': len(self.rates_df) if self.rates_df is not None else 0,
            'nppes_coverage': self.summary_stats.get('basic', {}).get('join_statistics', {}).get('match_rate_percent', 0),
            'avg_rate': self.summary_stats.get('rates', {}).get('overall_rate_statistics', {}).get('mean_rate', 0)
        }
        
        # Chart data
        if self.summary_stats.get('specialty', {}).get('specialty_distribution'):
            viz_data['charts']['specialty_distribution'] = {
                'type': 'bar',
                'data': self.summary_stats['specialty']['specialty_distribution']
            }
        
        if self.summary_stats.get('geographic', {}).get('state_distribution'):
            viz_data['charts']['state_distribution'] = {
                'type': 'bar',
                'data': self.summary_stats['geographic']['state_distribution']
            }
        
        if self.summary_stats.get('rates', {}).get('rate_distribution'):
            viz_data['charts']['rate_distribution'] = {
                'type': 'histogram',
                'data': self.summary_stats['rates']['rate_distribution']
            }
        
        # Table data
        if self.summary_stats.get('rates', {}).get('service_code_analysis', {}).get('top_service_codes'):
            viz_data['tables']['top_service_codes'] = self.summary_stats['rates']['service_code_analysis']['top_service_codes']
        
        # Save visualization data
        viz_file = self.output_path / "visualization_data.json"
        with open(viz_file, 'w') as f:
            json.dump(viz_data, f, indent=2, default=str)
        
        logger.info(f"Visualization data saved to: {viz_file}")
        
        return viz_data
    
    def run_full_analysis(self) -> Dict[str, Any]:
        """Run the complete analysis pipeline."""
        logger.info("Starting full ortho radiology data analysis...")
        
        # Load data
        self.load_ortho_radiology_data()
        self.load_nppes_data()
        
        # Join data
        self.join_data()
        
        # Generate summaries
        dashboard_summary = self.generate_dashboard_summary()
        viz_data = self.create_visualization_data()
        
        logger.info("Analysis completed successfully!")
        
        return {
            'summary': dashboard_summary,
            'visualization_data': viz_data,
            'output_directory': str(self.output_path)
        }

def main():
    """Main entry point for the ortho radiology data joiner."""
    config = JoinerConfig()
    
    joiner = OrthoRadiologyNPPESJoiner(config)
    results = joiner.run_full_analysis()
    
    print("\n" + "="*60)
    print("ORTHO RADIOLOGY DATA ANALYSIS COMPLETED")
    print("="*60)
    
    summary = results['summary']['summary']
    print(f"\n📊 SUMMARY STATISTICS:")
    print(f"   Total Providers: {summary['total_providers']:,}")
    print(f"   Total Rates: {summary['total_rates']:,}")
    print(f"   Total Organizations: {summary['total_organizations']:,}")
    print(f"   Total Payers: {summary['total_payers']:,}")
    print(f"   NPPES Match Rate: {summary['nppes_match_rate']:.1f}%")
    
    print(f"\n📁 OUTPUT FILES:")
    print(f"   Dashboard Summary: {results['output_directory']}/dashboard_summary.json")
    print(f"   Visualization Data: {results['output_directory']}/visualization_data.json")
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"   1. Review dashboard_summary.json for detailed statistics")
    print(f"   2. Use visualization_data.json for web dashboard")
    print(f"   3. Share summary with LLM for dashboard development")
    print(f"   4. Consider additional geographic or temporal analysis")

if __name__ == "__main__":
    main() 