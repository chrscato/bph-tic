#!/usr/bin/env python3
"""Data Inspector for Ortho Radiology and NPPES Data - Pre-Joiner Analysis."""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import logging
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class InspectionConfig:
    """Configuration for data inspection."""
    ortho_data_dir: str = "ortho_radiology_data"
    nppes_data_dir: str = "nppes_data"
    output_dir: str = "data_inspection"
    sample_size: int = 1000
    max_unique_values: int = 50
    use_samples: bool = True  # Use samples instead of full datasets
    sample_fraction: float = 0.05  # 1% sample for large datasets

class DataInspector:
    """Comprehensive data inspector for ortho radiology and NPPES datasets."""
    
    def __init__(self, config: InspectionConfig):
        self.config = config
        self.ortho_data_path = Path(config.ortho_data_dir)
        self.nppes_data_path = Path(config.nppes_data_dir)
        self.output_path = Path(config.output_dir)
        
        # Create output directory
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Data storage
        self.datasets = {}
        self.inspection_results = {}
        
    def inspect_dataset(self, name: str, df: pd.DataFrame, sample_size: int = None) -> Dict[str, Any]:
        """Inspect a single dataset and return comprehensive analysis."""
        if sample_size is None:
            sample_size = self.config.sample_size
            
        logger.info(f"Inspecting dataset: {name} (shape: {df.shape})")
        
        # Use sample if dataset is large and sampling is enabled
        if self.config.use_samples and len(df) > 10000:
            sample_df = df.sample(frac=self.config.sample_fraction, random_state=42)
            logger.info(f"Using sample of {len(sample_df):,} records for analysis")
        else:
            sample_df = df
            logger.info(f"Using full dataset for analysis")
        
        inspection = {
            'dataset_name': name,
            'full_shape': df.shape,
            'sample_shape': sample_df.shape if sample_df is not df else None,
            'basic_info': {
                'full_memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
                'dtypes': df.dtypes.to_dict(),
                'null_counts': df.isnull().sum().to_dict(),
                'null_percentages': (df.isnull().sum() / len(df) * 100).to_dict()
            },
            'columns': {}
        }
        
        # Analyze each column using sample
        for col in sample_df.columns:
            col_analysis = self._analyze_column(sample_df[col], col, sample_size)
            inspection['columns'][col] = col_analysis
        
        # Sample data
        inspection['sample_data'] = sample_df.head(5).to_dict('records')
        
        return inspection
    
    def _analyze_column(self, series: pd.Series, col_name: str, sample_size: int) -> Dict[str, Any]:
        """Analyze a single column in detail."""
        analysis = {
            'dtype': str(series.dtype),
            'null_count': series.isnull().sum(),
            'null_percentage': (series.isnull().sum() / len(series)) * 100
        }
        
        # Handle unique count carefully for complex data types
        try:
            unique_count = series.nunique()
            analysis['unique_count'] = unique_count
            analysis['unique_percentage'] = (unique_count / len(series)) * 100
        except (TypeError, ValueError) as e:
            analysis['unique_count'] = 'error'
            analysis['unique_percentage'] = 'error'
            analysis['unique_error'] = str(e)
            analysis['note'] = 'Column contains unhashable types (likely arrays/lists)'
        
        # Handle different data types
        if series.dtype in ['object', 'string']:
            analysis.update(self._analyze_string_column(series, sample_size))
        elif pd.api.types.is_numeric_dtype(series):
            analysis.update(self._analyze_numeric_column(series))
        elif pd.api.types.is_datetime64_any_dtype(series):
            analysis.update(self._analyze_datetime_column(series))
        elif series.dtype == 'bool':
            analysis.update(self._analyze_boolean_column(series))
        
        return analysis
    
    def _analyze_string_column(self, series: pd.Series, sample_size: int) -> Dict[str, Any]:
        """Analyze string/object column."""
        non_null_series = series.dropna()
        
        analysis = {
            'type': 'string',
            'min_length': non_null_series.astype(str).str.len().min() if len(non_null_series) > 0 else None,
            'max_length': non_null_series.astype(str).str.len().max() if len(non_null_series) > 0 else None,
            'avg_length': non_null_series.astype(str).str.len().mean() if len(non_null_series) > 0 else None,
        }
        
        # Top values (if not too many unique values)
        try:
            unique_count = series.nunique()
            if unique_count <= self.config.max_unique_values:
                analysis['top_values'] = series.value_counts().head(10).to_dict()
            else:
                analysis['top_values'] = series.value_counts().head(5).to_dict()
                analysis['note'] = f"Too many unique values ({unique_count}), showing top 5 only"
        except (TypeError, ValueError):
            analysis['top_values'] = 'error'
            analysis['note'] = 'Cannot compute value counts due to unhashable types'
        
        # Check for potential JSON/list structures
        sample_values = series.dropna().head(sample_size)
        json_like_count = 0
        list_like_count = 0
        
        for val in sample_values:
            if isinstance(val, str):
                if val.startswith('[') and val.endswith(']'):
                    list_like_count += 1
                elif val.startswith('{') and val.endswith('}'):
                    json_like_count += 1
        
        if json_like_count > 0:
            analysis['json_like_percentage'] = (json_like_count / len(sample_values)) * 100
        if list_like_count > 0:
            analysis['list_like_percentage'] = (list_like_count / len(sample_values)) * 100
        
        return analysis
    
    def _analyze_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze numeric column."""
        non_null_series = series.dropna()
        
        analysis = {
            'type': 'numeric'
        }
        
        # Handle mixed data types carefully
        try:
            # Convert to numeric, coercing errors to NaN
            numeric_series = pd.to_numeric(non_null_series, errors='coerce')
            numeric_series = numeric_series.dropna()
            
            if len(numeric_series) > 0:
                analysis.update({
                    'min': float(numeric_series.min()),
                    'max': float(numeric_series.max()),
                    'mean': float(numeric_series.mean()),
                    'median': float(numeric_series.median()),
                    'std': float(numeric_series.std()),
                    'percentiles': {
                        'p10': float(numeric_series.quantile(0.10)),
                        'p25': float(numeric_series.quantile(0.25)),
                        'p75': float(numeric_series.quantile(0.75)),
                        'p90': float(numeric_series.quantile(0.90))
                    }
                })
                
                # Check for potential IDs (high cardinality, mostly unique)
                try:
                    unique_ratio = numeric_series.nunique() / len(numeric_series)
                    if unique_ratio > 0.8:
                        analysis['likely_id'] = True
                        analysis['id_type'] = 'high_cardinality'
                except:
                    pass
            else:
                analysis['note'] = 'No valid numeric values found'
                
        except Exception as e:
            analysis['error'] = str(e)
            analysis['note'] = 'Error processing numeric column'
        
        return analysis
    
    def _analyze_datetime_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze datetime column."""
        non_null_series = series.dropna()
        
        analysis = {
            'type': 'datetime',
            'min_date': str(non_null_series.min()) if len(non_null_series) > 0 else None,
            'max_date': str(non_null_series.max()) if len(non_null_series) > 0 else None,
            'date_range_days': (non_null_series.max() - non_null_series.min()).days if len(non_null_series) > 1 else None
        }
        
        return analysis
    
    def _analyze_boolean_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze boolean column."""
        analysis = {
            'type': 'boolean',
            'true_count': series.sum(),
            'false_count': (series == False).sum(),
            'true_percentage': (series.sum() / len(series)) * 100
        }
        
        return analysis
    
    def load_and_inspect_ortho_data(self) -> Dict[str, Any]:
        """Load and inspect all production data files."""
        logger.info("Loading and inspecting production data...")
        
        ortho_files = {
            'rates': self.ortho_data_path / "rates" / "rates_final.parquet",
            'providers': self.ortho_data_path / "providers" / "providers_final.parquet",
            'organizations': self.ortho_data_path / "organizations" / "organizations_final.parquet",
            'payers': self.ortho_data_path / "payers" / "payers_final.parquet",
            'analytics': self.ortho_data_path / "analytics" / "analytics_final.parquet"
        }
        
        ortho_inspections = {}
        
        for name, file_path in ortho_files.items():
            if file_path.exists():
                logger.info(f"Loading {name} from {file_path}")
                df = pd.read_parquet(file_path)
                self.datasets[name] = df
                
                inspection = self.inspect_dataset(name, df)
                ortho_inspections[name] = inspection
                
                logger.info(f"Completed inspection of {name}: {df.shape}")
            else:
                logger.warning(f"File not found: {file_path}")
                ortho_inspections[name] = {'error': 'File not found'}
        
        return ortho_inspections
    
    def load_and_inspect_nppes_data(self) -> Dict[str, Any]:
        """Load and inspect NPPES data."""
        logger.info("Loading and inspecting NPPES data...")
        
        nppes_file = self.nppes_data_path / "nppes_providers.parquet"
        
        if nppes_file.exists():
            df = pd.read_parquet(nppes_file)
            self.datasets['nppes'] = df
            
            inspection = self.inspect_dataset('nppes', df)
            logger.info(f"Completed NPPES inspection: {df.shape}")
            
            return inspection
        else:
            logger.warning(f"NPPES file not found: {nppes_file}")
            return {'error': 'File not found'}
    
    def analyze_join_compatibility(self) -> Dict[str, Any]:
        """Analyze compatibility for joining datasets."""
        logger.info("Analyzing join compatibility...")
        
        compatibility = {
            'potential_joins': [],
            'join_challenges': [],
            'recommendations': []
        }
        
        # Check for common join keys
        if 'providers' in self.datasets and 'nppes' in self.datasets:
            providers_df = self.datasets['providers']
            nppes_df = self.datasets['nppes']
            
            # Check for NPI column
            if 'npi' in providers_df.columns and 'npi' in nppes_df.columns:
                # Use samples for large datasets
                if len(providers_df) > 10000:
                    providers_sample = providers_df.sample(frac=0.01, random_state=42)
                else:
                    providers_sample = providers_df
                
                if len(nppes_df) > 1000:
                    nppes_sample = nppes_df.sample(frac=0.1, random_state=42)
                else:
                    nppes_sample = nppes_df
                
                providers_npis = set(providers_sample['npi'].dropna().astype(str))
                nppes_npis = set(nppes_sample['npi'].dropna().astype(str))
                
                intersection = providers_npis.intersection(nppes_npis)
                
                # Estimate full counts
                providers_total_npis = providers_df['npi'].nunique()
                nppes_total_npis = nppes_df['npi'].nunique()
                estimated_intersection = len(intersection) * (providers_total_npis / len(providers_npis)) if len(providers_npis) > 0 else 0
                
                compatibility['potential_joins'].append({
                    'join_type': 'providers_to_nppes',
                    'join_key': 'npi',
                    'providers_unique_npis': providers_total_npis,
                    'nppes_unique_npis': nppes_total_npis,
                    'sample_intersection_count': len(intersection),
                    'estimated_intersection_count': int(estimated_intersection),
                    'estimated_match_rate': (estimated_intersection / providers_total_npis * 100) if providers_total_npis > 0 else 0
                })
        
        # Check for rates to providers join
        if 'rates' in self.datasets and 'providers' in self.datasets:
            rates_df = self.datasets['rates']
            providers_df = self.datasets['providers']
            
            # Look for potential join columns
            rates_cols = set(rates_df.columns)
            providers_cols = set(providers_df.columns)
            common_cols = rates_cols.intersection(providers_cols)
            
            for col in common_cols:
                if col != 'npi':  # Already covered above
                    try:
                        rates_unique = rates_df[col].nunique()
                        providers_unique = providers_df[col].nunique()
                        
                        compatibility['potential_joins'].append({
                            'join_type': 'rates_to_providers',
                            'join_key': col,
                            'rates_unique_values': rates_unique,
                            'providers_unique_values': providers_unique,
                            'note': 'Potential join column found'
                        })
                    except (TypeError, ValueError):
                        compatibility['potential_joins'].append({
                            'join_type': 'rates_to_providers',
                            'join_key': col,
                            'note': 'Column contains unhashable types - cannot compute unique values'
                        })
        
        # Check for data quality issues (using samples for large datasets)
        for name, df in self.datasets.items():
            sample_df = df.sample(frac=0.01, random_state=42) if len(df) > 10000 else df
            
            for col in sample_df.columns:
                try:
                    null_pct = sample_df[col].isnull().sum() / len(sample_df) * 100
                    if null_pct > 50:
                        compatibility['join_challenges'].append({
                            'dataset': name,
                            'column': col,
                            'null_percentage': null_pct,
                            'issue': 'High null percentage (sample-based)'
                        })
                except:
                    pass
        
        return compatibility
    
    def generate_join_strategy(self) -> Dict[str, Any]:
        """Generate recommended join strategy based on inspection results."""
        logger.info("Generating join strategy...")
        
        strategy = {
            'recommended_joins': [],
            'data_preparation_steps': [],
            'potential_issues': [],
            'estimated_results': {}
        }
        
        # Analyze NPI-based join
        if 'providers' in self.datasets and 'nppes' in self.datasets:
            providers_df = self.datasets['providers']
            nppes_df = self.datasets['nppes']
            
            if 'npi' in providers_df.columns and 'npi' in nppes_df.columns:
                # Check for duplicates
                providers_duplicates = providers_df['npi'].duplicated().sum()
                nppes_duplicates = nppes_df['npi'].duplicated().sum()
                
                strategy['recommended_joins'].append({
                    'name': 'providers_nppes_join',
                    'type': 'left_join',
                    'left_table': 'providers',
                    'right_table': 'nppes',
                    'join_key': 'npi',
                    'expected_matches': len(set(providers_df['npi'].dropna()) & set(nppes_df['npi'].dropna())),
                    'data_preparation': [
                        f"Remove {providers_duplicates} duplicate NPIs from providers (keep first)",
                        f"Remove {nppes_duplicates} duplicate NPIs from NPPES (keep first)"
                    ] if providers_duplicates > 0 or nppes_duplicates > 0 else ["No duplicates found"]
                })
        
        # Analyze rates join (if possible)
        if 'rates' in self.datasets and 'providers' in self.datasets:
            rates_df = self.datasets['rates']
            providers_df = self.datasets['providers']
            
            # Look for organization-based join
            if 'organization_uuid' in rates_df.columns and 'organization_uuid' in providers_df.columns:
                strategy['recommended_joins'].append({
                    'name': 'rates_providers_join',
                    'type': 'inner_join',
                    'left_table': 'rates',
                    'right_table': 'providers',
                    'join_key': 'organization_uuid',
                    'expected_matches': len(set(rates_df['organization_uuid'].dropna()) & set(providers_df['organization_uuid'].dropna())),
                    'data_preparation': ["Check for organization_uuid consistency"]
                })
        
        return strategy
    
    def save_inspection_report(self, ortho_inspections: Dict, nppes_inspection: Dict, 
                             compatibility: Dict, strategy: Dict) -> str:
        """Save comprehensive inspection report."""
        logger.info("Saving inspection report...")
        
        report = {
            'metadata': {
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'inspection_config': asdict(self.config)
            },
            'ortho_radiology_data': ortho_inspections,
            'nppes_data': nppes_inspection,
            'join_compatibility': compatibility,
            'join_strategy': strategy,
            'summary': {
                'total_datasets': len(self.datasets),
                'total_records': sum(len(df) for df in self.datasets.values()),
                'total_memory_mb': sum(df.memory_usage(deep=True).sum() / 1024 / 1024 for df in self.datasets.values())
            }
        }
        
        # Save detailed report
        report_file = self.output_path / "data_inspection_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Save summary report
        summary_file = self.output_path / "inspection_summary.json"
        summary = {
            'datasets': {name: {'shape': df.shape, 'columns': list(df.columns)} for name, df in self.datasets.items()},
            'join_compatibility': compatibility,
            'join_strategy': strategy
        }
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Inspection report saved to: {report_file}")
        logger.info(f"Summary saved to: {summary_file}")
        
        return str(report_file)
    
    def run_full_inspection(self) -> Dict[str, Any]:
        """Run complete data inspection."""
        logger.info("Starting comprehensive data inspection...")
        
        # Inspect all datasets
        ortho_inspections = self.load_and_inspect_ortho_data()
        nppes_inspection = self.load_and_inspect_nppes_data()
        
        # Analyze compatibility
        compatibility = self.analyze_join_compatibility()
        
        # Generate strategy
        strategy = self.generate_join_strategy()
        
        # Save report
        report_file = self.save_inspection_report(ortho_inspections, nppes_inspection, compatibility, strategy)
        
        return {
            'ortho_inspections': ortho_inspections,
            'nppes_inspection': nppes_inspection,
            'compatibility': compatibility,
            'strategy': strategy,
            'report_file': report_file
        }

def main():
    """Main entry point for data inspection."""
    config = InspectionConfig()
    
    inspector = DataInspector(config)
    results = inspector.run_full_inspection()
    
    print("\n" + "="*60)
    print("PRODUCTION DATA INSPECTION COMPLETED")
    print("="*60)
    
    print(f"\n📊 DATASET SUMMARY:")
    for name, df in inspector.datasets.items():
        print(f"   {name}: {df.shape[0]:,} records, {df.shape[1]} columns")
    
    print(f"\n🔗 JOIN COMPATIBILITY:")
    for join in results['compatibility']['potential_joins']:
        print(f"   {join['join_type']} via {join['join_key']}: {join.get('estimated_match_rate', 'N/A')}% match rate")
    
    print(f"\n📁 OUTPUT FILES:")
    print(f"   Detailed Report: {results['report_file']}")
    print(f"   Summary: {config.output_dir}/inspection_summary.json")
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"   1. Review inspection report for data quality issues")
    print(f"   2. Check join compatibility and strategy")
    print(f"   3. Compare with ortho_radiology_data findings")
    print(f"   4. Understand why NPPES script found limited NPIs")

if __name__ == "__main__":
    main() 