"""
Memory-Efficient Fact Table Builder Utility

A utility class for creating fact tables from healthcare rate data that can be easily used in notebooks.
Supports both S3 and local file sources with chunked processing for memory efficiency.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, timezone
import json
import gc
import pyarrow.parquet as pq
import boto3
from tqdm import tqdm
import os
from typing import Optional, List, Dict, Any

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FactTableBuilder:
    """Build a fact table with memory-efficient chunked processing from S3 or local files."""
    
    def __init__(self, 
                 data_dir: str = None,
                 s3_bucket: str = "commercial-rates", 
                 s3_prefix: str = "tic-mrf/test",
                 use_s3: bool = False, 
                 test_mode: bool = False, 
                 sample_size: int = 1000, 
                 nppes_inner_join: bool = False, 
                 chunk_size: int = 50000,
                 upload_to_s3: bool = False):
        """
        Initialize the Fact Table Builder.
        
        Args:
            data_dir: Local data directory path (e.g., "ortho_radiology_data_bcbs_la")
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix/path
            use_s3: Whether to use S3 as data source
            test_mode: Run in test mode with small sample
            sample_size: Sample size for test mode
            nppes_inner_join: Use inner join for NPPES data
            chunk_size: Chunk size for processing
            upload_to_s3: Upload results to S3
        """
        self.data_dir = Path(data_dir) if data_dir else None
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.use_s3 = use_s3
        self.upload_to_s3 = upload_to_s3
        
        # S3 client
        self.s3_client = boto3.client('s3') if use_s3 else None
        
        # Output configuration - use current working directory
        self.output_path = Path.cwd()
        self.test_mode = test_mode
        self.sample_size = sample_size
        self.nppes_inner_join = nppes_inner_join
        self.chunk_size = chunk_size
        
        # Load reference data (smaller files)
        self.organizations_df = None
        self.nppes_df = None
        
        if test_mode:
            logger.info(f"Running in TEST MODE with sample size: {sample_size:,}")
            # Create a test subdirectory in current working directory
            self.output_path = Path.cwd() / "test"
            self.output_path.mkdir(exist_ok=True)
        
        if nppes_inner_join:
            logger.info("Using INNER JOIN for NPPES data - only keeping records with NPPES enrichment")
        else:
            logger.info("Using LEFT JOIN for NPPES data - keeping all records")
        
        if use_s3:
            logger.info(f"Using S3: {s3_bucket}/{s3_prefix}")
        else:
            logger.info(f"Using local files from: {data_dir}")
    
    def list_s3_files(self, file_type: str) -> List[str]:
        """List S3 files of a specific type (rates, organizations, providers)."""
        if not self.use_s3 or not self.s3_client:
            return []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_prefix)
            
            all_files = []
            for page in pages:
                if 'Contents' in page:
                    all_files.extend([obj['Key'] for obj in page['Contents']])
            
            # Filter for specific file type
            filtered_files = [f for f in all_files if file_type in f and f.endswith('.parquet')]
            
            logger.info(f"Found {len(filtered_files)} {file_type} files in S3")
            return filtered_files
            
        except Exception as e:
            logger.error(f"Error listing S3 files for {file_type}: {str(e)}")
            return []
    
    def load_s3_parquet(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Load a single parquet file from S3."""
        if not self.use_s3 or not self.s3_client:
            return None
        
        try:
            # Create temporary file
            temp_dir = Path("temp_s3_downloads")
            temp_dir.mkdir(exist_ok=True)
            temp_file = temp_dir / f"temp_{hash(s3_key)}.parquet"
            
            # Download from S3
            self.s3_client.download_file(self.s3_bucket, s3_key, str(temp_file))
            
            # Read parquet
            df = pd.read_parquet(temp_file)
            
            # Clean up
            temp_file.unlink()
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading S3 file {s3_key}: {str(e)}")
            return None
    
    def load_reference_data(self):
        """Load smaller reference datasets that fit in memory."""
        logger.info("Loading reference datasets...")
        
        # Load organizations data
        if self.use_s3:
            org_files = self.list_s3_files('orgs')
            if org_files:
                # Load first organization file (they should be small)
                self.organizations_df = self.load_s3_parquet(org_files[0])
                if self.organizations_df is not None:
                    logger.info(f"Loaded organizations from S3: {len(self.organizations_df):,} records")
            else:
                logger.warning("No organization files found in S3")
        else:
            if self.data_dir:
                org_path = self.data_dir / "organizations" / "organizations_final.parquet"
                if org_path.exists():
                    self.organizations_df = pd.read_parquet(org_path)
                    logger.info(f"Loaded organizations: {len(self.organizations_df):,} records")
                else:
                    logger.warning(f"Organizations file not found: {org_path}")
        
        # Load NPPES data
        nppes_path = Path("nppes_data/nppes_providers.parquet")
        if nppes_path.exists():
            self.nppes_df = pd.read_parquet(nppes_path)
            logger.info(f"Loaded NPPES: {len(self.nppes_df):,} records")
        else:
            logger.warning(f"NPPES file not found: {nppes_path}")
    
    def get_rates_files(self) -> List[Path]:
        """Get list of rates files to process."""
        if self.use_s3:
            s3_files = self.list_s3_files('rates')
            return s3_files
        else:
            if self.data_dir:
                rates_path = self.data_dir / "rates" / "rates_final.parquet"
                if rates_path.exists():
                    return [rates_path]
                else:
                    logger.error(f"Rates file not found: {rates_path}")
                    return []
            else:
                logger.error("No data directory specified for local files")
                return []
    
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
    
    def process_chunk(self, chunk_df: pd.DataFrame) -> pd.DataFrame:
        """Process a single chunk of rates data."""
        logger.info(f"Processing chunk with {len(chunk_df):,} records...")
        
        # Create a copy to avoid SettingWithCopyWarning
        chunk_df = chunk_df.copy()
        
        # Extract NPIs from provider_network.npi_list
        chunk_df['rate_npis'] = chunk_df['provider_network'].apply(self.extract_npis_from_provider_network)
        
        # Join with organizations
        if self.organizations_df is not None:
            chunk_df = chunk_df.merge(
                self.organizations_df,
                on='organization_uuid',
                how='left',
                suffixes=('', '_org')
            )
        
        # Explode rates by NPI to create one row per rate/NPI combination
        exploded_rows = []
        for _, row in chunk_df.iterrows():
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
        chunk_df = pd.DataFrame(exploded_rows)
        logger.info(f"After exploding by NPI: {len(chunk_df):,} records")
        
        # Join with NPPES data using the exploded NPI
        if self.nppes_df is not None:
            # Prepare NPPES columns for joining
            nppes_join_cols = ['provider_type', 'primary_specialty', 'gender', 'addresses', 'credentials', 'provider_name', 'enumeration_date', 'last_updated', 'secondary_specialties', 'metadata']
            available_nppes_cols = [col for col in nppes_join_cols if col in self.nppes_df.columns]
            
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
            chunk_df = chunk_df.merge(
                nppes_join_df,
                on='npi',
                how=join_type,
                suffixes=('', '_nppes')
            )
            
            # Extract NPPES address fields into individual columns
            if 'nppes_addresses' in chunk_df.columns:
                address_fields = chunk_df['nppes_addresses'].apply(self.extract_nppes_address_fields)
                
                # Add extracted address fields to fact table
                for field_name in ['nppes_city', 'nppes_state', 'nppes_zip', 'nppes_country', 
                                  'nppes_street', 'nppes_phone', 'nppes_fax', 
                                  'nppes_address_type', 'nppes_address_purpose']:
                    chunk_df[field_name] = address_fields.apply(lambda x: x.get(field_name, ''))
        
        # Add derived columns
        chunk_df['rate_category'] = pd.cut(
            chunk_df['negotiated_rate'],
            bins=[0, 100, 500, 1000, 5000, 10000, float('inf')],
            labels=['$0-100', '$100-500', '$500-1K', '$1K-5K', '$5K-10K', '$10K+']
        )
        
        chunk_df['service_category'] = chunk_df['service_code'].apply(self.categorize_service_code)
        chunk_df['fact_key'] = chunk_df['rate_uuid'] + '_' + chunk_df['npi'].astype(str)
        
        return chunk_df
    
    def create_fact_table_chunked(self) -> Optional[Path]:
        """Create the fact table using chunked processing from S3 or local files."""
        logger.info("Creating fact table with chunked processing...")
        
        # Get rates files to process
        rates_files = self.get_rates_files()
        if not rates_files:
            logger.error("No rates files found to process")
            return None
        
        logger.info(f"Found {len(rates_files)} rates files to process")
        
        # Load reference data
        self.load_reference_data()
        
        # Process in chunks using temporary files
        temp_dir = self.output_path / "temp_chunks"
        temp_dir.mkdir(exist_ok=True)
        
        processed_records = 0
        chunk_files = []
        file_chunk_counter = 0
        
        # Process each rates file
        for file_idx, rates_file in enumerate(rates_files):
            logger.info(f"Processing rates file {file_idx + 1}/{len(rates_files)}: {rates_file}")
            
            # Load the rates file
            if self.use_s3:
                rates_df = self.load_s3_parquet(rates_file)
            else:
                rates_df = pd.read_parquet(rates_file)
            
            if rates_df is None or len(rates_df) == 0:
                logger.warning(f"Empty or failed to load rates file: {rates_file}")
                continue
            
            # Apply test mode sampling if needed
            if self.test_mode:
                if len(rates_df) > self.sample_size:
                    rates_df = rates_df.sample(n=self.sample_size, random_state=42)
                    logger.info(f"Sampled {len(rates_df)} records for test mode")
            
            # Process in chunks
            total_chunks = (len(rates_df) + self.chunk_size - 1) // self.chunk_size
            
            for chunk_idx in range(0, len(rates_df), self.chunk_size):
                chunk_df = rates_df.iloc[chunk_idx:chunk_idx + self.chunk_size]
                
                logger.info(f"Processing chunk {file_chunk_counter + 1} from file {file_idx + 1}...")
                
                # Process the chunk
                processed_chunk = self.process_chunk(chunk_df)
                
                if len(processed_chunk) > 0:
                    # Save chunk to temporary file
                    temp_file = temp_dir / f"chunk_{file_chunk_counter:04d}.parquet"
                    processed_chunk.to_parquet(temp_file, index=False, compression='snappy')
                    chunk_files.append(temp_file)
                    
                    processed_records += len(processed_chunk)
                    logger.info(f"Saved chunk {file_chunk_counter + 1} to {temp_file}, total processed: {processed_records:,}")
                
                file_chunk_counter += 1
                
                # Free memory
                del chunk_df, processed_chunk
                gc.collect()
            
            # Free memory after processing each file
            del rates_df
            gc.collect()
        
        # Combine all temporary files into final output
        logger.info(f"Combining {len(chunk_files)} temporary files...")
        output_file = self.output_path / "memory_efficient_fact_table.parquet"
        
        if chunk_files:
            # Read and combine all temporary files
            combined_chunks = []
            for temp_file in tqdm(chunk_files, desc="Combining chunks"):
                chunk_df = pd.read_parquet(temp_file)
                combined_chunks.append(chunk_df)
                del chunk_df  # Free memory
            
            # Combine all chunks
            final_df = pd.concat(combined_chunks, ignore_index=True)
            final_df.to_parquet(output_file, index=False, compression='snappy')
            
            # Clean up temporary files
            logger.info("Cleaning up temporary files...")
            for temp_file in chunk_files:
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                        logger.debug(f"Deleted temporary file: {temp_file}")
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {temp_file}: {e}")
            
            # Try to remove the temp directory, but don't fail if it doesn't work
            try:
                if temp_dir.exists():
                    temp_dir.rmdir()
                    logger.debug(f"Removed temporary directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to remove temporary directory {temp_dir}: {e}")
                logger.info("Temporary files may remain in the directory - you can clean them up manually if needed")
            
            logger.info(f"Completed chunked processing. Total records: {len(final_df):,}")
            
            # Upload to S3 if using S3 and upload is enabled
            s3_fact_table_url = None
            if self.use_s3 and self.upload_to_s3:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                s3_fact_table_key = f"{self.s3_prefix}/fact_tables/memory_efficient_fact_table_{timestamp}.parquet"
                s3_fact_table_url = self.upload_file_to_s3(output_file, s3_fact_table_key)
                
                if s3_fact_table_url:
                    logger.info(f"✅ Fact table uploaded to S3: {s3_fact_table_url}")
                else:
                    logger.warning("⚠️ Failed to upload fact table to S3, but local file is available")
            
            # Store S3 URL in output file metadata
            if s3_fact_table_url:
                metadata_file = self.output_path / "fact_table_s3_location.json"
                metadata = {
                    'local_file': str(output_file),
                    's3_url': s3_fact_table_url,
                    'uploaded_at': datetime.now(timezone.utc).isoformat(),
                    'total_records': len(final_df)
                }
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2, default=str)
                logger.info(f"Saved S3 location metadata to: {metadata_file}")
        else:
            logger.warning("No chunks were processed successfully")
            output_file = None
        
        return output_file
    
    def upload_file_to_s3(self, local_file: Path, s3_key: str) -> Optional[str]:
        """Upload a file to S3."""
        if not self.use_s3 or not self.s3_client:
            return None
        
        try:
            logger.info(f"Uploading {local_file} to s3://{self.s3_bucket}/{s3_key}")
            self.s3_client.upload_file(str(local_file), self.s3_bucket, s3_key)
            logger.info(f"✅ Successfully uploaded to s3://{self.s3_bucket}/{s3_key}")
            return f"s3://{self.s3_bucket}/{s3_key}"
        except Exception as e:
            logger.error(f"❌ Failed to upload to S3: {str(e)}")
            return None
    
    def create_summary(self, output_file: Path) -> Path:
        """Create summary statistics for the fact table."""
        logger.info("Creating summary statistics...")
        
        # Read the full dataset to get total records
        full_df = pd.read_parquet(output_file)
        total_records = len(full_df)
        
        # Take a sample for summary (first 10000 rows or all if less)
        sample_size = min(10000, total_records)
        sample_df = full_df.head(sample_size)
        
        summary = {
            'total_records': total_records,
            'sample_columns': list(sample_df.columns),
            'sample_size': len(sample_df),
            'generated_at': datetime.now(timezone.utc).isoformat(),
            's3_bucket': self.s3_bucket if self.use_s3 else None,
            's3_prefix': self.s3_prefix if self.use_s3 else None,
            'test_mode': self.test_mode,
            'sample_size_used': self.sample_size if self.test_mode else None,
            'file_size_mb': output_file.stat().st_size / 1024 / 1024 if output_file.exists() else 0
        }
        
        # Save summary locally
        summary_file = self.output_path / "memory_efficient_fact_table_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Saved summary to: {summary_file}")
        
        # Upload summary to S3 if using S3 and upload is enabled
        if self.use_s3 and self.upload_to_s3:
            s3_summary_key = f"{self.s3_prefix}/fact_table_summary.json"
            s3_summary_url = self.upload_file_to_s3(summary_file, s3_summary_key)
            if s3_summary_url:
                summary['s3_summary_url'] = s3_summary_url
        
        return summary_file
    
    def run_fact_table_creation(self) -> Optional[Path]:
        """Run the complete memory-efficient fact table creation process."""
        logger.info("Starting memory-efficient fact table creation...")
        
        # Create fact table
        output_file = self.create_fact_table_chunked()
        
        if output_file and output_file.exists():
            # Create summary
            summary_file = self.create_summary(output_file)
            
            logger.info("Memory-efficient fact table creation completed successfully!")
            logger.info(f"Output file: {output_file}")
            logger.info(f"File size: {output_file.stat().st_size / 1024 / 1024:.1f} MB")
            
            return output_file
        else:
            logger.error("Failed to create fact table")
            return None

# Convenience function for notebook usage
def create_fact_table_from_local_data(data_dir: str, 
                                    test_mode: bool = False,
                                    sample_size: int = 1000,
                                    nppes_inner_join: bool = False,
                                    chunk_size: int = 50000,
                                    output_dir: str = None) -> Optional[Path]:
    """
    Convenience function to create a fact table from local data.
    
    Args:
        data_dir: Path to the data directory (e.g., "ortho_radiology_data_bcbs_la")
        test_mode: Run in test mode with small sample
        sample_size: Sample size for test mode
        nppes_inner_join: Use inner join for NPPES data
        chunk_size: Chunk size for processing
        output_dir: Output directory (defaults to current working directory)
        
    Returns:
        Path to the created fact table file, or None if failed
    """
    builder = FactTableBuilder(
        data_dir=data_dir,
        use_s3=False,
        test_mode=test_mode,
        sample_size=sample_size,
        nppes_inner_join=nppes_inner_join,
        chunk_size=chunk_size,
        upload_to_s3=False
    )
    
    # Override output directory if specified
    if output_dir:
        builder.output_path = Path(output_dir)
        builder.output_path.mkdir(exist_ok=True)
    
    return builder.run_fact_table_creation()

def create_fact_table_from_s3(s3_bucket: str = "commercial-rates",
                             s3_prefix: str = "tic-mrf/test",
                             test_mode: bool = False,
                             sample_size: int = 1000,
                             nppes_inner_join: bool = False,
                             chunk_size: int = 50000,
                             upload_to_s3: bool = False,
                             output_dir: str = None) -> Optional[Path]:
    """
    Convenience function to create a fact table from S3 data.
    
    Args:
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix/path
        test_mode: Run in test mode with small sample
        sample_size: Sample size for test mode
        nppes_inner_join: Use inner join for NPPES data
        chunk_size: Chunk size for processing
        upload_to_s3: Upload results to S3
        output_dir: Output directory (defaults to current working directory)
        
    Returns:
        Path to the created fact table file, or None if failed
    """
    builder = FactTableBuilder(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        use_s3=True,
        test_mode=test_mode,
        sample_size=sample_size,
        nppes_inner_join=nppes_inner_join,
        chunk_size=chunk_size,
        upload_to_s3=upload_to_s3
    )
    
    # Override output directory if specified
    if output_dir:
        builder.output_path = Path(output_dir)
        builder.output_path.mkdir(exist_ok=True)
    
    return builder.run_fact_table_creation() 