#!/usr/bin/env python3
"""
Memory-efficient fact table creation that streams directly to S3.
This version avoids local chunk files and uploads data directly to S3.
"""

import pandas as pd
import boto3
import logging
from pathlib import Path
from datetime import datetime, timezone
import json
import argparse
from typing import List, Optional, Dict, Any
import io
import gc
import pyarrow as pa
import pyarrow.parquet as pq

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3StreamingFactTableBuilder:
    """Build fact tables with direct S3 streaming to avoid local storage."""
    
    def __init__(self, 
                 s3_bucket="commercial-rates", 
                 s3_prefix="tic-mrf/test",
                 test_mode=False, 
                 sample_size=1000, 
                 nppes_inner_join=False, 
                 chunk_size=50000,
                 use_s3=True,
                 upload_to_s3=True):
        
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.test_mode = test_mode
        self.sample_size = sample_size
        self.nppes_inner_join = nppes_inner_join
        self.chunk_size = chunk_size
        self.use_s3 = use_s3
        self.upload_to_s3 = upload_to_s3
        
        # Initialize S3 client
        self.s3_client = None
        if self.use_s3:
            try:
                self.s3_client = boto3.client('s3')
                logger.info("✅ S3 client initialized successfully")
            except Exception as e:
                logger.error(f"❌ Failed to initialize S3 client: {e}")
                self.use_s3 = False
        
        # Reference data
        self.organizations = None
        self.nppes_data = None
        
        # Output path
        self.output_path = Path("dashboard_data")
        self.output_path.mkdir(exist_ok=True)
        
        logger.info(f"Using S3: {self.s3_bucket}/{self.s3_prefix}")
        logger.info(f"Test mode: {self.test_mode}")
        logger.info(f"NPPES inner join: {self.nppes_inner_join}")
    
    def list_s3_files(self, file_type):
        """List files in S3 with the specified type."""
        if not self.use_s3 or not self.s3_client:
            return []
        
        try:
            prefix = f"{self.s3_prefix}/{file_type}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        files.append(obj['Key'])
            
            logger.info(f"Found {len(files)} {file_type} files in S3")
            return files
        except Exception as e:
            logger.error(f"Failed to list {file_type} files: {e}")
            return []
    
    def load_s3_parquet(self, s3_key):
        """Load a parquet file from S3."""
        if not self.use_s3 or not self.s3_client:
            return None
        
        try:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            logger.info(f"Loaded {len(df):,} records from {s3_key}")
            return df
        except Exception as e:
            logger.error(f"Failed to load {s3_key}: {e}")
            return None
    
    def load_reference_data(self):
        """Load organizations and NPPES data from S3."""
        logger.info("Loading reference datasets...")
        
        # Load organizations
        org_files = self.list_s3_files("orgs")
        if org_files:
            # Load the first org file as reference
            org_df = self.load_s3_parquet(org_files[0])
            if org_df is not None:
                self.organizations = org_df
                logger.info(f"Loaded organizations from S3: {len(self.organizations):,} records")
        
        # Load NPPES data
        nppes_file = Path("nppes_data/nppes_providers.parquet")
        if nppes_file.exists():
            self.nppes_data = pd.read_parquet(nppes_file)
            logger.info(f"Loaded NPPES: {len(self.nppes_data):,} records")
        else:
            logger.warning("NPPES data not found locally")
    
    def get_rates_files(self):
        """Get list of rates files to process."""
        rates_files = self.list_s3_files("rates")
        
        if self.test_mode:
            # Take only the first few files for testing
            rates_files = rates_files[:2]
            logger.info(f"Test mode: processing only {len(rates_files)} files")
        
        return rates_files
    
    def extract_npis_from_provider_network(self, provider_network_data):
        """Extract NPIs from provider network data."""
        if not provider_network_data or pd.isna(provider_network_data):
            return []
        
        try:
            if isinstance(provider_network_data, str):
                # Parse JSON-like string
                import ast
                data = ast.literal_eval(provider_network_data)
            else:
                data = provider_network_data
            
            npis = []
            if isinstance(data, dict) and 'npi_list' in data:
                # Handle the actual data structure with npi_list
                npi_list = data['npi_list']
                if hasattr(npi_list, 'tolist'):
                    npis = [str(npi) for npi in npi_list.tolist()]
                else:
                    npis = [str(npi) for npi in npi_list]
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and 'npi' in item:
                        npis.append(str(item['npi']))
            elif isinstance(data, dict):
                if 'npi' in data:
                    npis.append(str(data['npi']))
            
            return npis
        except Exception as e:
            logger.debug(f"Failed to extract NPIs: {e}")
            return []
    
    def extract_nppes_address_fields(self, addresses_data):
        """Extract address fields from NPPES addresses data."""
        if not addresses_data or pd.isna(addresses_data):
            return {}
        
        try:
            if isinstance(addresses_data, str):
                import ast
                addresses = ast.literal_eval(addresses_data)
            else:
                addresses = addresses_data
            
            if not addresses:
                return {}
            
            # Take the first address
            first_address = addresses[0] if isinstance(addresses, list) else addresses
            
            return {
                'address_line_1': first_address.get('address_line_1', ''),
                'address_line_2': first_address.get('address_line_2', ''),
                'city': first_address.get('city', ''),
                'state': first_address.get('state', ''),
                'zip_code': first_address.get('zip_code', ''),
                'country_code': first_address.get('country_code', '')
            }
        except Exception as e:
            logger.debug(f"Failed to extract address fields: {e}")
            return {}
    
    def categorize_service_code(self, service_code):
        """Categorize service codes into broader categories."""
        if pd.isna(service_code):
            return 'Unknown'
        
        service_code = str(service_code).upper()
        
        # CPT code categories
        if service_code.startswith('99'):
            return 'Evaluation and Management'
        elif service_code.startswith('10') or service_code.startswith('11'):
            return 'Anesthesia'
        elif service_code.startswith('20'):
            return 'Surgery'
        elif service_code.startswith('30'):
            return 'Radiology'
        elif service_code.startswith('40'):
            return 'Pathology and Laboratory'
        elif service_code.startswith('50'):
            return 'Medicine'
        elif service_code.startswith('60'):
            return 'Emergency Services'
        elif service_code.startswith('70'):
            return 'Critical Care'
        elif service_code.startswith('80'):
            return 'Specialty Services'
        elif service_code.startswith('90'):
            return 'Other Services'
        else:
            return 'Other'
    
    def process_chunk(self, chunk_df):
        """Process a chunk of rates data."""
        if chunk_df.empty:
            return None
        
        logger.info(f"Processing chunk with {len(chunk_df):,} records...")
        
        # Explode by NPI if provider_network exists
        if 'provider_network' in chunk_df.columns:
            # Extract NPIs from provider_network
            chunk_df['npis'] = chunk_df['provider_network'].apply(self.extract_npis_from_provider_network)
            
            # Explode by NPI
            chunk_df = chunk_df.explode('npis')
            chunk_df = chunk_df[chunk_df['npis'].notna() & (chunk_df['npis'] != '')]
            
            logger.info(f"After exploding by NPI: {len(chunk_df):,} records")
        elif 'rate_npis' in chunk_df.columns:
            # Fallback to rate_npis column if provider_network doesn't exist
            chunk_df['npis'] = chunk_df['rate_npis'].apply(lambda x: [str(npi) for npi in x.tolist()] if hasattr(x, 'tolist') else [str(npi) for npi in x])
            
            # Explode by NPI
            chunk_df = chunk_df.explode('npis')
            chunk_df = chunk_df[chunk_df['npis'].notna() & (chunk_df['npis'] != '')]
            
            logger.info(f"After exploding by rate_npis: {len(chunk_df):,} records")
        
        # Join with NPPES data if available
        if self.nppes_data is not None and not chunk_df.empty:
            if self.nppes_inner_join:
                # Inner join - only keep records with NPPES data
                chunk_df = chunk_df.merge(
                    self.nppes_data, 
                    left_on='npis', 
                    right_on='npi', 
                    how='inner'
                )
                logger.info(f"After NPPES inner join: {len(chunk_df):,} records")
            else:
                # Left join - keep all records, add NPPES data where available
                chunk_df = chunk_df.merge(
                    self.nppes_data, 
                    left_on='npis', 
                    right_on='npi', 
                    how='left'
                )
                logger.info(f"After NPPES left join: {len(chunk_df):,} records")
        
        # Join with organizations if available
        if self.organizations is not None and not chunk_df.empty:
            chunk_df = chunk_df.merge(
                self.organizations,
                left_on='organization_id',
                right_on='id',
                how='left',
                suffixes=('', '_org')
            )
        
        # Add service categories
        if 'service_code' in chunk_df.columns:
            chunk_df['service_category'] = chunk_df['service_code'].apply(self.categorize_service_code)
        
        # Add address fields if NPPES data is available
        if 'addresses' in chunk_df.columns:
            address_fields = chunk_df['addresses'].apply(self.extract_nppes_address_fields)
            for field in ['address_line_1', 'address_line_2', 'city', 'state', 'zip_code', 'country_code']:
                chunk_df[f'nppes_{field}'] = address_fields.apply(lambda x: x.get(field, ''))
        
        return chunk_df
    
    def stream_to_s3(self, df, s3_key):
        """Stream a DataFrame directly to S3 as parquet."""
        if not self.use_s3 or not self.s3_client:
            return False
        
        try:
            # Convert DataFrame to PyArrow table and write to parquet bytes
            table = pa.Table.from_pandas(df, preserve_index=False)
            parquet_buffer = io.BytesIO()
            pq.write_table(table, parquet_buffer, compression="snappy")
            parquet_buffer.seek(0)

            # Upload to S3
            self.s3_client.upload_fileobj(parquet_buffer, self.s3_bucket, s3_key)
            logger.info(f"✅ Streamed {len(df):,} records to s3://{self.s3_bucket}/{s3_key}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to stream to S3: {e}")
            return False
        finally:
            parquet_buffer.close()
            del table
    
    def create_fact_table_streaming(self):
        """Create fact table by streaming directly to S3."""
        logger.info("Starting S3 streaming fact table creation...")
        
        # Load reference data
        self.load_reference_data()
        
        # Get rates files
        rates_files = self.get_rates_files()
        if not rates_files:
            logger.error("No rates files found")
            return None
        
        logger.info(f"Found {len(rates_files)} rates files to process")
        
        # Process each file and stream to S3
        total_records = 0
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for i, rates_file in enumerate(rates_files, 1):
            logger.info(f"Processing rates file {i}/{len(rates_files)}: {rates_file}")
            
            # Load rates data
            rates_df = self.load_s3_parquet(rates_file)
            if rates_df is None:
                continue
            
            # Process in chunks
            for chunk_start in range(0, len(rates_df), self.chunk_size):
                chunk_end = min(chunk_start + self.chunk_size, len(rates_df))
                chunk_df = rates_df.iloc[chunk_start:chunk_end].copy()

                # Process chunk
                processed_chunk = self.process_chunk(chunk_df)
                if processed_chunk is None or processed_chunk.empty:
                    del chunk_df, processed_chunk
                    gc.collect()
                    continue

                # Stream to S3
                chunk_num = total_records // self.chunk_size
                s3_key = f"{self.s3_prefix}/fact_tables/streaming_chunks/chunk_{chunk_num:04d}_{timestamp}.parquet"

                if self.stream_to_s3(processed_chunk, s3_key):
                    total_records += len(processed_chunk)
                    logger.info(f"Total records processed: {total_records:,}")

                del chunk_df, processed_chunk
                gc.collect()

        del rates_df
        gc.collect()
        
        logger.info(f"✅ Completed streaming fact table creation. Total records: {total_records:,}")
        return total_records
    
    def run_fact_table_creation(self):
        """Run the complete S3 streaming fact table creation process."""
        logger.info("Starting S3 streaming fact table creation...")
        
        # Create fact table
        total_records = self.create_fact_table_streaming()
        
        if total_records:
            # Create summary
            summary = {
                'total_records': total_records,
                'generated_at': datetime.now(timezone.utc).isoformat(),
                's3_bucket': self.s3_bucket,
                's3_prefix': self.s3_prefix,
                'test_mode': self.test_mode,
                'nppes_inner_join': self.nppes_inner_join,
                'streaming_mode': True
            }
            
            # Save summary locally
            summary_file = self.output_path / "s3_streaming_fact_table_summary.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            logger.info(f"Saved summary to: {summary_file}")
            
            # Upload summary to S3
            if self.use_s3 and self.upload_to_s3:
                s3_summary_key = f"{self.s3_prefix}/s3_streaming_fact_table_summary.json"
                self.stream_to_s3(pd.DataFrame([summary]), s3_summary_key)
        
        return total_records

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Create memory-efficient fact table with S3 streaming")
    parser.add_argument("--s3-bucket", default="commercial-rates", help="S3 bucket name")
    parser.add_argument("--s3-prefix", default="tic-mrf/test", help="S3 prefix")
    parser.add_argument("--test-mode", action="store_true", help="Run in test mode with limited data")
    parser.add_argument("--sample-size", type=int, default=1000, help="Sample size for test mode")
    parser.add_argument("--nppes-inner-join", action="store_true", help="Use inner join for NPPES data")
    parser.add_argument("--chunk-size", type=int, default=50000, help="Chunk size for processing")
    parser.add_argument("--no-s3", action="store_true", help="Disable S3 operations")
    parser.add_argument("--no-upload", action="store_true", help="Disable S3 upload")
    
    args = parser.parse_args()
    
    # Create builder
    builder = S3StreamingFactTableBuilder(
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        test_mode=args.test_mode,
        sample_size=args.sample_size,
        nppes_inner_join=args.nppes_inner_join,
        chunk_size=args.chunk_size,
        use_s3=not args.no_s3,
        upload_to_s3=not args.no_upload
    )
    
    # Run fact table creation
    total_records = builder.run_fact_table_creation()
    
    if total_records:
        logger.info(f"✅ Successfully created streaming fact table with {total_records:,} records")
    else:
        logger.error("❌ Failed to create streaming fact table")

if __name__ == "__main__":
    main() 