#!/usr/bin/env python3
"""NPPES Provider Information Management Script using NPI Registry API."""

import os
import json
import time
import requests
import pandas as pd
import boto3
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Iterator
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import structlog
from tqdm import tqdm
import yaml
import argparse

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("python-dotenv not installed, using system environment variables")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class NPPESConfig:
    """Configuration for NPPES provider data management."""
    # API Configuration
    npi_api_base_url: str = "https://npiregistry.cms.hhs.gov/api/"
    api_version: str = "2.1"
    request_delay: float = 0.1  # Delay between requests to be respectful
    
    # Processing Configuration
    batch_size: int = 100
    max_workers: int = 5
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Storage Configuration
    s3_bucket: Optional[str] = None  # Will be set from environment variable
    s3_prefix: str = ""  # Will be set by user
    local_data_dir: str = "."  # Updated to use current directory for multi-payer structure
    local_base_pattern: str = "ortho_radiology_data_"  # Pattern to match payer directories
    
    # NPPES Data Configuration
    nppes_data_dir: str = "nppes_data"
    nppes_filename: str = "nppes_providers.parquet"
    
    # Quality Control
    min_success_rate: float = 0.95
    log_failed_npis: bool = True
    
    # Testing/Sampling Configuration
    limit: Optional[int] = None  # Limit number of NPIs to process for testing

class NPIAPIClient:
    """Client for interacting with the NPI Registry API."""
    
    def __init__(self, config: NPPESConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TiC-NPPES-Manager/1.0'
        })
    
    def get_provider_info(self, npi: str) -> Optional[Dict[str, Any]]:
        """Fetch provider information from NPI Registry API."""
        url = f"{self.config.npi_api_base_url}"
        params = {
            'number': npi,
            'version': self.config.api_version,
            'pretty': 'true'
        }
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('result_count', 0) > 0 and data.get('results'):
                    return data['results'][0]
                else:
                    logger.warning(f"No results found for NPI: {npi}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"API request failed for NPI {npi} (attempt {attempt + 1}): {str(e)}")
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay * (attempt + 1))
                else:
                    logger.error(f"Failed to fetch NPI {npi} after {self.config.max_retries} attempts")
                    return None
        
        return None
    
    def batch_get_provider_info(self, npis: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """Fetch provider information for multiple NPIs with rate limiting."""
        results = {}
        
        for npi in tqdm(npis, desc="Fetching provider data"):
            provider_info = self.get_provider_info(npi)
            results[npi] = provider_info
            
            # Rate limiting
            time.sleep(self.config.request_delay)
        
        return results

class NPPESDataManager:
    """Manages NPPES provider data as a separate, joinable dataset."""
    
    def __init__(self, config: NPPESConfig):
        self.config = config
        self.api_client = NPIAPIClient(config)
        self.s3_client = boto3.client('s3') if config.s3_bucket else None
        
        # Create NPPES data directory
        Path(config.nppes_data_dir).mkdir(parents=True, exist_ok=True)
        
        # NPPES file path
        self.nppes_file = Path(config.nppes_data_dir) / config.nppes_filename
    
    def load_existing_nppes_data(self) -> pd.DataFrame:
        """Load existing NPPES data if available."""
        if self.nppes_file.exists():
            logger.info(f"Loading existing NPPES data from {self.nppes_file}")
            return pd.read_parquet(self.nppes_file)
        else:
            logger.info("No existing NPPES data found. Starting fresh.")
            return pd.DataFrame()
    
    def extract_npis_from_provider_data(self) -> List[str]:
        """Extract unique NPIs from existing provider data."""
        logger.info("Extracting NPIs from existing provider data...")
        
        if self.s3_client:
            return self._extract_npis_from_s3()
        else:
            return self._extract_npis_from_local()
    
    def _extract_npis_from_s3(self) -> List[str]:
        """Extract NPIs from S3 provider data with payer/date partitioning."""
        # List all files in the S3 prefix
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=self.config.s3_bucket,
            Prefix=self.config.s3_prefix
        )
        
        all_files = []
        for page in pages:
            if 'Contents' in page:
                all_files.extend([obj['Key'] for obj in page['Contents']])
        
        if not all_files:
            raise ValueError(f"No files found in S3 bucket {self.config.s3_bucket} with prefix {self.config.s3_prefix}")
        
        logger.info(f"Found {len(all_files)} total files in S3")
        
        # Filter for provider files in the consolidated structure
        # Expected structure: {prefix}/consolidated/{payer}/providers*.parquet
        provider_files = []
        for file_key in all_files:
            # Check if it's a provider file in the consolidated structure
            if (file_key.endswith('.parquet') and 
                'providers' in file_key.lower()):
                provider_files.append(file_key)
        
        if not provider_files:
            logger.warning(f"No provider files found in S3. Available files: {[f.split('/')[-1] for f in all_files[:10]]}")
            # Fallback: look for any files with 'providers' in the name
            provider_files = [f for f in all_files if 'providers' in f and f.endswith('.parquet')]
            if not provider_files:
                raise ValueError("No provider files found in S3")
        
        logger.info(f"Found {len(provider_files)} provider files in S3")
        
        # Group files by payer for better logging
        payer_files = {}
        for s3_key in provider_files:
            # Extract payer from path like: tic-mrf/consolidated/bcbs_il/providers.parquet
            parts = s3_key.split('/')
            # The payer should be the directory after 'consolidated'
            try:
                consolidated_index = parts.index('consolidated')
                if len(parts) > consolidated_index + 1:
                    payer_part = parts[consolidated_index + 1]
                else:
                    payer_part = None
            except ValueError:
                payer_part = None
            
            if payer_part:
                if payer_part not in payer_files:
                    payer_files[payer_part] = []
                payer_files[payer_part].append(s3_key)
            else:
                # Fallback for non-partitioned files
                if 'unknown' not in payer_files:
                    payer_files['unknown'] = []
                payer_files['unknown'].append(s3_key)
        
        logger.info(f"Provider files by payer: {list(payer_files.keys())}")
        
        # Extract NPIs from all provider files
        all_npis = set()
        for payer, files in payer_files.items():
            logger.info(f"Processing {len(files)} files for payer: {payer}")
            
            for s3_key in tqdm(files, desc=f"Extracting NPIs from {payer}"):
                temp_file = Path(self.config.nppes_data_dir) / f"temp_{hash(s3_key)}.parquet"
                
                try:
                    logger.debug(f"Downloading {s3_key} to {temp_file}")
                    self.s3_client.download_file(self.config.s3_bucket, s3_key, str(temp_file))
                    df = pd.read_parquet(temp_file)
                    
                    # Check for NPI column (could be 'npi', 'provider_npi', etc.)
                    npi_columns = [col for col in df.columns if 'npi' in col.lower()]
                    if npi_columns:
                        npi_col = npi_columns[0]
                        npis = df[npi_col].dropna().astype(str).tolist()
                        all_npis.update(npis)
                        logger.debug(f"Found {len(npis)} NPIs in {s3_key}")
                    else:
                        logger.warning(f"No NPI column found in {s3_key}. Available columns: {list(df.columns)}")
                        
                except Exception as e:
                    logger.error(f"Error processing {s3_key}: {str(e)}")
                finally:
                    if temp_file.exists():
                        temp_file.unlink()
        
        logger.info(f"Total unique NPIs found: {len(all_npis)}")
        return list(all_npis)
    
    def _extract_npis_from_local(self) -> List[str]:
        """Extract NPIs from local provider data."""
        # Look for payer-specific directories (e.g., ortho_radiology_data_bcbs_il)
        base_dir = Path(self.config.local_data_dir)
        if not base_dir.exists():
            raise ValueError(f"Base directory not found: {base_dir}")
        
        # Find all payer directories that match the pattern
        payer_dirs = []
        for item in base_dir.iterdir():
            if item.is_dir() and item.name.startswith(self.config.local_base_pattern):
                providers_dir = item / "providers"
                if providers_dir.exists():
                    payer_dirs.append(providers_dir)
        
        if not payer_dirs:
            # Fallback to the original single directory structure
            providers_dir = base_dir / "providers"
            if providers_dir.exists():
                payer_dirs = [providers_dir]
            else:
                raise ValueError(f"No provider directories found in {base_dir}")
        
        logger.info(f"Found {len(payer_dirs)} payer directories with providers")
        
        # Extract NPIs from all provider files across all payers
        all_npis = set()
        for providers_dir in payer_dirs:
            logger.info(f"Processing providers from: {providers_dir}")
            
            # Find all parquet files in this provider directory
            provider_files = list(providers_dir.glob("*.parquet"))
            if not provider_files:
                logger.warning(f"No provider files found in {providers_dir}")
                continue
            
            logger.info(f"Found {len(provider_files)} provider files in {providers_dir}")
            
            # Extract NPIs from all provider files in this payer
            for file_path in tqdm(provider_files, desc=f"Extracting NPIs from {providers_dir.name}"):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # Check for NPI column (could be 'npi', 'provider_npi', etc.)
                    npi_columns = [col for col in df.columns if 'npi' in col.lower()]
                    if npi_columns:
                        npi_col = npi_columns[0]
                        npis = df[npi_col].dropna().astype(str).tolist()
                        all_npis.update(npis)
                        logger.debug(f"Found {len(npis)} NPIs in {file_path.name}")
                    else:
                        logger.warning(f"No NPI column found in {file_path.name}. Available columns: {list(df.columns)}")
                        
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")
        
        logger.info(f"Total unique NPIs found across all payers: {len(all_npis)}")
        return list(all_npis)
    
    def get_new_npis(self, existing_nppes_df: pd.DataFrame, all_npis: List[str]) -> List[str]:
        """Get NPIs that are not already in the NPPES dataset."""
        if existing_nppes_df.empty:
            new_npis = all_npis
        else:
            existing_npis = set(existing_nppes_df['npi'].astype(str))
            new_npis = [npi for npi in all_npis if npi not in existing_npis]
        
        logger.info(f"Found {len(new_npis)} new NPIs out of {len(all_npis)} total")
        
        # Apply limit if specified (for testing)
        if self.config.limit and len(new_npis) > self.config.limit:
            logger.info(f"Limiting to {self.config.limit} NPIs for testing")
            new_npis = new_npis[:self.config.limit]
        
        return new_npis
    
    def fetch_and_process_nppes_data(self, npis: List[str]) -> pd.DataFrame:
        """Fetch and process NPPES data for given NPIs."""
        logger.info(f"Fetching NPPES data for {len(npis)} NPIs...")
        
        # Fetch provider information from API
        npi_data = self.api_client.batch_get_provider_info(npis)
        
        # Process the data
        processed_records = []
        failed_npis = []
        
        for npi, api_data in npi_data.items():
            if api_data:
                processed_record = self._process_nppes_record(npi, api_data)
                processed_records.append(processed_record)
            else:
                failed_npis.append(npi)
        
        # Log failed NPIs if requested
        if self.config.log_failed_npis and failed_npis:
            failed_file = Path(self.config.nppes_data_dir) / "failed_npis.json"
            with open(failed_file, 'w') as f:
                json.dump(failed_npis, f, indent=2)
            logger.warning(f"Failed to fetch {len(failed_npis)} NPIs. See {failed_file}")
        
        # Calculate success rate
        success_rate = len(processed_records) / len(npis)
        logger.info(f"NPPES fetch success rate: {success_rate:.2%}")
        
        if success_rate < self.config.min_success_rate:
            logger.warning(f"Success rate {success_rate:.2%} below threshold {self.config.min_success_rate:.2%}")
        
        return pd.DataFrame(processed_records)
    
    def _process_nppes_record(self, npi: str, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single NPPES record from API data."""
        record = {'npi': npi}
        
        # Extract basic information
        basic = api_data.get('basic', {})
        record['provider_name'] = {
            'first': basic.get('first_name', ''),
            'last': basic.get('last_name', ''),
            'middle': basic.get('middle_name', ''),
            'suffix': ''
        }
        
        # Extract credentials
        credentials = []
        if basic.get('credential'):
            credentials.append(basic['credential'])
        record['credentials'] = credentials
        
        # Extract gender
        record['gender'] = basic.get('sex', 'Unknown')
        
        # Extract enumeration date
        if basic.get('enumeration_date'):
            try:
                record['enumeration_date'] = datetime.strptime(
                    basic['enumeration_date'], '%Y-%m-%d'
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                record['enumeration_date'] = None
        
        # Extract last updated
        if basic.get('last_updated'):
            try:
                record['last_updated'] = datetime.strptime(
                    basic['last_updated'], '%Y-%m-%d'
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                record['last_updated'] = None
        
        # Extract addresses
        addresses = []
        for addr in api_data.get('addresses', []):
            if addr.get('address_purpose') == 'LOCATION':  # Prefer location addresses
                address_record = {
                    'type': addr.get('address_type', ''),
                    'purpose': addr.get('address_purpose', ''),
                    'street': addr.get('address_1', ''),
                    'city': addr.get('city', ''),
                    'state': addr.get('state', ''),
                    'zip': addr.get('postal_code', ''),
                    'country': addr.get('country_name', ''),
                    'phone': addr.get('telephone_number', ''),
                    'fax': addr.get('fax_number', '')
                }
                addresses.append(address_record)
        
        record['addresses'] = addresses
        
        # Extract specialties from taxonomies
        primary_specialty = ""
        secondary_specialties = []
        
        for taxonomy in api_data.get('taxonomies', []):
            specialty_desc = taxonomy.get('desc', '')
            if taxonomy.get('primary'):
                primary_specialty = specialty_desc
            else:
                secondary_specialties.append(specialty_desc)
        
        record['primary_specialty'] = primary_specialty
        record['secondary_specialties'] = secondary_specialties
        
        # Determine provider type
        if api_data.get('enumeration_type') == 'NPI-1':
            record['provider_type'] = 'Individual'
        elif api_data.get('enumeration_type') == 'NPI-2':
            record['provider_type'] = 'Organization'
        else:
            record['provider_type'] = 'Unknown'
        
        # Add metadata
        record['metadata'] = {
            'fetched_at': datetime.now(timezone.utc),
            'api_version': self.config.api_version,
            'data_source': 'NPI_Registry_API',
            'fetch_status': 'success'
        }
        
        return record
    
    def update_nppes_dataset(self, new_data: pd.DataFrame):
        """Update the NPPES dataset with new data."""
        logger.info(f"Updating NPPES dataset with {len(new_data)} new records...")
        
        # Load existing data
        existing_data = self.load_existing_nppes_data()
        
        if existing_data.empty:
            # First time creation
            combined_data = new_data
        else:
            # Combine existing and new data, removing duplicates
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            combined_data = combined_data.drop_duplicates(subset=['npi'], keep='last')
        
        # Save the updated dataset
        combined_data.to_parquet(self.nppes_file, index=False, compression='snappy')
        
        logger.info(f"NPPES dataset updated: {len(combined_data)} total records")
        logger.info(f"NPPES file saved to: {self.nppes_file}")
    
    def generate_summary_stats(self):
        """Generate summary statistics for the NPPES dataset."""
        if not self.nppes_file.exists():
            logger.warning("No NPPES dataset found to generate statistics")
            return
        
        df = pd.read_parquet(self.nppes_file)
        
        stats = {
            'total_providers': len(df),
            'individual_providers': len(df[df['provider_type'] == 'Individual']),
            'organization_providers': len(df[df['provider_type'] == 'Organization']),
            'providers_with_addresses': df['addresses'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum(),
            'providers_with_specialties': df['primary_specialty'].apply(lambda x: isinstance(x, str) and bool(x.strip())).sum(),
            'providers_with_credentials': df['credentials'].apply(lambda x: isinstance(x, list) and len(x) > 0).sum(),
            'successfully_fetched': df['metadata'].apply(lambda x: isinstance(x, dict) and x.get('fetch_status') == 'success').sum(),
            'unique_states': len(set([addr.get('state') for addresses in df['addresses'] if isinstance(addresses, list) for addr in addresses if isinstance(addr, dict) and addr.get('state')])),
            'unique_primary_specialties': df['primary_specialty'].apply(lambda x: x if isinstance(x, str) and x.strip() else None).dropna().nunique(),
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
        
        # Save statistics
        stats_file = Path(self.config.nppes_data_dir) / "nppes_statistics.json"
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        
        logger.info(f"NPPES statistics saved to: {stats_file}")
        logger.info(f"Summary: {stats['total_providers']} total providers in NPPES dataset")
    
    def run_nppes_update(self):
        """Run the complete NPPES data update process."""
        logger.info("Starting NPPES data update process...")
        
        start_time = time.time()
        
        try:
            # Load existing NPPES data
            existing_nppes_df = self.load_existing_nppes_data()
            
            # Extract NPIs from provider data
            all_npis = self.extract_npis_from_provider_data()
            
            # Get new NPIs that need to be fetched
            new_npis = self.get_new_npis(existing_nppes_df, all_npis)
            
            if not new_npis:
                logger.info("No new NPIs to fetch. NPPES dataset is up to date.")
                return
            
            # Fetch and process new NPPES data
            new_nppes_data = self.fetch_and_process_nppes_data(new_npis)
            
            # Update the NPPES dataset
            self.update_nppes_dataset(new_nppes_data)
            
            # Generate summary statistics
            self.generate_summary_stats()
            
            elapsed_time = time.time() - start_time
            logger.info(f"NPPES update completed in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"NPPES update failed: {str(e)}")
            raise
                                                                                                                                                                                                                                                                                                                    
def create_nppes_config(limit: Optional[int] = None) -> NPPESConfig:
    """Create NPPES configuration from environment or defaults."""
    s3_bucket = os.getenv("S3_BUCKET")
    s3_prefix = os.getenv("S3_PREFIX")
    local_data_dir = os.getenv("LOCAL_DATA_DIR", ".")
    local_base_pattern = os.getenv("LOCAL_BASE_PATTERN", "ortho_radiology_data_")
    nppes_data_dir = os.getenv("NPPES_DATA_DIR", "nppes_data")

    return NPPESConfig(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        local_data_dir=local_data_dir,
        local_base_pattern=local_base_pattern,
        nppes_data_dir=nppes_data_dir,
        batch_size=int(os.getenv("BATCH_SIZE", "100")),
        max_workers=int(os.getenv("MAX_WORKERS", "5")),
        request_delay=float(os.getenv("REQUEST_DELAY", "0.1")),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
        limit=limit
    )

def main():
    """Main entry point for NPPES data management script."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="NPPES Provider Information Management Script",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with local data (current directory structure)
  python scripts/backfill_provider_info.py --limit 500
  
  # Run with S3 data (bucket from .env, default prefix)
  python scripts/backfill_provider_info.py --limit 500
  
  # Run with custom S3 prefix
  python scripts/backfill_provider_info.py --s3-prefix my-data/providers --limit 500
  
  # Run with custom S3 bucket and prefix
  python scripts/backfill_provider_info.py --s3-bucket my-bucket --s3-prefix my-data/providers --limit 500
  
  # Test with very small sample
  python scripts/backfill_provider_info.py --limit 50
  
  # Run with custom settings
  python scripts/backfill_provider_info.py --limit 1000 --request-delay 0.2
        """
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit number of NPIs to process (useful for testing)'
    )
    
    parser.add_argument(
        '--request-delay',
        type=float,
        default=0.1,
        help='Delay between API requests in seconds (default: 0.1)'
    )
    
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Maximum number of retries for failed API requests (default: 3)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Batch size for processing (default: 100)'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        default=5,
        help='Maximum number of worker threads (default: 5)'
    )
    
    parser.add_argument(
        '--s3-bucket',
        type=str,
        help='S3 bucket name (overrides S3_BUCKET env var)'
    )
    
    parser.add_argument(
        '--s3-prefix',
        type=str,
        default='tic-mrf/consolidated/',
        help='S3 prefix/path (default: tic-mrf/providers)'
    )
    
    parser.add_argument(
        '--local-data-dir',
        type=str,
        default='.',
        help='Local data directory (default: current directory)'
    )
    
    parser.add_argument(
        '--local-base-pattern',
        type=str,
        default='ortho_radiology_data_',
        help='Pattern to match payer directories (default: ortho_radiology_data_)'
    )
    
    args = parser.parse_args()
    
    try:
        # Load configuration with command line overrides
        config = create_nppes_config(limit=args.limit)
        
        # Override config with command line arguments
        if args.request_delay != 0.1:
            config.request_delay = args.request_delay
        if args.max_retries != 3:
            config.max_retries = args.max_retries
        if args.batch_size != 100:
            config.batch_size = args.batch_size
        if args.max_workers != 5:
            config.max_workers = args.max_workers
        if args.s3_bucket:
            config.s3_bucket = args.s3_bucket
        if args.s3_prefix:
            config.s3_prefix = args.s3_prefix
        if args.local_data_dir != '.':
            config.local_data_dir = args.local_data_dir
        if args.local_base_pattern != 'ortho_radiology_data_':
            config.local_base_pattern = args.local_base_pattern
        
        # Validate S3 configuration
        if config.s3_prefix:
            if not config.s3_bucket:
                raise ValueError("S3_BUCKET environment variable must be set when using S3 mode")
            logger.info("Using S3 mode")
        else:
            logger.info("Using local mode")
        
        # Log configuration
        logger.info("NPPES Configuration:")
        logger.info(f"  S3 Bucket: {config.s3_bucket}")
        logger.info(f"  S3 Prefix: {config.s3_prefix}")
        logger.info(f"  Local Data Dir: {config.local_data_dir}")
        logger.info(f"  Local Base Pattern: {config.local_base_pattern}")
        logger.info(f"  Limit: {config.limit or 'No limit'}")
        logger.info(f"  Request delay: {config.request_delay}s")
        logger.info(f"  Max retries: {config.max_retries}")
        logger.info(f"  Batch size: {config.batch_size}")
        logger.info(f"  Max workers: {config.max_workers}")
        
        # Initialize NPPES manager
        nppes_manager = NPPESDataManager(config)
        
        # Run NPPES update
        nppes_manager.run_nppes_update()
        
    except Exception as e:
        logger.error(f"NPPES update failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 