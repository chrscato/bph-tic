#!/usr/bin/env python3
"""S3 Batch Consolidation Script - Consolidates S3 batch files into final parquet files."""

import os
import boto3
import pandas as pd
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass
import json
import time
from tqdm import tqdm
import logging
import structlog
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Loaded environment variables from .env file")
except ImportError:
    print("python-dotenv not installed, using system environment variables")
except Exception as e:
    print(f"Warning: Could not load .env file: {e}")

# Configure structlog
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

@dataclass
class ConsolidationConfig:
    """Configuration for S3 batch consolidation."""
    s3_bucket: str
    s3_prefix: str = "commercial-rates/tic-mrf"
    consolidated_prefix: str = "commercial-rates/tic-mrf/consolidated"
    temp_dir: Optional[str] = None
    max_workers: int = 4
    chunk_size: int = 1000  # Number of files to process in memory at once
    cleanup_original_files: bool = False  # Whether to delete original batch files after consolidation
    dry_run: bool = False  # If True, only list files without processing

class S3BatchConsolidator:
    """Consolidates S3 batch files into final parquet files."""
    
    def __init__(self, config: ConsolidationConfig):
        self.config = config
        self.s3_client = boto3.client('s3')
        
        # Create temp directory if not provided
        if not self.config.temp_dir:
            self.temp_dir = tempfile.mkdtemp(prefix="s3_consolidation_")
        else:
            self.temp_dir = Path(self.config.temp_dir)
            self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Statistics tracking
        self.stats = {
            "payers_processed": 0,
            "total_batch_files": 0,
            "files_consolidated": 0,
            "bytes_processed": 0,
            "errors": [],
            "start_time": datetime.now(timezone.utc)
        }
        
        logger.info("initialized_consolidator", 
                   s3_bucket=config.s3_bucket,
                   s3_prefix=config.s3_prefix,
                   consolidated_prefix=config.consolidated_prefix,
                   temp_dir=str(self.temp_dir))
    
    def consolidate_all_payers(self):
        """Consolidate batch files for all payers found in S3."""
        logger.info("starting_consolidation")
        
        try:
            # Find all payers in the S3 structure
            payers = self.discover_payers()
            logger.info("discovered_payers", payers=payers, count=len(payers))
            
            if self.config.dry_run:
                logger.info("dry_run_mode", payers=payers)
                return
            
            # Process each payer
            for payer_name in payers:
                try:
                    payer_stats = self.consolidate_payer(payer_name)
                    self.stats["payers_processed"] += 1
                    logger.info("completed_payer_consolidation", 
                              payer=payer_name, stats=payer_stats)
                    
                except Exception as e:
                    error_msg = f"Failed consolidating {payer_name}: {str(e)}"
                    logger.error("payer_consolidation_failed", payer=payer_name, error=str(e))
                    self.stats["errors"].append(error_msg)
            
            self.log_final_statistics()
            
        finally:
            self.cleanup_temp_directory()
    
    def discover_payers(self) -> List[str]:
        """Discover all payers in the S3 structure."""
        payers = set()
        
        # Look for payers in rates, organizations, and providers paths
        for data_type in ["rates", "organizations", "providers"]:
            try:
                # List objects with the pattern: {prefix}/{data_type}/payer={payer_name}/
                prefix = f"{self.config.s3_prefix}/{data_type}/payer="
                response = self.s3_client.list_objects_v2(
                    Bucket=self.config.s3_bucket,
                    Prefix=prefix,
                    Delimiter='/'
                )
                
                # Extract payer names from CommonPrefixes
                if 'CommonPrefixes' in response:
                    for prefix_info in response['CommonPrefixes']:
                        prefix_path = prefix_info['Prefix']
                        # Extract payer name from path like "commercial-rates/tic-mrf/rates/payer=bcbs_il/"
                        payer_part = prefix_path.split('payer=')[-1].rstrip('/')
                        if payer_part:
                            payers.add(payer_part)
                
            except Exception as e:
                logger.error("failed_discovering_payers", data_type=data_type, error=str(e))
        
        return sorted(list(payers))
    
    def consolidate_payer(self, payer_name: str) -> Dict[str, Any]:
        """Consolidate all batch files for a specific payer."""
        logger.info("consolidating_payer", payer=payer_name)
        
        payer_stats = {
            "batch_files_found": 0,
            "files_consolidated": 0,
            "bytes_processed": 0,
            "start_time": time.time(),
            "data_types": {}
        }
        
        # Consolidate each data type
        for data_type in ["rates", "organizations", "providers"]:
            try:
                type_stats = self.consolidate_data_type(payer_name, data_type)
                payer_stats["data_types"][data_type] = type_stats
                payer_stats["batch_files_found"] += type_stats.get("batch_files_found", 0)
                payer_stats["files_consolidated"] += type_stats.get("files_consolidated", 0)
                payer_stats["bytes_processed"] += type_stats.get("bytes_processed", 0)
                
            except Exception as e:
                logger.error("failed_consolidating_data_type", 
                           payer=payer_name, data_type=data_type, error=str(e))
        
        payer_stats["processing_time"] = time.time() - payer_stats["start_time"]
        return payer_stats
    
    def consolidate_data_type(self, payer_name: str, data_type: str) -> Dict[str, Any]:
        """Consolidate all batch files for a specific payer and data type."""
        logger.info("consolidating_data_type", payer=payer_name, data_type=data_type)
        
        type_stats = {
            "batch_files_found": 0,
            "files_consolidated": 0,
            "bytes_processed": 0,
            "records_combined": 0
        }
        
        # Find all batch files for this payer and data type
        batch_files = self.list_batch_files(payer_name, data_type)
        type_stats["batch_files_found"] = len(batch_files)
        
        if not batch_files:
            logger.warning("no_batch_files_found", payer=payer_name, data_type=data_type)
            return type_stats
        
        logger.info("found_batch_files", 
                   payer=payer_name, data_type=data_type, count=len(batch_files))
        
        # Download and combine files in chunks to manage memory
        all_dfs = []
        processed_files = []
        
        # Process files in chunks
        for i in range(0, len(batch_files), self.config.chunk_size):
            chunk_files = batch_files[i:i + self.config.chunk_size]
            
            logger.info("processing_chunk", 
                       payer=payer_name, data_type=data_type, 
                       chunk_start=i, chunk_size=len(chunk_files))
            
            # Download and read chunk files
            chunk_dfs = []
            for s3_key in tqdm(chunk_files, desc=f"Downloading {data_type} chunk"):
                try:
                    df = self.download_and_read_parquet(s3_key)
                    if df is not None and not df.empty:
                        chunk_dfs.append(df)
                        type_stats["bytes_processed"] += self.get_s3_object_size(s3_key)
                        processed_files.append(s3_key)
                        
                except Exception as e:
                    logger.error("failed_downloading_file", s3_key=s3_key, error=str(e))
            
            # Combine chunk dataframes
            if chunk_dfs:
                chunk_combined = pd.concat(chunk_dfs, ignore_index=True)
                all_dfs.append(chunk_combined)
                type_stats["records_combined"] += len(chunk_combined)
                
                # Clear chunk dataframes to free memory
                del chunk_dfs
                del chunk_combined
        
        # Combine all chunks
        if all_dfs:
            logger.info("combining_all_chunks", 
                       payer=payer_name, data_type=data_type, chunks=len(all_dfs))
            
            final_df = pd.concat(all_dfs, ignore_index=True)
            
            # Deduplicate based on UUID
            uuid_col = f"{data_type.rstrip('s')}_uuid"
            if uuid_col in final_df.columns:
                original_count = len(final_df)
                final_df = final_df.drop_duplicates(subset=[uuid_col])
                deduped_count = len(final_df)
                logger.info("deduplicated_records", 
                           payer=payer_name, data_type=data_type,
                           original=original_count, final=deduped_count,
                           duplicates_removed=original_count - deduped_count)
            
            # Upload consolidated file
            consolidated_key = f"{self.config.consolidated_prefix}/{payer_name}/{data_type}_final.parquet"
            success = self.upload_consolidated_file(final_df, consolidated_key, data_type)
            
            if success:
                type_stats["files_consolidated"] = 1
                logger.info("uploaded_consolidated_file", 
                           payer=payer_name, data_type=data_type,
                           s3_key=consolidated_key, records=len(final_df))
                
                # Optionally cleanup original batch files
                if self.config.cleanup_original_files:
                    self.cleanup_batch_files(processed_files)
            
            # Clear final dataframe to free memory
            del final_df
        
        return type_stats
    
    def list_batch_files(self, payer_name: str, data_type: str) -> List[str]:
        """List all batch files for a specific payer and data type."""
        batch_files = []
        
        try:
            # List all objects with the pattern: {prefix}/{data_type}/payer={payer_name}/date=*/{filename}
            prefix = f"{self.config.s3_prefix}/{data_type}/payer={payer_name}/"
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.config.s3_bucket,
                Prefix=prefix
            )
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        # Only include parquet files that are batch files (not final files)
                        if key.endswith('.parquet') and 'batch_' in key:
                            batch_files.append(key)
        
        except Exception as e:
            logger.error("failed_listing_batch_files", 
                        payer=payer_name, data_type=data_type, error=str(e))
        
        return sorted(batch_files)
    
    def download_and_read_parquet(self, s3_key: str) -> Optional[pd.DataFrame]:
        """Download and read a parquet file from S3."""
        try:
            # Create temporary file
            temp_file = Path(self.temp_dir) / f"temp_{int(time.time())}_{hash(s3_key)}.parquet"
            
            # Download file from S3
            self.s3_client.download_file(self.config.s3_bucket, s3_key, str(temp_file))
            
            # Read parquet file
            df = pd.read_parquet(temp_file)
            
            # Clean up temp file
            temp_file.unlink()
            
            return df
            
        except Exception as e:
            logger.error("failed_downloading_parquet", s3_key=s3_key, error=str(e))
            return None
    
    def get_s3_object_size(self, s3_key: str) -> int:
        """Get the size of an S3 object in bytes."""
        try:
            response = self.s3_client.head_object(Bucket=self.config.s3_bucket, Key=s3_key)
            return response['ContentLength']
        except Exception:
            return 0
    
    def upload_consolidated_file(self, df: pd.DataFrame, s3_key: str, data_type: str) -> bool:
        """Upload a consolidated DataFrame to S3."""
        try:
            # Create temporary parquet file
            temp_file = Path(self.temp_dir) / f"consolidated_{data_type}_{int(time.time())}.parquet"
            
            # Write DataFrame to parquet
            df.to_parquet(temp_file, index=False, compression='snappy')
            
            # Upload to S3
            self.s3_client.upload_file(str(temp_file), self.config.s3_bucket, s3_key)
            
            # Get file size for stats
            file_size = temp_file.stat().st_size
            
            # Clean up temp file
            temp_file.unlink()
            
            logger.info("uploaded_consolidated_file",
                       s3_key=s3_key,
                       records=len(df),
                       file_size_mb=file_size / 1024 / 1024,
                       data_type=data_type)
            
            return True
            
        except Exception as e:
            logger.error("failed_uploading_consolidated_file",
                        s3_key=s3_key,
                        data_type=data_type,
                        records=len(df),
                        error=str(e))
            return False
    
    def cleanup_batch_files(self, batch_files: List[str]):
        """Delete original batch files after successful consolidation."""
        if not self.config.cleanup_original_files:
            return
        
        logger.info("cleaning_up_batch_files", count=len(batch_files))
        
        # Delete files in batches to avoid overwhelming S3
        batch_size = 100
        for i in range(0, len(batch_files), batch_size):
            batch = batch_files[i:i + batch_size]
            
            try:
                # Create delete objects request
                delete_objects = [{'Key': key} for key in batch]
                
                response = self.s3_client.delete_objects(
                    Bucket=self.config.s3_bucket,
                    Delete={'Objects': delete_objects}
                )
                
                deleted_count = len(response.get('Deleted', []))
                logger.info("deleted_batch_files", 
                           batch_start=i, batch_size=len(batch), 
                           deleted=deleted_count)
                
            except Exception as e:
                logger.error("failed_deleting_batch_files", 
                           batch_start=i, batch_size=len(batch), error=str(e))
    
    def cleanup_temp_directory(self):
        """Clean up temporary directory."""
        try:
            import shutil
            shutil.rmtree(self.temp_dir, ignore_errors=True)
            logger.info("cleaned_temp_directory", temp_dir=str(self.temp_dir))
        except Exception as e:
            logger.warning("failed_cleaning_temp_directory", temp_dir=str(self.temp_dir), error=str(e))
    
    def log_final_statistics(self):
        """Log final consolidation statistics."""
        processing_time = datetime.now(timezone.utc) - self.stats["start_time"]
        
        final_stats = {
            **self.stats,
            "processing_time_seconds": processing_time.total_seconds(),
            "completion_time": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info("consolidation_completed", final_stats=final_stats)
        
        # Save statistics to S3
        stats_key = f"{self.config.consolidated_prefix}/consolidation_statistics_{int(time.time())}.json"
        try:
            temp_stats_file = Path(self.temp_dir) / "consolidation_statistics.json"
            with open(temp_stats_file, 'w') as f:
                json.dump(final_stats, f, indent=2, default=str)
            
            self.s3_client.upload_file(str(temp_stats_file), self.config.s3_bucket, stats_key)
            logger.info("uploaded_stats_to_s3", s3_key=stats_key)
            
            temp_stats_file.unlink()
        except Exception as e:
            logger.error("failed_to_upload_stats", error=str(e))


def create_consolidation_config() -> ConsolidationConfig:
    """Create consolidation configuration from environment and defaults."""
    return ConsolidationConfig(
        s3_bucket=os.getenv("S3_BUCKET", "commercial-rates"),
        s3_prefix=os.getenv("S3_PREFIX", "tic-mrf"),
        consolidated_prefix=os.getenv("CONSOLIDATED_PREFIX", "tic-mrf/consolidated"),
        max_workers=int(os.getenv("MAX_WORKERS", "4")),
        chunk_size=int(os.getenv("CHUNK_SIZE", "1000")),
        cleanup_original_files=os.getenv("CLEANUP_ORIGINAL_FILES", "false").lower() == "true",
        dry_run=os.getenv("DRY_RUN", "false").lower() == "true"
    )


def main():
    """Main entry point for S3 batch consolidation."""
    try:
        # Load configuration
        config = create_consolidation_config()
        
        # Initialize consolidator
        consolidator = S3BatchConsolidator(config)
        
        # Consolidate all payers
        consolidator.consolidate_all_payers()
        
    except Exception as e:
        logger.error(f"Consolidation failed: {str(e)}")
        raise


if __name__ == "__main__":
    main() 