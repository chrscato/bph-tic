#!/usr/bin/env python3
"""Production ETL Pipeline with enhanced progress tracking."""

import os
import sys
import time
import json
import psutil
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Iterator
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import boto3

from tic_mrf_scraper.fetch.blobs import analyze_index_structure
from tic_mrf_scraper.payers import get_handler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ProgressData:
    """Progress tracking data structure."""
    current: int
    total: int
    payer: str
    records: int
    rate: float
    memory_mb: float
    stage: str
    time_remaining: float

    def to_csv(self) -> str:
        """Convert to CSV format for progress file."""
        return f"{self.current},{self.total},{self.payer},{self.records},{self.rate:.1f},{self.memory_mb:.1f},{self.stage},{self.time_remaining:.1f}"

class ProgressTracker:
    """Track and display ETL progress."""
    
    def __init__(self, progress_file: str, verbosity: str = "progress"):
        self.progress_file = progress_file
        self.verbosity = verbosity
        self.start_time = time.time()
        self.last_update = self.start_time
        self.last_records = 0
        self.pbar = None
    
    def update(self, data: ProgressData):
        """Update progress and write to file."""
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        # Calculate processing rate
        if elapsed > 0:
            data.rate = (data.records - self.last_records) / (current_time - self.last_update)
        
        # Estimate time remaining
        if data.rate > 0:
            data.time_remaining = (data.total - data.current) / data.rate
        
        # Update memory usage
        process = psutil.Process()
        data.memory_mb = process.memory_info().rss / 1024 / 1024
        
        # Write progress to file
        with open(self.progress_file, 'w') as f:
            f.write(data.to_csv())
        
        # Update progress bar if in progress mode
        if self.verbosity == "progress" and self.pbar is None:
            self.pbar = tqdm(
                total=data.total,
                desc=f"Processing {data.payer}",
                unit="records",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
            )
        
        if self.pbar:
            self.pbar.update(data.current - self.pbar.n)
            self.pbar.set_postfix({
                "rate": f"{data.rate:.1f} rec/s",
                "memory": f"{data.memory_mb:.1f} MB"
            })
        
        self.last_update = current_time
        self.last_records = data.records
    
    def close(self):
        """Close progress bar."""
        if self.pbar:
            self.pbar.close()

class ProductionETLPipelineQuiet:
    """Memory-efficient production ETL pipeline with progress tracking."""
    
    def __init__(self, config: Dict[str, Any], progress_file: str, verbosity: str = "progress"):
        self.config = config
        self.progress_tracker = ProgressTracker(progress_file, verbosity)
        self.s3_client = boto3.client('s3') if config.get('s3_bucket') else None
        
        # Initialize output directories
        self.setup_output_structure()
        
        # Processing statistics
        self.stats = {
            "payers_processed": 0,
            "files_processed": 0,
            "records_extracted": 0,
            "records_validated": 0,
            "processing_start": datetime.now(timezone.utc),
            "errors": []
        }
    
    def setup_output_structure(self):
        """Create local output directory structure."""
        base_dir = Path(self.config['local_output_dir'])
        for subdir in ["payers", "organizations", "providers", "rates", "analytics"]:
            (base_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    def process_all_payers(self):
        """Process all configured payers with progress tracking."""
        total_payers = len(self.config['payer_endpoints'])
        
        for i, (payer_name, index_url) in enumerate(self.config['payer_endpoints'].items(), 1):
            try:
                # Update progress
                self.progress_tracker.update(ProgressData(
                    current=i,
                    total=total_payers,
                    payer=payer_name,
                    records=self.stats["records_extracted"],
                    rate=0,
                    memory_mb=0,
                    stage="payer_processing",
                    time_remaining=0
                ))
                
                # Process payer
                self.process_payer(payer_name, index_url)
                self.stats["payers_processed"] += 1
                
            except Exception as e:
                logger.error(f"Failed processing {payer_name}: {str(e)}")
                self.stats["errors"].append(f"{payer_name}: {str(e)}")
        
        # Generate final outputs
        self.generate_aggregated_tables()
        self.upload_to_s3()
        self.save_statistics()
        
        # Close progress tracker
        self.progress_tracker.close()
    
    def process_payer(self, payer_name: str, index_url: str):
        """Process a single payer's MRF data with progress tracking."""
        # Get MRF files list using handler
        handler = get_handler(payer_name)
        mrf_files = handler.list_mrf_files(index_url)
        rate_files = [f for f in mrf_files if f["type"] == "in_network_rates"]
        
        if self.config.get('max_files_per_payer'):
            rate_files = rate_files[:self.config['max_files_per_payer']]
        
        total_files = len(rate_files)
        
        for i, file_info in enumerate(rate_files, 1):
            try:
                # Update progress
                self.progress_tracker.update(ProgressData(
                    current=i,
                    total=total_files,
                    payer=payer_name,
                    records=self.stats["records_extracted"],
                    rate=0,
                    memory_mb=0,
                    stage="file_processing",
                    time_remaining=0
                ))
                
                # Process file
                self.process_mrf_file(payer_name, file_info, handler)
                self.stats["files_processed"] += 1
                
            except Exception as e:
                logger.error(f"Failed processing file {file_info['url']}: {str(e)}")
                self.stats["errors"].append(f"{payer_name} - {file_info['url']}: {str(e)}")
    
    def process_mrf_file(self, payer_name: str, file_info: Dict[str, Any], handler):
        """Process a single MRF file with progress tracking."""
        records_processed = 0
        batch_size = self.config.get('batch_size', 10000)
        
        # Process records with streaming parser
        for raw_record in stream_parse_enhanced(
            file_info["url"],
            payer_name,
            file_info.get("provider_reference_url"),
            handler
        ):
            records_processed += 1
            
            # Update progress
            self.progress_tracker.update(ProgressData(
                current=records_processed,
                total=self.config.get('max_records_per_file', float('inf')),
                payer=payer_name,
                records=self.stats["records_extracted"] + records_processed,
                rate=0,
                memory_mb=0,
                stage="record_processing",
                time_remaining=0
            ))
            
            # Apply record limits
            if (self.config.get('max_records_per_file') and 
                records_processed > self.config['max_records_per_file']):
                break
            
            # Process record
            try:
                normalized = normalize_tic_record(
                    raw_record, 
                    set(self.config['cpt_whitelist']), 
                    payer_name
                )
                
                if normalized:
                    self.stats["records_validated"] += 1
                    # Process normalized record...
                    
            except Exception as e:
                logger.error(f"Failed processing record: {str(e)}")
                self.stats["errors"].append(f"Record processing error: {str(e)}")
        
        self.stats["records_extracted"] += records_processed
    
    def generate_aggregated_tables(self):
        """Generate final aggregated tables with progress tracking."""
        tables = ["rates", "organizations", "providers", "payers", "analytics"]
        
        for i, table in enumerate(tables, 1):
            self.progress_tracker.update(ProgressData(
                current=i,
                total=len(tables),
                payer="aggregation",
                records=self.stats["records_extracted"],
                rate=0,
                memory_mb=0,
                stage="table_generation",
                time_remaining=0
            ))
            
            self.combine_table_files(table)
    
    def upload_to_s3(self):
        """Upload final files to S3 with progress tracking."""
        if not self.s3_client or not self.config.get('s3_bucket'):
            return
        
        base_dir = Path(self.config['local_output_dir'])
        final_files = list(base_dir.rglob("*_final.parquet"))
        
        for i, local_file in enumerate(final_files, 1):
            self.progress_tracker.update(ProgressData(
                current=i,
                total=len(final_files),
                payer="s3_upload",
                records=self.stats["records_extracted"],
                rate=0,
                memory_mb=0,
                stage="s3_upload",
                time_remaining=0
            ))
            
            try:
                s3_key = f"{self.config['s3_prefix']}/{local_file.relative_to(base_dir)}"
                self.s3_client.upload_file(str(local_file), self.config['s3_bucket'], s3_key)
            except Exception as e:
                logger.error(f"Failed S3 upload {local_file}: {str(e)}")
                self.stats["errors"].append(f"S3 upload error: {str(e)}")
    
    def save_statistics(self):
        """Save processing statistics."""
        stats_file = Path(self.config['local_output_dir']) / "processing_statistics.json"
        with open(stats_file, 'w') as f:
            json.dump(self.stats, f, indent=2, default=str)

def main():
    """Run the production ETL pipeline."""
    # Get progress file from environment
    progress_file = os.getenv('ETL_PROGRESS_FILE')
    if not progress_file:
        print("Error: ETL_PROGRESS_FILE environment variable not set")
        sys.exit(1)
    
    # Get verbosity level
    verbosity = os.getenv('ETL_VERBOSITY', 'progress')
    
    # Load configuration
    config_file = Path('production_config.yaml')
    if not config_file.exists():
        print("Error: production_config.yaml not found")
        sys.exit(1)
    
    import yaml
    with open(config_file) as f:
        config = yaml.safe_load(f)
    
    # Run pipeline
    pipeline = ProductionETLPipelineQuiet(config, progress_file, verbosity)
    pipeline.process_all_payers()

if __name__ == "__main__":
    main() 