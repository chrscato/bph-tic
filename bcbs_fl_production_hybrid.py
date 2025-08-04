#!/usr/bin/env python3
"""
Hybrid production pipeline that combines test script simplicity with production efficiency.
Features early validation and efficient batch processing.
"""

import sys
import os
import json
import pandas as pd
import boto3
import hashlib
import psutil
import gc
from pathlib import Path
from datetime import datetime, timezone
import logging
import tempfile
from typing import Dict, List, Any, Optional
import uuid
from dotenv import load_dotenv
import time
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
import structlog
import requests
import gzip
import shutil

# Load environment variables from .env file
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.payers.bcbs_fl import BCBSFLHandler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()

def log_memory_stats(stage: str):
    """Log detailed memory statistics."""
    process = psutil.Process()
    memory = process.memory_info()
    logger.info("memory_stats",
                stage=stage,
                rss_mb=memory.rss / 1024 / 1024,
                vms_mb=memory.vms / 1024 / 1024,
                percent=process.memory_percent(),
                system_percent=psutil.virtual_memory().percent)

def check_memory_pressure(threshold_mb: int) -> bool:
    """Less aggressive memory check."""
    memory_mb = psutil.Process().memory_info().rss / (1024 * 1024)
    if memory_mb > threshold_mb * 0.95:  # Only act at 95% of threshold
        logger.warning("memory_pressure_detected",
                      current_mb=memory_mb,
                      threshold_mb=threshold_mb)
        return True
    return False

class UUIDGenerator:
    """UUID generation for entity identification."""
    
    @staticmethod
    def generate_uuid(namespace: str, *components: str) -> str:
        content = "|".join(str(c) for c in components)
        namespace_uuid = uuid.uuid5(uuid.NAMESPACE_DNS, f"healthcare.{namespace}")
        return str(uuid.uuid5(namespace_uuid, content))
    
    @staticmethod
    def payer_uuid(payer_name: str, parent_org: str = "") -> str:
        return UUIDGenerator.generate_uuid("payers", payer_name, parent_org)
    
    @staticmethod
    def organization_uuid(tin: str, org_name: str = "") -> str:
        return UUIDGenerator.generate_uuid("organizations", tin, org_name)
    
    @staticmethod
    def provider_uuid(npi: str) -> str:
        return UUIDGenerator.generate_uuid("providers", npi)
    
    @staticmethod
    def rate_uuid(payer_uuid: str, org_uuid: str, service_code: str, 
                  rate: float, effective_date: str) -> str:
        return UUIDGenerator.generate_uuid(
            "rates", payer_uuid, org_uuid, service_code, 
            f"{rate:.2f}", effective_date
        )

class RecordCreator:
    """Handles creation of standardized records."""
    
    def __init__(self, uuid_gen: UUIDGenerator):
        self.uuid_gen = uuid_gen
    
    def create_rate_record(self, payer_uuid: str, normalized: Dict[str, Any], 
                          file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Create a structured rate record."""
        
        # Generate organization UUID
        org_uuid = self.uuid_gen.organization_uuid(
            normalized.get("provider_tin", ""), 
            normalized.get("provider_name", "")
        )
        
        # Generate rate UUID - handle both service_code and service_codes
        service_code = normalized.get("service_code", "")
        if not service_code and normalized.get("service_codes"):
            service_codes = normalized["service_codes"]
            service_code = service_codes[0] if service_codes else ""
        
        rate_uuid = self.uuid_gen.rate_uuid(
            payer_uuid,
            org_uuid,
            service_code,
            normalized["negotiated_rate"],
            normalized.get("expiration_date", "")
        )
        
        # Extract NPI list - handle all formats
        npi_list = normalized.get("provider_npi", [])
        if npi_list is None:
            npi_list = []
        elif isinstance(npi_list, (int, str)):
            npi_list = [str(npi_list)]
        elif isinstance(npi_list, list):
            npi_list = [str(npi) for npi in npi_list]
        else:
            npi_list = []
        
        # Ensure all NPIs are strings
        npi_list = [str(npi) for npi in npi_list]
        
        return {
            "rate_uuid": rate_uuid,
            "payer_uuid": payer_uuid,
            "organization_uuid": org_uuid,
            "service_code": service_code,
            "service_description": normalized.get("description", ""),
            "billing_code_type": normalized.get("billing_code_type", ""),
            "negotiated_rate": float(normalized["negotiated_rate"]),
            "billing_class": normalized.get("billing_class", ""),
            "rate_type": normalized.get("negotiated_type", "negotiated"),
            "service_codes": normalized.get("service_codes", []),
            "plan_details": {
                "plan_name": file_info.get("plan_name", ""),
                "plan_id": file_info.get("plan_id", ""),
                "plan_type": file_info.get("plan_market_type", ""),
                "market_type": "Commercial"
            },
            "contract_period": {
                "effective_date": None,
                "expiration_date": normalized.get("expiration_date"),
                "last_updated_on": None
            },
            "provider_network": {
                "npi_list": npi_list,
                "npi_count": len(npi_list),
                "coverage_type": "Organization"
            },
            "geographic_scope": {
                "states": [],
                "zip_codes": [],
                "counties": []
            },
            "data_lineage": {
                "source_file_url": file_info["url"],
                "source_file_hash": hashlib.md5(file_info["url"].encode()).hexdigest(),
                "extraction_timestamp": datetime.now(timezone.utc),
                "processing_version": "test-v1.0"
            },
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
    
    def create_organization_record(self, normalized: Dict[str, Any]) -> Dict[str, Any]:
        """Create organization record."""
        tin = normalized.get("provider_tin", "")
        org_name = normalized.get("provider_name", "")
        
        org_uuid = self.uuid_gen.organization_uuid(tin, org_name)
        
        # Get NPI list safely - handle all formats
        npi_list = normalized.get("provider_npi", [])
        if npi_list is None:
            npi_list = []
        elif isinstance(npi_list, (int, str)):
            npi_list = [str(npi_list)]
        elif isinstance(npi_list, list):
            npi_list = [str(npi) for npi in npi_list]
        else:
            npi_list = []
        
        # Ensure all NPIs are strings
        npi_list = [str(npi) for npi in npi_list]
        
        return {
            "organization_uuid": org_uuid,
            "tin": tin,
            "organization_name": org_name or f"Organization-{tin}",
            "organization_type": "Unknown",
            "parent_system": "",
            "npi_count": len(npi_list),
            "primary_specialty": "",
            "is_facility": normalized.get("billing_class") == "facility",
            "headquarters_address": {
                "street": "",
                "city": "",
                "state": "",
                "zip": "",
                "lat": None,
                "lng": None
            },
            "service_areas": [],
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "data_quality_score": 0.8
        }
    
    def create_provider_records(self, normalized: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create provider records for NPIs."""
        # Get NPI list safely - handle all formats
        npi_list = normalized.get("provider_npi", [])
        if npi_list is None:
            npi_list = []
        elif isinstance(npi_list, (int, str)):
            npi_list = [str(npi_list)]
        elif isinstance(npi_list, list):
            npi_list = [str(npi) for npi in npi_list]
        else:
            npi_list = []
        
        # Ensure all NPIs are strings
        npi_list = [str(npi) for npi in npi_list]
        
        if not npi_list:
            return []
        
        org_uuid = self.uuid_gen.organization_uuid(
            normalized.get("provider_tin", ""), 
            normalized.get("provider_name", "")
        )
        
        provider_records = []
        for npi in npi_list:
            npi_str = str(npi)
            provider_uuid = self.uuid_gen.provider_uuid(npi_str)
            
            provider_record = {
                "provider_uuid": provider_uuid,
                "npi": npi_str,
                "organization_uuid": org_uuid,
                "provider_name": {
                    "first": "",
                    "last": "",
                    "middle": "",
                    "suffix": ""
                },
                "credentials": [],
                "primary_specialty": "",
                "secondary_specialties": [],
                "provider_type": "Individual",
                "gender": "Unknown",
                "addresses": [],
                "is_active": True,
                "enumeration_date": None,
                "last_updated": None,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            provider_records.append(provider_record)
        
        return provider_records

class S3Uploader:
    """Handles S3 uploads with retries and logging."""
    
    def __init__(self, bucket: str, prefix: str):
        self.bucket = bucket
        self.prefix = prefix.rstrip('/')
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        self.upload_stats = {
            "successful_uploads": 0,
            "failed_uploads": 0,
            "total_bytes": 0
        }
    
    def upload_dataframe(self, df: pd.DataFrame, key: str, temp_dir: str) -> bool:
        try:
            temp_file = Path(temp_dir) / f"temp_{int(datetime.now().timestamp())}.parquet"
            df.to_parquet(temp_file, index=False)
            
            self.s3_client.upload_file(str(temp_file), self.bucket, key)
            
            file_size = temp_file.stat().st_size
            self.upload_stats["successful_uploads"] += 1
            self.upload_stats["total_bytes"] += file_size
            
            temp_file.unlink()
            
            logger.info("s3_upload_success",
                       key=key,
                       size_mb=file_size / 1024 / 1024,
                       total_uploads=self.upload_stats["successful_uploads"])
            return True
            
        except Exception as e:
            self.upload_stats["failed_uploads"] += 1
            logger.error("s3_upload_failed", key=key, error=str(e))
            return False

def get_system_config() -> Dict[str, Any]:
    """Automatically determine optimal settings based on system RAM."""
    total_ram_mb = psutil.virtual_memory().total / (1024 * 1024)
    
    # Log system detection
    logger.info("system_detection", 
                total_ram_gb=f"{total_ram_mb/1024:.1f}GB",
                system_type="high_memory" if total_ram_mb >= 30000 else
                           "medium_memory" if total_ram_mb >= 14000 else
                           "low_memory")
    
    if total_ram_mb >= 30000:  # 32GB or higher system
        return {
            'initial_batch': 1000,
            'main_batch': 10000,
            'memory_threshold': 0.9,  # 90% threshold
            'system_type': 'high_memory'
        }
    elif total_ram_mb >= 14000:  # 16GB system
        return {
            'initial_batch': 500,
            'main_batch': 5000,
            'memory_threshold': 0.8,  # 80% threshold
            'system_type': 'medium_memory'
        }
    else:  # 8GB or less
        return {
            'initial_batch': 250,
            'main_batch': 2500,
            'memory_threshold': 0.7,  # 70% threshold
            'system_type': 'low_memory'
        }

class HybridProcessor:
    """Combines test script simplicity with production efficiency."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.uuid_gen = UUIDGenerator()
        self.record_creator = RecordCreator(self.uuid_gen)
        self.s3_uploader = S3Uploader(
            config['s3_bucket'],
            config['s3_prefix']
        )
        
        # Get system-specific settings
        system_config = get_system_config()
        
        # Processing settings
        self.early_validation_count = 5  # Upload first 5 records immediately
        self.initial_batch_size = system_config['initial_batch']
        self.main_batch_size = system_config['main_batch']
        self.memory_threshold_mb = int(psutil.virtual_memory().total / 1024 / 1024 * system_config['memory_threshold'])
        
        logger.info("processor_configuration",
                   initial_batch_size=self.initial_batch_size,
                   main_batch_size=self.main_batch_size,
                   memory_threshold_gb=f"{self.memory_threshold_mb/1024:.1f}GB",
                   system_type=system_config['system_type'])
        
        # Initialize collectors
        self.reset_collectors()
    
    def reset_collectors(self):
        """Reset data collectors."""
        self.rate_batch = []
        self.org_batch = []
        self.provider_batch = []
        self.seen_orgs = set()
    
    def process_record(self, normalized: Dict[str, Any], file_info: Dict[str, Any],
                      payer_uuid: str) -> bool:
        """Process a single normalized record."""
        try:
            # Create rate record
            rate_record = self.record_creator.create_rate_record(
                payer_uuid, normalized, file_info
            )
            self.rate_batch.append(rate_record)
            
            # Create organization record if new
            org_uuid = rate_record["organization_uuid"]
            if org_uuid not in self.seen_orgs:
                org_record = self.record_creator.create_organization_record(normalized)
                self.org_batch.append(org_record)
                self.seen_orgs.add(org_uuid)
            
            # Create provider records
            provider_records = self.record_creator.create_provider_records(normalized)
            self.provider_batch.extend(provider_records)
            
            return True
        except Exception as e:
            logger.error("record_processing_failed", error=str(e))
            return False
    
    def upload_batch(self, timestamp: str, current_date: str, 
                    payer_name: str, batch_num: int) -> bool:
        """Upload current batch to S3."""
        try:
            # Convert to DataFrames
            rates_df = pd.DataFrame(self.rate_batch) if self.rate_batch else None
            orgs_df = pd.DataFrame(self.org_batch) if self.org_batch else None
            providers_df = pd.DataFrame(self.provider_batch) if self.provider_batch else None
            
            # Upload each non-empty DataFrame
            if rates_df is not None:
                key = f"{self.config['s3_prefix']}/rates/payer={payer_name}/date={current_date}/rates_batch_{batch_num:04d}_{timestamp}.parquet"
                self.s3_uploader.upload_dataframe(rates_df, key, self.config['temp_dir'])
            
            if orgs_df is not None:
                key = f"{self.config['s3_prefix']}/organizations/payer={payer_name}/date={current_date}/orgs_batch_{batch_num:04d}_{timestamp}.parquet"
                self.s3_uploader.upload_dataframe(orgs_df, key, self.config['temp_dir'])
            
            if providers_df is not None:
                key = f"{self.config['s3_prefix']}/providers/payer={payer_name}/date={current_date}/providers_batch_{batch_num:04d}_{timestamp}.parquet"
                self.s3_uploader.upload_dataframe(providers_df, key, self.config['temp_dir'])
            
            # Reset collectors
            self.reset_collectors()
            return True
            
        except Exception as e:
            logger.error("batch_upload_failed", error=str(e))
            return False
    
    def download_with_retry(self, url: str, max_retries: int = 3, delay: int = 5) -> bytes:
        """Download file with retries."""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, application/octet-stream'
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=300,
                    stream=True
                )
                response.raise_for_status()
                
                chunks = []
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        chunks.append(chunk)
                
                content = b''.join(chunks)
                logger.info("download_successful",
                           attempt=attempt + 1,
                           size_mb=len(content)/1024/1024)
                return content
                
            except Exception as e:
                logger.warning("download_attempt_failed",
                             attempt=attempt + 1,
                             max_retries=max_retries,
                             error=str(e))
                if attempt < max_retries - 1:
                    time.sleep(delay)
                else:
                    raise
    
    def process_mrf_file(self, file_info: Dict[str, Any], handler, payer_name: str,
                        payer_uuid: str, cpt_whitelist: set) -> Dict[str, Any]:
        """Process a single MRF file with early validation and efficient batching."""
        # Set timeouts and limits - optimized for Pareto efficiency
        FILE_PROCESSING_TIMEOUT = 600    # 10 minutes max per file
        INITIAL_PROGRESS_TIMEOUT = 60    # Expect first record within 1 minute
        PROGRESS_STALL_TIMEOUT = 180     # Skip if no progress for 3 minutes
        
        # Pareto-optimized limits (focus on files we can process efficiently)
        MAX_FILE_SIZE_MB = 4000         # 4GB limit for better throughput
        MEMORY_HEADROOM_PCT = 25        # 25% memory headroom
        
        # Track skipped files for analysis
        self.skipped_files = getattr(self, 'skipped_files', {
            'too_large': [],
            'timeout': [],
            'memory_pressure': []
        })
            
        logger.info("memory_configuration",
                   total_ram_gb=f"{total_ram/1024:.1f}GB",
                   max_file_size_gb=f"{MAX_FILE_SIZE_MB/1024:.1f}GB",
                   headroom_pct=MEMORY_HEADROOM_PCT)
        
        stats = {
            "records_processed": 0,
            "records_normalized": 0,
            "batches_uploaded": 0,
            "start_time": time.time(),
            "last_progress": time.time()
        }
        
        try:
            # Check file size before downloading
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json, application/octet-stream'
            }
            
            try:
                # First, do a HEAD request to check file size
                head_response = requests.head(file_info["url"], headers=headers, timeout=30)
                file_size_mb = int(head_response.headers.get('content-length', 0)) / (1024 * 1024)
                
                # Check if file is too large
                if file_size_mb > MAX_FILE_SIZE_MB:
                    logger.warning("file_too_large",
                                url=file_info["url"],
                                size_mb=f"{file_size_mb:.1f}MB",
                                limit_mb=MAX_FILE_SIZE_MB)
                    self.skipped_files['too_large'].append({
                        'url': file_info["url"],
                        'size_mb': file_size_mb,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                    return stats
                
                # Check available memory - more lenient for 32GB systems
                mem = psutil.virtual_memory()
                available_mb = mem.available / (1024 * 1024)
                # On 32GB systems, we need less buffer since we have more RAM
                buffer_multiplier = 1.2 if total_ram >= 30000 else 1.5
                required_mb = file_size_mb * buffer_multiplier
                
                if available_mb < required_mb * (1 + MEMORY_HEADROOM_PCT/100):
                    logger.warning("insufficient_memory",
                                url=file_info["url"],
                                file_size_mb=f"{file_size_mb:.1f}MB",
                                available_mb=f"{available_mb:.1f}MB",
                                required_mb=f"{required_mb:.1f}MB")
                    return stats
                
                logger.info("downloading_file", 
                          url=file_info["url"],
                          size_mb=f"{file_size_mb:.1f}MB",
                          available_memory_mb=f"{available_mb:.1f}MB")
                
                # Download file with streaming and progress tracking
                response = requests.get(
                    file_info["url"], 
                    headers=headers,
                    timeout=300,
                    stream=True
                )
                response.raise_for_status()
                
                # Read content with memory monitoring
                chunks = []
                total_size = 0
                chunk_size = 8192
                
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        chunks.append(chunk)
                        total_size += len(chunk)
                        
                        # Check memory every 100MB
                        if total_size % (100 * 1024 * 1024) == 0:
                            if check_memory_pressure(self.memory_threshold_mb):
                                logger.warning("memory_pressure_during_download",
                                           url=file_info["url"],
                                           downloaded_mb=f"{total_size/1024/1024:.1f}MB")
                                return stats
                
                raw_content = b''.join(chunks)
                logger.info("download_complete", 
                           size_mb=len(raw_content)/1024/1024,
                           is_gzipped=file_info["url"].endswith('.gz'))
                
                try:
                    # Handle gzip content
                    if file_info["url"].endswith('.gz') or '.gz?' in file_info["url"]:
                        try:
                            content = gzip.decompress(raw_content).decode('utf-8')
                        except Exception as e:
                            logger.error("gzip_decompression_failed", error=str(e))
                            # Try without decompression as fallback
                            content = raw_content.decode('utf-8')
                    else:
                        content = raw_content.decode('utf-8')
                    
                    # Validate JSON before parsing
                    if not content.strip():
                        raise ValueError("Empty content received")
                    
                    if not content.strip().startswith('{'):
                        logger.error("invalid_json_content", 
                                   preview=content[:100] if content else "empty")
                        raise ValueError("Content is not valid JSON")
                    
                    mrf_data = json.loads(content)
                    logger.info("json_parsed_successfully", 
                              top_level_keys=list(mrf_data.keys()))
                    
                except json.JSONDecodeError as e:
                    logger.error("json_parse_failed", 
                               error=str(e),
                               content_preview=content[:200] if content else "empty")
                    raise
                
            except requests.exceptions.RequestException as e:
                logger.error("download_failed", 
                           error=str(e),
                           url=file_info["url"])
                raise
            
            # Process in-network items with detailed logging
            in_network = mrf_data.get('in_network', [])
            logger.info("processing_in_network_items",
                       total_items=len(in_network),
                       provider_refs=len(mrf_data.get('provider_references', [])))
            
            # Log sample of first item for debugging
            if in_network:
                sample_item = in_network[0]
                logger.info("first_item_sample",
                           billing_code=sample_item.get('billing_code'),
                           negotiation_arrangement=sample_item.get('negotiation_arrangement'),
                           keys=list(sample_item.keys()))
            
            current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            batch_num = 0
            
            for item_idx, item in enumerate(in_network):
                try:
                    parsed_records = handler.parse_in_network(item)
                    logger.info("item_parsed",
                              item_idx=item_idx,
                              billing_code=item.get('billing_code', 'N/A'),
                              parsed_count=len(parsed_records) if parsed_records else 0,
                              negotiation_arrangement=item.get('negotiation_arrangement'))
                    
                    for parsed in parsed_records:
                        try:
                            normalized = normalize_tic_record(parsed, cpt_whitelist, payer_name)
                            if normalized:
                                logger.info("record_normalized",
                                          item_idx=item_idx,
                                          billing_code=normalized.get('billing_code'),
                                          rate=normalized.get('negotiated_rate'))
                            else:
                                # Log more details about skipped records
                                logger.warning("normalization_skipped",
                                             item_idx=item_idx,
                                             billing_code=parsed.get('billing_code'),
                                             billing_code_type=parsed.get('billing_code_type'),
                                             negotiated_rate=parsed.get('negotiated_rate'),
                                             in_whitelist=parsed.get('billing_code') in cpt_whitelist,
                                             reason="normalize_tic_record returned None")
                                
                                # Log every 1000th skip for monitoring
                                if stats["records_normalized"] % 1000 == 0:
                                    logger.info("normalization_stats",
                                              total_processed=stats["records_processed"],
                                              total_normalized=stats["records_normalized"],
                                              skip_rate=f"{(stats['records_processed'] - stats['records_normalized'])/stats['records_processed']*100:.1f}%")
                                continue
                        except Exception as e:
                            logger.error("normalization_failed",
                                       item_idx=item_idx,
                                       billing_code=parsed.get('billing_code'),
                                       error=str(e))
                            continue
                            
                        stats["records_normalized"] += 1
                        stats["last_progress"] = time.time()
                        
                        # Check timeouts
                        time_elapsed = time.time() - stats["start_time"]
                        time_since_progress = time.time() - stats["last_progress"]
                        
                        # Skip file if taking too long overall
                        if time_elapsed > FILE_PROCESSING_TIMEOUT:
                            logger.warning("file_processing_timeout",
                                       file_url=file_info["url"],
                                       time_elapsed=f"{time_elapsed/60:.1f}m",
                                       records_processed=stats["records_processed"])
                            return stats
                        
                        # Skip file if no initial progress in first minute
                        if stats["records_normalized"] == 0 and time_elapsed > INITIAL_PROGRESS_TIMEOUT:
                            logger.warning("no_initial_progress_timeout",
                                       file_url=file_info["url"],
                                       time_elapsed=f"{time_elapsed/60:.1f}m")
                            return stats
                            
                        # Skip file if stalled (no progress for 5 minutes)
                        if time_since_progress > PROGRESS_STALL_TIMEOUT and stats["records_normalized"] > 0:
                            logger.warning("progress_stalled",
                                       file_url=file_info["url"],
                                       time_since_last_record=f"{time_since_progress/60:.1f}m",
                                       records_processed=stats["records_processed"])
                            return stats
                        
                        # Early validation phase
                        if stats["records_normalized"] <= self.early_validation_count:
                            self.process_record(normalized, file_info, payer_uuid)
                            
                            # Upload immediately for validation
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            self.upload_batch(timestamp, current_date, payer_name, batch_num)
                            batch_num += 1
                            stats["batches_uploaded"] += 1
                            
                            logger.info("early_validation_upload",
                                      record_num=stats["records_normalized"])
                            
                        else:
                            # Regular processing phase
                            self.process_record(normalized, file_info, payer_uuid)
                            
                            # Determine batch size
                            current_batch_size = (
                                self.initial_batch_size 
                                if stats["records_normalized"] < 10000
                                else self.main_batch_size
                            )
                            
                            # Upload batch if full
                            if len(self.rate_batch) >= current_batch_size:
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                self.upload_batch(timestamp, current_date, payer_name, batch_num)
                                batch_num += 1
                                stats["batches_uploaded"] += 1
                                
                                # Check memory pressure
                                if check_memory_pressure(self.memory_threshold_mb):
                                    gc.collect()
                                    log_memory_stats("post_batch_upload")
                        
                        stats["records_processed"] += 1
                        
                except Exception as e:
                    logger.error("item_processing_failed",
                               item_idx=item_idx,
                               error=str(e))
                    continue
            
            # Upload final batch
            if self.rate_batch:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                self.upload_batch(timestamp, current_date, payer_name, batch_num)
                stats["batches_uploaded"] += 1
            
            stats["processing_time"] = time.time() - stats["start_time"]
            logger.info("file_processing_completed", stats=stats)
            return stats
            
        except Exception as e:
            logger.error("file_processing_failed", error=str(e))
            return stats

def main():
    """Main entry point."""
    # Load configuration
    config = {
        's3_bucket': os.getenv('S3_BUCKET', 'commercial-rates'),
        's3_prefix': os.getenv('S3_PREFIX', 'healthcare-rates-test'),
        'temp_dir': tempfile.mkdtemp(prefix="hybrid_processor_"),
        'cpt_whitelist': {
            "99213", "99214", "72148", "73721", "70450",  # Common codes
            "0001U", "0002M", "0003M", "0004M", "0004U",  # Test codes
            "0008U", "0009U", "001", "0010U", "0016U"     # Additional codes
        }
    }
    
    try:
        # Initialize processor
        processor = HybridProcessor(config)
        
        # Initialize handler
        handler = BCBSFLHandler()
        logger.info("handler_initialized", handler_type=type(handler).__name__)
        
        # Get BCBS FL files
        bcbs_fl_url = "https://d1hgtx7rrdl2cn.cloudfront.net/mrf/toc/FloridaBlue_Health-Insurance-Issuer_index.json"
        mrf_files = list(list_mrf_blobs_enhanced(bcbs_fl_url))
        logger.info("files_discovered", count=len(mrf_files))
        
        if not mrf_files:
            logger.error("no_files_found")
            return
        
        # Process files
        payer_name = "bcbs_fl"
        payer_uuid = processor.uuid_gen.payer_uuid(payer_name)
        
        total_stats = {
            "files_processed": 0,
            "total_records": 0,
            "total_batches": 0,
            "start_time": time.time()
        }
        
        for file_idx, file_info in enumerate(mrf_files):
            logger.info("processing_file",
                       file_num=file_idx + 1,
                       total_files=len(mrf_files),
                       url=file_info["url"])
            
            stats = processor.process_mrf_file(
                file_info, handler, payer_name, payer_uuid, config['cpt_whitelist']
            )
            
            total_stats["files_processed"] += 1
            total_stats["total_records"] += stats["records_processed"]
            total_stats["total_batches"] += stats["batches_uploaded"]
            
            log_memory_stats(f"after_file_{file_idx + 1}")
        
        total_stats["total_time"] = time.time() - total_stats["start_time"]
        logger.info("processing_completed", stats=total_stats)
        
    finally:
        # Cleanup
        shutil.rmtree(config['temp_dir'], ignore_errors=True)

if __name__ == "__main__":
    main()