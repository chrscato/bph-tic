#!/usr/bin/env python3
"""Production ETL Pipeline for Healthcare Rates Data with Full Index Processing and S3 Upload."""

import os
import uuid
import hashlib
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Iterator
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
from tqdm import tqdm
import logging
import structlog
import yaml

from tic_mrf_scraper.fetch.blobs import analyze_index_structure
from tic_mrf_scraper.payers import get_handler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger
from tic_mrf_scraper.diagnostics import (
    identify_index,
    detect_compression,
    identify_in_network,
)

# Configure logging levels - suppress all debug output
logging.getLogger('tic_mrf_scraper.stream.parser').setLevel(logging.WARNING)
logging.getLogger('tic_mrf_scraper.fetch.blobs').setLevel(logging.WARNING)
logging.getLogger('tic_mrf_scraper.transform.normalize').setLevel(logging.WARNING)

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

# Get logger for main pipeline
logger = get_logger(__name__)

# Global progress tracking
class ProgressTracker:
    def __init__(self):
        self.current_payer = ""
        self.files_completed = 0
        self.total_files = 0
        self.records_processed = 0
        self.start_time = time.time()
        self.pbar = None
    
    def update_progress(self, payer: str, files_completed: int, total_files: int, 
                       records_processed: int):
        if self.pbar is None:
            self.pbar = tqdm(total=total_files, desc=f"Processing {payer}", 
                           unit="files", leave=True)
        
        self.current_payer = payer
        self.files_completed = files_completed
        self.total_files = total_files
        self.records_processed = records_processed
        
        # Calculate processing rate and ETA
        elapsed_time = time.time() - self.start_time
        if elapsed_time > 0:
            rate = records_processed / elapsed_time
            remaining_files = total_files - files_completed
            eta = remaining_files / (files_completed / elapsed_time) if files_completed > 0 else 0
            
            # Update progress bar description
            self.pbar.set_description(
                f"{payer} | {records_processed:,} records | {rate:.1f} rec/s | ETA: {eta:.1f}s"
            )
            self.pbar.update(files_completed - self.pbar.n)
    
    def close(self):
        if self.pbar:
            self.pbar.close()

progress = ProgressTracker()

@dataclass
class ETLConfig:
    """ETL Pipeline Configuration."""
    # Input sources
    payer_endpoints: Dict[str, str]
    cpt_whitelist: List[str]
    
    # Processing configuration
    batch_size: int = 10000
    parallel_workers: int = 2
    max_files_per_payer: Optional[int] = None
    max_records_per_file: Optional[int] = None
    
    # Output configuration
    local_output_dir: str = "ortho_radiology_data_default"
    s3_bucket: Optional[str] = None
    s3_prefix: str = "healthcare-rates-ortho-radiology"
    
    # Data versioning
    schema_version: str = "v2.1.0"
    processing_version: str = "tic-etl-v1.0"
    
    # Quality thresholds
    min_completeness_pct: float = 80.0
    min_accuracy_score: float = 0.85

class UUIDGenerator:
    """Deterministic UUID generation for consistent entity identification."""
    
    @staticmethod
    def generate_uuid(namespace: str, *components: str) -> str:
        """Generate deterministic UUID for deduplication."""
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

class DataQualityValidator:
    """Data quality validation and scoring."""
    
    @staticmethod
    def validate_rate_record(record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and score a rate record."""
        quality_flags = {
            "is_validated": True,
            "has_conflicts": False,
            "confidence_score": 1.0,
            "validation_notes": []
        }
        
        # Required field validation
        required_fields = ["service_code", "negotiated_rate", "payer_uuid", "organization_uuid"]
        missing_fields = [f for f in required_fields if not record.get(f)]
        
        if missing_fields:
            quality_flags["is_validated"] = False
            quality_flags["confidence_score"] -= 0.3
            quality_flags["validation_notes"].append(f"Missing required fields: {missing_fields}")
        
        # Rate reasonableness check
        rate = record.get("negotiated_rate", 0)
        if rate <= 0 or rate > 10000:  # Reasonable rate bounds
            quality_flags["has_conflicts"] = True
            quality_flags["confidence_score"] -= 0.2
            quality_flags["validation_notes"].append(f"Unusual rate value: ${rate}")
        
        # NPI validation
        npi_list = record.get("provider_network", {}).get("npi_list", [])
        if not npi_list:
            quality_flags["confidence_score"] -= 0.1
            quality_flags["validation_notes"].append("No NPIs associated")
        
        quality_flags["validation_notes"] = "; ".join(quality_flags["validation_notes"])
        return quality_flags

class ProductionETLPipeline:
    """Memory-efficient production ETL pipeline with full index processing and S3 upload."""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.uuid_gen = UUIDGenerator()
        self.validator = DataQualityValidator()
        self.s3_client = boto3.client('s3') if config.s3_bucket else None
        
        # Initialize local temp directory for S3 uploads
        if self.s3_client:
            self.temp_dir = tempfile.mkdtemp(prefix="etl_pipeline_")
            logger.info("created_temp_directory", temp_dir=self.temp_dir)
        else:
            self.setup_local_output_structure()
            self.temp_dir = None
        
        # Processing statistics
        self.stats = {
            "payers_processed": 0,
            "total_files_found": 0,
            "files_processed": 0,
            "files_succeeded": 0,
            "files_failed": 0,
            "records_extracted": 0,
            "records_validated": 0,
            "s3_uploads": 0,
            "processing_start": datetime.now(timezone.utc),
            "errors": []
        }
    
    def setup_local_output_structure(self):
        """Create local output directory structure (fallback if no S3)."""
        base_dir = Path(self.config.local_output_dir)
        for subdir in ["payers", "organizations", "providers", "rates", "analytics"]:
            (base_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    def cleanup_temp_directory(self):
        """Clean up temporary directory."""
        if self.temp_dir:
            import shutil
            shutil.rmtree(self.temp_dir, ignore_errors=True)
            logger.info("cleaned_temp_directory", temp_dir=self.temp_dir)
    
    def process_all_payers(self):
        """Process all configured payers with full index processing."""
        logger.info("starting_production_etl_full_index", 
                   payers=len(self.config.payer_endpoints),
                   s3_enabled=bool(self.s3_client),
                   config=asdict(self.config))
        
        try:
            # Process payers sequentially for better resource management
            for payer_name, index_url in self.config.payer_endpoints.items():
                try:
                    payer_stats = self.process_payer(payer_name, index_url)
                    logger.info("completed_payer", payer=payer_name, stats=payer_stats)
                    self.stats["payers_processed"] += 1
                    
                    # Update overall stats
                    self.stats["total_files_found"] += payer_stats.get("files_found", 0)
                    self.stats["files_processed"] += payer_stats.get("files_processed", 0)
                    self.stats["files_succeeded"] += payer_stats.get("files_succeeded", 0)
                    self.stats["files_failed"] += payer_stats.get("files_failed", 0)
                    self.stats["records_extracted"] += payer_stats.get("records_extracted", 0)
                    self.stats["records_validated"] += payer_stats.get("records_validated", 0)
                    
                except Exception as e:
                    error_msg = f"Failed processing {payer_name}: {str(e)}"
                    logger.error("payer_processing_failed", payer=payer_name, error=str(e))
                    self.stats["errors"].append(error_msg)
            
            # Generate final outputs if not using S3
            if not self.s3_client:
                self.generate_aggregated_tables()
            
            self.log_final_statistics()
            
        finally:
            # Always cleanup temp directory
            self.cleanup_temp_directory()
    
    def process_payer(self, payer_name: str, index_url: str) -> Dict[str, Any]:
        """Process a single payer's COMPLETE MRF index with all files."""
        logger.info(f"Starting processing for {payer_name}")
        
        payer_stats = {
            "files_found": 0,
            "files_processed": 0,
            "files_succeeded": 0,
            "files_failed": 0,
            "records_extracted": 0,
            "records_validated": 0,
            "failed_files": [],
            "start_time": time.time()
        }
        
        try:
            # Create payer record
            payer_uuid = self.create_payer_record(payer_name, index_url)

            # Analyze index structure
            index_info = identify_index(index_url)
            payer_stats["index_analysis"] = index_info
            logger.info("index_analysis", payer=payer_name, analysis=index_info)

            # Get ALL MRF files from index using handler
            handler = get_handler(payer_name)
            mrf_files = handler.list_mrf_files(index_url)
            
            # Filter to in-network rates files only
            rate_files = [f for f in mrf_files if f["type"] == "in_network_rates"]
            payer_stats["files_found"] = len(rate_files)

            if self.config.max_files_per_payer:
                rate_files = rate_files[: self.config.max_files_per_payer]

            if not rate_files:
                logger.warning(f"No rate files found for {payer_name}")
                return payer_stats

            logger.info(f"Found {len(rate_files)} rate files for {payer_name}")
            
            # Process ALL rate files
            for file_index, file_info in enumerate(rate_files, 1):
                payer_stats["files_processed"] += 1
                
                try:
                    file_stats = self.process_mrf_file_enhanced(
                        payer_uuid, payer_name, file_info, handler, file_index, len(rate_files)
                    )
                    
                    payer_stats["files_succeeded"] += 1
                    payer_stats["records_extracted"] += file_stats["records_extracted"]
                    payer_stats["records_validated"] += file_stats["records_validated"]
                    
                    # Update progress
                    progress.update_progress(
                        payer=payer_name,
                        files_completed=file_index,
                        total_files=len(rate_files),
                        records_processed=payer_stats["records_extracted"]
                    )
                    
                except Exception as e:
                    error_msg = f"Failed processing file {file_info['url']}: {str(e)}"
                    logger.error(error_msg)
                    payer_stats["files_failed"] += 1
                    payer_stats["failed_files"].append({
                        "url": file_info["url"],
                        "error": str(e)
                    })
            
            # Log completion
            elapsed = time.time() - payer_stats["start_time"]
            logger.info(
                f"Completed {payer_name}: {payer_stats['files_succeeded']}/{payer_stats['files_found']} "
                f"files, {payer_stats['records_extracted']:,} records in {elapsed:.1f}s"
            )
            
            return payer_stats
            
        except Exception as e:
            logger.error(f"Failed processing payer {payer_name}: {str(e)}")
            raise
    
    def process_mrf_file_enhanced(self, payer_uuid: str, payer_name: str,
                                file_info: Dict[str, Any], handler, file_index: int, total_files: int) -> Dict[str, Any]:
        """Process a single MRF file with direct S3 upload and enhanced logging."""
        file_stats = {
            "records_extracted": 0,
            "records_validated": 0,
            "organizations_created": set(),
            "start_time": time.time(),
            "s3_uploads": 0
        }
        
        # Create S3-friendly filename
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        plan_safe_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in file_info["plan_name"])
        filename_base = f"{payer_name}_{plan_safe_name}_{timestamp}"
        
        # Batch collectors for S3 upload
        rate_batch = []
        org_batch = []
        provider_batch = []
        
        # Use larger batch size for S3 efficiency
        batch_size = self.config.batch_size

        # Gather diagnostics before parsing
        compression = detect_compression(file_info["url"])
        in_net_info = identify_in_network(file_info["url"], sample_size=1)
        file_stats["compression"] = compression
        file_stats["in_network_sample"] = in_net_info
        logger.info(
            "file_diagnostics",
            url=file_info["url"],
            compression=compression,
            in_network_keys=in_net_info.get("sample_keys"),
        )
        
        # Process records with streaming parser
        try:
            for raw_record in stream_parse_enhanced(
                file_info["url"],
                payer_name,
                file_info.get("provider_reference_url"),
                handler
            ):
                if (
                    self.config.max_records_per_file is not None
                    and file_stats["records_extracted"] >= self.config.max_records_per_file
                ):
                    break

                file_stats["records_extracted"] += 1
                
                # Normalize and validate
                normalized = normalize_tic_record(
                    raw_record, 
                    set(self.config.cpt_whitelist), 
                    payer_name
                )
                
                if not normalized:
                    continue
                
                # Create structured records
                rate_record = self.create_rate_record(
                    payer_uuid, normalized, file_info, raw_record
                )
                
                # Validate quality
                quality_flags = self.validator.validate_rate_record(rate_record)
                rate_record["quality_flags"] = quality_flags
                
                if quality_flags["is_validated"]:
                    file_stats["records_validated"] += 1
                    rate_batch.append(rate_record)
                    
                    # Create organization record if new
                    org_uuid = rate_record["organization_uuid"]
                    if org_uuid not in file_stats["organizations_created"]:
                        org_record = self.create_organization_record(normalized, raw_record)
                        org_batch.append(org_record)
                        file_stats["organizations_created"].add(org_uuid)
                    
                    # Create provider records
                    provider_records = self.create_provider_records(normalized, raw_record)
                    provider_batch.extend(provider_records)
                
                # Write batches when full
                if len(rate_batch) >= batch_size:
                    upload_stats = self.write_batches_to_s3(
                        rate_batch, org_batch, provider_batch, 
                        payer_name, filename_base, file_stats["s3_uploads"]
                    )
                    file_stats["s3_uploads"] += upload_stats["files_uploaded"]
                    self.stats["s3_uploads"] += upload_stats["files_uploaded"]
                    rate_batch, org_batch, provider_batch = [], [], []
            
            # Write final batches
            if rate_batch:
                upload_stats = self.write_batches_to_s3(
                    rate_batch, org_batch, provider_batch, 
                    payer_name, filename_base, file_stats["s3_uploads"]
                )
                file_stats["s3_uploads"] += upload_stats["files_uploaded"]
                self.stats["s3_uploads"] += upload_stats["files_uploaded"]
        
        except Exception as e:
            logger.error(f"Failed processing file {file_info['url']}: {str(e)}")
            raise
        
        file_stats["processing_time"] = time.time() - file_stats["start_time"]
        return file_stats
    
    def write_batches_to_s3(self, rate_batch: List[Dict], org_batch: List[Dict], 
                           provider_batch: List[Dict], payer_name: str, 
                           filename_base: str, batch_number: int) -> Dict[str, Any]:
        """Write batches directly to S3 with organized paths."""
        upload_stats = {"files_uploaded": 0, "bytes_uploaded": 0}
        
        if not self.s3_client:
            # Fallback to local storage
            return self.write_batches_local(rate_batch, org_batch, provider_batch, payer_name)
        
        # Current date for partitioning
        current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        batch_timestamp = datetime.now(timezone.utc).strftime("%H%M%S")
        
        # Upload rates batch
        if rate_batch:
            rates_key = f"{self.config.s3_prefix}/rates/payer={payer_name}/date={current_date}/{filename_base}_rates_batch_{batch_number:04d}_{batch_timestamp}.parquet"
            success = self.upload_batch_to_s3(rate_batch, rates_key, "rates")
            if success:
                upload_stats["files_uploaded"] += 1
        
        # Upload organizations batch
        if org_batch:
            orgs_key = f"{self.config.s3_prefix}/organizations/payer={payer_name}/date={current_date}/{filename_base}_orgs_batch_{batch_number:04d}_{batch_timestamp}.parquet"
            success = self.upload_batch_to_s3(org_batch, orgs_key, "organizations")
            if success:
                upload_stats["files_uploaded"] += 1
        
        # Upload providers batch
        if provider_batch:
            providers_key = f"{self.config.s3_prefix}/providers/payer={payer_name}/date={current_date}/{filename_base}_providers_batch_{batch_number:04d}_{batch_timestamp}.parquet"
            success = self.upload_batch_to_s3(provider_batch, providers_key, "providers")
            if success:
                upload_stats["files_uploaded"] += 1
        
        return upload_stats
    
    def upload_batch_to_s3(self, batch_data: List[Dict], s3_key: str, data_type: str) -> bool:
        """Upload a single batch to S3."""
        if not batch_data:
            return True
        
        try:
            # Create temporary parquet file
            temp_file = Path(self.temp_dir) / f"temp_{data_type}_{int(time.time())}.parquet"
            
            # Convert to DataFrame and write to parquet
            df = pd.DataFrame(batch_data)
            df.to_parquet(temp_file, index=False, compression='snappy')
            
            # Upload to S3
            self.s3_client.upload_file(str(temp_file), self.config.s3_bucket, s3_key)
            
            # Get file size for stats
            file_size = temp_file.stat().st_size
            
            # Clean up temp file
            temp_file.unlink()
            
            logger.info("uploaded_batch_to_s3",
                       s3_key=s3_key,
                       records=len(batch_data),
                       file_size_mb=file_size / 1024 / 1024,
                       data_type=data_type)
            
            return True
            
        except Exception as e:
            logger.error("s3_upload_failed",
                        s3_key=s3_key,
                        data_type=data_type,
                        records=len(batch_data),
                        error=str(e))
            return False
    
    def write_batches_local(self, rate_batch: List[Dict], org_batch: List[Dict], 
                           provider_batch: List[Dict], payer_name: str) -> Dict[str, Any]:
        """Fallback: write batches to local files."""
        upload_stats = {"files_uploaded": 0, "bytes_uploaded": 0}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if rate_batch:
            rates_file = (Path(self.config.local_output_dir) / "rates" / 
                         f"rates_{payer_name}_{timestamp}.parquet")
            self.append_to_parquet(rates_file, rate_batch)
            upload_stats["files_uploaded"] += 1
        
        if org_batch:
            orgs_file = (Path(self.config.local_output_dir) / "organizations" / 
                        f"organizations_{payer_name}_{timestamp}.parquet")
            self.append_to_parquet(orgs_file, org_batch)
            upload_stats["files_uploaded"] += 1
        
        if provider_batch:
            providers_file = (Path(self.config.local_output_dir) / "providers" / 
                            f"providers_{payer_name}_{timestamp}.parquet")
            self.append_to_parquet(providers_file, provider_batch)
            upload_stats["files_uploaded"] += 1
        
        return upload_stats
    
    def create_payer_record(self, payer_name: str, index_url: str) -> str:
        """Create and store payer master record."""
        payer_uuid = self.uuid_gen.payer_uuid(payer_name)
        
        payer_record = {
            "payer_uuid": payer_uuid,
            "payer_name": payer_name,
            "payer_type": "Commercial",
            "parent_organization": "",
            "state_licenses": [],
            "market_type": "Unknown",
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "data_source": "TiC_MRF",
            "index_url": index_url,
            "last_scraped": datetime.now(timezone.utc)
        }
        
        return payer_uuid
    
    def create_rate_record(self, payer_uuid: str, normalized: Dict[str, Any], 
                          file_info: Dict[str, Any], raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Create a structured rate record."""
        
        # Generate organization UUID
        org_uuid = self.uuid_gen.organization_uuid(
            normalized.get("provider_tin", ""), 
            normalized.get("provider_name", "")
        )
        
        # Generate rate UUID
        rate_uuid = self.uuid_gen.rate_uuid(
            payer_uuid,
            org_uuid,
            normalized["service_code"],
            normalized["negotiated_rate"],
            normalized.get("expiration_date", "")
        )
        
        # Extract NPI list
        npi_list = normalized.get("provider_npi", [])
        if isinstance(npi_list, (int, str)):
            npi_list = [str(npi_list)]
        elif npi_list:
            npi_list = [str(npi) for npi in npi_list]
        
        return {
            "rate_uuid": rate_uuid,
            "payer_uuid": payer_uuid,
            "organization_uuid": org_uuid,
            "service_code": normalized["service_code"],
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
                "processing_version": self.config.processing_version
            },
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
    
    def create_organization_record(self, normalized: Dict[str, Any], raw_record: Dict[str, Any]) -> Dict[str, Any]:
        """Create organization record."""
        tin = normalized.get("provider_tin", "")
        org_name = normalized.get("provider_name", "")
        
        org_uuid = self.uuid_gen.organization_uuid(tin, org_name)
        
        return {
            "organization_uuid": org_uuid,
            "tin": tin,
            "organization_name": org_name or f"Organization-{tin}",
            "organization_type": "Unknown",
            "parent_system": "",
            "npi_count": len(normalized.get("provider_npi", [])),
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
    
    def create_provider_records(self, normalized: Dict[str, Any], raw_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create provider records for NPIs."""
        npi_list = normalized.get("provider_npi", [])
        if not npi_list:
            return []
        
        if isinstance(npi_list, (int, str)):
            npi_list = [str(npi_list)]
        
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
    
    def append_to_parquet(self, file_path: Path, records: List[Dict]):
        """Append records to parquet file (local fallback)."""
        if not records:
            return
        
        df = pd.DataFrame(records)
        
        if file_path.exists():
            existing_df = pd.read_parquet(file_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_parquet(file_path, index=False)
        else:
            df.to_parquet(file_path, index=False)
        
        logger.info("wrote_local_batch", file=str(file_path), records=len(records))
    
    def generate_aggregated_tables(self):
        """Generate final aggregated parquet tables (for local storage only)."""
        if self.s3_client:
            logger.info("skipping_local_aggregation", reason="using_s3_storage")
            return
        
        logger.info("generating_aggregated_tables")
        
        # Combine all rate files
        self.combine_table_files("rates")
        self.combine_table_files("organizations") 
        self.combine_table_files("providers")
        
        # Generate analytics table
        self.generate_analytics_table()
    
    def combine_table_files(self, table_name: str):
        """Combine all staging files for a table into final parquet."""
        staging_dir = Path(self.config.local_output_dir) / table_name
        staging_files = list(staging_dir.glob("*.parquet"))
        
        if not staging_files:
            logger.warning("no_staging_files", table=table_name)
            return
        
        logger.info("combining_table_files", table=table_name, files=len(staging_files))
        
        # Read all files and combine
        all_dfs = []
        for file_path in staging_files:
            df = pd.read_parquet(file_path)
            all_dfs.append(df)
        
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            
            # Deduplicate based on UUID
            uuid_col = f"{table_name.rstrip('s')}_uuid"
            if uuid_col in combined_df.columns:
                combined_df = combined_df.drop_duplicates(subset=[uuid_col])
            
            # Write final table
            final_file = staging_dir / f"{table_name}_final.parquet"
            combined_df.to_parquet(final_file, index=False)
            
            logger.info("created_final_table", 
                       table=table_name, 
                       records=len(combined_df),
                       file=str(final_file))
            
            # Clean up staging files
            for file_path in staging_files:
                if file_path.name != f"{table_name}_final.parquet":
                    file_path.unlink()
    
    def generate_analytics_table(self):
        """Generate pre-computed analytics table."""
        logger.info("generating_analytics_table")
        
        rates_file = (Path(self.config.local_output_dir) / "rates" / "rates_final.parquet")
        if not rates_file.exists():
            logger.warning("no_rates_data_for_analytics")
            return
        
        df = pd.read_parquet(rates_file)
        
        # National-level analytics by service code
        analytics_records = []
        
        for service_code in df['service_code'].unique():
            code_data = df[df['service_code'] == service_code]
            rates = code_data['negotiated_rate']
            
            analytics_record = {
                "analytics_uuid": self.uuid_gen.generate_uuid("analytics", "national", service_code),
                "service_code": service_code,
                "geographic_scope": {
                    "level": "National",
                    "identifier": "US",
                    "name": "United States"
                },
                "market_statistics": {
                    "provider_count": code_data['organization_uuid'].nunique(),
                    "payer_count": code_data['payer_uuid'].nunique(),
                    "rate_observations": len(code_data),
                    "median_rate": float(rates.median()),
                    "mean_rate": float(rates.mean()),
                    "std_dev": float(rates.std()),
                    "percentiles": {
                        "p10": float(rates.quantile(0.10)),
                        "p25": float(rates.quantile(0.25)),
                        "p75": float(rates.quantile(0.75)),
                        "p90": float(rates.quantile(0.90)),
                        "p95": float(rates.quantile(0.95))
                    }
                },
                "payer_analysis": [],
                "trend_analysis": {
                    "rate_change_6m": 0.0,
                    "rate_change_12m": 0.0,
                    "volatility_score": float(rates.std() / rates.mean() if rates.mean() > 0 else 0)
                },
                "computation_date": datetime.now(timezone.utc),
                "data_freshness": datetime.now(timezone.utc)
            }
            
            analytics_records.append(analytics_record)
        
        # Write analytics table
        if analytics_records:
            analytics_file = (Path(self.config.local_output_dir) / "analytics" / "analytics_final.parquet")
            df_analytics = pd.DataFrame(analytics_records)
            df_analytics.to_parquet(analytics_file, index=False)
            
            logger.info("created_analytics_table", 
                       records=len(analytics_records),
                       file=str(analytics_file))
    
    def log_final_statistics(self):
        """Log final processing statistics."""
        processing_time = datetime.now(timezone.utc) - self.stats["processing_start"]
        
        final_stats = {
            **self.stats,
            "processing_time_seconds": processing_time.total_seconds(),
            "processing_rate_per_second": self.stats["records_validated"] / processing_time.total_seconds() if processing_time.total_seconds() > 0 else 0,
            "completion_time": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info("etl_pipeline_completed", final_stats=final_stats)
        
        # Save statistics to file
        if self.s3_client:
            # Save stats to S3
            stats_key = f"{self.config.s3_prefix}/processing_statistics/{datetime.now().strftime('%Y-%m-%d')}/processing_statistics_{int(time.time())}.json"
            try:
                temp_stats_file = Path(self.temp_dir) / "processing_statistics.json"
                with open(temp_stats_file, 'w') as f:
                    json.dump(final_stats, f, indent=2, default=str)
                
                self.s3_client.upload_file(str(temp_stats_file), self.config.s3_bucket, stats_key)
                logger.info("uploaded_stats_to_s3", s3_key=stats_key)
                
                temp_stats_file.unlink()
            except Exception as e:
                logger.error("failed_to_upload_stats", error=str(e))
        else:
            # Save stats locally
            stats_file = Path(self.config.local_output_dir) / "processing_statistics.json"
            with open(stats_file, 'w') as f:
                json.dump(final_stats, f, indent=2, default=str)


def create_production_config() -> ETLConfig:
    """Create production ETL configuration from YAML file."""
    # Read YAML configuration
    with open('production_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Get active payers (non-commented ones)
    active_payers = [payer for payer in config['payer_endpoints'].keys() 
                    if not payer.startswith('#')]
    
    # Generate output directory name based on active payers
    if active_payers:
        # Use first active payer as directory name
        payer_name = active_payers[0]
        output_dir = f"ortho_radiology_data_{payer_name}"
    else:
        # Fallback if no active payers
        output_dir = "ortho_radiology_data_default"
    
    return ETLConfig(
        payer_endpoints=config['payer_endpoints'],
        cpt_whitelist=config['cpt_whitelist'],
        batch_size=config['processing']['batch_size'],
        parallel_workers=config['processing']['parallel_workers'],
        max_files_per_payer=config['processing'].get('max_files_per_payer'),
        max_records_per_file=config['processing'].get('max_records_per_file'),
        local_output_dir=output_dir,
        s3_bucket=os.getenv("S3_BUCKET"),
        s3_prefix=config['output']['s3']['prefix'],
        schema_version=config['versioning']['schema_version'],
        processing_version=config['versioning']['processing_version'],
        min_completeness_pct=config['processing']['min_completeness_pct'],
        min_accuracy_score=config['processing']['min_accuracy_score']
    )


def main():
    """Main entry point for production ETL pipeline."""
    try:
        # Load configuration
        config = create_production_config()
        
        # Initialize pipeline
        pipeline = ProductionETLPipeline(config)
        
        # Process all payers
        pipeline.process_all_payers()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    finally:
        # Clean up progress tracker
        progress.close()


if __name__ == "__main__":
    main()