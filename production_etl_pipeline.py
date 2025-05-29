#!/usr/bin/env python3
"""Production ETL Pipeline for Healthcare Rates Data."""

import os
import uuid
import hashlib
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Iterator
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time

from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced, analyze_index_structure
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

logger = get_logger(__name__)

@dataclass
class ETLConfig:
    """ETL Pipeline Configuration."""
    # Input sources
    payer_endpoints: Dict[str, str]
    cpt_whitelist: List[str]
    
    # Processing limits
    max_files_per_payer: Optional[int] = None
    max_records_per_file: Optional[int] = None
    batch_size: int = 10000
    parallel_workers: int = 4
    
    # Output configuration
    local_output_dir: str = "data"
    s3_bucket: Optional[str] = None
    s3_prefix: str = "healthcare-rates"
    
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
    """Memory-efficient production ETL pipeline."""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.uuid_gen = UUIDGenerator()
        self.validator = DataQualityValidator()
        self.s3_client = boto3.client('s3') if config.s3_bucket else None
        
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
        base_dir = Path(self.config.local_output_dir)
        for subdir in ["payers", "organizations", "providers", "rates", "analytics"]:
            (base_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    def process_all_payers(self):
        """Process all configured payers with parallel execution."""
        logger.info("starting_production_etl", 
                   payers=len(self.config.payer_endpoints),
                   config=asdict(self.config))
        
        # Process payers in parallel
        with ThreadPoolExecutor(max_workers=self.config.parallel_workers) as executor:
            future_to_payer = {
                executor.submit(self.process_payer, payer_name, index_url): payer_name
                for payer_name, index_url in self.config.payer_endpoints.items()
            }
            
            for future in as_completed(future_to_payer):
                payer_name = future_to_payer[future]
                try:
                    payer_stats = future.result()
                    logger.info("completed_payer", payer=payer_name, stats=payer_stats)
                    self.stats["payers_processed"] += 1
                except Exception as e:
                    error_msg = f"Failed processing {payer_name}: {str(e)}"
                    logger.error("payer_processing_failed", payer=payer_name, error=str(e))
                    self.stats["errors"].append(error_msg)
        
        # Generate final outputs
        self.generate_aggregated_tables()
        self.upload_to_s3()
        self.log_final_statistics()
    
    def process_payer(self, payer_name: str, index_url: str) -> Dict[str, Any]:
        """Process a single payer's MRF data."""
        logger.info("processing_payer", payer=payer_name, url=index_url)
        
        payer_stats = {
            "files_processed": 0,
            "records_extracted": 0,
            "records_validated": 0,
            "start_time": time.time()
        }
        
        try:
            # Create payer record
            payer_uuid = self.create_payer_record(payer_name, index_url)
            
            # Get MRF files list
            mrf_files = list_mrf_blobs_enhanced(index_url)
            
            # Filter to in-network rates files
            rate_files = [f for f in mrf_files if f["type"] == "in_network_rates"]
            
            # Apply file limits
            if self.config.max_files_per_payer:
                rate_files = rate_files[:self.config.max_files_per_payer]
            
            logger.info("found_mrf_files", 
                       payer=payer_name, 
                       total_files=len(mrf_files),
                       rate_files=len(rate_files))
            
            # Process each MRF file
            for file_info in rate_files:
                try:
                    file_stats = self.process_mrf_file(payer_uuid, payer_name, file_info)
                    payer_stats["files_processed"] += 1
                    payer_stats["records_extracted"] += file_stats["records_extracted"]
                    payer_stats["records_validated"] += file_stats["records_validated"]
                    
                except Exception as e:
                    logger.error("mrf_file_processing_failed", 
                               payer=payer_name,
                               file_url=file_info["url"][:100],
                               error=str(e))
                    self.stats["errors"].append(f"{payer_name}: {str(e)}")
        
        except Exception as e:
            logger.error("payer_processing_failed", payer=payer_name, error=str(e))
            raise
        
        payer_stats["processing_time"] = time.time() - payer_stats["start_time"]
        return payer_stats
    
    def create_payer_record(self, payer_name: str, index_url: str) -> str:
        """Create and store payer master record."""
        payer_uuid = self.uuid_gen.payer_uuid(payer_name)
        
        payer_record = {
            "payer_uuid": payer_uuid,
            "payer_name": payer_name,
            "payer_type": "Commercial",  # Could be inferred from name
            "parent_organization": "",   # TODO: Add mapping logic
            "state_licenses": [],        # TODO: Extract from MRF data
            "market_type": "Unknown",
            "is_active": True,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "data_source": "TiC_MRF",
            "index_url": index_url,
            "last_scraped": datetime.now(timezone.utc)
        }
        
        # Store payer record (append to payers file)
        payers_file = Path(self.config.local_output_dir) / "payers" / "payers_staging.parquet"
        self.append_to_parquet(payers_file, [payer_record])
        
        return payer_uuid
    
    def process_mrf_file(self, payer_uuid: str, payer_name: str, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single MRF file with memory-efficient streaming."""
        logger.info("processing_mrf_file", 
                   payer=payer_name,
                   plan=file_info["plan_name"],
                   file_type=file_info["type"])
        
        file_stats = {
            "records_extracted": 0,
            "records_validated": 0,
            "organizations_created": set(),
            "start_time": time.time()
        }
        
        # Batch collectors
        rate_batch = []
        org_batch = []
        provider_batch = []
        
        # Process records with streaming parser
        for raw_record in stream_parse_enhanced(
            file_info["url"], 
            payer_name, 
            file_info.get("provider_reference_url")
        ):
            file_stats["records_extracted"] += 1
            
            # Apply record limits
            if (self.config.max_records_per_file and 
                file_stats["records_extracted"] > self.config.max_records_per_file):
                break
            
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
            if len(rate_batch) >= self.config.batch_size:
                self.write_batches(rate_batch, org_batch, provider_batch, payer_name)
                rate_batch, org_batch, provider_batch = [], [], []
        
        # Write final batches
        if rate_batch:
            self.write_batches(rate_batch, org_batch, provider_batch, payer_name)
        
        file_stats["processing_time"] = time.time() - file_stats["start_time"]
        logger.info("completed_mrf_file", 
                   payer=payer_name,
                   plan=file_info["plan_name"],
                   stats=file_stats)
        
        return file_stats
    
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
                "effective_date": None,  # TODO: Extract from MRF
                "expiration_date": normalized.get("expiration_date"),
                "last_updated_on": None
            },
            "provider_network": {
                "npi_list": npi_list,
                "npi_count": len(npi_list),
                "coverage_type": "Organization"
            },
            "geographic_scope": {
                "states": [],  # TODO: Extract from plan data
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
            "organization_type": "Unknown",  # TODO: Classify from name/data
            "parent_system": "",
            "npi_count": len(normalized.get("provider_npi", [])),
            "primary_specialty": "",  # TODO: Extract from NPI data
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
            "data_quality_score": 0.8  # Base score
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
                "addresses": [],  # Will be populated from NPPES data
                "is_active": True,
                "enumeration_date": None,
                "last_updated": None,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
            provider_records.append(provider_record)
        
        return provider_records
    
    def write_batches(self, rate_batch: List[Dict], org_batch: List[Dict], 
                     provider_batch: List[Dict], payer_name: str):
        """Write batches to parquet files."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if rate_batch:
            rates_file = (Path(self.config.local_output_dir) / "rates" / 
                         f"rates_{payer_name}_{timestamp}.parquet")
            self.append_to_parquet(rates_file, rate_batch)
        
        if org_batch:
            orgs_file = (Path(self.config.local_output_dir) / "organizations" / 
                        f"organizations_{payer_name}_{timestamp}.parquet")
            self.append_to_parquet(orgs_file, org_batch)
        
        if provider_batch:
            providers_file = (Path(self.config.local_output_dir) / "providers" / 
                            f"providers_{payer_name}_{timestamp}.parquet")
            self.append_to_parquet(providers_file, provider_batch)
    
    def append_to_parquet(self, file_path: Path, records: List[Dict]):
        """Append records to parquet file."""
        if not records:
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Write or append to parquet
        if file_path.exists():
            # Read existing data and append
            existing_df = pd.read_parquet(file_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df.to_parquet(file_path, index=False)
        else:
            df.to_parquet(file_path, index=False)
        
        logger.info("wrote_batch", file=str(file_path), records=len(records))
    
    def generate_aggregated_tables(self):
        """Generate final aggregated parquet tables."""
        logger.info("generating_aggregated_tables")
        
        # Combine all rate files
        self.combine_table_files("rates")
        self.combine_table_files("organizations")
        self.combine_table_files("providers")
        self.combine_table_files("payers")
        
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
                "payer_analysis": [],  # TODO: Implement payer breakdown
                "trend_analysis": {
                    "rate_change_6m": 0.0,  # TODO: Implement trend analysis
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
    
    def upload_to_s3(self):
        """Upload final parquet files to S3."""
        if not self.s3_client or not self.config.s3_bucket:
            logger.info("skipping_s3_upload", reason="not_configured")
            return
        
        logger.info("uploading_to_s3", bucket=self.config.s3_bucket)
        
        base_dir = Path(self.config.local_output_dir)
        current_date = datetime.now().strftime("%Y/%m/%d")
        
        # Upload final tables
        for table_dir in ["rates", "organizations", "providers", "payers", "analytics"]:
            table_path = base_dir / table_dir
            final_files = list(table_path.glob("*_final.parquet"))
            
            for local_file in final_files:
                # S3 key with date partitioning
                s3_key = f"{self.config.s3_prefix}/{table_dir}/date={current_date}/{local_file.name}"
                
                try:
                    self.s3_client.upload_file(
                        str(local_file), 
                        self.config.s3_bucket, 
                        s3_key
                    )
                    logger.info("uploaded_to_s3", 
                               local_file=str(local_file),
                               s3_key=s3_key)
                except Exception as e:
                    logger.error("s3_upload_failed", 
                               file=str(local_file),
                               error=str(e))
                    self.stats["errors"].append(f"S3 upload failed: {local_file}")
    
    def log_final_statistics(self):
        """Log final processing statistics."""
        processing_time = datetime.now(timezone.utc) - self.stats["processing_start"]
        
        # Calculate final statistics
        rates_file = (Path(self.config.local_output_dir) / "rates" / "rates_final.parquet")
        final_records = 0
        if rates_file.exists():
            df = pd.read_parquet(rates_file)
            final_records = len(df)
        
        final_stats = {
            **self.stats,
            "processing_time_seconds": processing_time.total_seconds(),
            "final_rate_records": final_records,
            "processing_rate_per_second": final_records / processing_time.total_seconds() if processing_time.total_seconds() > 0 else 0,
            "completion_time": datetime.now(timezone.utc).isoformat()
        }
        
        logger.info("etl_pipeline_completed", final_stats=final_stats)
        
        # Save statistics to file
        stats_file = Path(self.config.local_output_dir) / "processing_statistics.json"
        with open(stats_file, 'w') as f:
            json.dump(final_stats, f, indent=2, default=str)


def create_production_config() -> ETLConfig:
    """Create production ETL configuration."""
    return ETLConfig(
        payer_endpoints={
            "centene_fidelis": "https://www.centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-04-29_fidelis_index.json",
            # Add more payers here
        },
        cpt_whitelist=[
            "99213", "99214", "99215",  # Office visits
            "70450", "72148",           # Imaging
            "0240U", "0241U",           # Lab tests
            "10005", "10006", "10040"   # Procedures
        ],
        max_files_per_payer=10,         # Limit for testing
        max_records_per_file=100000,    # Memory management
        batch_size=5000,                # Parquet batch size
        parallel_workers=2,             # Parallel payer processing
        local_output_dir="production_data",
        s3_bucket=os.getenv("S3_BUCKET"),  # Set via environment
        s3_prefix="healthcare-rates-v2",
        schema_version="v2.1.0",
        processing_version="tic-etl-v1.0"
    )


def main():
    """Run the production ETL pipeline."""
    # Setup logging
    setup_logging("INFO")
    
    # Create configuration
    config = create_production_config()
    
    # Initialize and run pipeline
    pipeline = ProductionETLPipeline(config)
    pipeline.process_all_payers()
    
    print(f"""
🎉 Production ETL Pipeline Complete!
=====================================
📊 Final Data Location: {config.local_output_dir}/
📈 Processing Statistics: {config.local_output_dir}/processing_statistics.json

Next Steps:
1. Review data quality in processing_statistics.json
2. Query final parquet files for validation
3. Set up S3_BUCKET environment variable for cloud storage
4. Add more payers to the configuration
5. Integrate NPPES NPI registry data
""")


if __name__ == "__main__":
    main() 