#!/usr/bin/env python3
"""
Enhanced BCBS FL test script that mirrors production ETL pipeline's output structure and S3 upload.
"""

import sys
import os
import json
import pandas as pd
import boto3
import hashlib
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()  # this will load .env from current directory
from pathlib import Path
from datetime import datetime, timezone
import logging
import tempfile
from typing import Dict, List, Any
import uuid

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.payers.bcbs_fl import BCBSFLHandler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def create_rate_record(uuid_gen: UUIDGenerator, payer_uuid: str, normalized: Dict[str, Any], 
                      file_info: Dict[str, Any]) -> Dict[str, Any]:
    """Create a structured rate record."""
    
    # Generate organization UUID
    org_uuid = uuid_gen.organization_uuid(
        normalized.get("provider_tin", ""), 
        normalized.get("provider_name", "")
    )
    
    # Generate rate UUID - handle both service_code and service_codes
    service_code = normalized.get("service_code", "")
    if not service_code and normalized.get("service_codes"):
        service_codes = normalized["service_codes"]
        service_code = service_codes[0] if service_codes else ""
    
    rate_uuid = uuid_gen.rate_uuid(
        payer_uuid,
        org_uuid,
        service_code,
        normalized["negotiated_rate"],
        normalized.get("expiration_date", "")
    )
    
    # Extract NPI list - handle all possible formats
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

def create_organization_record(uuid_gen: UUIDGenerator, normalized: Dict[str, Any]) -> Dict[str, Any]:
    """Create organization record."""
    tin = normalized.get("provider_tin", "")
    org_name = normalized.get("provider_name", "")
    
    org_uuid = uuid_gen.organization_uuid(tin, org_name)
    
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

def create_provider_records(uuid_gen: UUIDGenerator, normalized: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    
    org_uuid = uuid_gen.organization_uuid(
        normalized.get("provider_tin", ""), 
        normalized.get("provider_name", "")
    )
    
    provider_records = []
    for npi in npi_list:
        npi_str = str(npi)
        provider_uuid = uuid_gen.provider_uuid(npi_str)
        
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

def upload_to_s3(df: pd.DataFrame, s3_bucket: str, s3_key: str, temp_dir: str) -> bool:
    """Upload DataFrame to S3 as parquet."""
    try:
        # Initialize S3 client with explicit credentials from environment
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        
        temp_file = Path(temp_dir) / f"temp_{int(datetime.now().timestamp())}.parquet"
        
        # Save to temp file
        df.to_parquet(temp_file, index=False)
        
        # Upload to S3
        s3_client.upload_file(str(temp_file), s3_bucket, s3_key)
        
        # Cleanup
        temp_file.unlink()
        
        logger.info(f"Successfully uploaded to s3://{s3_bucket}/{s3_key}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload to S3: {str(e)}")
        return False

def test_bcbs_fl_enhanced(s3_bucket: str = None, s3_prefix: str = "healthcare-rates-test"):
    """Enhanced test of BCBS FL handler with production-like output and S3 upload."""
    
    print("="*80)
    print("BCBS FL ENHANCED TEST")
    print("="*80)
    
    # Define CPT whitelist based on BCBS FL analysis - expanded for testing
    cpt_whitelist = {
        # Common orthopedic/radiology codes
        "99213", "99214", "72148", "73721", "70450",
        # Test codes from file
        "0001U", "0002M", "0003M", "0004M", "0004U",
        # Additional codes seen in the data
        "0008U", "0009U", "001", "0010U", "0016U", "0017U", "0018U"
    }
    
    payer_name = "bcbs_fl"
    
    # Initialize UUID generator
    uuid_gen = UUIDGenerator()
    
    # Create payer UUID
    payer_uuid = uuid_gen.payer_uuid(payer_name)
    
    # Create output directory and temp directory
    output_dir = Path("./test_bcbs_fl_enhanced")
    output_dir.mkdir(exist_ok=True)
    
    temp_dir = tempfile.mkdtemp(prefix="bcbs_fl_test_")
    
    print(f"\n1. TESTING HANDLER AND FILE DISCOVERY")
    
    try:
        handler = BCBSFLHandler() 
        print(f"   ✅ Handler created: {type(handler).__name__}")
    except Exception as e:
        print(f"   ❌ Handler creation failed: {e}")
        return False
    
    # Get file list
    bcbs_fl_url = "https://d1hgtx7rrdl2cn.cloudfront.net/mrf/toc/FloridaBlue_Health-Insurance-Issuer_index.json"
    
    try:
        mrf_files = list(list_mrf_blobs_enhanced(bcbs_fl_url))
        print(f"   ✅ Found {len(mrf_files)} MRF files")
        
        if not mrf_files:
            print("   ❌ No MRF files found")
            return False
        
        # Get first file URL
        first_file = mrf_files[0]
        test_url = first_file.get('location', first_file.get('url', ''))
        
        if not test_url:
            print(f"   ❌ No valid URL found")
            print(f"   Available keys: {list(first_file.keys())}")
            return False
            
        print(f"   ✅ Test file URL: {test_url[:100]}...")
        
    except Exception as e:
        print(f"   ❌ File discovery failed: {e}")
        return False
    
    print(f"\n2. PROCESSING REAL MRF DATA")
    
    # Initialize data collectors
    rate_records = []
    org_records = []
    provider_records = []
    seen_orgs = set()
    
    try:
        import requests
        import gzip
        
        # Fetch MRF file
        print(f"   Fetching MRF file...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(test_url, headers=headers, timeout=120)
        response.raise_for_status()
        
        # Handle compression
        if test_url.endswith('.gz') or '.gz?' in test_url:
            content = gzip.decompress(response.content).decode('utf-8')
        else:
            content = response.text
        
        # Parse JSON
        mrf_data = json.loads(content)
        print(f"   ✅ Successfully parsed MRF file")
        
        # Preprocess for provider caching
        if hasattr(handler, 'preprocess_mrf_file'):
            handler.preprocess_mrf_file(mrf_data)
            cache_size = len(getattr(handler, 'provider_references_cache', {}))
            print(f"   ✅ Built provider cache: {cache_size} entries")
        
        # Process in-network items
        in_network = mrf_data.get('in_network', [])
        max_items = 20  # Small number for testing
        test_items = in_network[:max_items]
        
        print(f"\n3. PROCESSING AND NORMALIZING RECORDS")
        
        for i, item in enumerate(test_items):
            try:
                # Parse with handler
                parsed_records = handler.parse_in_network(item)
                print(f"   Item {i+1}: {item.get('billing_code', 'N/A')} -> {len(parsed_records)} parsed")
                
                # Process each parsed record
                for parsed in parsed_records:
                    try:
                        # Normalize record
                        normalized = normalize_tic_record(parsed, cpt_whitelist, payer_name)
                        if not normalized:
                            continue
                        
                        # Create rate record
                        rate_record = create_rate_record(uuid_gen, payer_uuid, normalized, first_file)
                        rate_records.append(rate_record)
                        
                        # Create organization record if new
                        org_uuid = rate_record["organization_uuid"]
                        if org_uuid not in seen_orgs:
                            org_record = create_organization_record(uuid_gen, normalized)
                            org_records.append(org_record)
                            seen_orgs.add(org_uuid)
                        
                        # Create provider records
                        new_provider_records = create_provider_records(uuid_gen, normalized)
                        provider_records.extend(new_provider_records)
                        
                    except Exception as e:
                        print(f"      ⚠️ Failed to process parsed record: {e}")
                        continue
                    
            except Exception as e:
                print(f"   ❌ Processing failed for item {i+1}: {e}")
                continue
        
        print(f"\n4. CREATING OUTPUT FILES")
        
        # Create DataFrames
        rates_df = pd.DataFrame(rate_records) if rate_records else pd.DataFrame()
        orgs_df = pd.DataFrame(org_records) if org_records else pd.DataFrame()
        providers_df = pd.DataFrame(provider_records) if provider_records else pd.DataFrame()
        
        print(f"   Created DataFrames:")
        print(f"   - Rates: {len(rates_df)} records")
        print(f"   - Organizations: {len(orgs_df)} records")
        print(f"   - Providers: {len(providers_df)} records")
        
        # Save locally
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        rates_file = output_dir / f"rates_{timestamp}.parquet"
        orgs_file = output_dir / f"organizations_{timestamp}.parquet"
        providers_file = output_dir / f"providers_{timestamp}.parquet"
        
        if not rates_df.empty:
            rates_df.to_parquet(rates_file)
        if not orgs_df.empty:
            orgs_df.to_parquet(orgs_file)
        if not providers_df.empty:
            providers_df.to_parquet(providers_file)
        
        print(f"\n5. UPLOADING TO S3")
        
        if s3_bucket:
            # Check AWS credentials
            if not os.environ.get('AWS_ACCESS_KEY_ID') or not os.environ.get('AWS_SECRET_ACCESS_KEY'):
                print("   ⚠️ AWS credentials not found in environment variables")
                print("   Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
            else:
                current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                
                # Upload rates
                if not rates_df.empty:
                    rates_key = f"{s3_prefix}/rates/payer={payer_name}/date={current_date}/rates_{timestamp}.parquet"
                    upload_to_s3(rates_df, s3_bucket, rates_key, temp_dir)
                
                # Upload organizations
                if not orgs_df.empty:
                    orgs_key = f"{s3_prefix}/organizations/payer={payer_name}/date={current_date}/organizations_{timestamp}.parquet"
                    upload_to_s3(orgs_df, s3_bucket, orgs_key, temp_dir)
                
                # Upload providers
                if not providers_df.empty:
                    providers_key = f"{s3_prefix}/providers/payer={payer_name}/date={current_date}/providers_{timestamp}.parquet"
                    upload_to_s3(providers_df, s3_bucket, providers_key, temp_dir)
                
                print(f"   ✅ Uploaded files to S3 bucket: {s3_bucket}")
        else:
            print(f"   ℹ️ S3 upload skipped (no bucket provided)")
            print(f"   Local files saved in: {output_dir}")
        
        # Save test report
        test_report = {
            "timestamp": datetime.now().isoformat(),
            "source_url": bcbs_fl_url,
            "test_file_url": test_url,
            "processing_summary": {
                "items_processed": len(test_items),
                "rates_created": len(rates_df),
                "organizations_created": len(orgs_df),
                "providers_created": len(providers_df)
            },
            "output_files": {
                "local": [
                    str(rates_file),
                    str(orgs_file),
                    str(providers_file)
                ],
                "s3": {
                    "bucket": s3_bucket,
                    "prefix": s3_prefix,
                    "keys": [
                        f"{s3_prefix}/rates/payer={payer_name}/date={current_date}/rates_{timestamp}.parquet",
                        f"{s3_prefix}/organizations/payer={payer_name}/date={current_date}/organizations_{timestamp}.parquet",
                        f"{s3_prefix}/providers/payer={payer_name}/date={current_date}/providers_{timestamp}.parquet"
                    ] if s3_bucket else []
                }
            }
        }
        
        report_file = output_dir / f"test_report_{timestamp}.json"
        with open(report_file, 'w') as f:
            json.dump(test_report, f, indent=2)
        
        print(f"\n" + "="*80)
        print(f"🎉 BCBS FL ENHANCED TEST COMPLETED SUCCESSFULLY")
        print(f"="*80)
        print(f"✅ Records processed:")
        print(f"   - Rates: {len(rates_df)}")
        print(f"   - Organizations: {len(orgs_df)}")
        print(f"   - Providers: {len(providers_df)}")
        print(f"✅ Files created:")
        print(f"   - {rates_file.name}")
        print(f"   - {orgs_file.name}")
        print(f"   - {providers_file.name}")
        print(f"✅ Test report: {report_file.name}")
        if s3_bucket:
            print(f"✅ S3 uploads completed to: s3://{s3_bucket}/{s3_prefix}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Processing failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Cleanup temp directory
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced BCBS FL test with S3 upload')
    parser.add_argument('--s3-bucket', help='S3 bucket for uploads')
    parser.add_argument('--s3-prefix', default='healthcare-rates-test',
                      help='S3 prefix/path for uploads')
    
    args = parser.parse_args()
    
    success = test_bcbs_fl_enhanced(args.s3_bucket, args.s3_prefix)
    sys.exit(0 if success else 1)