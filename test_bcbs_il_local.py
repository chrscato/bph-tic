#!/usr/bin/env python3
"""
Small-scale BCBS IL test script to prove parsing and record extraction.
This script processes only BCBS IL files with minimal configuration for local testing.
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Add the src directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.stream.parser import TiCMRFParser, stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.payers.bcbs_il import Bcbs_IlHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('test_bcbs_il.log')
    ]
)
logger = logging.getLogger(__name__)

# Test configuration - minimal settings for BCBS IL only
TEST_CONFIG = {
    "cpt_whitelist": [
        # Remove CPT filtering for testing - process all codes
    ],
    "payer_endpoints": {
        "bcbs_il": "https://app0004702110a5prdnc868.blob.core.windows.net/toc/2025-07-23_Blue-Cross-and-Blue-Shield-of-Illinois_index.json"
    },
    "processing": {
        "batch_size": 1000,  # Small batch size for testing
        "max_files_per_payer": 1,  # Only process 1 file for testing
        "max_records_per_file": 1000,  # Only process 1000 records for testing
        "safety_limit_records_per_file": 1000,
        "enable_streaming": True,
        "chunk_size_mb": 10,  # Small chunks
        "enable_data_dumping": False,  # No dumping for simple test
    },
    "output": {
        "local_directory": "./test_output",
        "s3": {
            "bucket": "commercial-rates",
            "prefix": "tic-mrf/test-bcbs-il",
            "region": "us-east-1"
        }
    }
}

def create_test_output_dir():
    """Create test output directory."""
    output_dir = Path(TEST_CONFIG["output"]["local_directory"])
    output_dir.mkdir(exist_ok=True)
    return output_dir

def save_test_results(records: List[Dict[str, Any]], output_dir: Path):
    """Save test results to local files."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save raw records
    raw_file = output_dir / f"bcbs_il_test_raw_{timestamp}.json"
    with open(raw_file, 'w') as f:
        json.dump(records, f, indent=2)
    logger.info(f"Saved {len(records)} raw records to {raw_file}")
    
    # Save summary
    summary_file = output_dir / f"bcbs_il_test_summary_{timestamp}.json"
    summary = {
        "total_records": len(records),
        "unique_cpt_codes": list(set(r.get("billing_code", "") for r in records)),
        "records_with_provider_npi": len([r for r in records if r.get("provider_npi")]),
        "records_with_provider_name": len([r for r in records if r.get("provider_name")]),
        "records_with_provider_tin": len([r for r in records if r.get("provider_tin")]),
        "sample_records": records[:5] if records else []
    }
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    logger.info(f"Saved summary to {summary_file}")
    
    return summary

def test_bcbs_il_parsing():
    """Test BCBS IL parsing with minimal configuration."""
    logger.info("Starting BCBS IL test parsing...")
    
    # Create output directory
    output_dir = create_test_output_dir()
    
    # Initialize parser and handler
    parser = TiCMRFParser()
    handler = Bcbs_IlHandler()  # No arguments needed
    
    # Get BCBS IL endpoint
    bcbs_il_url = TEST_CONFIG["payer_endpoints"]["bcbs_il"]
    logger.info(f"Processing BCBS IL from: {bcbs_il_url}")
    
    # Process with minimal settings
    all_records = []
    record_count = 0
    max_records = TEST_CONFIG["processing"]["max_records_per_file"]
    
    try:
        # Use the streaming parser to get records
        for record in stream_parse_enhanced(
            bcbs_il_url, 
            "bcbs_il", 
            handler=handler
        ):
            if record_count >= max_records:
                logger.info(f"Reached max records limit: {max_records}")
                break
                
            # Normalize the record
            normalized = normalize_tic_record(record)
            if normalized:
                all_records.append(normalized)
                record_count += 1
                
                if record_count % 100 == 0:
                    logger.info(f"Processed {record_count} records...")
    
    except Exception as e:
        logger.error(f"Error during parsing: {e}")
        raise
    
    logger.info(f"Completed processing. Total records: {len(all_records)}")
    
    # Save results
    summary = save_test_results(all_records, output_dir)
    
    # Print summary
    print("\n" + "="*50)
    print("BCBS IL TEST RESULTS")
    print("="*50)
    print(f"Total records processed: {summary['total_records']}")
    print(f"Unique CPT codes found: {len(summary['unique_cpt_codes'])}")
    if summary['unique_cpt_codes']:
        print(f"Sample CPT codes: {summary['unique_cpt_codes'][:10]}")
    print(f"Records with provider NPI: {summary['records_with_provider_npi']}")
    print(f"Records with provider name: {summary['records_with_provider_name']}")
    print(f"Records with provider TIN: {summary['records_with_provider_tin']}")
    
    if summary['sample_records']:
        print("\nSample record:")
        print(json.dumps(summary['sample_records'][0], indent=2))
    
    print("\nTest completed! Check the test_output directory for detailed results.")
    return summary

if __name__ == "__main__":
    try:
        test_bcbs_il_parsing()
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1) 