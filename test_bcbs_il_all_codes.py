#!/usr/bin/env python3
"""
Test BCBS IL parsing with all billing code types to see what's actually in the files.
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
        logging.FileHandler('test_bcbs_il_all_codes.log')
    ]
)
logger = logging.getLogger(__name__)

def test_bcbs_il_all_codes():
    """Test BCBS IL parsing with all billing code types."""
    logger.info("Starting BCBS IL test with all billing code types...")
    
    # Create output directory
    output_dir = Path("./test_output")
    output_dir.mkdir(exist_ok=True)
    
    # Initialize parser and handler
    parser = TiCMRFParser()
    handler = Bcbs_IlHandler()
    
    # BCBS IL endpoint
    bcbs_il_url = "https://app0004702110a5prdnc868.blob.core.windows.net/toc/2025-07-23_Blue-Cross-and-Blue-Shield-of-Illinois_index.json"
    logger.info(f"Processing BCBS IL from: {bcbs_il_url}")
    
    # Process all records (no filtering)
    all_records = []
    record_count = 0
    max_records = 1000  # Limit for testing
    
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
    
    # Analyze results
    billing_code_types = {}
    billing_codes = {}
    provider_stats = {
        "with_npi": 0,
        "with_name": 0,
        "with_tin": 0
    }
    
    for record in all_records:
        # Count billing code types
        code_type = record.get("billing_code_type", "unknown")
        billing_code_types[code_type] = billing_code_types.get(code_type, 0) + 1
        
        # Count billing codes
        code = record.get("billing_code", "unknown")
        billing_codes[code] = billing_codes.get(code, 0) + 1
        
        # Count provider info
        if record.get("provider_npi"):
            provider_stats["with_npi"] += 1
        if record.get("provider_name"):
            provider_stats["with_name"] += 1
        if record.get("provider_tin"):
            provider_stats["with_tin"] += 1
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_file = output_dir / f"bcbs_il_all_codes_summary_{timestamp}.json"
    
    summary = {
        "total_records": len(all_records),
        "billing_code_types": billing_code_types,
        "billing_codes": billing_codes,
        "provider_stats": provider_stats,
        "sample_records": all_records[:5] if all_records else []
    }
    
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    # Print summary
    print("\n" + "="*60)
    print("BCBS IL ALL CODES TEST RESULTS")
    print("="*60)
    print(f"Total records processed: {len(all_records)}")
    print(f"\nBilling Code Types Found:")
    for code_type, count in billing_code_types.items():
        print(f"  {code_type}: {count}")
    
    print(f"\nTop 10 Billing Codes:")
    sorted_codes = sorted(billing_codes.items(), key=lambda x: x[1], reverse=True)
    for code, count in sorted_codes[:10]:
        print(f"  {code}: {count}")
    
    print(f"\nProvider Information:")
    print(f"  Records with NPI: {provider_stats['with_npi']}")
    print(f"  Records with name: {provider_stats['with_name']}")
    print(f"  Records with TIN: {provider_stats['with_tin']}")
    
    if summary['sample_records']:
        print(f"\nSample record:")
        print(json.dumps(summary['sample_records'][0], indent=2))
    
    print(f"\nDetailed results saved to: {summary_file}")
    return summary

if __name__ == "__main__":
    try:
        test_bcbs_il_all_codes()
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1) 