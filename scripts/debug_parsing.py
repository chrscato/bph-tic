#!/usr/bin/env python3
"""Debug script for parsing Centene MRF data."""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Set
from datetime import datetime
from collections import defaultdict

from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.utils.backoff_logger import get_logger

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = get_logger(__name__)

def analyze_record_structure(record: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze the structure of a parsed record."""
    analysis = {
        "fields": {},
        "empty_fields": [],
        "nested_structures": []
    }
    
    for key, value in record.items():
        if value is None:
            analysis["empty_fields"].append(key)
        elif isinstance(value, (dict, list)):
            analysis["nested_structures"].append(key)
        else:
            analysis["fields"][key] = type(value).__name__
    
    return analysis

def debug_parsing(url: str, cpt_whitelist: Set[str], output_dir: str = "debug_output"):
    """Debug the parsing of Centene MRF data with limited sample."""
    logger.info("=== Debug Parsing Process (LIMITED SAMPLE) ===")
    logger.info(f"URL: {url}")
    logger.info(f"CPT Whitelist: {cpt_whitelist}")
    
    # Create debug directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_path = Path(output_dir) / f"debug_{timestamp}"
    debug_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize statistics
    stats = {
        "total_records": 0,
        "successful_records": 0,
        "failed_records": 0,
        "unique_billing_codes": set(),
        "errors": defaultdict(int),
        "normalization_failures": defaultdict(int)
    }
    
    # Store sample records
    successful_records = []
    failed_records = []
    billing_codes_seen = set()
    
    # LIMITS FOR SAMPLING
    MAX_RECORDS_TO_CHECK = 100  # Only check first 100 parsed records
    MAX_SUCCESSFUL_TO_FIND = 5  # Stop after finding 5 successful normalizations
    
    try:
        # Process records with strict limits
        record_iterator = stream_parse_enhanced(url, payer="fidelis")
        
        for raw_record in record_iterator:
            stats["total_records"] += 1
            
            # HARD LIMIT: Stop after checking MAX_RECORDS_TO_CHECK
            if stats["total_records"] > MAX_RECORDS_TO_CHECK:
                logger.info(f"Reached hard limit of {MAX_RECORDS_TO_CHECK} records, stopping...")
                break
            
            # Collect billing code
            billing_code = raw_record.get('billing_code')
            if billing_code:
                billing_codes_seen.add(billing_code)
                stats["unique_billing_codes"].add(billing_code)
            
            # Show first few raw records
            if stats["total_records"] <= 5:
                logger.info(f"\nRaw record {stats['total_records']}:")
                logger.info(f"  Keys: {list(raw_record.keys())}")
                logger.info(f"  billing_code: {raw_record.get('billing_code')}")
                logger.info(f"  negotiated_rate: {raw_record.get('negotiated_rate')}")
                logger.info(f"  service_codes: {raw_record.get('service_codes')}")
                logger.info(f"  billing_class: {raw_record.get('billing_class')}")
            
            try:
                # Try normalization
                normalized = normalize_tic_record(raw_record, cpt_whitelist, "fidelis")
                if normalized:
                    stats["successful_records"] += 1
                    logger.info(f"\n🎉 SUCCESS! Normalized record {stats['successful_records']}:")
                    logger.info(json.dumps(normalized, indent=2))
                    
                    # Store successful record
                    successful_records.append({
                        "raw": raw_record,
                        "normalized": normalized,
                        "structure": analyze_record_structure(raw_record)
                    })
                    
                    # Stop after finding enough successful matches
                    if stats["successful_records"] >= MAX_SUCCESSFUL_TO_FIND:
                        logger.info(f"Found {MAX_SUCCESSFUL_TO_FIND} successful matches, stopping...")
                        break
                else:
                    # Debug why normalization failed (only for first few)
                    if stats["total_records"] <= 10:
                        logger.info(f"❌ Normalization failed for billing_code: {billing_code} (in whitelist: {billing_code in cpt_whitelist})")
                
            except Exception as e:
                stats["failed_records"] += 1
                error_type = type(e).__name__
                stats["errors"][error_type] += 1
                if len(failed_records) < 3:  # Only keep first 3 failures
                    failed_records.append({
                        "error": str(e),
                        "error_type": error_type,
                        "record": raw_record
                    })
                logger.error(f"❌ Failed to process record: {e}")
        
        # Save debug information
        debug_info = {
            "timestamp": datetime.now().isoformat(),
            "url": url,
            "cpt_whitelist": list(cpt_whitelist),
            "statistics": {
                "total_records": stats["total_records"],
                "successful_records": stats["successful_records"],
                "failed_records": stats["failed_records"],
                "unique_billing_codes": list(stats["unique_billing_codes"]),
                "errors": dict(stats["errors"]),
                "normalization_failures": dict(stats["normalization_failures"])
            },
            "successful_records": successful_records,
            "failed_records": failed_records[:3],  # Only keep first 3 failures
            "billing_codes_seen": sorted(list(billing_codes_seen))
        }
        
        # Save debug report
        report_path = debug_path / "debug_report.json"
        with open(report_path, "w") as f:
            json.dump(debug_info, f, indent=2)
        
        logger.info(f"\nDebug report saved to: {report_path}")
        
        # Print summary
        logger.info("\n=== Debug Parsing Summary ===")
        logger.info(f"Raw records parsed: {stats['total_records']}")
        logger.info(f"Successfully normalized: {stats['successful_records']}")
        logger.info(f"Failed records: {stats['failed_records']}")
        logger.info(f"Unique billing codes seen: {sorted(list(billing_codes_seen))}")
        
        if stats["errors"]:
            logger.info("\nErrors encountered:")
            for error_type, count in stats["errors"].items():
                logger.info(f"  {error_type}: {count}")
        
        if stats["normalization_failures"]:
            logger.info("\nNormalization failures:")
            for failure_type, count in stats["normalization_failures"].items():
                logger.info(f"  {failure_type}: {count}")
        
    except Exception as e:
        logger.error(f"Debug parsing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

def main():
    """Main function to run debug parsing."""
    print("🔍 STARTING LIMITED DEBUG PARSING")
    print("📊 Will check max 100 records and stop after finding 5 successful normalizations")
    print("⏱️ This should take under 30 seconds...")
    print()
    
    url = "http://centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-04-29_centene-management-company-llc_fidelis-ex_in-network.json"
    # Use codes we actually see in the data
    cpt_whitelist = {"99213", "99214", "99215", "70450", "72148", 
                     "T4539", "T4540", "V5020", "0240U", "0241U", 
                     "10005", "10006", "10040"}
    debug_parsing(url, cpt_whitelist)

if __name__ == "__main__":
    main() 