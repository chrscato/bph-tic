#!/usr/bin/env python3
"""Validation script to compare old vs enhanced parser performance and output."""

import time
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any
from collections import defaultdict
import logging
from datetime import datetime

from tic_mrf_scraper.stream.parser import stream_parse, stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_record, normalize_tic_record
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    import yaml
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def compare_record_structures(old_record: Dict, enhanced_record: Dict) -> Dict[str, Any]:
    """Compare the structure of old and enhanced records."""
    comparison = {
        "common_fields": [],
        "old_only_fields": [],
        "enhanced_only_fields": [],
        "field_type_differences": []
    }
    
    old_keys = set(old_record.keys())
    enhanced_keys = set(enhanced_record.keys())
    
    # Find common and unique fields
    comparison["common_fields"] = list(old_keys & enhanced_keys)
    comparison["old_only_fields"] = list(old_keys - enhanced_keys)
    comparison["enhanced_only_fields"] = list(enhanced_keys - old_keys)
    
    # Compare field types for common fields
    for field in comparison["common_fields"]:
        old_type = type(old_record[field]).__name__
        enhanced_type = type(enhanced_record[field]).__name__
        if old_type != enhanced_type:
            comparison["field_type_differences"].append({
                "field": field,
                "old_type": old_type,
                "enhanced_type": enhanced_type
            })
    
    return comparison

def analyze_parser_performance(url: str, payer: str, sample_limit: int = 100) -> Dict[str, Any]:
    """Analyze and compare parser performance."""
    results = {
        "legacy": {"time": 0, "records": [], "errors": []},
        "enhanced": {"time": 0, "records": [], "errors": []}
    }
    
    # Test legacy parser
    logger.info("Testing legacy parser...")
    start_time = time.time()
    try:
        for record in stream_parse(url):
            results["legacy"]["records"].append(record)
            if len(results["legacy"]["records"]) >= sample_limit:
                break
    except Exception as e:
        results["legacy"]["errors"].append(str(e))
    
    results["legacy"]["time"] = time.time() - start_time
    
    # Test enhanced parser
    logger.info("Testing enhanced parser...")
    start_time = time.time()
    try:
        for record in stream_parse_enhanced(url, payer):
            results["enhanced"]["records"].append(record)
            if len(results["enhanced"]["records"]) >= sample_limit:
                break
    except Exception as e:
        results["enhanced"]["errors"].append(str(e))
    
    results["enhanced"]["time"] = time.time() - start_time
    
    return results

def compare_normalization(old_record: Dict, enhanced_record: Dict, cpt_whitelist: List[str]) -> Dict[str, Any]:
    """Compare normalization results between old and enhanced records."""
    comparison = {
        "old_normalized": None,
        "enhanced_normalized": None,
        "differences": []
    }
    
    try:
        comparison["old_normalized"] = normalize_record(old_record, set(cpt_whitelist), payer="TEST_PAYER")
    except Exception as e:
        comparison["old_normalized"] = f"Error: {str(e)}"
    
    try:
        comparison["enhanced_normalized"] = normalize_tic_record(enhanced_record, set(cpt_whitelist), payer="TEST_PAYER")
    except Exception as e:
        comparison["enhanced_normalized"] = f"Error: {str(e)}"
    
    # Compare normalized records if both succeeded
    if isinstance(comparison["old_normalized"], dict) and isinstance(comparison["enhanced_normalized"], dict):
        for key in set(comparison["old_normalized"].keys()) | set(comparison["enhanced_normalized"].keys()):
            old_val = comparison["old_normalized"].get(key)
            enhanced_val = comparison["enhanced_normalized"].get(key)
            if old_val != enhanced_val:
                comparison["differences"].append({
                    "field": key,
                    "old_value": old_val,
                    "enhanced_value": enhanced_val
                })
    
    return comparison

def generate_report(results: Dict[str, Any], output_dir: str):
    """Generate a detailed comparison report."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = Path(output_dir) / f"parser_comparison_{timestamp}.json"
    
    with open(report_path, 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info(f"Report generated: {report_path}")

def main():
    parser = argparse.ArgumentParser(description="Compare old vs enhanced parser performance")
    parser.add_argument("--config", required=True, help="Path to config file")
    parser.add_argument("--output", required=True, help="Output directory for reports")
    parser.add_argument("--sample-limit", type=int, default=100, help="Maximum number of records to sample")
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    Path(args.output).mkdir(parents=True, exist_ok=True)
    
    # Load configuration
    config = load_config(args.config)
    
    results = {
        "timestamp": datetime.now().isoformat(),
        "config": config,
        "endpoints": {}
    }
    
    # Process each endpoint
    for payer, url in config["endpoints"].items():
        logger.info(f"Processing endpoint: {payer}")
        endpoint_results = {
            "url": url,
            "parser_comparison": analyze_parser_performance(url, payer, args.sample_limit)
        }
        
        # Compare record structures if we have records from both parsers
        if (endpoint_results["parser_comparison"]["legacy"]["records"] and 
            endpoint_results["parser_comparison"]["enhanced"]["records"]):
            
            structure_comparison = compare_record_structures(
                endpoint_results["parser_comparison"]["legacy"]["records"][0],
                endpoint_results["parser_comparison"]["enhanced"]["records"][0]
            )
            endpoint_results["structure_comparison"] = structure_comparison
            
            # Compare normalization
            normalization_comparison = compare_normalization(
                endpoint_results["parser_comparison"]["legacy"]["records"][0],
                endpoint_results["parser_comparison"]["enhanced"]["records"][0],
                config["cpt_whitelist"]
            )
            endpoint_results["normalization_comparison"] = normalization_comparison
        
        results["endpoints"][payer] = endpoint_results
    
    # Generate report
    generate_report(results, args.output)
    
    # Print summary
    logger.info("\n=== Summary ===")
    for payer, endpoint_results in results["endpoints"].items():
        logger.info(f"\nPayer: {payer}")
        legacy = endpoint_results["parser_comparison"]["legacy"]
        enhanced = endpoint_results["parser_comparison"]["enhanced"]
        
        logger.info(f"Legacy parser: {len(legacy['records'])} records in {legacy['time']:.2f}s")
        if legacy["errors"]:
            logger.info(f"Legacy parser errors: {legacy['errors']}")
        
        logger.info(f"Enhanced parser: {len(enhanced['records'])} records in {enhanced['time']:.2f}s")
        if enhanced["errors"]:
            logger.info(f"Enhanced parser errors: {enhanced['errors']}")

if __name__ == "__main__":
    main() 