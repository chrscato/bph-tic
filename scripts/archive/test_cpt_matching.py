#!/usr/bin/env python3
"""Test CPT code matching with actual Centene data."""

import requests
import json
import logging
from pathlib import Path
from typing import Dict, Any, Set, List
from datetime import datetime
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_matching_codes(item: Dict[str, Any], whitelist: Set[str]) -> Dict[str, Any]:
    """Analyze a matching code's structure and rates."""
    billing_code = item.get("billing_code")
    analysis = {
        "billing_code": billing_code,
        "billing_code_type": item.get("billing_code_type"),
        "description": item.get("description"),
        "has_negotiated_rates": False,
        "negotiated_rates_count": 0,
        "rates_with_providers": 0,
        "rates_without_providers": 0,
        "total_negotiated_prices": 0,
        "prices_with_rates": 0
    }
    
    if "negotiated_rates" in item:
        neg_rates = item["negotiated_rates"]
        analysis["has_negotiated_rates"] = True
        analysis["negotiated_rates_count"] = len(neg_rates)
        
        for rate in neg_rates:
            if "provider_groups" in rate or "provider_references" in rate:
                analysis["rates_with_providers"] += 1
            else:
                analysis["rates_without_providers"] += 1
            
            if "negotiated_prices" in rate:
                prices = rate["negotiated_prices"]
                analysis["total_negotiated_prices"] += len(prices)
                
                for price in prices:
                    if "negotiated_rate" in price:
                        analysis["prices_with_rates"] += 1
    
    return analysis

def test_cpt_matching(url: str, output_dir: str = "cpt_analysis"):
    """Test if our CPT codes exist in the Centene data."""
    whitelist = {"99213", "99214", "99215", "70450", "72148"}
    logger.info("Testing CPT code matching...")
    logger.info(f"Whitelist: {whitelist}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Create output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(output_dir) / f"cpt_analysis_{timestamp}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        analysis = {
            "timestamp": datetime.now().isoformat(),
            "url": url,
            "whitelist": list(whitelist),
            "matching_codes": [],
            "code_type_distribution": defaultdict(int),
            "sample_codes": []
        }
        
        if "in_network" in data:
            all_codes = set()
            matching_codes = set()
            
            for item in data["in_network"]:
                billing_code = item.get("billing_code")
                if billing_code:
                    all_codes.add(billing_code)
                    billing_type = item.get("billing_code_type", "unknown")
                    analysis["code_type_distribution"][billing_type] += 1
                    
                    if billing_code in whitelist:
                        matching_codes.add(billing_code)
                        logger.info(f"MATCH FOUND: {billing_code}")
                        logger.info(f"  Description: {item.get('description', 'N/A')}")
                        logger.info(f"  Type: {billing_type}")
                        
                        # Analyze matching code
                        code_analysis = analyze_matching_codes(item, whitelist)
                        analysis["matching_codes"].append(code_analysis)
                        
                        logger.info("  Rate Analysis:")
                        logger.info(f"    Has negotiated rates: {code_analysis['has_negotiated_rates']}")
                        logger.info(f"    Negotiated rates count: {code_analysis['negotiated_rates_count']}")
                        logger.info(f"    Rates with providers: {code_analysis['rates_with_providers']}")
                        logger.info(f"    Rates without providers: {code_analysis['rates_without_providers']}")
                        logger.info(f"    Total negotiated prices: {code_analysis['total_negotiated_prices']}")
                        logger.info(f"    Prices with rates: {code_analysis['prices_with_rates']}")
                        logger.info()
            
            # Add sample codes to analysis
            analysis["sample_codes"] = sorted(list(all_codes))[:20]
            
            # Save analysis to file
            report_path = output_path / "cpt_analysis.json"
            with open(report_path, "w") as f:
                json.dump(analysis, f, indent=2)
            
            logger.info(f"\nAnalysis saved to: {report_path}")
            
            # Print summary
            logger.info("\n=== CPT Matching Summary ===")
            logger.info(f"Total unique billing codes: {len(all_codes)}")
            logger.info(f"Matching codes found: {matching_codes}")
            
            logger.info("\nCode type distribution:")
            for code_type, count in analysis["code_type_distribution"].items():
                logger.info(f"  {code_type}: {count}")
            
            logger.info("\nSample codes from data:")
            for code in analysis["sample_codes"]:
                logger.info(f"  {code}")
            
            if not matching_codes:
                logger.warning("\nNO MATCHES! This explains why 0 records were written.")
                logger.warning("Consider expanding the CPT whitelist or using different codes.")
            
            # Analyze matching codes
            if matching_codes:
                logger.info("\nMatching Code Analysis:")
                for code_analysis in analysis["matching_codes"]:
                    logger.info(f"\nCode: {code_analysis['billing_code']}")
                    logger.info(f"Type: {code_analysis['billing_code_type']}")
                    logger.info(f"Has negotiated rates: {code_analysis['has_negotiated_rates']}")
                    logger.info(f"Total prices with rates: {code_analysis['prices_with_rates']}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch URL: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())

def main():
    """Main function to run CPT matching test."""
    url = "http://centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-04-29_centene-management-company-llc_fidelis-ex_in-network.json"
    test_cpt_matching(url)

if __name__ == "__main__":
    main() 