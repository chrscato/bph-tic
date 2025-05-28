#!/usr/bin/env python3
"""Inspect the actual MRF data file structure."""

import requests
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Set
from datetime import datetime
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_negotiated_rates(neg_rates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze negotiated rates structure."""
    analysis = {
        "total_rates": len(neg_rates),
        "rates_with_providers": 0,
        "rates_without_providers": 0,
        "rates_with_negotiated_prices": 0,
        "provider_types": defaultdict(int),
        "price_analysis": {
            "total_prices": 0,
            "prices_with_rates": 0,
            "prices_without_rates": 0,
            "rate_values": []
        }
    }
    
    for rate in neg_rates:
        # Check provider information
        if "provider_groups" in rate:
            analysis["rates_with_providers"] += 1
            analysis["provider_types"]["provider_groups"] += 1
        elif "provider_references" in rate:
            analysis["rates_with_providers"] += 1
            analysis["provider_types"]["provider_references"] += 1
        else:
            analysis["rates_without_providers"] += 1
        
        # Analyze negotiated prices
        if "negotiated_prices" in rate:
            prices = rate["negotiated_prices"]
            analysis["rates_with_negotiated_prices"] += 1
            analysis["price_analysis"]["total_prices"] += len(prices)
            
            for price in prices:
                if "negotiated_rate" in price:
                    analysis["price_analysis"]["prices_with_rates"] += 1
                    analysis["price_analysis"]["rate_values"].append(price["negotiated_rate"])
                else:
                    analysis["price_analysis"]["prices_without_rates"] += 1
    
    return analysis

def inspect_actual_mrf(url: str, output_dir: str = "mrf_analysis"):
    """Inspect the actual Centene MRF data file."""
    logger.info(f"Fetching actual MRF data file from {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Create output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(output_dir) / f"mrf_analysis_{timestamp}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Basic structure analysis
        logger.info(f"Root keys: {list(data.keys())}")
        logger.info(f"Root type: {type(data)}")
        
        analysis = {
            "timestamp": datetime.now().isoformat(),
            "url": url,
            "root_keys": list(data.keys()),
            "billing_code_analysis": defaultdict(int),
            "whitelist_matches": [],
            "sample_records": []
        }
        
        if "in_network" in data:
            in_network = data["in_network"]
            logger.info(f"in_network count: {len(in_network)}")
            
            # Analyze first few items in detail
            for i, item in enumerate(in_network[:5]):
                logger.info(f"\nAnalyzing item {i}:")
                logger.info(f"Keys: {list(item.keys())}")
                
                billing_code = item.get("billing_code")
                billing_type = item.get("billing_code_type")
                description = item.get("description", "")[:50]
                
                analysis["billing_code_analysis"][billing_type] += 1
                
                logger.info(f"Billing code: {billing_code} ({billing_type})")
                logger.info(f"Description: {description}")
                
                # Analyze negotiated rates
                if "negotiated_rates" in item:
                    neg_rates = item["negotiated_rates"]
                    rate_analysis = analyze_negotiated_rates(neg_rates)
                    
                    logger.info("\nNegotiated rates analysis:")
                    for key, value in rate_analysis.items():
                        if isinstance(value, dict):
                            logger.info(f"  {key}:")
                            for k, v in value.items():
                                logger.info(f"    {k}: {v}")
                        else:
                            logger.info(f"  {key}: {value}")
                    
                    # Save sample record
                    analysis["sample_records"].append({
                        "billing_code": billing_code,
                        "billing_type": billing_type,
                        "description": description,
                        "rate_analysis": rate_analysis
                    })
        
        # Check whitelist matches
        whitelist = {"99213", "99214", "99215", "70450", "72148"}
        matches = []
        
        if "in_network" in data:
            for item in data["in_network"]:
                billing_code = item.get("billing_code")
                if billing_code in whitelist:
                    matches.append({
                        "code": billing_code,
                        "type": item.get("billing_code_type"),
                        "description": item.get("description", "")[:50]
                    })
        
        analysis["whitelist_matches"] = matches
        
        # Save analysis to file
        report_path = output_path / "mrf_analysis.json"
        with open(report_path, "w") as f:
            json.dump(analysis, f, indent=2)
        
        logger.info(f"\nAnalysis saved to: {report_path}")
        
        # Print summary
        logger.info("\n=== MRF Analysis Summary ===")
        logger.info(f"Total in_network items: {len(in_network)}")
        
        logger.info("\nBilling code types:")
        for code_type, count in analysis["billing_code_analysis"].items():
            logger.info(f"  {code_type}: {count}")
        
        logger.info("\nWhitelist matches:")
        for match in matches:
            logger.info(f"  {match['code']} ({match['type']}): {match['description']}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch URL: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())

def main():
    """Main function to run MRF inspection."""
    url = "http://centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-04-29_centene-management-company-llc_fidelis-ex_in-network.json"
    inspect_actual_mrf(url)

if __name__ == "__main__":
    main() 