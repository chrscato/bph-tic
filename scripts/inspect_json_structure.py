#!/usr/bin/env python3
"""Direct JSON inspection to understand the data structure."""

import requests
import json
import logging
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def inspect_centene_json():
    """Inspect the raw JSON structure directly."""
    url = "http://centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-04-29_centene-management-company-llc_fidelis-ex_in-network.json"
    
    logger.info("Fetching and inspecting raw JSON structure...")
    logger.info("=" * 60)
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad status codes
        data = response.json()
        
        # Create output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path("json_analysis") / f"analysis_{timestamp}"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Basic structure analysis
        logger.info(f"Root keys: {list(data.keys())}")
        logger.info(f"in_network count: {len(data['in_network'])}")
        logger.info("")
        
        # Look at first item in detail
        first_item = data["in_network"][0]
        logger.info("FIRST IN_NETWORK ITEM:")
        logger.info(f"  billing_code: {first_item.get('billing_code')}")
        logger.info(f"  billing_code_type: {first_item.get('billing_code_type')}")
        logger.info(f"  description: {first_item.get('description')}")
        logger.info(f"  keys: {list(first_item.keys())}")
        logger.info("")
        
        # Look at negotiated_rates structure
        if "negotiated_rates" in first_item:
            neg_rates = first_item["negotiated_rates"]
            logger.info(f"NEGOTIATED_RATES (count: {len(neg_rates)}):")
            
            for i, rate_group in enumerate(neg_rates[:2]):  # First 2 rate groups
                logger.info(f"  Rate group {i}:")
                logger.info(f"    keys: {list(rate_group.keys())}")
                
                if "negotiated_prices" in rate_group:
                    neg_prices = rate_group["negotiated_prices"]
                    logger.info(f"    negotiated_prices count: {len(neg_prices)}")
                    
                    if neg_prices:
                        first_price = neg_prices[0]
                        logger.info(f"    First price item:")
                        logger.info(f"      keys: {list(first_price.keys())}")
                        logger.info(f"      negotiated_rate: {first_price.get('negotiated_rate')}")
                        logger.info(f"      service_code: {first_price.get('service_code')}")
                        logger.info(f"      billing_class: {first_price.get('billing_class')}")
                
                if "provider_groups" in rate_group:
                    prov_groups = rate_group["provider_groups"]
                    logger.info(f"    provider_groups count: {len(prov_groups)}")
                    
                    if prov_groups:
                        first_group = prov_groups[0]
                        logger.info(f"    First provider group keys: {list(first_group.keys())}")
                        
                        if "providers" in first_group:
                            providers = first_group["providers"]
                            logger.info(f"    providers count: {len(providers)}")
                            if providers:
                                logger.info(f"    First provider: {providers[0]}")
                logger.info("")
        
        # Look for our whitelist codes
        logger.info("SEARCHING FOR WHITELIST CODES:")
        whitelist = {"99213", "99214", "99215", "70450", "72148", "T4539", "T4540", "V5020"}
        found_codes = {}
        
        for i, item in enumerate(data["in_network"][:100]):  # Check first 100
            billing_code = item.get("billing_code")
            if billing_code in whitelist:
                found_codes[billing_code] = i
                logger.info(f"  FOUND: {billing_code} at index {i}")
        
        logger.info(f"\nFound {len(found_codes)} whitelist codes in first 100 items")
        
        # Show a complete example of a whitelist code
        if found_codes:
            code, index = next(iter(found_codes.items()))
            example_item = data["in_network"][index]
            logger.info(f"\nCOMPLETE EXAMPLE - {code}:")
            logger.info(json.dumps(example_item, indent=2)[:1000] + "...")
            
            # Save complete example to file
            example_path = output_dir / f"example_{code}.json"
            with open(example_path, "w") as f:
                json.dump(example_item, f, indent=2)
            logger.info(f"\nComplete example saved to: {example_path}")
        
        # Save analysis summary
        summary = {
            "timestamp": datetime.now().isoformat(),
            "url": url,
            "root_keys": list(data.keys()),
            "in_network_count": len(data["in_network"]),
            "first_item_keys": list(first_item.keys()),
            "whitelist_codes_found": found_codes,
            "example_code": code if found_codes else None
        }
        
        summary_path = output_dir / "analysis_summary.json"
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"\nAnalysis summary saved to: {summary_path}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    inspect_centene_json() 