#!/usr/bin/env python3
"""Quick inspection of Centene MRF JSON structure."""

import requests
import json
import logging
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def analyze_nested_structure(data: Dict[str, Any], path: str = "") -> Dict[str, Any]:
    """Analyze nested structure of JSON data."""
    analysis = {
        "type": type(data).__name__,
        "keys": list(data.keys()) if isinstance(data, dict) else None,
        "length": len(data) if isinstance(data, (dict, list)) else None,
        "sample_values": {},
        "nested_analysis": {}
    }
    
    if isinstance(data, dict):
        for key, value in data.items():
            current_path = f"{path}.{key}" if path else key
            if isinstance(value, (dict, list)):
                analysis["nested_analysis"][key] = analyze_nested_structure(value, current_path)
            else:
                analysis["sample_values"][key] = value
    
    return analysis

def inspect_centene_structure(url: str, output_dir: str = "structure_analysis"):
    """Inspect the actual Centene file structure."""
    logger.info(f"Fetching Centene file from {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Create output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(output_dir) / f"centene_structure_{timestamp}"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Basic structure analysis
        logger.info(f"Root keys: {list(data.keys())}")
        
        # Detailed analysis
        analysis = {
            "timestamp": datetime.now().isoformat(),
            "url": url,
            "root_structure": analyze_nested_structure(data),
            "billing_code_analysis": defaultdict(int),
            "rate_analysis": {
                "total_rates": 0,
                "rates_with_providers": 0,
                "rates_without_providers": 0,
                "rates_with_negotiated_prices": 0
            }
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
                analysis["billing_code_analysis"][billing_type] += 1
                
                logger.info(f"Billing code: {billing_code} ({billing_type})")
                
                if "negotiated_rates" in item:
                    neg_rates = item["negotiated_rates"]
                    analysis["rate_analysis"]["total_rates"] += len(neg_rates)
                    
                    for rate in neg_rates:
                        if "provider_groups" in rate or "provider_references" in rate:
                            analysis["rate_analysis"]["rates_with_providers"] += 1
                        else:
                            analysis["rate_analysis"]["rates_without_providers"] += 1
                            
                        if "negotiated_prices" in rate:
                            analysis["rate_analysis"]["rates_with_negotiated_prices"] += len(rate["negotiated_prices"])
                            
                            # Analyze first negotiated price
                            if rate["negotiated_prices"]:
                                price = rate["negotiated_prices"][0]
                                logger.info(f"First negotiated price structure:")
                                logger.info(json.dumps(price, indent=2))
        
        # Save analysis to file
        report_path = output_path / "structure_analysis.json"
        with open(report_path, "w") as f:
            json.dump(analysis, f, indent=2)
        
        logger.info(f"\nAnalysis saved to: {report_path}")
        
        # Print summary
        logger.info("\n=== Structure Analysis Summary ===")
        logger.info(f"Total in_network items: {len(in_network)}")
        logger.info("\nBilling code types:")
        for code_type, count in analysis["billing_code_analysis"].items():
            logger.info(f"  {code_type}: {count}")
        
        logger.info("\nRate analysis:")
        for key, value in analysis["rate_analysis"].items():
            logger.info(f"  {key}: {value}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch URL: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def main():
    """Main function to run structure inspection."""
    url = "https://www.centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-04-29_fidelis_index.json"
    inspect_centene_structure(url)

if __name__ == "__main__":
    main() 