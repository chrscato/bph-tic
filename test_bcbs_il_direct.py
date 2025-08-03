#!/usr/bin/env python3
"""
Direct test of BCBS IL handler to isolate parsing issues.
"""

import sys
import json
import logging
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.payers.bcbs_il import Bcbs_IlHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bcbs_il_handler():
    """Test BCBS IL handler directly with sample data."""
    
    # Sample BCBS IL record structure
    sample_record = {
        "billing_code": "99213",
        "billing_code_type": "CPT",
        "description": "Office/outpatient visit established patient",
        "negotiated_rates": [
            {
                "negotiated_prices": [
                    {
                        "negotiated_rate": 150.00,
                        "negotiated_type": "negotiated",
                        "billing_class": "professional",
                        "service_code": ["1"],
                        "expiration_date": "2025-12-31"
                    }
                ],
                "provider_references": [
                    {
                        "provider_groups": [
                            {
                                "npi": ["1234567890"],
                                "tin": {"type": "EIN", "value": "123456789"},
                                "provider_group_name": "Sample Medical Group"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    # Test the handler
    handler = Bcbs_IlHandler()
    results = handler.parse_in_network(sample_record)
    
    print("="*50)
    print("BCBS IL HANDLER DIRECT TEST")
    print("="*50)
    print(f"Input record: {json.dumps(sample_record, indent=2)}")
    print(f"\nParsed results: {len(results)} records")
    
    for i, result in enumerate(results):
        print(f"\nRecord {i+1}:")
        print(json.dumps(result, indent=2))
    
    return results

if __name__ == "__main__":
    test_bcbs_il_handler() 