#!/usr/bin/env python3
"""
Detailed debug test for BCBS IL parsing to identify where the issue is.
"""

import sys
import json
import logging
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.stream.parser import TiCMRFParser, stream_parse_enhanced
from tic_mrf_scraper.payers.bcbs_il import Bcbs_IlHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bcbs_il_debug():
    """Debug BCBS IL parsing step by step."""
    
    print("="*60)
    print("BCBS IL DEBUG TEST")
    print("="*60)
    
    # Test 1: Check if we can list files
    print("\n1. Testing file listing...")
    handler = Bcbs_IlHandler()
    bcbs_il_url = "https://app0004702110a5prdnc868.blob.core.windows.net/toc/2025-07-23_Blue-Cross-and-Blue-Shield-of-Illinois_index.json"
    
    try:
        files = list(handler.list_mrf_files(bcbs_il_url))
        print(f"Found {len(files)} files")
        if files:
            print(f"First file: {files[0].get('description', 'No description')}")
            print(f"First file URL: {files[0].get('location', 'No location')}")
    except Exception as e:
        print(f"Error listing files: {e}")
    
    # Test 2: Check streaming parser directly
    print("\n2. Testing streaming parser...")
    parser = TiCMRFParser()
    
    try:
        record_count = 0
        for record in stream_parse_enhanced(bcbs_il_url, "bcbs_il", handler=handler):
            record_count += 1
            if record_count <= 3:  # Show first 3 records
                print(f"\nRecord {record_count}:")
                print(f"  Keys: {list(record.keys())}")
                print(f"  Billing code: {record.get('billing_code', 'N/A')}")
                print(f"  Billing code type: {record.get('billing_code_type', 'N/A')}")
                print(f"  Description: {record.get('description', 'N/A')[:100]}...")
                if 'negotiated_rates' in record:
                    rates = record['negotiated_rates']
                    print(f"  Negotiated rates type: {type(rates)}")
                    if isinstance(rates, list):
                        print(f"  Negotiated rates count: {len(rates)}")
                    else:
                        print(f"  Negotiated rates value: {rates}")
            
            if record_count >= 10:  # Limit for debugging
                print(f"\nStopping at {record_count} records for debugging...")
                break
                
    except Exception as e:
        print(f"Error in streaming parser: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"\nTotal records found: {record_count}")
    
    # Test 3: Test handler directly with sample data
    print("\n3. Testing handler with sample data...")
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
    
    try:
        results = handler.parse_in_network(sample_record)
        print(f"Handler processed sample record: {len(results)} results")
        if results:
            print(f"First result keys: {list(results[0].keys())}")
    except Exception as e:
        print(f"Error in handler: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bcbs_il_debug() 