#!/usr/bin/env python3
"""
Test BCBS IL parsing with a specific MRF file URL.
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

def test_bcbs_il_specific_file():
    """Test BCBS IL parsing with a specific MRF file."""
    
    print("="*60)
    print("BCBS IL SPECIFIC FILE TEST")
    print("="*60)
    
    # Get a specific MRF file URL from the index
    handler = Bcbs_IlHandler()
    bcbs_il_index_url = "https://app0004702110a5prdnc868.blob.core.windows.net/toc/2025-07-23_Blue-Cross-and-Blue-Shield-of-Illinois_index.json"
    
    print("\n1. Getting specific MRF file URL...")
    try:
        files = list(handler.list_mrf_files(bcbs_il_index_url))
        if files:
            # Get the first in_network file
            first_file = files[0]
            mrf_url = first_file.get('location')
            description = first_file.get('description', 'No description')
            
            print(f"Selected file: {description}")
            print(f"MRF URL: {mrf_url}")
            
            if mrf_url:
                print(f"\n2. Testing streaming parser with specific file...")
                parser = TiCMRFParser()
                
                record_count = 0
                for record in stream_parse_enhanced(mrf_url, "bcbs_il", handler=handler):
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
                
                print(f"\nTotal records found: {record_count}")
                
                if record_count > 0:
                    print(f"\n✅ SUCCESS! Found {record_count} records in specific file.")
                    print("This proves the BCBS IL parser is working correctly!")
                else:
                    print(f"\n❌ Still 0 records. The specific file may be empty or have issues.")
                    
            else:
                print("❌ No location URL found in file metadata")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bcbs_il_specific_file() 