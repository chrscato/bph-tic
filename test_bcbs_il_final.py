#!/usr/bin/env python3
"""
Final BCBS IL test using the correct 'url' field to access MRF files.
"""

import sys
import json
import logging
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.stream.parser import TiCMRFParser, stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.payers.bcbs_il import Bcbs_IlHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bcbs_il_final():
    """Final BCBS IL test using correct URL field."""
    
    print("="*60)
    print("BCBS IL FINAL TEST - USING CORRECT URL FIELD")
    print("="*60)
    
    handler = Bcbs_IlHandler()
    bcbs_il_index_url = "https://app0004702110a5prdnc868.blob.core.windows.net/toc/2025-07-23_Blue-Cross-and-Blue-Shield-of-Illinois_index.json"
    
    print("\n1. Getting BCBS IL file with correct URL field...")
    try:
        files = list(handler.list_mrf_files(bcbs_il_index_url))
        if files:
            # Get the first in_network file using 'url' field
            first_file = files[0]
            mrf_url = first_file.get('url')  # Use 'url' instead of 'location'
            description = first_file.get('description', 'No description')
            
            print(f"Selected file: {description}")
            print(f"MRF URL: {mrf_url}")
            
            if mrf_url:
                print(f"\n2. Testing streaming parser with specific file...")
                parser = TiCMRFParser()
                
                all_records = []
                record_count = 0
                max_records = 100  # Limit for testing
                
                for record in stream_parse_enhanced(mrf_url, "bcbs_il", handler=handler):
                    if record_count >= max_records:
                        print(f"\nReached max records limit: {max_records}")
                        break
                    
                    # Normalize the record
                    normalized = normalize_tic_record(record)
                    if normalized:
                        all_records.append(normalized)
                        record_count += 1
                        
                        if record_count <= 3:  # Show first 3 records
                            print(f"\nRecord {record_count}:")
                            print(f"  Billing code: {normalized.get('billing_code', 'N/A')}")
                            print(f"  Billing code type: {normalized.get('billing_code_type', 'N/A')}")
                            print(f"  Description: {normalized.get('description', 'N/A')[:100]}...")
                            print(f"  Provider NPI: {normalized.get('provider_npi', 'N/A')}")
                            print(f"  Provider TIN: {normalized.get('provider_tin', 'N/A')}")
                            print(f"  Provider name: {normalized.get('provider_name', 'N/A')}")
                            print(f"  Negotiated rate: {normalized.get('negotiated_rate', 'N/A')}")
                        
                        if record_count % 10 == 0:
                            print(f"Processed {record_count} records...")
                
                print(f"\nTotal records found: {record_count}")
                
                if record_count > 0:
                    print(f"\n✅ SUCCESS! Found {record_count} records!")
                    print("This proves the BCBS IL parser is working correctly!")
                    
                    # Analyze results
                    billing_code_types = {}
                    provider_stats = {
                        "with_npi": 0,
                        "with_name": 0,
                        "with_tin": 0
                    }
                    
                    for record in all_records:
                        code_type = record.get("billing_code_type", "unknown")
                        billing_code_types[code_type] = billing_code_types.get(code_type, 0) + 1
                        
                        if record.get("provider_npi"):
                            provider_stats["with_npi"] += 1
                        if record.get("provider_name"):
                            provider_stats["with_name"] += 1
                        if record.get("provider_tin"):
                            provider_stats["with_tin"] += 1
                    
                    print(f"\nResults Analysis:")
                    print(f"  Billing code types: {billing_code_types}")
                    print(f"  Records with provider NPI: {provider_stats['with_npi']}")
                    print(f"  Records with provider name: {provider_stats['with_name']}")
                    print(f"  Records with provider TIN: {provider_stats['with_tin']}")
                    
                else:
                    print(f"\n❌ Still 0 records. The specific file may be empty.")
                    
            else:
                print("❌ No URL found in file metadata")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bcbs_il_final() 