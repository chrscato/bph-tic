import json
import gzip
import requests
from src.tic_mrf_scraper.payers.bcbs_il import Bcbs_IlHandler

# Download the same file
url = 'https://app0004702110a5prdnc868.blob.core.windows.net/output/2025-07-18_Blue-Cross-and-Blue-Shield-of-Illinois_Blue-Advantage-HMO_in-network-rates.json.gz'
response = requests.get(url)
data = gzip.decompress(response.content)
mrf_data = json.loads(data)

# Test the parser
handler = Bcbs_IlHandler()
in_network = mrf_data.get('in_network', [])

# Find a record with a target CPT code
target_codes = ['99213', '99214', '72148', '73721', '70450']
test_record = None

for record in in_network:
    if record.get('billing_code') in target_codes:
        test_record = record
        break

if test_record:
    print(f"\nTesting with record containing CPT code: {test_record.get('billing_code')}")
    print(f"Record structure: {list(test_record.keys())}")
    print(f"negotiated_rates type: {type(test_record.get('negotiated_rates'))}")
    print(f"negotiated_rates value: {test_record.get('negotiated_rates')}")
    
    # Test the parser step by step
    try:
        print("\nTesting parse_in_network...")
        results = handler.parse_in_network(test_record)
        print(f"Parser returned {len(results)} results")
        if results:
            print(f"First result: {results[0]}")
        else:
            print("No results returned!")
    except Exception as e:
        print(f"Parse error: {e}")
        import traceback
        traceback.print_exc()
else:
    print("No records with target CPT codes found!")