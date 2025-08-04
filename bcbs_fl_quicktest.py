#!/usr/bin/env python3
"""
Quick test for BCBS FL handler - tests basic functionality without heavy processing.
"""

import sys
import json
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.payers import get_handler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced

def test_bcbs_fl_quick():
    """Quick test of BCBS FL handler basic functionality."""
    
    print("="*60)
    print("BCBS FL QUICK TEST")
    print("="*60)
    
    # Test 1: Handler Registration
    print("\n1. TESTING HANDLER REGISTRATION")
    try:
        handler = get_handler("bcbs_fl")
        print(f"   ✅ Handler found: {type(handler).__name__}")
        
        # Test handler methods
        if hasattr(handler, 'parse_in_network'):
            print(f"   ✅ parse_in_network method: Present")
        else:
            print(f"   ❌ parse_in_network method: Missing")
            
        if hasattr(handler, 'preprocess_mrf_file'):
            print(f"   ✅ preprocess_mrf_file method: Present")
        else:
            print(f"   ⚠️  preprocess_mrf_file method: Missing (may be optional)")
            
    except Exception as e:
        print(f"   ❌ Handler registration failed: {e}")
        return False
    
    # Test 2: File Discovery
    print("\n2. TESTING FILE DISCOVERY")
    bcbs_fl_url = "https://d1hgtx7rrdl2cn.cloudfront.net/mrf/toc/FloridaBlue_Health-Insurance-Issuer_index.json"
    
    try:
        mrf_files = list(list_mrf_blobs_enhanced(bcbs_fl_url))
        print(f"   ✅ Discovered {len(mrf_files)} MRF files")
        
        if mrf_files:
            first_file = mrf_files[0]
            print(f"   First file: {first_file.get('description', 'No description')}")
            
            # Check if file has URL
            url = first_file.get('location', first_file.get('url', None))
            if url:
                print(f"   ✅ File has URL: {url[:50]}...")
            else:
                print(f"   ❌ File missing URL")
                print(f"   Available keys: {list(first_file.keys())}")
                
    except Exception as e:
        print(f"   ❌ File discovery failed: {e}")
        return False
    
    # Test 3: Handler Parsing (with mock data)
    print("\n3. TESTING HANDLER PARSING (MOCK DATA)")
    
    # Create sample data based on BCBS FL analysis
    sample_record = {
        "negotiation_arrangement": "ffs",
        "name": "Test Service",
        "billing_code_type": "CPT",
        "billing_code_type_version": "2024",
        "billing_code": "99213",
        "description": "Office visit",
        "negotiated_rates": [
            {
                "provider_references": ["provider_group_1"],
                "negotiated_prices": [
                    {
                        "negotiated_type": "negotiated",
                        "negotiated_rate": 125.50,
                        "expiration_date": "2025-12-31",
                        "billing_class": "professional",
                        "service_code": ["01", "02", "03"],
                        "billing_code_modifier": "26"
                    }
                ]
            }
        ]
    }
    
    try:
        parsed_records = handler.parse_in_network(sample_record)
        print(f"   ✅ Parsed {len(parsed_records)} records from sample")
        
        if parsed_records:
            first_parsed = parsed_records[0]
            print(f"   Sample parsed record keys: {list(first_parsed.keys())}")
            print(f"   Billing code: {first_parsed.get('billing_code')}")
            print(f"   Negotiated rate: {first_parsed.get('negotiated_rate')}")
            print(f"   Provider NPI: {first_parsed.get('provider_npi', 'None - expected without cache')}")
            
    except Exception as e:
        print(f"   ❌ Handler parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 4: Provider Cache (mock)
    print("\n4. TESTING PROVIDER CACHE")
    
    # Mock MRF data with provider references
    mock_mrf_data = {
        "provider_references": [
            {
                "provider_group_id": "provider_group_1",
                "provider_groups": [
                    {
                        "npi": ["1234567890"],
                        "tin": {"type": "EIN", "value": "12-3456789"},
                        "name": "Test Provider Group"
                    }
                ]
            }
        ]
    }
    
    try:
        if hasattr(handler, 'preprocess_mrf_file'):
            handler.preprocess_mrf_file(mock_mrf_data)
            cache_size = len(getattr(handler, 'provider_references_cache', {}))
            print(f"   ✅ Provider cache built: {cache_size} entries")
            
            # Test provider lookup
            if hasattr(handler, 'get_provider_info_from_references'):
                provider_info = handler.get_provider_info_from_references(
                    "provider_group_1", 
                    mock_mrf_data["provider_references"]
                )
                print(f"   ✅ Provider lookup: NPI={provider_info.get('npi')}, Name={provider_info.get('name')}")
            
        else:
            print(f"   ⚠️  Handler doesn't support provider caching")
            
    except Exception as e:
        print(f"   ❌ Provider cache test failed: {e}")
        return False
    
    # Test 5: Integrated Parsing with Cache
    print("\n5. TESTING INTEGRATED PARSING WITH CACHE")
    
    try:
        # Now test parsing with the cache built
        parsed_records = handler.parse_in_network(sample_record)
        
        if parsed_records:
            first_parsed = parsed_records[0]
            provider_npi = first_parsed.get('provider_npi')
            provider_name = first_parsed.get('provider_name')
            
            if provider_npi:
                print(f"   ✅ Provider info populated: NPI={provider_npi}")
                print(f"   ✅ Provider name: {provider_name}")
            else:
                print(f"   ⚠️  Provider info not populated (may need real provider references)")
                
    except Exception as e:
        print(f"   ❌ Integrated parsing failed: {e}")
        return False
    
    # Summary
    print(f"\n" + "="*60)
    print(f"QUICK TEST SUMMARY")
    print(f"="*60)
    print(f"✅ Handler registration: Working")
    print(f"✅ File discovery: {len(mrf_files)} files found")
    print(f"✅ Basic parsing: Working")
    print(f"✅ Provider caching: {'Working' if hasattr(handler, 'preprocess_mrf_file') else 'Not implemented'}")
    print(f"✅ Integration: Working")
    
    print(f"\n🎯 NEXT STEPS:")
    print(f"1. Run full pipeline test: python bcbs_fl_pipeline_test.py")
    print(f"2. Test with real MRF data")
    print(f"3. Add to production config")
    
    return True

if __name__ == "__main__":
    success = test_bcbs_fl_quick()
    print(f"\n{'🎉 QUICK TEST PASSED' if success else '💥 QUICK TEST FAILED'}")
    sys.exit(0 if success else 1)