#!/usr/bin/env python3
"""Test script for BCBS FL handler based on the actual structure analysis."""

import json
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.payers.bcbs_fl import BCBSFLHandler

def test_bcbs_fl_handler():
    """Test the BCBS FL handler with sample data based on analysis."""
    
    print("="*60)
    print("BCBS FL HANDLER TEST")
    print("="*60)
    
    # Sample MRF data based on the actual BCBS FL analysis
    sample_mrf_data = {
        "reporting_entity_name": "Florida Blue",
        "reporting_entity_type": "health_insurance_issuer",
        "last_updated_on": "2025-07-01",
        "version": "1.0",
        "provider_references": [
            {
                "provider_group_id": "provider_group_001",
                "provider_groups": [
                    {
                        "npi": ["1234567890"],
                        "tin": {"type": "EIN", "value": "12-3456789"},
                        "name": "Sample Florida Provider Group"
                    }
                ]
            },
            {
                "provider_group_id": "provider_group_002", 
                "provider_groups": [
                    {
                        "npi": ["0987654321"],
                        "tin": {"type": "EIN", "value": "98-7654321"},
                        "name": "Another Florida Provider"
                    }
                ]
            }
        ],
        "in_network": [
            {
                "negotiation_arrangement": "ffs",
                "name": "RED BLOOD CELL ANTIGEN, DNA, HUMAN ERYTHROCYTE ANT",
                "billing_code_type": "CPT",
                "billing_code_type_version": "2024",
                "billing_code": "0001U",
                "description": "RED BLOOD CELL ANTIGEN, DNA, HUMAN ERYTHROCYTE ANT",
                "negotiated_rates": [
                    {
                        "provider_references": ["provider_group_001"],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 125.50,
                                "expiration_date": "2025-12-31",
                                "billing_class": "professional",
                                "service_code": ["01", "02", "03", "04", "05"],
                                "billing_code_modifier": "26"
                            }
                        ]
                    },
                    {
                        "provider_references": ["provider_group_002"],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated", 
                                "negotiated_rate": 98.75,
                                "expiration_date": "2025-12-31",
                                "billing_class": "professional",
                                "service_code": ["01", "11", "12"]
                            }
                        ]
                    }
                ]
            },
            {
                "negotiation_arrangement": "ffs",
                "name": "LIVER DISEASE ASSAY",
                "billing_code_type": "CPT",
                "billing_code_type_version": "2024",
                "billing_code": "0002M",
                "description": "LIVER DISEASE, TEN BIOCHEMICAL ASSAYS (ALT, A2-MAC",
                "negotiated_rates": [
                    {
                        "provider_references": ["provider_group_001"],
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 245.00,
                                "expiration_date": "2025-12-31",
                                "billing_class": "professional",
                                "service_code": ["81", "99"]
                            }
                        ]
                    }
                ]
            }
        ]
    }
    
    # Create handler instance
    handler = BCBSFLHandler()
    
    # Test 1: Preprocess the MRF file to build provider references cache
    print("\n1. Testing provider references preprocessing...")
    handler.preprocess_mrf_file(sample_mrf_data)
    print(f"   ✓ Cached {len(handler.provider_references_cache)} provider references")
    
    # Verify cache contents
    for ref_id, ref_data in handler.provider_references_cache.items():
        print(f"   - {ref_id}: {len(ref_data.get('provider_groups', []))} provider groups")
    
    # Test 2: Parse sample records
    print("\n2. Testing record parsing...")
    total_results = []
    
    for i, sample_record in enumerate(sample_mrf_data["in_network"]):
        print(f"\n   Processing record {i+1}: {sample_record['billing_code']}")
        results = handler.parse_in_network(sample_record)
        total_results.extend(results)
        
        print(f"   ✓ Generated {len(results)} normalized records")
        
        # Show first result details
        if results:
            first_result = results[0]
            print(f"     - Billing Code: {first_result.get('billing_code')}")
            print(f"     - Rate: ${first_result.get('negotiated_rate')}")
            print(f"     - Provider NPI: {first_result.get('provider_npi')}")
            print(f"     - Service Codes: {first_result.get('service_codes')}")
    
    # Test 3: Validate results structure
    print(f"\n3. Validating results...")
    print(f"   ✓ Total records generated: {len(total_results)}")
    
    # Check required fields
    required_fields = [
        'billing_code', 'billing_code_type', 'description', 
        'negotiated_rate', 'negotiated_type', 'billing_class',
        'provider_npi', 'provider_name', 'payer_name'
    ]
    
    valid_records = 0
    for record in total_results:
        if all(field in record for field in required_fields):
            valid_records += 1
    
    print(f"   ✓ Records with all required fields: {valid_records}/{len(total_results)}")
    
    # Test 4: Show sample output
    print(f"\n4. Sample normalized records:")
    for i, result in enumerate(total_results[:3]):  # Show first 3
        print(f"\n   Record {i+1}:")
        print(json.dumps(result, indent=4))
    
    # Test 5: Test provider reference lookup function
    print(f"\n5. Testing provider reference lookup...")
    test_provider_info = handler.get_provider_info_from_references(
        "provider_group_001", 
        sample_mrf_data["provider_references"]
    )
    print(f"   ✓ Provider info lookup result:")
    print(json.dumps(test_provider_info, indent=4))
    
    # Summary
    print(f"\n" + "="*60)
    print(f"TEST SUMMARY")
    print(f"="*60)
    print(f"✅ Provider cache built: {len(handler.provider_references_cache)} entries")
    print(f"✅ Records processed: {len(sample_mrf_data['in_network'])}")
    print(f"✅ Normalized records generated: {len(total_results)}")
    print(f"✅ Valid records: {valid_records}/{len(total_results)}")
    
    # Check for common issues
    rates_with_providers = sum(1 for r in total_results if r.get('provider_npi'))
    print(f"✅ Records with provider info: {rates_with_providers}/{len(total_results)}")
    
    service_codes_present = sum(1 for r in total_results if r.get('service_codes'))
    print(f"✅ Records with service codes: {service_codes_present}/{len(total_results)}")
    
    return len(total_results) > 0 and valid_records == len(total_results)

if __name__ == "__main__":
    success = test_bcbs_fl_handler()
    print(f"\n{'✅ TEST PASSED' if success else '❌ TEST FAILED'}")
    sys.exit(0 if success else 1)