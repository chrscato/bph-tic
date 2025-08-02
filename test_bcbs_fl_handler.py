#!/usr/bin/env python3
"""Test script for bcbs_fl handler validation."""

import json
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.payers import get_handler

def test_bcbs_fl_handler():
    """Test the bcbs_fl handler with sample data."""
    
    # Sample data based on the analysis structure
    sample_record = {
        "negotiation_arrangement": "ffs",
        "name": "Sample Service",
        "billing_code_type": "CPT",
        "billing_code_type_version": "2023",
        "billing_code": "99213",
        "description": "Office visit",
        "negotiated_rates": [
            {
                "provider_references": ["provider_group_1"],
                "negotiated_prices": [
                    {
                        "negotiated_type": "negotiated",
                        "negotiated_rate": 85.50,
                        "expiration_date": "2025-12-31",
                        "billing_class": "professional",
                        "service_code": "01",
                        "billing_code_modifier": "26",
                        "service_codes": ["01", "02", "03"]
                    }
                ]
            },
            {
                "provider_references": ["provider_group_2"],
                "negotiated_prices": [
                    {
                        "negotiated_type": "negotiated",
                        "negotiated_rate": 92.00,
                        "expiration_date": "2025-12-31",
                        "billing_class": "professional",
                        "service_code": "01"
                    }
                ]
            }
        ]
    }
    
    # Get the handler
    handler = get_handler("bcbs_fl")
    if not handler:
        print("❌ Handler not found for bcbs_fl")
        return False
    
    print(f"✅ Found handler: {handler.__class__.__name__}")
    
    # Test parsing
    try:
        results = handler.parse_in_network(sample_record)
        print(f"✅ Parsed {len(results)} records")
        
        # Validate results
        for i, result in enumerate(results):
            print(f"\nRecord {i+1}:")
            print(f"  - billing_code: {result.get('billing_code')}")
            print(f"  - negotiated_rate: {result.get('negotiated_rate')}")
            print(f"  - provider_references: {result.get('provider_references')}")
            print(f"  - service_codes: {result.get('service_codes')}")
            
            # Check that we have the expected fields
            required_fields = ['billing_code', 'negotiated_rate', 'provider_references']
            missing_fields = [f for f in required_fields if f not in result]
            if missing_fields:
                print(f"  ⚠️  Missing fields: {missing_fields}")
            else:
                print(f"  ✅ All required fields present")
        
        return True
        
    except Exception as e:
        print(f"❌ Error parsing record: {e}")
        return False

if __name__ == "__main__":
    success = test_bcbs_fl_handler()
    sys.exit(0 if success else 1) 