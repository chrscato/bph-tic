#!/usr/bin/env python3
"""Test script for BCBS MI handler."""

import json
from src.tic_mrf_scraper.payers.bcbs_mi import BCBSMIHandler

def test_bcbs_mi_handler():
    """Test the BCBS MI handler with sample data."""
    
    # Sample MRF data based on the analysis
    sample_mrf_data = {
        "reporting_entity_name": "Blue Cross Blue Shield of Michigan",
        "reporting_entity_type": "health_insurance_issuer",
        "in_network": [
            {
                "negotiation_arrangement": "ffs",
                "name": "Sample Service",
                "billing_code_type": "HCPCS",
                "billing_code_type_version": "2023",
                "billing_code": "A4206",
                "description": "Syringe with needle, sterile, 1 cc or less, each",
                "negotiated_rates": [
                    {
                        "negotiated_prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 0.18,
                                "expiration_date": "2025-12-31",
                                "service_code": ["12"],
                                "billing_class": "professional",
                                "billing_code_modifier": []
                            }
                        ],
                        "provider_references": ["provider_group_001"]
                    }
                ]
            }
        ],
        "provider_references": [
            {
                "provider_group_id": "provider_group_001",
                "provider_groups": [
                    {
                        "npi": ["1234567890"],
                        "tin": {"type": "EIN", "value": "12-3456789"},
                        "name": "Sample Provider Group"
                    }
                ]
            }
        ],
        "last_updated_on": "2025-07-01",
        "version": "1.0"
    }
    
    # Create handler instance
    handler = BCBSMIHandler()
    
    # Preprocess the MRF file to build provider references cache
    handler.preprocess_mrf_file(sample_mrf_data)
    
    # Test parsing a sample record
    sample_record = sample_mrf_data["in_network"][0]
    results = handler.parse_in_network(sample_record)
    
    print("=== BCBS MI Handler Test ===")
    print(f"Input record: {json.dumps(sample_record, indent=2)}")
    print(f"\nParsed results: {json.dumps(results, indent=2)}")
    
    # Test provider reference lookup
    provider_info = handler.get_provider_info_from_references(
        "provider_group_001", 
        sample_mrf_data["provider_references"]
    )
    print(f"\nProvider info lookup: {json.dumps(provider_info, indent=2)}")
    
    return results

if __name__ == "__main__":
    test_bcbs_mi_handler() 