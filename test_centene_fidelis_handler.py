#!/usr/bin/env python3
"""Test script for Centene Fidelis handler with actual data structure."""

import json
from src.tic_mrf_scraper.payers import get_handler

def test_centene_fidelis_handler():
    """Test the Centene Fidelis handler with sample data from analysis."""
    
    # Sample record based on the analysis
    sample_record = {
        "negotiation_arrangement": "ffs",
        "name": "Sample Service",
        "billing_code_type": "HCPCS",
        "billing_code_type_version": "2024",
        "billing_code": "0202U",
        "description": "NFCT DS BCT/VIR RESPIR DNA/RNA 22 TRGT SARSCOV2",
        "negotiated_rates": [
            {
                "negotiated_prices": [
                    {
                        "negotiated_type": "negotiated",
                        "negotiated_rate": 131.38,
                        "expiration_date": "2025-12-31",
                        "service_code": ["81"],
                        "billing_class": "professional"
                    }
                ],
                "provider_groups": [
                    {
                        "npi": "1234567890",
                        "tin": "123456789"
                    }
                ]
            }
        ]
    }
    
    # Get the handler
    handler = get_handler("centene_fidelis")
    
    # Process the record
    processed_records = handler.parse_in_network(sample_record)
    
    print("=== Centene Fidelis Handler Test ===")
    print(f"Input record keys: {list(sample_record.keys())}")
    print(f"Processed records count: {len(processed_records)}")
    
    if processed_records:
        processed = processed_records[0]
        print(f"Processed record keys: {list(processed.keys())}")
        
        # Check negotiated rates structure
        if "negotiated_rates" in processed:
            rate_group = processed["negotiated_rates"][0]
            print(f"Rate group keys: {list(rate_group.keys())}")
            
            # Check provider groups
            if "provider_groups" in rate_group:
                pg = rate_group["provider_groups"][0]
                print(f"Provider group keys: {list(pg.keys())}")
                print(f"Has providers array: {'providers' in pg}")
                print(f"Direct NPI: {pg.get('npi')}")
                print(f"Direct TIN: {pg.get('tin')}")
            
            # Check negotiated prices
            if "negotiated_prices" in rate_group:
                price = rate_group["negotiated_prices"][0]
                print(f"Price keys: {list(price.keys())}")
                print(f"Negotiated rate: {price.get('negotiated_rate')}")
                print(f"Service codes: {price.get('service_code')}")
                print(f"Billing class: {price.get('billing_class')}")
    
    print("\n=== Test Complete ===")

if __name__ == "__main__":
    test_centene_fidelis_handler() 