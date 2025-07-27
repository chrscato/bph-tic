#!/usr/bin/env python3
"""Debug script to test the full parsing pipeline for Centene Fidelis."""

from src.tic_mrf_scraper.payers import get_handler
from src.tic_mrf_scraper.stream.parser import TiCMRFParser
from production_etl_pipeline import DataQualityValidator

def test_full_pipeline():
    """Test the full parsing pipeline step by step."""
    
    # Sample record based on Centene Fidelis analysis
    sample_record = {
        "negotiation_arrangement": "ffs",
        "name": "Test Service",
        "billing_code_type": "HCPCS",
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
    
    print("=== Testing Full Pipeline ===")
    
    # Step 1: Handler processing
    handler = get_handler('centene_fidelis')
    processed_records = handler.parse_in_network(sample_record)
    print(f"1. Handler processed {len(processed_records)} records")
    
    if processed_records:
        processed = processed_records[0]
        print(f"   Processed record keys: {list(processed.keys())}")
        
        # Debug: Show the provider_groups structure after handler processing
        if "negotiated_rates" in processed:
            rate_group = processed["negotiated_rates"][0]
            if "provider_groups" in rate_group:
                pg = rate_group["provider_groups"][0]
                print(f"   Provider group after handler: {pg}")
                print(f"   Provider group keys: {list(pg.keys())}")
                print(f"   NPI in provider group: {pg.get('npi')}")
                print(f"   TIN in provider group: {pg.get('tin')}")
                
                # Debug: Show the providers array
                if "providers" in pg:
                    provider = pg["providers"][0]
                    print(f"   Individual provider: {provider}")
                    print(f"   Provider keys: {list(provider.keys())}")
                    print(f"   Provider NPI: {provider.get('npi')}")
                    print(f"   Provider TIN: {provider.get('tin')}")
        
        # Step 2: Parser processing
        parser = TiCMRFParser()
        parsed_records = list(parser.parse_negotiated_rates(processed, "centene_fidelis"))
        print(f"2. Parser generated {len(parsed_records)} rate records")
        
        if parsed_records:
            parsed = parsed_records[0]
            print(f"   Parsed record keys: {list(parsed.keys())}")
            print(f"   Service codes: {parsed.get('service_codes')}")
            print(f"   Negotiated rate: {parsed.get('negotiated_rate')}")
            print(f"   Provider NPI: {parsed.get('provider_npi')}")
            print(f"   Provider TIN: {parsed.get('provider_tin')}")
            
            # Step 3: Validation
            validator = DataQualityValidator()
            validation_result = validator.validate_rate_record(parsed)
            print(f"3. Validation result: {validation_result}")
            
            # Step 4: Check what fields are missing for final record creation
            print(f"4. Missing fields for final record:")
            required_for_final = ["service_code", "negotiated_rate", "payer_uuid", "organization_uuid"]
            for field in required_for_final:
                has_field = field in parsed or (field == "service_code" and "service_codes" in parsed)
                print(f"   {field}: {'✓' if has_field else '✗'}")
    
    # Step 5: Direct test of _create_rate_record
    print(f"\n5. Direct test of _create_rate_record:")
    parser = TiCMRFParser()
    test_provider_info = {"npi": "1234567890", "tin": "123456789"}
    test_record = parser._create_rate_record(
        billing_code="0202U",
        billing_code_type="HCPCS", 
        description="Test",
        negotiated_rate=131.38,
        service_codes=["81"],
        billing_class="professional",
        negotiated_type="negotiated",
        expiration_date="2025-12-31",
        provider_info=test_provider_info,
        payer="centene_fidelis"
    )
    print(f"   Test provider info: {test_provider_info}")
    print(f"   Test record provider NPI: {test_record.get('provider_npi')}")
    print(f"   Test record provider TIN: {test_record.get('provider_tin')}")
    
    print("\n=== Pipeline Test Complete ===")

if __name__ == "__main__":
    test_full_pipeline() 