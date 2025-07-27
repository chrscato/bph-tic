#!/usr/bin/env python3
"""Mini test of the exact pipeline flow."""

import sys
import os
import yaml
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from production_etl_pipeline import ProductionETLPipeline, ETLConfig, DataQualityValidator
from src.tic_mrf_scraper.payers import get_handler
from src.tic_mrf_scraper.transform.normalize import normalize_tic_record

def test_pipeline_mini():
    """Test the exact pipeline flow with a single record."""
    
    print("=== MINI PIPELINE TEST ===")
    
    # Load config
    with open("production_config.yaml", 'r') as f:
        config_data = yaml.safe_load(f)
    
    config = ETLConfig(
        payer_endpoints=config_data.get("endpoints", {}),
        cpt_whitelist=config_data.get("cpt_whitelist", [])
    )
    
    pipeline = ProductionETLPipeline(config)
    validator = DataQualityValidator()
    
    # Create sample raw record (exactly like pipeline sees it)
    raw_record = {
        "billing_code": "99213",  # Use a valid CPT code from the whitelist
        "billing_code_type": "CPT",
        "billing_code_type_version": "2024",
        "description": "Office visit established patient",
        "negotiation_arrangement": "ffs",
        "name": "Test Service",
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
    
    # Step 0: Process through handler and parser (like stream_parse_enhanced does)
    print("\n0. Testing handler and parser processing...")
    handler = get_handler("centene_fidelis")
    from src.tic_mrf_scraper.stream.parser import TiCMRFParser
    parser = TiCMRFParser()
    
    # Process through handler
    processed = handler.parse_in_network(raw_record)
    print(f"   ✓ Handler processed: {len(processed)} records")
    
    # Process through parser
    parsed_records = []
    for processed_record in processed:
        parsed_records.extend(parser.parse_negotiated_rates(processed_record, "centene_fidelis"))
    
    print(f"   ✓ Parser processed: {len(parsed_records)} records")
    
    if not parsed_records:
        print("   ✗ No records from parser")
        return
    
    # Use the first parsed record for normalization
    normalized_record = parsed_records[0]
    print(f"   ✓ Parsed record keys: {list(normalized_record.keys())}")
    print(f"   ✓ Provider NPI: {normalized_record.get('provider_npi')}")
    print(f"   ✓ Provider TIN: {normalized_record.get('provider_tin')}")
    print(f"   ✓ Negotiated rate: {normalized_record.get('negotiated_rate')}")
    print(f"   ✓ Service codes: {normalized_record.get('service_codes')}")
    
    print(f"\n1. Raw record keys: {list(raw_record.keys())}")
    
    # Step 1: Normalize (like pipeline does)
    print("\n2. Testing normalization...")
    normalized = normalize_tic_record(
        normalized_record,  # Use the parsed record, not raw record
        set(config.cpt_whitelist), 
        "centene_fidelis"
    )
    
    print(f"   ✓ Normalized: {bool(normalized)}")
    if normalized:
        print(f"   ✓ Normalized keys: {list(normalized.keys())}")
        print(f"   ✓ Provider NPI: {normalized.get('provider_npi')}")
        print(f"   ✓ Provider TIN: {normalized.get('provider_tin')}")
        print(f"   ✓ Negotiated rate: {normalized.get('negotiated_rate')}")
        print(f"   ✓ Service codes: {normalized.get('service_codes')}")
    else:
        print("   ✗ Normalization failed")
        return
    
    # Step 2: Create rate record (like pipeline does)
    print("\n3. Testing rate record creation...")
    payer_uuid = pipeline.uuid_gen.payer_uuid("centene_fidelis")
    file_info = {
        "url": "http://test.com/file.json",
        "plan_name": "Test Plan",
        "plan_id": "TEST123",
        "plan_market_type": "Commercial"
    }
    
    rate_record = pipeline.create_rate_record(payer_uuid, normalized, file_info, raw_record)
    
    print(f"   ✓ Rate record created: {bool(rate_record)}")
    if rate_record:
        print(f"   ✓ Rate record keys: {list(rate_record.keys())}")
        print(f"   ✓ Has payer_uuid: {bool(rate_record.get('payer_uuid'))}")
        print(f"   ✓ Has organization_uuid: {bool(rate_record.get('organization_uuid'))}")
        print(f"   ✓ NPI list: {rate_record.get('provider_network', {}).get('npi_list', [])}")
    
    # Step 3: Validate (like pipeline does)
    print("\n4. Testing validation...")
    quality_flags = validator.validate_rate_record(rate_record)
    
    print(f"   ✓ Is validated: {quality_flags['is_validated']}")
    print(f"   ✓ Confidence score: {quality_flags['confidence_score']}")
    print(f"   ✓ Validation notes: {quality_flags['validation_notes']}")
    
    # Step 4: Check if it would be written
    print("\n5. Pipeline decision...")
    if quality_flags["is_validated"]:
        print("   ✓ Record would be written to output")
    else:
        print("   ✗ Record would be skipped")

if __name__ == "__main__":
    test_pipeline_mini() 