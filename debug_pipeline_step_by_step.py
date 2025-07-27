#!/usr/bin/env python3
"""Systematic debug of pipeline steps."""

import sys
import os
import json
import yaml
from pathlib import Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from production_etl_pipeline import ProductionETLPipeline, ETLConfig, DataQualityValidator
from src.tic_mrf_scraper.payers import get_handler
from src.tic_mrf_scraper.stream.parser import TiCMRFParser

def debug_step_by_step():
    """Debug each step of the pipeline systematically."""
    
    print("=== SYSTEMATIC PIPELINE DEBUG ===")
    
    # Step 1: Load config
    print("\n1. Loading configuration...")
    with open("production_config.yaml", 'r') as f:
        config_data = yaml.safe_load(f)
    
    config = ETLConfig(
        payer_endpoints=config_data.get("endpoints", {}),
        cpt_whitelist=config_data.get("cpt_whitelist", [])
    )
    print(f"   ✓ Config loaded: {len(config.payer_endpoints)} payers")
    
    # Step 2: Get handler
    print("\n2. Getting Centene handler...")
    handler = get_handler("centene_fidelis")
    print(f"   ✓ Handler: {type(handler).__name__}")
    
    # Step 3: Create sample raw record
    print("\n3. Creating sample raw record...")
    sample_raw_record = {
        "billing_code": "0202U",
        "billing_code_type": "HCPCS",
        "billing_code_type_version": "2024",
        "description": "NFCT DS BCT/VIR RESPIR DNA/RNA 22 TRGT SARSCOV2",
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
    print(f"   ✓ Sample record created with {len(sample_raw_record['negotiated_rates'])} rate groups")
    
    # Step 4: Test handler parsing
    print("\n4. Testing handler parsing...")
    try:
        parsed_records = handler.parse_in_network(sample_raw_record)
        print(f"   ✓ Handler parsed {len(parsed_records)} records")
        if parsed_records:
            first_parsed = parsed_records[0]
            print(f"   ✓ First parsed record keys: {list(first_parsed.keys())}")
            print(f"   ✓ Provider NPI: {first_parsed.get('provider_npi')}")
            print(f"   ✓ Provider TIN: {first_parsed.get('provider_tin')}")
            print(f"   ✓ Negotiated rate: {first_parsed.get('negotiated_rate')}")
            print(f"   ✓ Service codes: {first_parsed.get('service_codes')}")
    except Exception as e:
        print(f"   ✗ Handler parsing failed: {str(e)}")
        return
    
    # Step 5: Test parser processing
    print("\n5. Testing parser processing...")
    parser = TiCMRFParser()
    try:
        parser_records = []
        for parsed in parsed_records:
            parser_records.extend(parser.parse_negotiated_rates(parsed, "centene_fidelis"))
        
        print(f"   ✓ Parser processed {len(parser_records)} records")
        if parser_records:
            first_parser = parser_records[0]
            print(f"   ✓ First parser record keys: {list(first_parser.keys())}")
            print(f"   ✓ Provider NPI: {first_parser.get('provider_npi')}")
            print(f"   ✓ Provider TIN: {first_parser.get('provider_tin')}")
            print(f"   ✓ Negotiated rate: {first_parser.get('negotiated_rate')}")
            print(f"   ✓ Service codes: {first_parser.get('service_codes')}")
    except Exception as e:
        print(f"   ✗ Parser processing failed: {str(e)}")
        return
    
    # Step 6: Test pipeline record creation
    print("\n6. Testing pipeline record creation...")
    pipeline = ProductionETLPipeline(config)
    validator = DataQualityValidator()
    
    try:
        # Create payer UUID
        payer_uuid = pipeline.uuid_gen.payer_uuid("centene_fidelis")
        print(f"   ✓ Payer UUID: {payer_uuid}")
        
        # Create file info
        file_info = {
            "url": "http://test.com/file.json",
            "plan_name": "Test Plan",
            "plan_id": "TEST123",
            "plan_market_type": "Commercial"
        }
        
        # Create rate record
        if parser_records:
            normalized = parser_records[0]
            rate_record = pipeline.create_rate_record(payer_uuid, normalized, file_info, sample_raw_record)
            
            print(f"   ✓ Rate record created with keys: {list(rate_record.keys())}")
            print(f"   ✓ Has payer_uuid: {bool(rate_record.get('payer_uuid'))}")
            print(f"   ✓ Has organization_uuid: {bool(rate_record.get('organization_uuid'))}")
            print(f"   ✓ Provider network: {rate_record.get('provider_network')}")
            print(f"   ✓ NPI list: {rate_record.get('provider_network', {}).get('npi_list', [])}")
            
            # Test validation
            validation_result = validator.validate_rate_record(rate_record)
            print(f"   ✓ Validation result: {validation_result['is_validated']}")
            print(f"   ✓ Validation notes: {validation_result['validation_notes']}")
            
        else:
            print("   ✗ No parser records to test")
            
    except Exception as e:
        print(f"   ✗ Pipeline record creation failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return
    
    # Step 7: Test with actual data from the pipeline
    print("\n7. Testing with actual pipeline data...")
    try:
        # Get the first few records from the actual MRF file
        mrf_url = "http://centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-06-27_centene-management-company-llc_fidelis-ex_in-network.json"
        
        import requests
        response = requests.get(mrf_url)
        data = response.json()
        
        if "in_network" in data and data["in_network"]:
            first_item = data["in_network"][0]
            print(f"   ✓ Loaded actual MRF data with {len(data['in_network'])} items")
            print(f"   ✓ First item keys: {list(first_item.keys())}")
            
            # Test handler with real data
            real_parsed = handler.parse_in_network(first_item)
            print(f"   ✓ Handler parsed {len(real_parsed)} records from real data")
            
            if real_parsed:
                real_parser_records = []
                for parsed in real_parsed:
                    real_parser_records.extend(parser.parse_negotiated_rates(parsed, "centene_fidelis"))
                
                print(f"   ✓ Parser processed {len(real_parser_records)} records from real data")
                
                if real_parser_records:
                    real_normalized = real_parser_records[0]
                    real_rate_record = pipeline.create_rate_record(payer_uuid, real_normalized, file_info, first_item)
                    
                    print(f"   ✓ Real rate record keys: {list(real_rate_record.keys())}")
                    print(f"   ✓ Real record has payer_uuid: {bool(real_rate_record.get('payer_uuid'))}")
                    print(f"   ✓ Real record has organization_uuid: {bool(real_rate_record.get('organization_uuid'))}")
                    print(f"   ✓ Real record NPI list: {real_rate_record.get('provider_network', {}).get('npi_list', [])}")
                    
                    real_validation = validator.validate_rate_record(real_rate_record)
                    print(f"   ✓ Real validation result: {real_validation['is_validated']}")
                    print(f"   ✓ Real validation notes: {real_validation['validation_notes']}")
                    
        else:
            print("   ✗ No in_network data found in MRF")
            
    except Exception as e:
        print(f"   ✗ Real data test failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_step_by_step()