#!/usr/bin/env python3
"""Test custom payer handlers."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.payers import get_handler

def test_handlers():
    """Test that all handlers are registered correctly."""
    
    payers = {
        "bcbsil": "BCBSILHandler",
        "horizon_bcbs": "HorizonHandler", 
        "aetna_florida": "AetnaHandler",
        "centene_fidelis": "CenteneHandler"
    }
    
    print("🧪 Testing Custom Payer Handlers")
    print("=" * 50)
    
    for payer, expected_class in payers.items():
        try:
            handler = get_handler(payer)
            actual_class = handler.__class__.__name__
            
            if actual_class == expected_class:
                print(f"✅ {payer}: {actual_class}")
            else:
                print(f"⚠️  {payer}: Expected {expected_class}, got {actual_class}")
            
            # Test with sample record
            sample_record = {
                "billing_code": "99213",
                "billing_code_type": "CPT",
                "description": "Office visit",
                "negotiated_rates": [{
                    "provider_groups": [{"npi": "1234567890"}],
                    "negotiated_prices": [{"negotiated_rate": "125.00"}]
                }]
            }
            
            result = handler.parse_in_network(sample_record)
            if result and len(result) > 0:
                print(f"   ✅ Handler processes sample record successfully")
            else:
                print(f"   ❌ Handler returned empty result")
                
        except Exception as e:
            print(f"❌ {payer}: Error loading handler - {e}")
    
    print("\n" + "=" * 50)
    print("Handler testing complete!")

if __name__ == "__main__":
    test_handlers() 