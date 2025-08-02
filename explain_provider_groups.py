#!/usr/bin/env python3
"""Demonstrate provider groups in MRF files."""

# Sample MRF structure showing provider groups
sample_mrf_structure = {
    "reporting_entity_name": "Florida Blue",
    "reporting_entity_type": "health_insurance_issuer",
    "version": "1.0.0",
    "last_updated_on": "2025-01-01",
    
    # Top-level provider_references - contains full provider group definitions
    "provider_references": [
        {
            "provider_group_id": "provider_group_1",
            "provider_groups": [
                {
                    "npi": ["1234567890"],
                    "tin": {"type": "ein", "value": "12-3456789"},
                    "addresses": [
                        {
                            "address_1": "123 Main St",
                            "city": "Miami",
                            "state": "FL",
                            "zip": "33101"
                        }
                    ]
                }
            ]
        },
        {
            "provider_group_id": "provider_group_2", 
            "provider_groups": [
                {
                    "npi": ["0987654321", "1122334455"],
                    "tin": {"type": "ein", "value": "98-7654321"},
                    "addresses": [
                        {
                            "address_1": "456 Oak Ave",
                            "city": "Orlando", 
                            "state": "FL",
                            "zip": "32801"
                        }
                    ]
                }
            ]
        }
    ],
    
    # In-network rates with provider group references
    "in_network": [
        {
            "billing_code": "99213",
            "billing_code_type": "CPT",
            "description": "Office visit",
            "negotiated_rates": [
                {
                    # References to provider groups defined above
                    "provider_references": ["provider_group_1"],
                    "negotiated_prices": [
                        {
                            "negotiated_rate": 85.50,
                            "negotiated_type": "negotiated"
                        }
                    ]
                },
                {
                    # Different provider group, different rate
                    "provider_references": ["provider_group_2"], 
                    "negotiated_prices": [
                        {
                            "negotiated_rate": 92.00,
                            "negotiated_type": "negotiated"
                        }
                    ]
                }
            ]
        }
    ]
}

def explain_provider_groups():
    """Explain how provider groups work."""
    
    print("=" * 60)
    print("PROVIDER GROUPS IN MRF FILES")
    print("=" * 60)
    
    print("\n1. TOP-LEVEL PROVIDER REFERENCES")
    print("- Contains full provider group definitions")
    print("- Each group has a unique ID (e.g., 'provider_group_1')")
    print("- Contains actual provider information (NPIs, addresses, etc.)")
    
    print("\n2. RATE-LEVEL PROVIDER REFERENCES") 
    print("- Contains only references to provider group IDs")
    print("- Links rates to specific provider groups")
    print("- Same service can have different rates for different groups")
    
    print("\n3. EXAMPLE STRUCTURE:")
    print("   Top-level: provider_references[0].provider_group_id = 'provider_group_1'")
    print("   Rate-level: negotiated_rates[0].provider_references = ['provider_group_1']")
    
    print("\n4. WHY THIS MATTERS:")
    print("- Different provider groups can have different rates for the same service")
    print("- Allows payers to negotiate different rates with different provider networks")
    print("- Enables tiered pricing (e.g., preferred vs. standard providers)")
    
    print("\n5. REAL-WORLD EXAMPLE:")
    print("- provider_group_1: Primary care physicians in Miami")
    print("- provider_group_2: Specialists in Orlando") 
    print("- Same CPT code 99213, but different rates: $85.50 vs $92.00")
    
    print("\n6. BENEFITS:")
    print("- Efficient storage (don't repeat provider info for each rate)")
    print("- Flexible pricing (different rates for different provider types)")
    print("- Network management (organize providers by specialty, location, etc.)")

if __name__ == "__main__":
    explain_provider_groups() 