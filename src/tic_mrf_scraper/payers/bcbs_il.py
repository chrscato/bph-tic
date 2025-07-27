from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("bcbs_il")
class Bcbs_IlHandler(PayerHandler):
    """Handler for Bcbs_Il MRF files.
    
    Generated based on structure analysis:
    - Complexity: complex
    - Provider structure: top_level_providers
    - Rate structure: nested_rates
    - Custom requirements: top_level_provider_references, non_standard_billing_codes: ['LOCAL'], nested_negotiated_rates, rate_level_provider_references, service_codes_array, covered_services_field
    """

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse bcbs_il in_network records with complex structure."""
        results = []
        
        # Extract basic fields
        billing_code = record.get("billing_code", "")
        billing_code_type = record.get("billing_code_type", "")
        description = record.get("description", "")
        
        # Handle different complexity levels
        if patterns["handler_complexity"] == "complex":
            results = self._parse_complex_structure(record, billing_code, billing_code_type, description)
        elif patterns["handler_complexity"] == "moderate":
            results = self._parse_moderate_structure(record, billing_code, billing_code_type, description)
        else:
            results = self._parse_standard_structure(record, billing_code, billing_code_type, description)
        
        return results

    
    def _parse_complex_structure(self, record: Dict[str, Any], billing_code: str, billing_code_type: str, description: str) -> List[Dict[str, Any]]:
        """Parse complex structure with nested rates and provider references."""
        results = []
        
        # Handle nested negotiated_rates
        negotiated_rates = record.get("negotiated_rates", [])
        for rate_group in negotiated_rates:
            negotiated_prices = rate_group.get("negotiated_prices", [])
            provider_references = rate_group.get("provider_references", [])
            
            # Process each negotiated price
            for price in negotiated_prices:
                negotiated_rate = price.get("negotiated_rate")
                negotiated_type = price.get("negotiated_type", "")
                billing_class = price.get("billing_class", "")
                service_codes = price.get("service_code", [])
                if isinstance(service_codes, str):
                    service_codes = [service_codes]
                
                # Process each provider reference
                for provider_ref in provider_references:
                    provider_group_id = provider_ref.get("provider_group_id", "")
                    provider_groups = provider_ref.get("provider_groups", [])
                    
                    # Create normalized record
                    normalized_record = {
                        "billing_code": billing_code,
                        "billing_code_type": billing_code_type,
                        "description": description,
                        "negotiated_rate": negotiated_rate,
                        "negotiated_type": negotiated_type,
                        "billing_class": billing_class,
                        "service_codes": service_codes,
                        "provider_group_id": provider_group_id,
                        "provider_groups": provider_groups,
                        "payer_name": "bcbs_il"
                    }
                    
                    results.append(normalized_record)
        
        return results
    
    def get_provider_references(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract provider references from complex structure."""
        provider_refs = data.get("provider_references", [])
        results = []
        
        for ref in provider_refs:
            provider_group_id = ref.get("provider_group_id", "")
            provider_groups = ref.get("provider_groups", [])
            
            # Process provider groups
            for group in provider_groups:
                providers = group.get("providers", [])
                for provider in providers:
                    provider_info = {
                        "provider_group_id": provider_group_id,
                        "provider_npi": provider.get("npi"),
                        "provider_tin": provider.get("tin"),
                        "provider_name": provider.get("name", ""),
                        "payer_name": "bcbs_il"
                    }
                    results.append(provider_info)
        
        return results
