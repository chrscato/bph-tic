from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("bcbs_il")
class Bcbs_IlHandler(PayerHandler):
    """Handler for Bcbs_Il MRF files."""

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse bcbs_il in_network records with complex structure."""
        results = []
        
        # Extract basic fields
        billing_code = record.get("billing_code", "")
        billing_code_type = record.get("billing_code_type", "")
        description = record.get("description", "")
        
        # Handle different complexity levels - use complex structure for BCBS IL
        results = self._parse_complex_structure(record, billing_code, billing_code_type, description)
        
        return results

    
    def _parse_complex_structure(self, record: Dict[str, Any], billing_code: str, billing_code_type: str, description: str) -> List[Dict[str, Any]]:
        """Parse complex structure with nested rates and provider references."""
        results = []
        
        # Handle negotiated_rates - could be float or dict structure
        negotiated_rates = record.get("negotiated_rates", [])
        
        # If negotiated_rates is a float (direct rate), create simple record
        if isinstance(negotiated_rates, (int, float)):
            normalized_record = {
                "billing_code": billing_code,
                "billing_code_type": billing_code_type,
                "description": description,
                "negotiated_rate": negotiated_rates,
                "negotiated_type": "",
                "billing_class": "",
                "service_codes": [],
                "provider_group_id": "",
                "provider_groups": [],
                "payer_name": "bcbs_il"
            }
            results.append(normalized_record)
        else:
            # Handle complex nested structure
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
                        # Provider references are just float IDs, not dictionaries
                        if isinstance(provider_ref, (int, float)):
                            provider_group_id = str(provider_ref)
                            provider_groups = []
                        else:
                            # Handle dictionary format if it exists
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