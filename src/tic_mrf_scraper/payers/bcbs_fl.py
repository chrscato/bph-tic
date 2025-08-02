from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("bcbs_fl")
class Bcbs_FlHandler(PayerHandler):
    """Handler for Bcbs_Fl MRF files.
    
    Generated based on structure analysis:
    - Complexity: complex
    - Provider structure: top_level_providers
    - Rate structure: nested_rates
    - Custom requirements: top_level_provider_references, nested_negotiated_rates, rate_level_provider_references, service_codes_array
    """

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse bcbs_fl in_network records with complex nested structure."""
        results = []
        
        # Extract basic fields
        billing_code = record.get("billing_code", "")
        billing_code_type = record.get("billing_code_type", "")
        description = record.get("description", "")
        
        # Handle nested negotiated_rates structure
        negotiated_rates = record.get("negotiated_rates", [])
        if not negotiated_rates:
            # If no negotiated_rates, return the record as-is
            return [record]
        
        # Process each negotiated_rate
        for rate_group in negotiated_rates:
            result = self._create_base_record(record, billing_code, billing_code_type, description)
            
            # Extract provider references from rate level
            provider_refs = rate_group.get("provider_references", [])
            if provider_refs:
                result["provider_references"] = provider_refs
            
            # Extract negotiated prices
            negotiated_prices = rate_group.get("negotiated_prices", [])
            if negotiated_prices:
                # Take the first price for now (could be expanded to create multiple records)
                price = negotiated_prices[0]
                result.update(self._extract_price_fields(price))
            
            results.append(result)
        
        return results if results else [record]
    
    def _create_base_record(self, record: Dict[str, Any], billing_code: str, billing_code_type: str, description: str) -> Dict[str, Any]:
        """Create base record with common fields."""
        return {
            "billing_code": billing_code,
            "billing_code_type": billing_code_type,
            "description": description,
            "negotiation_arrangement": record.get("negotiation_arrangement", ""),
            "name": record.get("name", ""),
            "billing_code_type_version": record.get("billing_code_type_version", ""),
            # Preserve other top-level fields
            **{k: v for k, v in record.items() if k not in ["negotiated_rates", "provider_references"]}
        }
    
    def _extract_price_fields(self, price: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and normalize price fields."""
        return {
            "negotiated_type": price.get("negotiated_type", ""),
            "negotiated_rate": price.get("negotiated_rate", 0),
            "expiration_date": price.get("expiration_date", ""),
            "billing_class": price.get("billing_class", ""),
            "service_code": price.get("service_code", ""),
            "billing_code_modifier": price.get("billing_code_modifier", ""),
            # Handle service codes array if present
            "service_codes": price.get("service_codes", [])
        }
