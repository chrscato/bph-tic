from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("centene")
@register_handler("centene_fidelis")
@register_handler("fidelis")
@register_handler("centene_ambetter")
class CenteneHandler(PayerHandler):
    """Enhanced handler for Centene-family payers with embedded provider information."""

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse Centene records with embedded provider information."""
        results = []
        
        # Extract basic fields
        billing_code = record.get("billing_code", "")
        billing_code_type = record.get("billing_code_type", "")
        description = record.get("description", "")
        
        # Handle negotiated_rates structure
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
                "provider_npi": None,  # No provider info for direct rates
                "provider_name": None,
                "provider_tin": None,
                "payer_name": "centene"
            }
            results.append(normalized_record)
        else:
            # Handle complex nested structure
            for rate_group in negotiated_rates:
                negotiated_prices = rate_group.get("negotiated_prices", [])
                provider_groups = rate_group.get("provider_groups", [])
                
                # Process each negotiated price
                for price in negotiated_prices:
                    negotiated_rate = price.get("negotiated_rate")
                    negotiated_type = price.get("negotiated_type", "")
                    billing_class = price.get("billing_class", "")
                    service_codes = price.get("service_code", [])
                    if isinstance(service_codes, str):
                        service_codes = [service_codes]
                    
                    # Extract provider information from embedded provider_groups
                    provider_info = self._extract_embedded_provider_info(provider_groups)
                    
                    # Create normalized record with embedded provider info
                    normalized_record = {
                        "billing_code": billing_code,
                        "billing_code_type": billing_code_type,
                        "description": description,
                        "negotiated_rate": negotiated_rate,
                        "negotiated_type": negotiated_type,
                        "billing_class": billing_class,
                        "service_codes": service_codes,
                        "provider_npi": provider_info.get("npi"),
                        "provider_name": provider_info.get("name"),
                        "provider_tin": provider_info.get("tin"),
                        "payer_name": "centene"
                    }
                    
                    results.append(normalized_record)
        
        return results
    
    def _extract_embedded_provider_info(self, provider_groups: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract provider information from embedded provider_groups structure."""
        if not provider_groups:
            return {}
        
        # Use first provider group
        provider_group = provider_groups[0]
        
        # Extract NPI (could be list or single value)
        npi = provider_group.get("npi")
        if isinstance(npi, list) and npi:
            npi = npi[0]  # Use first NPI
        elif not npi:
            npi = None
        
        # Extract TIN
        tin = provider_group.get("tin")
        if isinstance(tin, dict):
            tin = tin.get("value", "")
        elif not tin:
            tin = ""
        
        # Extract name (Centene might not have name field)
        name = provider_group.get("name", "")
        
        return {
            "npi": npi,
            "name": name,
            "tin": tin
        }
