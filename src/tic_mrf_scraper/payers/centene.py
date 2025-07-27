from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("centene")
@register_handler("centene_fidelis")
@register_handler("fidelis")
@register_handler("centene_ambetter")
class CenteneHandler(PayerHandler):
    """Enhanced handler for Centene-family payers including Fidelis."""

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Centene/Fidelis has:
        - Standard CMS-compliant structure with in_network array
        - Direct provider info in provider_groups (NPI/TIN)
        - Negotiated rates with provider_groups and negotiated_prices
        - HCPCS and CPT billing codes
        - Service codes as arrays
        """
        if "negotiated_rates" in record:
            for rate_group in record.get("negotiated_rates", []):
                self._normalize_centene_provider_structure(rate_group)
                self._normalize_centene_pricing(rate_group)
                self._normalize_centene_metadata(rate_group)
                
        return [record]
    
    def _normalize_centene_provider_structure(self, rate_group: Dict[str, Any]) -> None:
        """Normalize Centene's provider structure."""
        # Handle provider_references (external lookup)
        if "provider_references" in rate_group:
            # Ensure provider references are properly formatted
            refs = rate_group["provider_references"]
            if not isinstance(refs, list):
                rate_group["provider_references"] = [refs]
        
        # Handle provider_groups (embedded providers)
        # Note: Centene has direct NPI/TIN in provider_groups, which the parser can handle
        # No transformation needed - let the parser handle the original structure
        if "provider_groups" in rate_group:
            # Just ensure provider_groups is a list
            if not isinstance(rate_group["provider_groups"], list):
                rate_group["provider_groups"] = [rate_group["provider_groups"]]
    
    def _normalize_centene_pricing(self, rate_group: Dict[str, Any]) -> None:
        """Normalize Centene's negotiated prices."""
        if "negotiated_prices" not in rate_group:
            return
            
        for price in rate_group["negotiated_prices"]:
            # Ensure rate is float
            if "negotiated_rate" in price:
                try:
                    price["negotiated_rate"] = float(price["negotiated_rate"])
                except (ValueError, TypeError):
                    pass
            
            # Standardize negotiated type
            if "negotiated_type" in price:
                price["negotiated_type"] = price["negotiated_type"].lower()
            
            # Ensure service codes are list
            if "service_code" in price:
                service_code = price["service_code"]
                if isinstance(service_code, str):
                    price["service_code"] = [service_code]
                elif not isinstance(service_code, list):
                    price["service_code"] = []
            
            # Handle billing code modifiers if present
            if "billing_code_modifier" in price:
                modifier = price["billing_code_modifier"]
                if isinstance(modifier, str):
                    price["billing_code_modifier"] = [modifier]
                elif not isinstance(modifier, list):
                    price["billing_code_modifier"] = []
    
    def _normalize_centene_metadata(self, rate_group: Dict[str, Any]) -> None:
        """Normalize Centene-specific metadata fields."""
        # Ensure negotiation arrangement is standardized
        if "negotiation_arrangement" in rate_group:
            arrangement = rate_group["negotiation_arrangement"]
            if isinstance(arrangement, str):
                rate_group["negotiation_arrangement"] = arrangement.lower()
        
        # Handle billing code type version if present
        if "billing_code_type_version" in rate_group:
            version = rate_group["billing_code_type_version"]
            if isinstance(version, str):
                # Standardize version format
                rate_group["billing_code_type_version"] = version.strip()
