from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("centene")
@register_handler("centene_fidelis")
@register_handler("fidelis")
class CenteneHandler(PayerHandler):
    """Enhanced handler for Centene-family payers."""

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Centene/Fidelis has:
        - Provider references instead of embedded providers
        - Simple consolidated file structure
        - Direct provider info in provider_groups
        - Minimal additional fields
        """
        if "negotiated_rates" in record:
            for rate_group in record.get("negotiated_rates", []):
                self._normalize_centene_provider_structure(rate_group)
                self._normalize_centene_pricing(rate_group)
                
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
        if "provider_groups" in rate_group:
            normalized_groups = []
            for pg in rate_group["provider_groups"]:
                if "npi" in pg and "providers" not in pg:
                    # Centene puts provider info directly in provider_group
                    # Wrap in providers array for consistency
                    normalized_group = {
                        "providers": [pg.copy()]
                    }
                    # Keep TIN at group level if present
                    if "tin" in pg:
                        normalized_group["tin"] = pg["tin"]
                    normalized_groups.append(normalized_group)
                else:
                    # Standard structure
                    normalized_groups.append(pg)
            
            rate_group["provider_groups"] = normalized_groups
    
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
