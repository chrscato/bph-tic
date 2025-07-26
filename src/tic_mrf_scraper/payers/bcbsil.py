from typing import Dict, Any, List
from . import PayerHandler, register_handler


@register_handler("bcbsil")
@register_handler("blue_cross_blue_shield_illinois")
class BCBSILHandler(PayerHandler):
    """Handler for Blue Cross Blue Shield Illinois complex provider structures."""

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        BCBSIL has complex nested structures with:
        - Extended provider address fields
        - Additional service codes and modifiers
        - Covered services details
        - Bundled codes relationships
        - EyeMed vision integration
        """
        if "negotiated_rates" in record:
            for rate_group in record.get("negotiated_rates", []):
                self._normalize_bcbsil_provider_groups(rate_group)
                self._normalize_bcbsil_negotiated_prices(rate_group)
                
        # Handle BCBSIL-specific top-level fields
        if "bundled_codes" in record:
            # Normalize bundled codes to standard format
            record["related_codes"] = record.pop("bundled_codes")
            
        if "prior_authorization_required" in record:
            # Ensure boolean type
            record["prior_auth_required"] = bool(record.get("prior_authorization_required", False))
            
        return [record]
    
    def _normalize_bcbsil_provider_groups(self, rate_group: Dict[str, Any]) -> None:
        """Normalize BCBSIL's complex provider group structure."""
        if "provider_groups" not in rate_group:
            return
            
        for provider_group in rate_group["provider_groups"]:
            if "providers" in provider_group:
                for provider in provider_group["providers"]:
                    # Standardize provider address structure
                    if "provider_address" in provider:
                        self._normalize_provider_address(provider)
                    
                    # Standardize specialty information
                    if "provider_specialty" in provider:
                        provider["specialty"] = provider.pop("provider_specialty")
                    
                    # Ensure NPI is integer
                    if "npi" in provider and isinstance(provider["npi"], str):
                        try:
                            provider["npi"] = int(provider["npi"])
                        except ValueError:
                            pass
    
    def _normalize_provider_address(self, provider: Dict[str, Any]) -> None:
        """Convert BCBSIL's nested address to standard format."""
        addr = provider.pop("provider_address", {})
        provider["address"] = {
            "street": addr.get("street", ""),
            "city": addr.get("city", ""),
            "state": addr.get("state", ""),
            "zip": str(addr.get("zip", "")),
            "country": addr.get("country", "US")
        }
    
    def _normalize_bcbsil_negotiated_prices(self, rate_group: Dict[str, Any]) -> None:
        """Normalize BCBSIL's extended negotiated prices structure."""
        if "negotiated_prices" not in rate_group:
            return
            
        for price in rate_group["negotiated_prices"]:
            # Standardize additional fees
            if "additional_fees" in price:
                fees = price.pop("additional_fees")
                price["fees"] = [
                    {
                        "type": fee.get("fee_type", "unknown"),
                        "amount": float(fee.get("amount", 0))
                    }
                    for fee in fees
                ]
            
            # Standardize covered services
            if "covered_services" in price:
                services = price.pop("covered_services")
                price["service_details"] = [
                    {
                        "code": svc.get("service_code", ""),
                        "description": svc.get("service_description", ""),
                        "unit": svc.get("unit_type", "visit")
                    }
                    for svc in services
                ]
            
            # Standardize modifiers
            if "modifiers" in price:
                price["billing_modifiers"] = price.pop("modifiers")
            
            # Ensure place of service is string
            if "place_of_service" in price:
                price["place_of_service"] = str(price["place_of_service"]) 