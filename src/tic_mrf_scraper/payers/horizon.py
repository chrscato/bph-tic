from typing import Dict, Any, List
from . import PayerHandler, register_handler


@register_handler("horizon_bcbs")
@register_handler("horizon")
@register_handler("horizon_healthcare")
class HorizonHandler(PayerHandler):
    """Handler for Horizon Blue Cross Blue Shield with geographic regions."""

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Horizon has:
        - Geographic region specifications
        - Service-type based file splitting
        - Moderate provider complexity
        - Regional pricing variations
        """
        if "negotiated_rates" in record:
            for rate_group in record.get("negotiated_rates", []):
                self._normalize_horizon_geographic_data(rate_group)
                self._normalize_horizon_provider_groups(rate_group)
                
        return [record]
    
    def _normalize_horizon_geographic_data(self, rate_group: Dict[str, Any]) -> None:
        """Standardize Horizon's geographic region fields."""
        if "negotiated_prices" not in rate_group:
            return
            
        for price in rate_group["negotiated_prices"]:
            # Standardize geographic region field
            if "geographic_region" in price:
                region = price.pop("geographic_region")
                price["service_geography"] = self._parse_horizon_region(region)
            
            # Standardize billing class
            if "billing_class" in price:
                price["billing_class"] = price["billing_class"].lower()
    
    def _parse_horizon_region(self, region: str) -> Dict[str, str]:
        """Parse Horizon's geographic region codes."""
        # Horizon uses codes like "NJ_NORTH", "NJ_SOUTH", "NY_METRO"
        if "_" in region:
            state, area = region.split("_", 1)
            return {
                "state": state,
                "region": area.lower(),
                "full_code": region
            }
        return {
            "state": region,
            "region": "statewide",
            "full_code": region
        }
    
    def _normalize_horizon_provider_groups(self, rate_group: Dict[str, Any]) -> None:
        """Normalize Horizon's provider group structure."""
        if "provider_groups" not in rate_group:
            return
            
        for provider_group in rate_group["provider_groups"]:
            # Ensure TIN structure is consistent
            if "tin" in provider_group:
                tin = provider_group["tin"]
                if isinstance(tin, str):
                    # Convert string TIN to object format
                    provider_group["tin"] = {
                        "type": "ein",
                        "value": tin
                    }
            
            # Standardize provider array
            if "providers" in provider_group:
                for provider in provider_group["providers"]:
                    # Ensure NPI is integer
                    if "npi" in provider and isinstance(provider["npi"], str):
                        try:
                            provider["npi"] = int(provider["npi"])
                        except ValueError:
                            pass 