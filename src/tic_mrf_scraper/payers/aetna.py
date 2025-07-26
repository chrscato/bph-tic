from typing import Dict, Any, List
from . import PayerHandler, register_handler


@register_handler("aetna")
@register_handler("aetna_florida")
@register_handler("aetna_health_inc")
class AetnaHandler(PayerHandler):
    """Handler for Aetna's hybrid provider reference structure."""

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Aetna has:
        - Hybrid provider references (both embedded and referenced)
        - CVS Health integration fields
        - State-specific processing
        - HealthSparq platform formatting
        """
        if "negotiated_rates" in record:
            for rate_group in record.get("negotiated_rates", []):
                self._normalize_aetna_hybrid_providers(rate_group)
                self._normalize_aetna_pricing(rate_group)
                
        # Handle Aetna-specific top-level fields
        self._normalize_aetna_metadata(record)
                
        return [record]
    
    def _normalize_aetna_hybrid_providers(self, rate_group: Dict[str, Any]) -> None:
        """Handle Aetna's hybrid provider structure."""
        provider_groups = rate_group.get("provider_groups", [])
        provider_refs = rate_group.get("provider_references", [])
        
        # If we have both, merge the information
        if provider_groups and provider_refs:
            for i, provider_group in enumerate(provider_groups):
                # Add reference ID for lookup if embedded data is incomplete
                if "npi" not in provider_group and i < len(provider_refs):
                    provider_group["provider_reference_id"] = provider_refs[i]
                
                # Ensure provider group has minimum required fields
                if "providers" not in provider_group and "npi" in provider_group:
                    # Convert direct NPI to providers array
                    provider_group["providers"] = [{
                        "npi": provider_group["npi"],
                        "provider_name": provider_group.get("provider_name", "")
                    }]
        
        # Standardize provider groups
        for provider_group in provider_groups:
            self._normalize_aetna_provider_group(provider_group)
    
    def _normalize_aetna_provider_group(self, provider_group: Dict[str, Any]) -> None:
        """Normalize individual Aetna provider group."""
        # Standardize TIN format
        if "tin" in provider_group:
            tin = provider_group["tin"]
            if isinstance(tin, str):
                provider_group["tin"] = {
                    "type": "ein",
                    "value": tin
                }
        
        # Standardize providers array
        if "providers" in provider_group:
            for provider in provider_group["providers"]:
                # Add CVS-specific fields if present
                if "cvs_location_id" in provider:
                    provider["location_id"] = provider.pop("cvs_location_id")
                
                # Ensure NPI is integer
                if "npi" in provider and isinstance(provider["npi"], str):
                    try:
                        provider["npi"] = int(provider["npi"])
                    except ValueError:
                        pass
    
    def _normalize_aetna_pricing(self, rate_group: Dict[str, Any]) -> None:
        """Normalize Aetna's negotiated prices structure."""
        if "negotiated_prices" not in rate_group:
            return
            
        for price in rate_group["negotiated_prices"]:
            # Standardize billing class
            if "billing_class" in price:
                price["billing_class"] = price["billing_class"].lower()
            
            # Handle CVS-specific pricing fields
            if "cvs_pricing_tier" in price:
                price["pricing_tier"] = price.pop("cvs_pricing_tier")
            
            # Ensure service codes are list
            if "service_code" in price and isinstance(price["service_code"], str):
                price["service_code"] = [price["service_code"]]
    
    def _normalize_aetna_metadata(self, record: Dict[str, Any]) -> None:
        """Normalize Aetna-specific metadata fields."""
        # Handle state-specific identifiers
        if "state_specific_id" in record:
            record["regional_id"] = record.pop("state_specific_id")
        
        # Normalize plan type for Florida-specific plans
        if "plan_type" in record and "florida" in record.get("description", "").lower():
            record["state_plan"] = "FL" 