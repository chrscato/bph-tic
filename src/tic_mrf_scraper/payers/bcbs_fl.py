from typing import Dict, Any, List, Optional

from . import PayerHandler, register_handler


@register_handler("bcbs_fl")
@register_handler("florida_blue")
class BCBSFLHandler(PayerHandler):
    """Handler for Blue Cross Blue Shield of Florida (Florida Blue) with provider_references structure.
    
    Based on structure analysis:
    - Uses provider_references (external, not embedded)
    - Standard negotiated_rates structure
    - Complex service codes arrays
    - CPT billing codes
    """

    def __init__(self):
        super().__init__()
        self.provider_references_cache = {}  # Cache for provider reference lookups

    def preprocess_mrf_file(self, mrf_data: Dict[str, Any]) -> None:
        """Preprocess MRF file to build provider references cache."""
        provider_references = mrf_data.get("provider_references", [])
        
        for provider_ref in provider_references:
            provider_group_id = provider_ref.get("provider_group_id")
            if provider_group_id:
                self.provider_references_cache[provider_group_id] = provider_ref

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse BCBS FL records with provider_references structure."""
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
                "payer_name": "bcbs_fl"
            }
            results.append(normalized_record)
        else:
            # Handle complex nested structure with provider_references
            for rate_group in negotiated_rates:
                negotiated_prices = rate_group.get("negotiated_prices", [])
                provider_references = rate_group.get("provider_references", [])
                
                # Process each negotiated price
                for price in negotiated_prices:
                    negotiated_rate = price.get("negotiated_rate")
                    if negotiated_rate is None or negotiated_rate <= 0:
                        continue  # Skip invalid rates
                    
                    negotiated_type = price.get("negotiated_type", "")
                    billing_class = price.get("billing_class", "")
                    expiration_date = price.get("expiration_date", "")
                    billing_code_modifier = price.get("billing_code_modifier", "")
                    
                    # Handle service codes (can be string or array)
                    service_codes = price.get("service_code", [])
                    if isinstance(service_codes, str):
                        service_codes = [service_codes]
                    elif service_codes is None:
                        service_codes = []
                    
                    # Extract provider information from provider_references
                    provider_info = self._extract_provider_references_info(provider_references)
                    
                    # Create normalized record
                    normalized_record = {
                        "billing_code": billing_code,
                        "billing_code_type": billing_code_type,
                        "description": description,
                        "negotiated_rate": negotiated_rate,
                        "negotiated_type": negotiated_type,
                        "billing_class": billing_class,
                        "expiration_date": expiration_date,
                        "billing_code_modifier": billing_code_modifier,
                        "service_codes": service_codes,
                        "provider_npi": provider_info.get("npi"),
                        "provider_name": provider_info.get("name"),
                        "provider_tin": provider_info.get("tin"),
                        "payer_name": "bcbs_fl"
                    }
                    results.append(normalized_record)
        
        return results

    def _extract_provider_references_info(self, provider_references: List[str]) -> Dict[str, Any]:
        """Extract provider information from provider reference IDs."""
        provider_info = {
            "npi": None,
            "name": None,
            "tin": None
        }
        
        if not provider_references:
            return provider_info
        
        # Use the first provider reference (could be enhanced to handle multiple)
        first_provider_ref = provider_references[0] if provider_references else None
        if not first_provider_ref:
            return provider_info
        
        # Look up provider info from cache
        provider_data = self.provider_references_cache.get(first_provider_ref)
        if not provider_data:
            return provider_info
        
        # Extract provider groups information
        provider_groups = provider_data.get("provider_groups", [])
        if provider_groups:
            first_group = provider_groups[0]
            
            # Extract NPI (handle both list and string formats)
            npi = first_group.get("npi")
            if isinstance(npi, list) and npi:
                provider_info["npi"] = npi[0]
            elif isinstance(npi, str):
                provider_info["npi"] = npi
            
            # Extract provider name
            provider_info["name"] = first_group.get("name") or first_group.get("provider_group_name")
            
            # Extract TIN (handle different formats)
            tin = first_group.get("tin")
            if isinstance(tin, dict):
                provider_info["tin"] = tin.get("value")
            elif isinstance(tin, str):
                provider_info["tin"] = tin
        
        return provider_info

    def get_provider_info_from_references(self, provider_ref_id: str, provider_references: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get provider information from provider reference ID (for testing/debugging)."""
        for provider_ref in provider_references:
            if provider_ref.get("provider_group_id") == provider_ref_id:
                provider_groups = provider_ref.get("provider_groups", [])
                if provider_groups:
                    first_group = provider_groups[0]
                    
                    # Extract information in standardized format
                    provider_info = {
                        "npi": None,
                        "name": None,
                        "tin": None
                    }
                    
                    # Handle NPI
                    npi = first_group.get("npi")
                    if isinstance(npi, list) and npi:
                        provider_info["npi"] = npi[0]
                    elif isinstance(npi, str):
                        provider_info["npi"] = npi
                    
                    # Handle name
                    provider_info["name"] = first_group.get("name") or first_group.get("provider_group_name")
                    
                    # Handle TIN
                    tin = first_group.get("tin")
                    if isinstance(tin, dict):
                        provider_info["tin"] = tin.get("value")
                    elif isinstance(tin, str):
                        provider_info["tin"] = tin
                    
                    return provider_info
        
        return {"npi": None, "name": None, "tin": None}