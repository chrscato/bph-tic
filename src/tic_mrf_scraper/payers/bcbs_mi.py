from typing import Dict, Any, List, Optional

from . import PayerHandler, register_handler


@register_handler("bcbs_mi")
@register_handler("bcbsm")
class BCBSMIHandler(PayerHandler):
    """Handler for Blue Cross Blue Shield of Michigan with provider_references structure."""

    def __init__(self):
        super().__init__()
        self.provider_references_cache = {}  # Cache for provider reference lookups

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse BCBS MI records with provider_references structure."""
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
                "payer_name": "bcbs_mi"
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
                    negotiated_type = price.get("negotiated_type", "")
                    billing_class = price.get("billing_class", "")
                    service_codes = price.get("service_code", [])
                    if isinstance(service_codes, str):
                        service_codes = [service_codes]
                    
                    # Extract provider information from provider_references
                    provider_info = self._extract_provider_references_info(provider_references)
                    
                    # Create normalized record with provider reference info
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
                        "payer_name": "bcbs_mi"
                    }
                    
                    results.append(normalized_record)
        
        return results
    
    def _extract_provider_references_info(self, provider_references: List[str]) -> Dict[str, Any]:
        """Extract provider information from provider_references array.
        
        BCBS MI uses provider_references which are IDs that map to the provider_references
        section at the top level of the MRF file.
        """
        if not provider_references:
            return {}
        
        # Use the first provider reference ID
        provider_ref_id = provider_references[0] if provider_references else None
        
        # Look up provider info from cache
        if provider_ref_id and provider_ref_id in self.provider_references_cache:
            return self.provider_references_cache[provider_ref_id]
        
        # If not in cache, return basic info with reference ID
        return {
            "npi": None,
            "name": None,
            "tin": None,
            "provider_ref_id": provider_ref_id
        }
    
    def preprocess_mrf_file(self, mrf_data: Dict[str, Any]) -> None:
        """Preprocess the MRF file to extract and cache provider references.
        
        This should be called before processing in-network records to build the
        provider references cache.
        """
        provider_references_section = mrf_data.get("provider_references", [])
        
        # Build cache of provider information
        for provider_ref in provider_references_section:
            provider_group_id = provider_ref.get("provider_group_id")
            if provider_group_id:
                provider_groups = provider_ref.get("provider_groups", [])
                if provider_groups:
                    provider_group = provider_groups[0]  # Use first provider group
                    
                    # Extract NPI
                    npi = provider_group.get("npi")
                    if isinstance(npi, list) and npi:
                        npi = npi[0]  # Use first NPI
                    
                    # Extract TIN
                    tin = provider_group.get("tin")
                    if isinstance(tin, dict):
                        tin = tin.get("value", "")
                    
                    # Extract name
                    name = provider_group.get("name", "")
                    
                    # Cache the provider information
                    self.provider_references_cache[provider_group_id] = {
                        "npi": npi,
                        "name": name,
                        "tin": tin
                    }
    
    def get_provider_info_from_references(self, provider_ref_id: str, provider_references_section: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Look up provider information from the provider_references section.
        
        This method should be called with the full provider_references section from the MRF file
        to get actual provider details.
        """
        if not provider_ref_id or not provider_references_section:
            return {}
        
        # Find the provider reference by ID
        for provider_ref in provider_references_section:
            if provider_ref.get("provider_group_id") == provider_ref_id:
                # Extract provider information from the provider_groups
                provider_groups = provider_ref.get("provider_groups", [])
                if provider_groups:
                    provider_group = provider_groups[0]  # Use first provider group
                    
                    # Extract NPI
                    npi = provider_group.get("npi")
                    if isinstance(npi, list) and npi:
                        npi = npi[0]  # Use first NPI
                    
                    # Extract TIN
                    tin = provider_group.get("tin")
                    if isinstance(tin, dict):
                        tin = tin.get("value", "")
                    
                    # Extract name
                    name = provider_group.get("name", "")
                    
                    return {
                        "npi": npi,
                        "name": name,
                        "tin": tin
                    }
        
        return {} 