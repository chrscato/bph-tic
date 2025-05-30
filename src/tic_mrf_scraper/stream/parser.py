"""Enhanced module for streaming and parsing TiC MRF data with proper structure traversal."""

import gzip
import json
from io import BytesIO
from typing import Iterator, Dict, Any, Optional, List

from ..fetch.blobs import fetch_url
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

class TiCMRFParser:
    """Parser for Transparency in Coverage Machine Readable Files."""
    
    def __init__(self):
        self.provider_references = {}
        
    def load_provider_references(self, provider_ref_url: str) -> Dict[int, Dict[str, Any]]:
        """Load external provider reference file if specified.
        
        Args:
            provider_ref_url: URL to provider reference file
            
        Returns:
            Dictionary mapping provider IDs to provider info
        """
        try:
            content = fetch_url(provider_ref_url)
            if provider_ref_url.endswith('.gz'):
                with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                    data = json.load(gz)
            else:
                data = json.loads(content.decode('utf-8'))
                
            # Build lookup table from provider references
            provider_lookup = {}
            if "provider_references" in data:
                for i, provider in enumerate(data["provider_references"]):
                    provider_lookup[i] = provider
                    
            return provider_lookup
            
        except Exception as e:
            logger.warning("failed_to_load_provider_references", 
                          url=provider_ref_url, error=str(e))
            return {}

    def parse_negotiated_rates(self, 
                              in_network_item: Dict[str, Any], 
                              payer: str) -> Iterator[Dict[str, Any]]:
        """Parse negotiated rates from an in-network item.
        
        Args:
            in_network_item: Single item from in_network array
            payer: Payer name
            
        Yields:
            Individual rate records
        """
        billing_code = in_network_item.get("billing_code")
        billing_code_type = in_network_item.get("billing_code_type", "")
        description = in_network_item.get("description", "")
        
        # DEBUG: Log what we're processing
        logger.debug("debug_parsing_item", 
                   billing_code=billing_code, 
                   billing_code_type=billing_code_type,
                   has_negotiated_rates="negotiated_rates" in in_network_item,
                   item_keys=list(in_network_item.keys()))
        
        # Handle negotiated_rates array
        negotiated_rates = in_network_item.get("negotiated_rates", [])
        logger.debug("debug_negotiated_rates", 
                   billing_code=billing_code,
                   negotiated_rates_count=len(negotiated_rates))
        
        if not negotiated_rates:
            logger.warning("no_negotiated_rates", billing_code=billing_code)
            return
        
        for i, rate_group in enumerate(negotiated_rates):
            logger.debug("debug_processing_rate_group",
                       billing_code=billing_code,
                       group_index=i,
                       group_keys=list(rate_group.keys()))
            
            # Get provider information
            provider_refs = rate_group.get("provider_references", [])
            provider_groups = rate_group.get("provider_groups", [])
            
            logger.debug("debug_provider_info",
                       billing_code=billing_code,
                       group_index=i,
                       has_provider_refs=bool(provider_refs),
                       has_provider_groups=bool(provider_groups),
                       provider_refs_count=len(provider_refs),
                       provider_groups_count=len(provider_groups))
            
            # Handle negotiated_prices array within each rate group
            negotiated_prices = rate_group.get("negotiated_prices", [])
            logger.debug("debug_negotiated_prices",
                       billing_code=billing_code,
                       group_index=i,
                       prices_count=len(negotiated_prices))
            
            for j, price_item in enumerate(negotiated_prices):
                logger.debug("debug_processing_price",
                           billing_code=billing_code,
                           group_index=i,
                           price_index=j,
                           price_keys=list(price_item.keys()))
                
                negotiated_rate = price_item.get("negotiated_rate")
                logger.debug("debug_price_details",
                           billing_code=billing_code,
                           group_index=i,
                           price_index=j,
                           has_negotiated_rate=negotiated_rate is not None,
                           negotiated_rate=negotiated_rate)
                
                if negotiated_rate is None:
                    logger.warning("skipping_price_no_rate",
                                 billing_code=billing_code,
                                 group_index=i,
                                 price_index=j)
                    continue
                
                # Extract additional fields
                service_codes = price_item.get("service_code", [])
                billing_class = price_item.get("billing_class", "")
                negotiated_type = price_item.get("negotiated_type", "")
                expiration_date = price_item.get("expiration_date", "")
                
                logger.debug("debug_yielding_record",
                           billing_code=billing_code,
                           group_index=i,
                           price_index=j,
                           rate=negotiated_rate,
                           service_codes=service_codes,
                           billing_class=billing_class)
                
                # Process provider references
                if provider_refs:
                    for provider_ref in provider_refs:
                        provider_info = self.provider_references.get(provider_ref, {})
                        logger.debug("debug_provider_ref",
                                  billing_code=billing_code,
                                  group_index=i,
                                  price_index=j,
                                  provider_ref=provider_ref,
                                  has_provider_info=bool(provider_info))
                        yield self._create_rate_record(
                            billing_code=billing_code,
                            billing_code_type=billing_code_type,
                            description=description,
                            negotiated_rate=negotiated_rate,
                            service_codes=service_codes,
                            billing_class=billing_class,
                            negotiated_type=negotiated_type,
                            expiration_date=expiration_date,
                            provider_info=provider_info,
                            payer=payer
                        )
                # Handle provider_groups (when providers are embedded)
                elif provider_groups:
                    for provider_group in provider_groups:
                        # Handle direct provider info in the provider_group itself
                        if "npi" in provider_group or "tin" in provider_group:
                            # Direct provider info in the provider_group itself
                            yield self._create_rate_record(
                                billing_code=billing_code,
                                billing_code_type=billing_code_type,
                                description=description,
                                negotiated_rate=negotiated_rate,
                                service_codes=service_codes,
                                billing_class=billing_class,
                                negotiated_type=negotiated_type,
                                expiration_date=expiration_date,
                                provider_info=provider_group,  # Use the provider_group directly
                                payer=payer
                            )
                        elif "providers" in provider_group:
                            # Handle nested providers array (fallback for other payers)
                            providers = provider_group.get("providers", [])
                            for provider in providers:
                                yield self._create_rate_record(
                                    billing_code=billing_code,
                                    billing_code_type=billing_code_type,
                                    description=description,
                                    negotiated_rate=negotiated_rate,
                                    service_codes=service_codes,
                                    billing_class=billing_class,
                                    negotiated_type=negotiated_type,
                                    expiration_date=expiration_date,
                                    provider_info=provider,
                                    payer=payer
                                )
                        else:
                            # No provider info - create generic record
                            logger.debug("debug_generic_record",
                                      billing_code=billing_code,
                                      group_index=i,
                                      price_index=j)
                            yield self._create_rate_record(
                                billing_code=billing_code,
                                billing_code_type=billing_code_type,
                                description=description,
                                negotiated_rate=negotiated_rate,
                                service_codes=service_codes,
                                billing_class=billing_class,
                                payer=payer
                            )
                else:
                    # No provider info - create generic record
                    logger.debug("debug_generic_record",
                              billing_code=billing_code,
                              group_index=i,
                              price_index=j)
                    yield self._create_rate_record(
                        billing_code=billing_code,
                        billing_code_type=billing_code_type,
                        description=description,
                        negotiated_rate=negotiated_rate,
                        service_codes=service_codes,
                        billing_class=billing_class,
                        payer=payer
                    )

    def _create_rate_record(self, 
                           billing_code: str,
                           billing_code_type: str,
                           description: str,
                           negotiated_rate: float,
                           service_codes: List[str],
                           billing_class: str,
                           negotiated_type: str,
                           expiration_date: str,
                           provider_info: Dict[str, Any],
                           payer: str) -> Dict[str, Any]:
        """Create a normalized rate record.
        
        Returns:
            Normalized rate record
        """
        return {
            "billing_code": billing_code,
            "billing_code_type": billing_code_type,
            "description": description,
            "negotiated_rate": float(negotiated_rate),
            "service_codes": service_codes,
            "billing_class": billing_class,
            "negotiated_type": negotiated_type,
            "expiration_date": expiration_date,
            "provider_npi": provider_info.get("npi"),
            "provider_name": provider_info.get("provider_group_name"),
            "provider_tin": provider_info.get("tin", {}).get("value") if provider_info.get("tin") else None,
            "payer": payer
        }

def stream_parse_enhanced(url: str, payer: str, 
                         provider_ref_url: Optional[str] = None) -> Iterator[Dict[str, Any]]:
    """Enhanced streaming parser for TiC MRF data.
    
    Args:
        url: URL to MRF data file
        payer: Payer name
        provider_ref_url: Optional URL to provider reference file
        
    Yields:
        Parsed and normalized MRF records
    """
    logger.info("streaming_tic_mrf", url=url, payer=payer)
    
    parser = TiCMRFParser()
    
    # Load provider references if specified
    if provider_ref_url:
        parser.provider_references = parser.load_provider_references(provider_ref_url)
        logger.info("loaded_provider_references", 
                   count=len(parser.provider_references))
    
    try:
        content = fetch_url(url)
        
        # Handle gzipped content
        if url.endswith('.gz'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                data = json.load(gz)
        else:
            data = json.loads(content.decode('utf-8'))
        
        logger.info("loaded_mrf_structure", 
                   top_level_keys=list(data.keys()) if isinstance(data, dict) else "array")
        
        # Handle different MRF structures
        if isinstance(data, dict):
            # Standard TiC structure with in_network array
            if "in_network" in data:
                in_network_items = data["in_network"]
                logger.info("processing_in_network_items", count=len(in_network_items))
                
                for item in in_network_items:
                    # Parse each in-network item and yield individual rate records
                    for rate_record in parser.parse_negotiated_rates(item, payer):
                        yield rate_record
                        
            # Handle provider_references at top level (for reference files)
            elif "provider_references" in data:
                logger.info("found_provider_reference_file", 
                           count=len(data["provider_references"]))
                # This is a provider reference file, not a rates file
                return
                
            # Handle allowed amounts structure (out-of-network)
            elif "allowed_amounts" in data:
                logger.info("found_allowed_amounts_structure")
                # Handle allowed amounts data structure
                # (Implementation depends on your requirements)
                return
                
            else:
                logger.warning("unknown_mrf_structure", 
                              available_keys=list(data.keys()))
                
        elif isinstance(data, list):
            # Legacy or non-standard structure
            logger.info("processing_legacy_array_structure", count=len(data))
            for record in data:
                yield record
                
    except Exception as e:
        logger.error("parsing_failed", url=url, error=str(e))
        raise

# Backward compatibility function
def stream_parse(url: str) -> Iterator[Dict[str, Any]]:
    """Backward compatible parsing function."""
    return stream_parse_enhanced(url, "unknown_payer")
