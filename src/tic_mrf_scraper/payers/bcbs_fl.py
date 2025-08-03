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

    # Remove custom parse_in_network method to use streaming parser's provider extraction
    # The streaming parser will automatically handle provider extraction from provider_groups and provider_references
