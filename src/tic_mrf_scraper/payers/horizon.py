from typing import Dict, Any, List
from . import PayerHandler, register_handler


@register_handler("horizon_bcbs")
@register_handler("horizon")
@register_handler("horizon_healthcare")
class HorizonHandler(PayerHandler):
    """Handler for Horizon Blue Cross Blue Shield with geographic regions."""

    # Remove custom parse_in_network method to use streaming parser's provider extraction
    # The streaming parser will automatically handle provider extraction from provider_groups and provider_references 