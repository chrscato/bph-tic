from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("aetna")
@register_handler("aetna_florida")
@register_handler("aetna_health_inc")
class AetnaHandler(PayerHandler):
    """Handler for Aetna's hybrid provider reference structure."""

    # Remove custom parse_in_network method to use streaming parser's provider extraction
    # The streaming parser will automatically handle provider extraction from provider_groups and provider_references 