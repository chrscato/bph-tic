from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("centene")
@register_handler("centene_fidelis")
class CenteneHandler(PayerHandler):
    """Handler for Centene-family payers."""

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Centene sometimes places provider info directly under provider_groups
        if "negotiated_rates" in record:
            for group in record.get("negotiated_rates", []):
                normalized = []
                for pg in group.get("provider_groups", []):
                    if "npi" in pg:
                        normalized.append({"providers": [pg]})
                    else:
                        normalized.append(pg)
                group["provider_groups"] = normalized
        return [record]
