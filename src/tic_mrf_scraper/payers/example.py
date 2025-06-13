from typing import Dict, Any, List

from . import PayerHandler, register_handler


@register_handler("example")
class ExampleHandler(PayerHandler):
    """Skeleton payer handler for custom formats."""

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Modify an ``in_network`` item if needed and return one or more records."""
        return super().parse_in_network(record)
