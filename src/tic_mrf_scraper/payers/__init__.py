from typing import Dict, Any, List, Type

from ..fetch.blobs import list_mrf_blobs_enhanced


class PayerHandler:
    """Base class for payer specific logic."""

    def list_mrf_files(self, index_url: str) -> List[Dict[str, Any]]:
        """Return list of MRF metadata dictionaries for an index."""
        return list_mrf_blobs_enhanced(index_url)

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Hook to massage an in_network record before parsing."""
        return [record]


_handler_registry: Dict[str, Type[PayerHandler]] = {}


def register_handler(name: str):
    """Decorator to register a handler for a payer."""
    def wrapper(cls: Type[PayerHandler]):
        _handler_registry[name.lower()] = cls
        return cls
    return wrapper


def get_handler(name: str) -> PayerHandler:
    """Return handler instance for payer name."""
    cls = _handler_registry.get(name.lower(), PayerHandler)
    return cls()
