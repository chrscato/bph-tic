from typing import Dict, Any, List, Type
import warnings

from ..fetch.blobs import list_mrf_blobs_enhanced


class PayerHandler:
    """Base class for payer specific logic."""

    def list_mrf_files(self, index_url: str) -> List[Dict[str, Any]]:
        """Return list of MRF metadata dictionaries for an index."""
        return list_mrf_blobs_enhanced(index_url)

    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Modify one ``in_network`` item before normalization.

        Subclasses should override this method when a payer's MRF deviates from
        the standard structure. The method must return a list of records to be
        passed into the normal parser.
        """
        return [record]


_handler_registry: Dict[str, Type[PayerHandler]] = {}


def register_handler(name: str):
    """Decorator to register a handler for a payer."""
    def wrapper(cls: Type[PayerHandler]):
        if cls.parse_in_network is PayerHandler.parse_in_network:
            warnings.warn(
                f"{cls.__name__} does not override parse_in_network; "
                "copy the base implementation or provide custom logic if needed",
                UserWarning,
            )
        _handler_registry[name.lower()] = cls
        return cls
    return wrapper


def get_handler(name: str) -> PayerHandler:
    """Return handler instance for payer name."""
    cls = _handler_registry.get(name.lower(), PayerHandler)
    return cls()
