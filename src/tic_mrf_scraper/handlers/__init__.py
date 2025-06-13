"""Payer handler plugin system."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any
from importlib import metadata


@dataclass
class BasePayerHandler:
    """Base class for payer specific logic."""
    payer: str

    def process(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Process or modify a normalized record."""
        return record


def get_handler(payer: str) -> BasePayerHandler:
    """Load a registered handler for ``payer`` if available."""
    try:
        eps = metadata.entry_points(group="tic_mrf_scraper.payer_handlers")
    except Exception:
        eps = []
    for ep in eps:
        if ep.name == payer:
            handler_cls = ep.load()
            return handler_cls(payer=payer)
    return BasePayerHandler(payer=payer)
