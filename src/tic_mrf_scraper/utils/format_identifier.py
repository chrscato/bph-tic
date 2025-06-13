"""Utility helpers for detecting MRF formats and structures."""

from typing import Dict, Any
from urllib.parse import urlparse

from ..transform.normalize import validate_mrf_structure


def detect_compression(url: str) -> str:
    """Detect the file compression type based on URL extension."""
    path = urlparse(url).path.lower()
    if path.endswith(".json.gz"):
        return "json.gz"
    if path.endswith(".json"):
        return "json"
    if path.endswith(".tar.gz"):
        return "tar.gz"
    if path.endswith(".gz"):
        return "gz"
    if path.endswith(".zip"):
        return "zip"
    if "." in path:
        return path.rsplit(".", 1)[-1]
    return "unknown"


def identify_index(url: str) -> Dict[str, Any]:
    """Analyze an index URL and return structure information."""
    from ..fetch.blobs import analyze_index_structure
    return analyze_index_structure(url)


def identify_in_network(data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate an in-network MRF JSON structure."""
    return validate_mrf_structure(data)
