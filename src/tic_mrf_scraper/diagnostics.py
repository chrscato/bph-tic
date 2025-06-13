import json
import gzip
from io import BytesIO
from typing import Dict, Any

from .fetch.blobs import fetch_url, analyze_index_structure
from .utils.backoff_logger import get_logger

logger = get_logger(__name__)


def identify_index(index_url: str) -> Dict[str, Any]:
    """Analyze an index URL and log the structure information."""
    analysis = analyze_index_structure(index_url)
    logger.info("index_identified", analysis=analysis)
    return analysis


def detect_compression(url: str) -> str:
    """Return the compression type based on the URL or content."""
    if url.endswith('.gz'):
        compression = 'gzip'
    else:
        try:
            head = fetch_url(url)[:2]
            compression = 'gzip' if head.startswith(b'\x1f\x8b') else 'none'
        except Exception:
            compression = 'unknown'
    logger.info("detected_compression", url=url, compression=compression)
    return compression


def identify_in_network(url: str, sample_size: int = 1) -> Dict[str, Any]:
    """Inspect a small sample of the in_network structure from an MRF."""
    try:
        content = fetch_url(url)
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                data = json.load(gz)
        else:
            data = json.loads(content.decode('utf-8'))

        if isinstance(data, dict) and 'in_network' in data:
            in_net = data['in_network'][:sample_size]
            keys = sorted({k for item in in_net for k in item.keys()})
            info = {"total_in_network": len(data['in_network']), "sample_keys": keys}
        else:
            info = {"total_in_network": 0, "sample_keys": []}
    except Exception as e:
        logger.warning("identify_in_network_failed", url=url, error=str(e))
        info = {"total_in_network": 0, "sample_keys": []}
    logger.info("identified_in_network", url=url, info=info)
    return info
