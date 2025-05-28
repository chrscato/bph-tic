"""Module for streaming and parsing MRF data."""

import gzip
import ijson
import json
from io import BytesIO
from typing import Iterator, Dict, Any

from ..fetch.blobs import fetch_url
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

def stream_parse(url: str) -> Iterator[Dict[str, Any]]:
    """Stream and parse MRF data from URL.
    
    Args:
        url: URL to MRF data file
        
    Yields:
        Parsed MRF records
    """
    logger.info("streaming_mrf", url=url)
    
    try:
        content = fetch_url(url)
        
        # Handle gzipped content
        if url.endswith('.gz'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                data = json.load(gz)
        else:
            data = json.loads(content.decode('utf-8'))
        
        # Handle different MRF structures
        if isinstance(data, list):
            # Direct array of records
            for record in data:
                yield record
        elif isinstance(data, dict):
            # Look for common MRF structures
            if "in_network" in data:
                for record in data["in_network"]:
                    yield record
            elif "provider_references" in data:
                for record in data["provider_references"]:
                    yield record
            else:
                # Single record
                yield data
                
    except Exception as e:
        logger.error("parsing_failed", url=url, error=str(e))
        raise
