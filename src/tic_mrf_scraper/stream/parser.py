"""Module for streaming and parsing MRF data."""

import gzip
import ijson
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
    content = fetch_url(url)
    
    # Decompress gzipped content
    with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
        # Parse JSON stream
        parser = ijson.parse(gz)
        
        # Skip to in_network array
        for prefix, event, value in parser:
            if prefix == "in_network.item":
                yield value
