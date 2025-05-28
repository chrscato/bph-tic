"""Module for fetching MRF blob URLs from index files."""

import json
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def list_mrf_blobs(index_url: str) -> list[str]:
    """Fetch list of MRF blob URLs from index file.
    
    Args:
        index_url: URL to the index file
        
    Returns:
        List of MRF blob URLs
    """
    logger.info("fetching_index", url=index_url)
    resp = requests.get(index_url)
    resp.raise_for_status()
    
    data = resp.json()
    return [blob["url"] for blob in data["blobs"]]

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def fetch_url(url: str) -> bytes:
    """Fetch data from URL with retry logic.
    
    Args:
        url: URL to fetch
        
    Returns:
        Response content as bytes
    """
    logger.info("fetching_url", url=url)
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.content
