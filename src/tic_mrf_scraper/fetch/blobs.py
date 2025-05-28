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
    logger.info("index_response", data=data)
    
    # Check if data has the expected structure
    if not isinstance(data, dict):
        raise ValueError(f"Expected dict response, got {type(data)}")
    
    # Handle Centene API structure
    if "reporting_structure" in data:
        urls = []
        for structure in data["reporting_structure"]:
            # Get in-network files
            if "in_network_files" in structure:
                for file_info in structure["in_network_files"]:
                    if "location" in file_info:
                        urls.append(file_info["location"])
            # Get allowed amount files
            if "allowed_amount_file" in structure and "location" in structure["allowed_amount_file"]:
                urls.append(structure["allowed_amount_file"]["location"])
        return urls
    
    # Handle standard blobs structure
    if "blobs" in data:
        return [blob["url"] for blob in data["blobs"]]
    
    raise ValueError(f"Response missing expected keys. Available keys: {list(data.keys())}")

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
