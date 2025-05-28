"""Enhanced module for fetching MRF blob URLs with Table of Contents support."""

import json
import requests
from typing import List, Dict, Any, Optional, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    reraise=True
)
def list_mrf_blobs_enhanced(index_url: str) -> List[Dict[str, Any]]:
    """Fetch comprehensive list of MRF blob URLs with metadata from index file.
    
    Args:
        index_url: URL to the index file
        
    Returns:
        List of MRF blob information with metadata
    """
    logger.info("fetching_enhanced_index", url=index_url)
    resp = requests.get(index_url)
    resp.raise_for_status()
    
    data = resp.json()
    logger.info("index_response_keys", keys=list(data.keys()) if isinstance(data, dict) else "array")
    
    if not isinstance(data, dict):
        raise ValueError(f"Expected dict response, got {type(data)}")
    
    mrfs = []
    
    # Handle standard Table of Contents structure
    if "reporting_structure" in data:
        logger.info("processing_table_of_contents")
        
        for i, structure in enumerate(data["reporting_structure"]):
            logger.info("processing_reporting_structure", 
                       index=i, 
                       keys=list(structure.keys()))
            
            # Extract plan information
            plan_name = structure.get("plan_name", f"plan_{i}")
            plan_id = structure.get("plan_id")
            plan_market_type = structure.get("plan_market_type")
            
            # Process in-network files
            if "in_network_files" in structure:
                for j, file_info in enumerate(structure["in_network_files"]):
                    if "location" in file_info:
                        mrf_info = {
                            "url": file_info["location"],
                            "type": "in_network_rates",
                            "plan_name": plan_name,
                            "plan_id": plan_id,
                            "plan_market_type": plan_market_type,
                            "description": file_info.get("description", ""),
                            "reporting_structure_index": i,
                            "file_index": j
                        }
                        
                        # Check for provider reference file
                        if "provider_references" in structure:
                            for provider_ref in structure["provider_references"]:
                                if "location" in provider_ref:
                                    mrf_info["provider_reference_url"] = provider_ref["location"]
                                    break
                        
                        mrfs.append(mrf_info)
            
            # Process allowed amount files
            if "allowed_amount_file" in structure:
                allowed_file = structure["allowed_amount_file"]
                if "location" in allowed_file:
                    mrf_info = {
                        "url": allowed_file["location"],
                        "type": "allowed_amounts",
                        "plan_name": plan_name,
                        "plan_id": plan_id,
                        "plan_market_type": plan_market_type,
                        "description": allowed_file.get("description", ""),
                        "reporting_structure_index": i,
                        "file_index": 0
                    }
                    mrfs.append(mrf_info)
    
    # Handle legacy blobs structure
    elif "blobs" in data:
        logger.info("processing_legacy_blobs")
        for i, blob in enumerate(data["blobs"]):
            if "url" in blob:
                mrf_info = {
                    "url": blob["url"],
                    "type": "unknown",
                    "plan_name": blob.get("name", f"blob_{i}"),
                    "plan_id": None,
                    "plan_market_type": None,
                    "description": blob.get("description", ""),
                    "reporting_structure_index": 0,
                    "file_index": i
                }
                mrfs.append(mrf_info)
    
    # Handle direct file links (some payers use this)
    elif "in_network_files" in data:
        logger.info("processing_direct_in_network_files")
        for i, file_info in enumerate(data["in_network_files"]):
            if "location" in file_info:
                mrf_info = {
                    "url": file_info["location"],
                    "type": "in_network_rates",
                    "plan_name": file_info.get("description", f"file_{i}"),
                    "plan_id": None,
                    "plan_market_type": None,
                    "description": file_info.get("description", ""),
                    "reporting_structure_index": 0,
                    "file_index": i
                }
                mrfs.append(mrf_info)
    
    else:
        available_keys = list(data.keys())
        logger.error("unknown_index_structure", keys=available_keys)
        raise ValueError(f"Response missing expected keys. Available keys: {available_keys}")
    
    logger.info("found_mrf_files", count=len(mrfs))
    return mrfs

def list_mrf_blobs(index_url: str) -> List[str]:
    """Legacy function for backward compatibility - returns just URLs."""
    enhanced_results = list_mrf_blobs_enhanced(index_url)
    return [mrf["url"] for mrf in enhanced_results]

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
    resp = requests.get(url, stream=True)  # Stream for large files
    resp.raise_for_status()
    return resp.content

def analyze_index_structure(index_url: str) -> Dict[str, Any]:
    """Analyze the structure of an index file for debugging.
    
    Args:
        index_url: URL to the index file
        
    Returns:
        Analysis of the index structure
    """
    logger.info("analyzing_index_structure", url=index_url)
    
    try:
        resp = requests.get(index_url)
        resp.raise_for_status()
        data = resp.json()
        
        analysis = {
            "url": index_url,
            "status": "success",
            "root_type": type(data).__name__,
            "top_level_keys": list(data.keys()) if isinstance(data, dict) else [],
            "estimated_mrf_count": 0,
            "structure_type": "unknown",
            "plans_identified": [],
            "sample_urls": []
        }
        
        if isinstance(data, dict):
            if "reporting_structure" in data:
                analysis["structure_type"] = "table_of_contents"
                analysis["estimated_mrf_count"] = len(data["reporting_structure"])
                
                # Sample first few plans
                for structure in data["reporting_structure"][:3]:
                    plan_info = {
                        "plan_name": structure.get("plan_name"),
                        "plan_id": structure.get("plan_id"),
                        "in_network_files": len(structure.get("in_network_files", [])),
                        "has_allowed_amounts": "allowed_amount_file" in structure,
                        "has_provider_references": "provider_references" in structure
                    }
                    analysis["plans_identified"].append(plan_info)
                    
                    # Collect sample URLs
                    if "in_network_files" in structure:
                        for file_info in structure["in_network_files"][:2]:
                            if "location" in file_info:
                                analysis["sample_urls"].append(file_info["location"])
            
            elif "blobs" in data:
                analysis["structure_type"] = "legacy_blobs"
                analysis["estimated_mrf_count"] = len(data["blobs"])
                analysis["sample_urls"] = [blob.get("url") for blob in data["blobs"][:3] if blob.get("url")]
        
        return analysis
        
    except Exception as e:
        return {
            "url": index_url,
            "status": "error",
            "error": str(e)
        }
