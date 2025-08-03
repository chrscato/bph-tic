"""Enhanced module for streaming and parsing TiC MRF data with proper structure traversal."""

import json
import gzip
import logging
import gc
import psutil
import os
from io import BytesIO
from typing import Dict, Any, List, Optional, Iterator
from urllib.parse import urlparse
import requests

from ..fetch.blobs import fetch_url
from ..payers import PayerHandler
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

# Try to import ijson for streaming JSON parsing
try:
    import ijson
    IJSON_AVAILABLE = True
except ImportError:
    IJSON_AVAILABLE = False
    logger.warning("ijson not available, falling back to memory-intensive parsing")

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def log_memory_usage(stage: str):
    """Log memory usage for monitoring."""
    memory_mb = get_memory_usage()
    logger.info("memory_usage", stage=stage, memory_mb=memory_mb)
    return memory_mb

class TiCMRFParser:
    """Memory-efficient TiC MRF parser with streaming support."""
    
    def __init__(self):
        self.provider_references = {}
    
    def load_provider_references(self, provider_ref_url: str) -> Dict[int, Dict[str, Any]]:
        """Load provider references with memory-efficient streaming."""
        logger.info("loading_provider_references", url=provider_ref_url)
        
        try:
            # Use streaming for large provider reference files
            if IJSON_AVAILABLE:
                return self._load_provider_references_streaming(provider_ref_url)
            else:
                return self._load_provider_references_memory(provider_ref_url)
        except Exception as e:
            logger.error("failed_to_load_provider_references", error=str(e))
            return {}
    
    def _load_provider_references_streaming(self, url: str) -> Dict[int, Dict[str, Any]]:
        """Load provider references using streaming JSON parser."""
        refs = {}
        
        response = None
        try:
            # Use requests with streaming to avoid loading entire file into memory
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Handle gzipped content with true streaming
            if url.endswith('.gz') or response.headers.get('content-encoding') == 'gzip':
                gz_file = None
                try:
                    gz_file = gzip.GzipFile(fileobj=response.raw)
                    # Use ijson for streaming parsing
                    parser = ijson.parse(gz_file)
                    for prefix, event, value in parser:
                        if prefix == "provider_references.item" and event == "start_map":
                            # Start of a provider reference object
                            current_ref = {}
                        elif prefix.startswith("provider_references.item.") and event == "map_key":
                            current_key = value
                        elif prefix.startswith("provider_references.item.") and event in ("string", "number"):
                            if 'current_ref' in locals() and 'current_key' in locals():
                                current_ref[current_key] = value
                        elif prefix == "provider_references.item" and event == "end_map":
                            # End of provider reference object
                            if 'current_ref' in locals():
                                ref_id = current_ref.get("provider_group_id")
                                if ref_id:
                                    refs[ref_id] = current_ref
                                current_ref = {}
                finally:
                    if gz_file:
                        gz_file.close()
            else:
                # For non-gzipped content
                parser = ijson.parse(response.raw)
                for prefix, event, value in parser:
                    if prefix == "provider_references.item" and event == "start_map":
                        # Start of a provider reference object
                        current_ref = {}
                    elif prefix.startswith("provider_references.item.") and event == "map_key":
                        current_key = value
                    elif prefix.startswith("provider_references.item.") and event in ("string", "number"):
                        if 'current_ref' in locals() and 'current_key' in locals():
                            current_ref[current_key] = value
                    elif prefix == "provider_references.item" and event == "end_map":
                        # End of provider reference object
                        if 'current_ref' in locals():
                            ref_id = current_ref.get("provider_group_id")
                            if ref_id:
                                refs[ref_id] = current_ref
                            current_ref = {}
        except Exception as e:
            logger.error("streaming_provider_refs_failed", error=str(e))
        finally:
            # Ensure response is properly closed to prevent file locking issues
            if response:
                response.close()
        
        logger.info("loaded_provider_references_streaming", count=len(refs))
        return refs
    
    def _load_provider_references_memory(self, url: str) -> Dict[int, Dict[str, Any]]:
        """Fallback: load provider references using memory-intensive method."""
        try:
            content = fetch_url(url)
            
            if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
                with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                    data = json.load(gz)
            else:
                data = json.loads(content.decode('utf-8'))
            
            refs = {}
            if "provider_references" in data:
                for ref in data["provider_references"]:
                    ref_id = ref.get("provider_group_id")
                    if ref_id:
                        refs[ref_id] = ref
            
            logger.info("loaded_provider_references_memory", count=len(refs))
            return refs
            
        except Exception as e:
            logger.error("memory_provider_refs_failed", error=str(e))
            return {}

    def parse_negotiated_rates(self, 
                              in_network_item: Dict[str, Any], 
                              payer: str) -> Iterator[Dict[str, Any]]:
        """Parse negotiated rates with memory-efficient processing."""
        
        # Extract basic fields
        billing_code = in_network_item.get("billing_code", "")
        billing_code_type = in_network_item.get("billing_code_type", "")
        description = in_network_item.get("description", "")
        
        # Handle nested negotiated_rates structure
        negotiated_rates = in_network_item.get("negotiated_rates", [])
        if not negotiated_rates:
            # If no negotiated_rates, try to extract from top level
            rate_record = self._create_rate_record(
                billing_code, billing_code_type, description,
                in_network_item.get("negotiated_rate", 0),
                in_network_item.get("service_codes", []),
                in_network_item.get("billing_class", ""),
                in_network_item.get("negotiated_type", ""),
                in_network_item.get("expiration_date", ""),
                self._extract_provider_info(in_network_item),
                payer
            )
            if rate_record:
                yield rate_record
            return
        
        # Process each negotiated_rate (memory-efficient iteration)
        for rate_group in negotiated_rates:
            # Extract provider references from rate level
            provider_refs = rate_group.get("provider_references", [])
            provider_info = self._extract_provider_info_from_refs(provider_refs)
            
            # Extract negotiated prices
            negotiated_prices = rate_group.get("negotiated_prices", [])
            if negotiated_prices:
                # Process each price (could create multiple records)
                for price in negotiated_prices:
                    rate_record = self._create_rate_record(
                        billing_code, billing_code_type, description,
                        price.get("negotiated_rate", 0),
                        price.get("service_codes", []),
                        price.get("billing_class", ""),
                        price.get("negotiated_type", ""),
                        price.get("expiration_date", ""),
                        provider_info,
                        payer
                    )
                    if rate_record:
                        yield rate_record
            else:
                # Fallback: try to extract from rate_group directly
                rate_record = self._create_rate_record(
                    billing_code, billing_code_type, description,
                    rate_group.get("negotiated_rate", 0),
                    rate_group.get("service_codes", []),
                    rate_group.get("billing_class", ""),
                    rate_group.get("negotiated_type", ""),
                    rate_group.get("expiration_date", ""),
                    provider_info,
                    payer
                )
                if rate_record:
                    yield rate_record
    
    def _extract_provider_info(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Extract provider information from item."""
        return {
            "npi": item.get("provider_npi"),
            "provider_group_name": item.get("provider_name"),
            "tin": item.get("provider_tin")
        }
    
    def _extract_provider_info_from_refs(self, provider_refs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract provider information from provider references."""
        if not provider_refs:
            return {}
        
        # Use first provider reference
        ref = provider_refs[0]
        ref_id = ref.get("provider_group_id")
        
        if ref_id and ref_id in self.provider_references:
            provider_data = self.provider_references[ref_id]
            return {
                "npi": provider_data.get("npi"),
                "provider_group_name": provider_data.get("provider_group_name"),
                "tin": provider_data.get("tin")
            }
        
        return {}
    
    def _extract_tin_value(self, tin_data) -> Optional[str]:
        """Extract TIN value from various formats."""
        if isinstance(tin_data, str):
            return tin_data
        elif isinstance(tin_data, dict):
            return tin_data.get("value") or tin_data.get("tin")
        return None
    
    def _create_rate_record(self, 
                           billing_code: str,
                           billing_code_type: str,
                           description: str,
                           negotiated_rate: float,
                           service_codes: List[str],
                           billing_class: str,
                           negotiated_type: str,
                           expiration_date: str,
                           provider_info: Dict[str, Any],
                           payer: str) -> Optional[Dict[str, Any]]:
        """Create a rate record with validation."""
        
        # Skip if no negotiated rate
        if not negotiated_rate or negotiated_rate <= 0:
            return None
        
        return {
            "billing_code": billing_code,
            "billing_code_type": billing_code_type,
            "description": description,
            "negotiated_rate": float(negotiated_rate),
            "service_codes": service_codes,
            "billing_class": billing_class,
            "negotiated_type": negotiated_type,
            "expiration_date": expiration_date,
            "provider_npi": provider_info.get("npi"),
            "provider_name": provider_info.get("provider_group_name"),
            "provider_tin": self._extract_tin_value(provider_info.get("tin")),
            "payer": payer
        }

def stream_parse_enhanced(url: str, payer: str,
                         provider_ref_url: Optional[str] = None,
                         handler: Optional[PayerHandler] = None) -> Iterator[Dict[str, Any]]:
    """Enhanced streaming parser for TiC MRF data with memory optimization.
    
    Args:
        url: URL to MRF data file
        payer: Payer name
        provider_ref_url: Optional URL to provider reference file
        
    Yields:
        Parsed and normalized MRF records
    """
    logger.info("streaming_tic_mrf", url=url, payer=payer)
    
    parser = TiCMRFParser()
    if handler is None:
        handler = PayerHandler()
    
    # Load provider references if specified (with memory optimization)
    if provider_ref_url:
        parser.provider_references = parser.load_provider_references(provider_ref_url)
        logger.info("loaded_provider_references", 
                   count=len(parser.provider_references))
    
    try:
        # Use streaming parsing for large files
        if IJSON_AVAILABLE and _is_large_file(url):
            yield from _stream_parse_large_file(url, payer, parser, handler)
        else:
            yield from _stream_parse_memory(url, payer, parser, handler)
                
    except Exception as e:
        logger.error("parsing_failed", url=url, error=str(e))
        raise

def _is_large_file(url: str) -> bool:
    """Determine if a file is large enough to require streaming."""
    try:
        # Check file size first
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            content_length = response.headers.get('content-length')
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                # If file is larger than 100MB, definitely use streaming
                if size_mb > 100:
                    logger.info("file_size_large", url=url, size_mb=size_mb)
                    return True
                # If file is larger than 10MB, use streaming
                elif size_mb > 10:
                    return True
        
        # Also check URL patterns that suggest large files
        if any(pattern in url.lower() for pattern in ['in_network', 'rates', '.gz']):
            return True
            
        return False
    except Exception as e:
        logger.warning("could_not_check_file_size", url=url, error=str(e))
        # Default to streaming for unknown files
        return True

def _stream_parse_large_file(url: str, payer: str, parser: TiCMRFParser, handler: PayerHandler) -> Iterator[Dict[str, Any]]:
    """Stream parse large files using ijson with true streaming."""
    logger.info("using_streaming_parser_for_large_file", url=url)
    
    response = None
    try:
        # Use requests with streaming to avoid loading entire file into memory
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Handle gzipped content with true streaming
        if url.endswith('.gz') or response.headers.get('content-encoding') == 'gzip':
            # Stream the gzipped content directly with proper cleanup
            gz_file = None
            try:
                gz_file = gzip.GzipFile(fileobj=response.raw)
                yield from _parse_json_stream(gz_file, payer, parser, handler)
            finally:
                if gz_file:
                    gz_file.close()
        else:
            # For non-gzipped content, stream directly
            yield from _parse_json_stream(response.raw, payer, parser, handler)
            
    except Exception as e:
        logger.error("streaming_parse_failed", error=str(e))
        # Fall back to memory parsing for smaller files
        yield from _stream_parse_memory(url, payer, parser, handler)
    finally:
        # Ensure response is properly closed to prevent file locking issues
        if response:
            response.close()

def _parse_json_stream(stream, payer: str, parser: TiCMRFParser, handler: PayerHandler) -> Iterator[Dict[str, Any]]:
    """Parse JSON stream using ijson."""
    try:
        # Parse the stream
        json_parser = ijson.parse(stream)
        
        in_network_items = []
        current_item = {}
        in_in_network = False
        record_count = 0
        
        # Log initial memory usage
        log_memory_usage("stream_parse_start")
        
        for prefix, event, value in json_parser:
            if prefix == "in_network" and event == "start_array":
                in_in_network = True
                logger.info("found_in_network_array")
            elif prefix == "in_network" and event == "end_array":
                in_in_network = False
                break
            elif prefix.startswith("in_network.item") and event == "start_map":
                current_item = {}
            elif prefix.startswith("in_network.item.") and event == "map_key":
                current_key = value
            elif prefix.startswith("in_network.item.") and event in ("string", "number", "boolean"):
                if 'current_item' in locals() and 'current_key' in locals():
                    current_item[current_key] = value
            elif prefix.startswith("in_network.item") and event == "end_map":
                if 'current_item' in locals() and current_item:
                    # Process the item
                    for parsed_item in handler.parse_in_network(current_item):
                        for rate_record in parser.parse_negotiated_rates(parsed_item, payer):
                            yield rate_record
                            record_count += 1
                            
                            # Monitor memory every 1000 records
                            if record_count % 1000 == 0:
                                memory_mb = log_memory_usage(f"stream_parse_records_{record_count}")
                                # Force garbage collection if memory usage is high
                                if memory_mb > 1000:  # 1GB threshold
                                    gc.collect()
                                    logger.warning("forced_garbage_collection", 
                                                 memory_mb=memory_mb, 
                                                 record_count=record_count)
                    
                    current_item = {}
                    
        # Log final memory usage
        log_memory_usage("stream_parse_end")
                    
    except Exception as e:
        logger.error("ijson_parse_failed", error=str(e))
        raise

def _stream_parse_memory(url: str, payer: str, parser: TiCMRFParser, handler: PayerHandler) -> Iterator[Dict[str, Any]]:
    """Fallback: parse using memory-intensive method."""
    logger.info("using_memory_parser", url=url)
    
    try:
        content = fetch_url(url)
        
        # Handle gzipped content
        if url.endswith('.gz') or '.gz?' in url or content.startswith(b'\x1f\x8b'):
            gz_file = None
            try:
                gz_file = gzip.GzipFile(fileobj=BytesIO(content))
                data = json.load(gz_file)
            finally:
                if gz_file:
                    gz_file.close()
        else:
            data = json.loads(content.decode('utf-8'))
        
        logger.info("loaded_mrf_structure", 
                   top_level_keys=list(data.keys()) if isinstance(data, dict) else "array")
        
        # Handle different MRF structures
        if isinstance(data, dict):
            # Standard TiC structure with in_network array
            if "in_network" in data:
                in_network_items = data["in_network"]
                logger.info("processing_in_network_items", count=len(in_network_items))
                
                for item in in_network_items:
                    for parsed_item in handler.parse_in_network(item):
                        for rate_record in parser.parse_negotiated_rates(parsed_item, payer):
                            yield rate_record
                        
            # Handle provider_references at top level (for reference files)
            elif "provider_references" in data:
                logger.info("found_provider_reference_file", 
                           count=len(data["provider_references"]))
                # This is a provider reference file, not a rates file
                return
                
            # Handle allowed amounts structure (out-of-network)
            elif "allowed_amounts" in data:
                logger.info("found_allowed_amounts_structure")
                # Handle allowed amounts data structure
                # (Implementation depends on your requirements)
                return
                
            else:
                logger.warning("unknown_mrf_structure", 
                              available_keys=list(data.keys()))
                
        elif isinstance(data, list):
            # Legacy or non-standard structure
            logger.info("processing_legacy_array_structure", count=len(data))
            for record in data:
                yield record
                
    except Exception as e:
        logger.error("memory_parse_failed", url=url, error=str(e))
        raise

# Backward compatibility function
def stream_parse(url: str) -> Iterator[Dict[str, Any]]:
    """Backward compatible parsing function."""
    return stream_parse_enhanced(url, "unknown_payer")
