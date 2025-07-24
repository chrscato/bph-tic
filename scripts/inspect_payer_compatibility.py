#!/usr/bin/env python3
"""
Payer Compatibility Inspector

This script compares a new payer endpoint's JSON structure with the known working
centene_fidelis structure to determine if it will work with the production pipeline.
"""

import json
import requests
import yaml
import os
from pathlib import Path
from typing import Dict, Any, List, Set
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PayerCompatibilityInspector:
    """Compare payer endpoint structures for compatibility."""
    
    def __init__(self):
        # Known working centene_fidelis structure
        self.centene_fidelis_url = "https://www.centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-06-27_fidelis_index.json"
        self.reference_structure = None
        
    def fetch_json(self, url: str) -> Dict[str, Any]:
        """Fetch JSON from URL or local file with error handling and streaming for large files."""
        try:
            logger.info(f"Fetching JSON from: {url}")
            
            # Handle local files
            if os.path.exists(url) or url.startswith(('file://', 'C:', 'D:', '/', '\\')):
                return self.load_local_json(url)
            
            # Handle HTTP URLs
            response = requests.get(url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Check content length for large files
            content_length = response.headers.get('content-length')
            if content_length:
                size_mb = int(content_length) / (1024 * 1024)
                logger.info(f"File size: {size_mb:.1f} MB")
                
                if size_mb > 100:  # Large file warning
                    logger.warning(f"Large file detected ({size_mb:.1f} MB). Using streaming analysis.")
                    return self.analyze_large_json_streaming(response)
            
            # For smaller files, load normally
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to fetch JSON from {url}: {e}")
            return {}
    
    def load_local_json(self, file_path: str) -> Dict[str, Any]:
        """Load JSON from local file, handling gzip compression."""
        logger.info(f"Loading local file: {file_path}")
        
        # Remove file:// prefix if present
        if file_path.startswith('file://'):
            file_path = file_path[7:]
        
        # Handle gzip compression
        if file_path.endswith('.gz'):
            import gzip
            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                data = json.load(f)
        else:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        
        logger.info(f"Local file loaded: {len(data.keys()) if isinstance(data, dict) else 'array'} keys")
        return data
    
    def analyze_large_json_streaming(self, response) -> Dict[str, Any]:
        """Analyze large JSON files using streaming to avoid memory issues."""
        logger.info("Using streaming analysis for large JSON file...")
        
        analysis = {
            "type": "object",
            "keys": [],
            "key_count": 0,
            "sample_values": {},
            "file_size_mb": 0,
            "analysis_method": "streaming"
        }
        
        try:
            # Get file size
            content_length = response.headers.get('content-length')
            if content_length:
                analysis["file_size_mb"] = int(content_length) / (1024 * 1024)
            
            # Stream and analyze the JSON structure
            import json
            import ijson  # For streaming JSON parsing
            
            # Use ijson for streaming if available, otherwise fallback
            try:
                import ijson
                return self._analyze_with_ijson(response, analysis)
            except ImportError:
                logger.warning("ijson not available, using fallback method")
                return self._analyze_with_fallback(response, analysis)
                
        except Exception as e:
            logger.error(f"Streaming analysis failed: {e}")
            return analysis
    
    def _analyze_with_ijson(self, response, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze JSON using ijson for streaming parsing."""
        import ijson
        
        # Parse the JSON stream
        parser = ijson.parse(response.raw)
        
        keys_found = set()
        sample_values = {}
        key_count = 0
        
        for prefix, event, value in parser:
            if prefix == "" and event == "start_map":
                # Start of root object
                continue
            elif prefix == "" and event == "end_map":
                # End of root object
                break
            elif "." not in prefix and event == "map_key":
                # Top-level key
                key = value
                keys_found.add(key)
                key_count += 1
                
                # Store first few keys for analysis
                if len(sample_values) < 5:
                    sample_values[key] = {"type": "unknown", "analyzed": False}
                    
            elif "." not in prefix and event in ["string", "number", "boolean", "null"]:
                # Top-level value
                if prefix in sample_values:
                    sample_values[prefix]["type"] = type(value).__name__
                    sample_values[prefix]["value"] = str(value)[:100]
                    sample_values[prefix]["analyzed"] = True
                    
            elif prefix.count(".") == 1 and event == "start_array":
                # Array at top level
                if prefix in sample_values:
                    sample_values[prefix]["type"] = "array"
                    sample_values[prefix]["analyzed"] = True
                    
            elif prefix.count(".") == 1 and event == "start_map":
                # Object at top level
                if prefix in sample_values:
                    sample_values[prefix]["type"] = "object"
                    sample_values[prefix]["analyzed"] = True
        
        analysis["keys"] = list(keys_found)
        analysis["key_count"] = key_count
        analysis["sample_values"] = sample_values
        
        return analysis
    
    def _analyze_with_fallback(self, response, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback analysis for large files without ijson."""
        logger.info("Using fallback analysis method...")
        
        # Read in chunks and look for JSON structure
        chunk_size = 8192
        buffer = ""
        keys_found = set()
        sample_values = {}
        
        for chunk in response.iter_content(chunk_size=chunk_size, decode_unicode=True):
            buffer += chunk
            
            # Look for JSON keys in the buffer
            if '"' in buffer:
                # Simple regex-like key extraction
                import re
                key_matches = re.findall(r'"([^"]+)"\s*:', buffer)
                for key in key_matches:
                    if key not in keys_found:
                        keys_found.add(key)
                        
                        # Analyze first few keys
                        if len(sample_values) < 5:
                            sample_values[key] = {"type": "unknown", "analyzed": False}
                
                # Keep only the last part of buffer to avoid memory issues
                buffer = buffer[-chunk_size:]
        
        analysis["keys"] = list(keys_found)
        analysis["key_count"] = len(keys_found)
        analysis["sample_values"] = sample_values
        
        return analysis
    
    def analyze_structure(self, data: Dict[str, Any], max_depth: int = 3) -> Dict[str, Any]:
        """Analyze JSON structure recursively."""
        if not isinstance(data, dict):
            return {"type": type(data).__name__, "value": str(data)[:100]}
        
        analysis = {
            "type": "object",
            "keys": list(data.keys()),
            "key_count": len(data.keys()),
            "sample_values": {}
        }
        
        # Analyze first few keys in detail
        for i, (key, value) in enumerate(data.items()):
            if i >= 5:  # Limit to first 5 keys for performance
                break
                
            if isinstance(value, dict):
                analysis["sample_values"][key] = {
                    "type": "object",
                    "keys": list(value.keys())[:10],  # First 10 keys
                    "key_count": len(value.keys())
                }
            elif isinstance(value, list):
                analysis["sample_values"][key] = {
                    "type": "array",
                    "length": len(value),
                    "sample_item": str(value[0])[:100] if value else None
                }
            else:
                analysis["sample_values"][key] = {
                    "type": type(value).__name__,
                    "value": str(value)[:100]
                }
        
        return analysis
    
    def compare_structures(self, structure1: Dict[str, Any], structure2: Dict[str, Any]) -> Dict[str, Any]:
        """Compare two JSON structures for compatibility."""
        comparison = {
            "compatible": True,
            "similarity_score": 0.0,
            "key_overlap": 0,
            "missing_keys": [],
            "extra_keys": [],
            "structural_differences": [],
            "recommendation": ""
        }
        
        # Get all keys from both structures
        keys1 = set(structure1.get("keys", []))
        keys2 = set(structure2.get("keys", []))
        
        # Calculate overlap
        overlap = keys1.intersection(keys2)
        comparison["key_overlap"] = len(overlap)
        
        # Find missing and extra keys
        comparison["missing_keys"] = list(keys1 - keys2)
        comparison["extra_keys"] = list(keys2 - keys1)
        
        # Calculate similarity score
        total_keys = len(keys1.union(keys2))
        if total_keys > 0:
            comparison["similarity_score"] = len(overlap) / total_keys
        
        # Check for critical keys (those needed by the pipeline)
        critical_keys = ["reporting_structure", "in_network_files", "allowed_amount_file"]
        missing_critical = [key for key in critical_keys if key not in keys2]
        
        # Only mark as incompatible if critical keys are missing AND similarity is low
        if missing_critical and comparison["similarity_score"] < 0.5:
            comparison["compatible"] = False
            comparison["structural_differences"].append(f"Missing critical keys: {missing_critical}")
        elif missing_critical:
            # If similarity is high but some keys are missing, add warning but don't mark incompatible
            comparison["structural_differences"].append(f"Note: Missing some keys but high similarity suggests compatibility")
        
        # Generate recommendation based on similarity and compatibility
        if comparison["compatible"]:
            if comparison["similarity_score"] >= 0.8:
                comparison["recommendation"] = "✅ HIGH COMPATIBILITY - Should work with minimal changes"
            elif comparison["similarity_score"] >= 0.6:
                comparison["recommendation"] = "⚠️ MODERATE COMPATIBILITY - May need minor adjustments"
            else:
                comparison["recommendation"] = "🔧 LOW COMPATIBILITY - Likely needs custom handler"
        else:
            if comparison["similarity_score"] >= 0.6:
                comparison["recommendation"] = "⚠️ MODERATE COMPATIBILITY - High similarity but missing some keys"
            else:
                comparison["recommendation"] = "❌ INCOMPATIBLE - Will need significant custom development"
        
        return comparison
    
    def inspect_payer_endpoint(self, new_url: str, payer_name: str = "unknown") -> Dict[str, Any]:
        """Inspect a new payer endpoint and compare with centene_fidelis."""
        logger.info(f"🔍 Inspecting payer: {payer_name}")
        
        # Fetch reference structure (centene_fidelis)
        if not self.reference_structure:
            logger.info("📋 Loading reference structure (centene_fidelis)...")
            reference_data = self.fetch_json(self.centene_fidelis_url)
            self.reference_structure = self.analyze_structure(reference_data)
            logger.info(f"✅ Reference structure loaded: {self.reference_structure['key_count']} keys")
        
        # Fetch new payer structure
        logger.info(f"📋 Loading new payer structure: {payer_name}")
        new_data = self.fetch_json(new_url)
        new_structure = self.analyze_structure(new_data)
        
        # Compare structures
        comparison = self.compare_structures(self.reference_structure, new_structure)
        
        # Deep analysis: Compare actual rate files
        logger.info("🔍 Performing deep analysis of rate files...")
        deep_analysis = self.compare_rate_files(new_data, payer_name)
        
        # Generate detailed report
        report = {
            "payer_name": payer_name,
            "url": new_url,
            "inspection_time": datetime.now().isoformat(),
            "reference_structure": self.reference_structure,
            "new_structure": new_structure,
            "comparison": comparison,
            "detailed_analysis": self.generate_detailed_analysis(new_data, new_structure),
            "deep_analysis": deep_analysis
        }
        
        return report
    
    def compare_rate_files(self, new_data: Dict[str, Any], payer_name: str) -> Dict[str, Any]:
        """Compare actual rate files between reference and new payer."""
        deep_analysis = {
            "rate_files_analyzed": 0,
            "compatible_rate_files": 0,
            "incompatible_rate_files": 0,
            "sample_rate_analysis": [],
            "overall_rate_compatibility": "unknown"
        }
        
        try:
            # Get reference rate file URLs (from centene_fidelis)
            reference_rate_urls = self.get_reference_rate_urls()
            
            # Get new payer rate file URLs
            new_rate_urls = self.extract_rate_urls(new_data)
            
            if not new_rate_urls:
                logger.warning("No rate file URLs found in new payer data")
                return deep_analysis
            
            # Analyze first few rate files from new payer
            sample_size = min(3, len(new_rate_urls))
            logger.info(f"Analyzing {sample_size} sample rate files...")
            
            for i, rate_url in enumerate(new_rate_urls[:sample_size]):
                logger.info(f"Analyzing rate file {i+1}/{sample_size}: {rate_url[:100]}...")
                
                try:
                    rate_analysis = self.analyze_rate_file(rate_url, payer_name)
                    deep_analysis["sample_rate_analysis"].append(rate_analysis)
                    deep_analysis["rate_files_analyzed"] += 1
                    
                    if rate_analysis["compatible"]:
                        deep_analysis["compatible_rate_files"] += 1
                    else:
                        deep_analysis["incompatible_rate_files"] += 1
                        
                except Exception as e:
                    logger.error(f"Failed to analyze rate file {rate_url}: {e}")
                    deep_analysis["sample_rate_analysis"].append({
                        "url": rate_url,
                        "error": str(e),
                        "compatible": False
                    })
            
            # Determine overall compatibility
            if deep_analysis["rate_files_analyzed"] > 0:
                compatibility_ratio = deep_analysis["compatible_rate_files"] / deep_analysis["rate_files_analyzed"]
                if compatibility_ratio >= 0.8:
                    deep_analysis["overall_rate_compatibility"] = "high"
                elif compatibility_ratio >= 0.5:
                    deep_analysis["overall_rate_compatibility"] = "moderate"
                else:
                    deep_analysis["overall_rate_compatibility"] = "low"
            
        except Exception as e:
            logger.error(f"Deep analysis failed: {e}")
        
        return deep_analysis
    
    def get_reference_rate_urls(self) -> List[str]:
        """Get sample rate file URLs from centene_fidelis for comparison."""
        try:
            reference_data = self.fetch_json(self.centene_fidelis_url)
            return self.extract_rate_urls(reference_data)
        except Exception as e:
            logger.error(f"Failed to get reference rate URLs: {e}")
            return []
    
    def extract_rate_urls(self, data: Dict[str, Any]) -> List[str]:
        """Extract rate file URLs from payer data structure."""
        urls = []
        
        try:
            if "reporting_structure" in data:
                for item in data["reporting_structure"]:
                    if isinstance(item, dict):
                        # Look for in_network_files
                        if "in_network_files" in item:
                            in_network_files = item["in_network_files"]
                            if isinstance(in_network_files, list):
                                for file_info in in_network_files:
                                    if isinstance(file_info, dict):
                                        url = file_info.get("location") or file_info.get("url")
                                        if url:
                                            urls.append(url)
                                    elif isinstance(file_info, str):
                                        urls.append(file_info)
            
            # Also check for direct in_network_files at top level
            if "in_network_files" in data:
                in_network_files = data["in_network_files"]
                if isinstance(in_network_files, list):
                    for file_info in in_network_files:
                        if isinstance(file_info, dict):
                            url = file_info.get("location") or file_info.get("url")
                            if url:
                                urls.append(url)
                        elif isinstance(file_info, str):
                            urls.append(file_info)
                            
        except Exception as e:
            logger.error(f"Failed to extract rate URLs: {e}")
        
        return urls
    
    def analyze_rate_file(self, rate_url: str, payer_name: str) -> Dict[str, Any]:
        """Analyze a single rate file for compatibility."""
        analysis = {
            "url": rate_url,
            "compatible": False,
            "structure_type": "unknown",
            "has_in_network": False,
            "sample_keys": [],
            "error": None
        }
        
        try:
            # Fetch rate file (with size check for large files)
            response = requests.get(rate_url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Check if it's a large file
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > 50 * 1024 * 1024:  # 50MB
                logger.warning(f"Large rate file detected ({int(content_length)/1024/1024:.1f} MB), using streaming analysis")
                rate_data = self.analyze_large_json_streaming_gzipped(response, rate_url)
            else:
                # Handle gzipped content
                if rate_url.endswith('.gz'):
                    import gzip
                    import io
                    content = response.content
                    with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                        rate_data = json.load(gz)
                else:
                    rate_data = response.json()
                rate_data = self.analyze_structure(rate_data)
            
            # Check for in_network structure
            if "in_network" in rate_data.get("keys", []):
                analysis["has_in_network"] = True
                analysis["structure_type"] = "in_network_rates"
                
                # Check if structure matches expected pattern
                if self.is_compatible_rate_structure(rate_data):
                    analysis["compatible"] = True
                
                analysis["sample_keys"] = rate_data.get("keys", [])[:10]
            
            elif "allowed_amounts" in rate_data.get("keys", []):
                analysis["structure_type"] = "allowed_amounts"
                analysis["sample_keys"] = rate_data.get("keys", [])[:10]
            
        except Exception as e:
            analysis["error"] = str(e)
            logger.error(f"Failed to analyze rate file {rate_url}: {e}")
        
        return analysis
    
    def analyze_large_json_streaming_gzipped(self, response, url: str) -> Dict[str, Any]:
        """Analyze large gzipped JSON files using streaming to avoid memory issues."""
        logger.info("Using streaming analysis for large gzipped JSON file...")
        
        analysis = {
            "type": "object",
            "keys": [],
            "key_count": 0,
            "sample_values": {},
            "file_size_mb": 0,
            "analysis_method": "streaming_gzipped"
        }
        
        try:
            # Get file size
            content_length = response.headers.get('content-length')
            if content_length:
                analysis["file_size_mb"] = int(content_length) / (1024 * 1024)
            
            # For gzipped files, we need to decompress first
            import gzip
            import io
            
            # Read the gzipped content
            content = response.content
            
            # Decompress and analyze
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                # Read first chunk to analyze structure
                chunk = gz.read(1024 * 1024)  # Read 1MB chunk
                
                # Look for JSON structure in the chunk
                import re
                
                # Find JSON keys in the chunk
                key_matches = re.findall(r'"([^"]+)"\s*:', chunk.decode('utf-8', errors='ignore'))
                keys_found = set()
                
                for key in key_matches:
                    if key not in keys_found:
                        keys_found.add(key)
                        
                        # Analyze first few keys
                        if len(analysis["sample_values"]) < 5:
                            analysis["sample_values"][key] = {"type": "unknown", "analyzed": False}
                
                analysis["keys"] = list(keys_found)
                analysis["key_count"] = len(keys_found)
            
        except Exception as e:
            logger.error(f"Streaming gzipped analysis failed: {e}")
            analysis["error"] = str(e)
        
        return analysis
    
    def is_compatible_rate_structure(self, rate_data: Dict[str, Any]) -> bool:
        """Check if rate file structure is compatible with pipeline expectations."""
        keys = rate_data.get("keys", [])
        
        # Check for expected keys in rate files
        expected_keys = ["billing_code", "negotiated_rates", "provider_references"]
        found_keys = [key for key in expected_keys if key in keys]
        
        # Consider compatible if at least 2 expected keys are found
        return len(found_keys) >= 2
    
    def generate_detailed_analysis(self, data: Dict[str, Any], structure: Dict[str, Any]) -> Dict[str, Any]:
        """Generate detailed analysis of the new payer's structure."""
        analysis = {
            "structure_type": "unknown",
            "has_in_network_files": False,
            "has_allowed_amounts": False,
            "has_provider_references": False,
            "estimated_file_count": 0,
            "sample_urls": [],
            "potential_issues": [],
            "analysis_method": structure.get("analysis_method", "standard")
        }
        
        # Handle streaming analysis results
        if structure.get("analysis_method") == "streaming":
            return self._analyze_streaming_structure(structure, analysis)
        
        # Standard analysis for smaller files
        # Determine structure type
        if "reporting_structure" in data:
            analysis["structure_type"] = "table_of_contents"
            
            # Analyze reporting structure
            reporting_structure = data.get("reporting_structure", [])
            if isinstance(reporting_structure, list):
                analysis["estimated_file_count"] = len(reporting_structure)
                
                # Check for in_network_files
                for item in reporting_structure:
                    if isinstance(item, dict):
                        if "in_network_files" in item:
                            analysis["has_in_network_files"] = True
                            # Extract sample URLs
                            in_network_files = item.get("in_network_files", [])
                            if isinstance(in_network_files, list) and in_network_files:
                                analysis["sample_urls"].extend(in_network_files[:3])
                        
                        if "allowed_amount_file" in item:
                            analysis["has_allowed_amounts"] = True
                        
                        if "provider_references" in item:
                            analysis["has_provider_references"] = True
        
        elif "in_network" in data:
            analysis["structure_type"] = "direct_mrf"
            analysis["has_in_network_files"] = True
        
        # Check for potential issues
        if not analysis["has_in_network_files"]:
            analysis["potential_issues"].append("No in_network_files found")
        
        if analysis["estimated_file_count"] == 0:
            analysis["potential_issues"].append("No files detected in structure")
        
        return analysis
    
    def _analyze_streaming_structure(self, structure: Dict[str, Any], analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze structure from streaming analysis."""
        keys = structure.get("keys", [])
        
        # Check for critical keys
        if "reporting_structure" in keys:
            analysis["structure_type"] = "table_of_contents"
            analysis["has_in_network_files"] = "in_network_files" in keys
            analysis["has_allowed_amounts"] = "allowed_amount_file" in keys
            analysis["has_provider_references"] = "provider_references" in keys
            
            # Estimate file count based on key patterns
            if analysis["has_in_network_files"]:
                analysis["estimated_file_count"] = "multiple"  # Can't determine exact count from streaming
        elif "in_network" in keys:
            analysis["structure_type"] = "direct_mrf"
            analysis["has_in_network_files"] = True
        
        # Check for potential issues
        if not analysis["has_in_network_files"]:
            analysis["potential_issues"].append("No in_network_files found")
        
        if analysis["estimated_file_count"] == 0:
            analysis["potential_issues"].append("No files detected in structure")
        
        # Add streaming-specific notes
        if structure.get("file_size_mb", 0) > 100:
            analysis["potential_issues"].append(f"Large file ({structure['file_size_mb']:.1f} MB) - consider testing with small sample first")
        
        return analysis
    
    def print_report(self, report: Dict[str, Any]):
        """Print a formatted compatibility report."""
        print("\n" + "="*80)
        print(f"🔍 PAYER COMPATIBILITY REPORT")
        print("="*80)
        
        print(f"\n📋 PAYER DETAILS:")
        print(f"   Name: {report['payer_name']}")
        print(f"   URL: {report['url']}")
        print(f"   Inspection Time: {report['inspection_time']}")
        
        print(f"\n📊 STRUCTURE COMPARISON:")
        comparison = report['comparison']
        print(f"   Compatibility: {'✅ COMPATIBLE' if comparison['compatible'] else '❌ INCOMPATIBLE'}")
        print(f"   Similarity Score: {comparison['similarity_score']:.1%}")
        print(f"   Key Overlap: {comparison['key_overlap']} keys")
        print(f"   Missing Keys: {len(comparison['missing_keys'])}")
        print(f"   Extra Keys: {len(comparison['extra_keys'])}")
        
        print(f"\n📋 DETAILED ANALYSIS:")
        analysis = report['detailed_analysis']
        print(f"   Structure Type: {analysis['structure_type']}")
        print(f"   Analysis Method: {analysis.get('analysis_method', 'standard')}")
        print(f"   Has In-Network Files: {'✅' if analysis['has_in_network_files'] else '❌'}")
        print(f"   Has Allowed Amounts: {'✅' if analysis['has_allowed_amounts'] else '❌'}")
        print(f"   Has Provider References: {'✅' if analysis['has_provider_references'] else '❌'}")
        print(f"   Estimated File Count: {analysis['estimated_file_count']}")
        
        # Show file size for large files
        if report['new_structure'].get('file_size_mb', 0) > 0:
            print(f"   File Size: {report['new_structure']['file_size_mb']:.1f} MB")
        
        if analysis['sample_urls']:
            print(f"   Sample URLs: {len(analysis['sample_urls'])} found")
            for i, url in enumerate(analysis['sample_urls'][:3], 1):
                print(f"     {i}. {url}")
        
        if analysis['potential_issues']:
            print(f"\n⚠️ POTENTIAL ISSUES:")
            for issue in analysis['potential_issues']:
                print(f"   • {issue}")
        
        # Deep analysis results
        if 'deep_analysis' in report:
            deep_analysis = report['deep_analysis']
            print(f"\n🔍 DEEP ANALYSIS (Rate Files):")
            print(f"   Rate Files Analyzed: {deep_analysis['rate_files_analyzed']}")
            print(f"   Compatible Rate Files: {deep_analysis['compatible_rate_files']}")
            print(f"   Incompatible Rate Files: {deep_analysis['incompatible_rate_files']}")
            print(f"   Overall Rate Compatibility: {deep_analysis['overall_rate_compatibility'].upper()}")
            
            if deep_analysis['sample_rate_analysis']:
                print(f"\n📋 SAMPLE RATE FILE ANALYSIS:")
                for i, rate_analysis in enumerate(deep_analysis['sample_rate_analysis'][:3], 1):
                    print(f"   {i}. {rate_analysis['structure_type']} - {'✅' if rate_analysis['compatible'] else '❌'}")
                    if rate_analysis.get('sample_keys'):
                        print(f"      Keys: {', '.join(rate_analysis['sample_keys'][:5])}")
                    if rate_analysis.get('error'):
                        print(f"      Error: {rate_analysis['error']}")
        
        print(f"\n🎯 RECOMMENDATION:")
        print(f"   {comparison['recommendation']}")
        
        # Enhanced recommendation based on deep analysis
        if 'deep_analysis' in report and report['deep_analysis']['overall_rate_compatibility'] != 'unknown':
            rate_compat = report['deep_analysis']['overall_rate_compatibility']
            if rate_compat == 'high':
                print(f"   📊 Rate files show HIGH compatibility - should work well")
            elif rate_compat == 'moderate':
                print(f"   📊 Rate files show MODERATE compatibility - may need adjustments")
            else:
                print(f"   📊 Rate files show LOW compatibility - likely needs custom handler")
        
        print(f"\n📝 NEXT STEPS:")
        if comparison['compatible']:
            print("   1. Add the payer to production_config.yaml")
            print("   2. Test with a small sample (max_files_per_payer: 1)")
            print("   3. Monitor the pipeline logs for any issues")
        else:
            print("   1. Review the structural differences")
            print("   2. Consider creating a custom payer handler")
            print("   3. Test with the payer_development_workflow.py script")
        
        print("\n" + "="*80)
    
    def save_report(self, report: Dict[str, Any], output_file: str = None):
        """Save the compatibility report to a JSON file."""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"payer_compatibility_report_{report['payer_name']}_{timestamp}.json"
        
        output_path = Path("scripts") / output_file
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"📄 Report saved to: {output_path}")
        return output_path

def main():
    """Main function for payer compatibility inspection."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Inspect payer endpoint compatibility")
    parser.add_argument("url", help="URL of the payer endpoint to inspect")
    parser.add_argument("--name", default="unknown", help="Name of the payer")
    parser.add_argument("--output", help="Output file for the report")
    parser.add_argument("--save", action="store_true", help="Save detailed report to JSON")
    
    args = parser.parse_args()
    
    # Create inspector
    inspector = PayerCompatibilityInspector()
    
    try:
        # Inspect the payer
        report = inspector.inspect_payer_endpoint(args.url, args.name)
        
        # Print report
        inspector.print_report(report)
        
        # Save report if requested
        if args.save or args.output:
            inspector.save_report(report, args.output)
        
    except Exception as e:
        logger.error(f"❌ Inspection failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 