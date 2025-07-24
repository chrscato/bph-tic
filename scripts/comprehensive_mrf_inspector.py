#!/usr/bin/env python3
"""
Comprehensive MRF Inspector - Deep Analysis Tool

This script performs comprehensive analysis of MRF (Machine Readable Files) structures
to understand compatibility patterns and build rules for processing.

Usage:
    python scripts/comprehensive_mrf_inspector.py <payer_name> <index_url>
    
Example:
    python scripts/comprehensive_mrf_inspector.py centene_fidelis "https://www.centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-06-27_fidelis_index.json"
"""

import json
import requests
import yaml
import os
import gzip
import io
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging
import argparse
from collections import defaultdict, Counter

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ComprehensiveMRFInspector:
    """Comprehensive MRF structure analyzer and rule builder."""
    
    def __init__(self):
        self.structure_rules = {}
        self.compatibility_patterns = {}
        self.analysis_results = {}
        
    def load_config(self) -> Dict[str, Any]:
        """Load production configuration."""
        try:
            with open('production_config.yaml', 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning("production_config.yaml not found, using defaults")
            return {}
    
    def is_local_file(self, path: str) -> bool:
        """Check if path is a local file."""
        return (os.path.exists(path) or 
                path.startswith('C:') or 
                path.startswith('/') or 
                path.startswith('file://'))
    
    def load_local_json(self, file_path: str) -> Dict[str, Any]:
        """Load JSON from local file, handling gzip compression."""
        try:
            if file_path.endswith('.gz'):
                with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                    return json.load(f)
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load local file {file_path}: {e}")
            raise
    
    def fetch_json(self, url: str) -> Dict[str, Any]:
        """Fetch JSON from URL or local file."""
        if self.is_local_file(url):
            logger.info(f"Loading local file: {url}")
            return self.load_local_json(url)
        else:
            logger.info(f"Fetching JSON from: {url}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
    
    def analyze_index_structure(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the structure of an MRF index file."""
        analysis = {
            "type": "index",
            "keys": list(data.keys()),
            "key_count": len(data.keys()),
            "file_types": [],
            "url_patterns": [],
            "structure_pattern": "unknown",
            "sample_urls": [],
            "analysis_method": "comprehensive"
        }
        
        # Handle centene-style nested structure
        if "reporting_structure" in data:
            analysis["structure_pattern"] = "table_of_contents"
            analysis["file_types"].append("in_network_rates")
            
            # Extract URLs from nested structure
            for structure in data["reporting_structure"]:
                if isinstance(structure, dict) and "in_network_files" in structure:
                    in_network_files = structure["in_network_files"]
                    if isinstance(in_network_files, list):
                        for file_info in in_network_files:
                            if isinstance(file_info, dict) and "location" in file_info:
                                analysis["sample_urls"].append(file_info["location"])
                                analysis["url_patterns"].append(self.extract_url_pattern(file_info["location"]))
        
        # Handle direct in_network_files structure
        elif "in_network_files" in data:
            analysis["structure_pattern"] = "standard_mrf"
            analysis["file_types"].append("in_network_rates")
            
            # Analyze in-network file URLs
            in_network_files = data["in_network_files"]
            if isinstance(in_network_files, list):
                analysis["sample_urls"].extend([f.get("location", "") for f in in_network_files[:5]])
                analysis["url_patterns"].extend([self.extract_url_pattern(f.get("location", "")) for f in in_network_files[:5]])
        
        if "allowed_amounts" in data:
            analysis["file_types"].append("allowed_amounts")
            
        if "provider_references" in data:
            analysis["file_types"].append("provider_references")
        
        return analysis
    
    def extract_rate_urls(self, data: Dict[str, Any]) -> List[str]:
        """Extract rate file URLs from payer data structure."""
        urls = []
        
        try:
            if "reporting_structure" in data:
                # Handle centene-style nested structure
                for structure in data["reporting_structure"]:
                    if isinstance(structure, dict):
                        # Look for in_network_files in each structure
                        if "in_network_files" in structure:
                            in_network_files = structure["in_network_files"]
                            if isinstance(in_network_files, list):
                                for file_info in in_network_files:
                                    if isinstance(file_info, dict):
                                        url = file_info.get("location") or file_info.get("url")
                                        if url:
                                            urls.append(url)
                                    elif isinstance(file_info, str):
                                        urls.append(file_info)
            
            elif "in_network_files" in data:
                # Direct in_network_files structure
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
    
    def extract_url_pattern(self, url: str) -> str:
        """Extract pattern from URL for analysis."""
        if not url:
            return ""
        
        # Extract domain and path structure
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            path_parts = parsed.path.split('/')
            
            # Look for common patterns
            if 'mrf' in path_parts:
                mrf_index = path_parts.index('mrf')
                if mrf_index + 1 < len(path_parts):
                    return f"{parsed.netloc}/mrf/{path_parts[mrf_index + 1]}/..."
            
            return f"{parsed.netloc}{parsed.path[:50]}..."
        except:
            return url[:50] + "..."
    
    def analyze_rate_file_deep(self, rate_url: str, sample_size: int = 1000) -> Dict[str, Any]:
        """Perform deep analysis of a rate file structure."""
        analysis = {
            "url": rate_url,
            "file_size_mb": 0,
            "compression": "none",
            "structure": {},
            "key_analysis": {},
            "data_patterns": {},
            "compatibility_score": 0,
            "required_fields": [],
            "optional_fields": [],
            "issues": [],
            "recommendations": []
        }
        
        try:
            # Fetch file info
            response = requests.head(rate_url, timeout=30)
            if response.status_code == 200:
                content_length = response.headers.get('content-length')
                if content_length:
                    analysis["file_size_mb"] = int(content_length) / (1024 * 1024)
            
            # Determine compression
            if rate_url.endswith('.gz'):
                analysis["compression"] = "gzip"
            
            # Fetch and analyze content
            response = requests.get(rate_url, timeout=60, stream=True)
            response.raise_for_status()
            
            # Handle gzipped content
            if analysis["compression"] == "gzip":
                content = response.content
                with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
                    # Read first chunk for analysis
                    chunk = gz.read(min(1024 * 1024, sample_size * 100))  # 1MB or sample_size * 100
                    chunk_str = chunk.decode('utf-8', errors='ignore')
                    
                    # Analyze JSON structure in chunk
                    analysis.update(self.analyze_json_chunk(chunk_str, sample_size))
            else:
                # For smaller files, load full content
                if analysis["file_size_mb"] < 10:  # Less than 10MB
                    data = response.json()
                    analysis.update(self.analyze_full_json(data))
                else:
                    # Stream large files
                    chunk = response.content[:1024 * 1024]  # First 1MB
                    chunk_str = chunk.decode('utf-8', errors='ignore')
                    analysis.update(self.analyze_json_chunk(chunk_str, sample_size))
            
            # Calculate compatibility score
            analysis["compatibility_score"] = self.calculate_compatibility_score(analysis)
            
            # Generate recommendations
            analysis["recommendations"] = self.generate_recommendations(analysis)
            
        except Exception as e:
            analysis["issues"].append(f"Analysis failed: {str(e)}")
            logger.error(f"Failed to analyze rate file {rate_url}: {e}")
        
        return analysis
    
    def analyze_json_chunk(self, chunk_str: str, sample_size: int) -> Dict[str, Any]:
        """Analyze JSON structure from a chunk of data."""
        analysis = {
            "structure": {},
            "key_analysis": {},
            "data_patterns": {},
            "required_fields": [],
            "optional_fields": []
        }
        
        try:
            # Look for MRF rate file structure first
            if '"in_network"' in chunk_str:
                # This is likely an MRF rate file, look for rate records
                rate_pattern = r'"in_network"\s*:\s*\[(.*?)\]'
                rate_match = re.search(rate_pattern, chunk_str, re.DOTALL)
                if rate_match:
                    # Extract individual rate records
                    rate_content = rate_match.group(1)
                    # Find individual rate objects
                    rate_objects = re.findall(r'\{[^{}]*\}', rate_content)
                    
                    if rate_objects:
                        # Analyze rate records instead of wrapper
                        sample_objects = rate_objects[:min(5, len(rate_objects))]
                        
                        all_keys = set()
                        key_types = defaultdict(set)
                        value_patterns = defaultdict(list)
                        
                        for obj_str in sample_objects:
                            try:
                                obj = json.loads(obj_str)
                                obj_keys = list(obj.keys())
                                all_keys.update(obj_keys)
                                
                                # Analyze key types and values
                                for key, value in obj.items():
                                    key_types[key].add(type(value).__name__)
                                    
                                    # Analyze value patterns
                                    if isinstance(value, str):
                                        if len(value) > 50:
                                            value_patterns[key].append("long_string")
                                        elif re.match(r'^\d+$', value):
                                            value_patterns[key].append("numeric_string")
                                        elif re.match(r'^[A-Z]{2}$', value):
                                            value_patterns[key].append("state_code")
                                        else:
                                            value_patterns[key].append("text")
                                    elif isinstance(value, (int, float)):
                                        value_patterns[key].append("numeric")
                                    elif isinstance(value, list):
                                        value_patterns[key].append(f"list({len(value)})")
                                    elif isinstance(value, dict):
                                        value_patterns[key].append("object")
                            
                            except json.JSONDecodeError:
                                continue
                        
                        # Build analysis
                        analysis["structure"]["total_keys"] = len(all_keys)
                        analysis["structure"]["sample_keys"] = list(all_keys)[:20]
                        analysis["structure"]["rate_records_found"] = len(rate_objects)
                        
                        analysis["key_analysis"] = {
                            key: {
                                "types": list(types),
                                "patterns": list(set(patterns))[:5]
                            }
                            for key, types in key_types.items()
                            for patterns in [value_patterns.get(key, [])]
                        }
                        
                        # Identify required vs optional fields
                        for key, info in analysis["key_analysis"].items():
                            if "numeric" in info["patterns"] or "numeric_string" in info["patterns"]:
                                analysis["required_fields"].append(key)
                            else:
                                analysis["optional_fields"].append(key)
                        
                        # Analyze data patterns
                        analysis["data_patterns"] = {
                            "common_keys": [key for key, count in Counter(all_keys).most_common(10)],
                            "value_types": dict(value_patterns)
                        }
                        
                        return analysis
            
            # Fall back to general JSON analysis
            json_objects = re.findall(r'\{[^{}]*\}', chunk_str)
            
            if json_objects:
                # Analyze first few objects
                sample_objects = json_objects[:min(10, len(json_objects))]
                
                all_keys = set()
                key_types = defaultdict(set)
                value_patterns = defaultdict(list)
                
                for obj_str in sample_objects:
                    try:
                        obj = json.loads(obj_str)
                        obj_keys = list(obj.keys())
                        all_keys.update(obj_keys)
                        
                        # Analyze key types and values
                        for key, value in obj.items():
                            key_types[key].add(type(value).__name__)
                            
                            # Analyze value patterns
                            if isinstance(value, str):
                                if len(value) > 50:
                                    value_patterns[key].append("long_string")
                                elif re.match(r'^\d+$', value):
                                    value_patterns[key].append("numeric_string")
                                elif re.match(r'^[A-Z]{2}$', value):
                                    value_patterns[key].append("state_code")
                                else:
                                    value_patterns[key].append("text")
                            elif isinstance(value, (int, float)):
                                value_patterns[key].append("numeric")
                            elif isinstance(value, list):
                                value_patterns[key].append(f"list({len(value)})")
                            elif isinstance(value, dict):
                                value_patterns[key].append("object")
                    
                    except json.JSONDecodeError:
                        continue
                
                # Build analysis
                analysis["structure"]["total_keys"] = len(all_keys)
                analysis["structure"]["sample_keys"] = list(all_keys)[:20]
                
                analysis["key_analysis"] = {
                    key: {
                        "types": list(types),
                        "patterns": list(set(patterns))[:5]
                    }
                    for key, types in key_types.items()
                    for patterns in [value_patterns.get(key, [])]
                }
                
                # Identify required vs optional fields
                for key, info in analysis["key_analysis"].items():
                    if "numeric" in info["patterns"] or "numeric_string" in info["patterns"]:
                        analysis["required_fields"].append(key)
                    else:
                        analysis["optional_fields"].append(key)
                
                # Analyze data patterns
                analysis["data_patterns"] = {
                    "key_count_range": f"{min(len(obj.split(',')) for obj in sample_objects)}-{max(len(obj.split(',')) for obj in sample_objects)}",
                    "common_keys": [key for key, count in Counter(all_keys).most_common(10)],
                    "value_types": dict(value_patterns)
                }
        
        except Exception as e:
            logger.error(f"Chunk analysis failed: {e}")
        
        return analysis
    
    def analyze_full_json(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze full JSON structure."""
        analysis = {
            "structure": {},
            "key_analysis": {},
            "data_patterns": {},
            "required_fields": [],
            "optional_fields": []
        }
        
        def analyze_object(obj, depth=0, max_depth=3):
            if depth > max_depth:
                return
            
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key not in analysis["key_analysis"]:
                        analysis["key_analysis"][key] = {
                            "types": set(),
                            "patterns": set(),
                            "sample_values": []
                        }
                    
                    analysis["key_analysis"][key]["types"].add(type(value).__name__)
                    
                    if len(analysis["key_analysis"][key]["sample_values"]) < 3:
                        analysis["key_analysis"][key]["sample_values"].append(str(value)[:100])
                    
                    analyze_object(value, depth + 1, max_depth)
            
            elif isinstance(obj, list) and len(obj) > 0:
                analyze_object(obj[0], depth + 1, max_depth)
        
        # Special handling for MRF rate files - focus on in_network array
        if "in_network" in data and isinstance(data["in_network"], list):
            # Analyze the actual rate records, not the wrapper structure
            rate_records = data["in_network"]
            if rate_records:
                # Analyze first few rate records in detail
                for i, record in enumerate(rate_records[:5]):  # Analyze first 5 records
                    analyze_object(record, depth=0, max_depth=2)
                
                # Also analyze the overall structure
                analysis["structure"]["total_rate_records"] = len(rate_records)
                analysis["structure"]["sample_rate_records"] = min(5, len(rate_records))
        else:
            # Fall back to general analysis
            analyze_object(data)
        
        # Convert sets to lists for JSON serialization
        for key, info in analysis["key_analysis"].items():
            info["types"] = list(info["types"])
            info["patterns"] = list(info["patterns"])
        
        return analysis
    
    def calculate_compatibility_score(self, analysis: Dict[str, Any]) -> float:
        """Calculate compatibility score based on analysis."""
        score = 0.0
        max_score = 100.0
        
        # Check for required fields
        required_fields = ["billing_code", "negotiated_rates", "provider_references"]
        found_required = sum(1 for field in required_fields if field in analysis["key_analysis"])
        score += (found_required / len(required_fields)) * 40  # 40% for required fields
        
        # Check for optional but common fields
        optional_fields = ["billing_code_type", "service_code", "rate_type"]
        found_optional = sum(1 for field in optional_fields if field in analysis["key_analysis"])
        score += (found_optional / len(optional_fields)) * 20  # 20% for optional fields
        
        # Check data quality
        if analysis["file_size_mb"] > 0:
            score += min(20, analysis["file_size_mb"] / 10)  # Up to 20% for file size
        
        # Check for issues
        if analysis["issues"]:
            score -= len(analysis["issues"]) * 10  # Penalty for issues
        
        return max(0, min(100, score))
    
    def generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on analysis."""
        recommendations = []
        
        if analysis["compatibility_score"] < 50:
            recommendations.append("LOW COMPATIBILITY - May need custom handler")
        elif analysis["compatibility_score"] < 80:
            recommendations.append("MEDIUM COMPATIBILITY - May need minor adjustments")
        else:
            recommendations.append("HIGH COMPATIBILITY - Should work with standard pipeline")
        
        # Check for specific issues
        if "billing_code" not in analysis["key_analysis"]:
            recommendations.append("Missing billing_code field - critical for rate processing")
        
        if "negotiated_rates" not in analysis["key_analysis"]:
            recommendations.append("Missing negotiated_rates field - critical for rate processing")
        
        if analysis["file_size_mb"] > 100:
            recommendations.append("Large file detected - ensure streaming processing is enabled")
        
        return recommendations
    
    def inspect_payer(self, payer_name: str, index_url: str, sample_rate_files: int = 3) -> Dict[str, Any]:
        """Perform comprehensive inspection of a payer's MRF structure."""
        logger.info(f"🔍 Starting comprehensive inspection of {payer_name}")
        
        inspection = {
            "payer_name": payer_name,
            "index_url": index_url,
            "inspection_time": datetime.now().isoformat(),
            "index_analysis": {},
            "rate_file_analysis": [],
            "compatibility_rules": {},
            "overall_score": 0,
            "recommendations": []
        }
        
        try:
            # 1. Analyze index structure
            logger.info("📋 Analyzing index structure...")
            index_data = self.fetch_json(index_url)
            inspection["index_analysis"] = self.analyze_index_structure(index_data)
            
            # 2. Extract rate file URLs
            rate_urls = self.extract_rate_urls(index_data)
            
            # 3. Analyze sample rate files
            logger.info(f"🔍 Analyzing {min(sample_rate_files, len(rate_urls))} sample rate files...")
            for i, rate_url in enumerate(rate_urls[:sample_rate_files]):
                logger.info(f"Analyzing rate file {i+1}/{min(sample_rate_files, len(rate_urls))}: {rate_url[:80]}...")
                rate_analysis = self.analyze_rate_file_deep(rate_url)
                inspection["rate_file_analysis"].append(rate_analysis)
            
            # 4. Build compatibility rules
            inspection["compatibility_rules"] = self.build_compatibility_rules(inspection)
            
            # 5. Calculate overall score
            if inspection["rate_file_analysis"]:
                scores = [r["compatibility_score"] for r in inspection["rate_file_analysis"]]
                inspection["overall_score"] = sum(scores) / len(scores)
            
            # 6. Generate overall recommendations
            inspection["recommendations"] = self.generate_overall_recommendations(inspection)
            
        except Exception as e:
            logger.error(f"Inspection failed: {e}")
            inspection["recommendations"].append(f"INSPECTION FAILED: {str(e)}")
        
        return inspection
    
    def build_compatibility_rules(self, inspection: Dict[str, Any]) -> Dict[str, Any]:
        """Build compatibility rules based on analysis."""
        rules = {
            "required_fields": set(),
            "optional_fields": set(),
            "data_patterns": {},
            "processing_requirements": [],
            "file_handling": {}
        }
        
        # Aggregate fields from rate file analysis
        for rate_analysis in inspection["rate_file_analysis"]:
            rules["required_fields"].update(rate_analysis.get("required_fields", []))
            rules["optional_fields"].update(rate_analysis.get("optional_fields", []))
            
            # Aggregate data patterns
            for key, patterns in rate_analysis.get("data_patterns", {}).get("value_types", {}).items():
                if key not in rules["data_patterns"]:
                    rules["data_patterns"][key] = set()
                rules["data_patterns"][key].update(patterns)
        
        # Determine processing requirements
        if any(r["file_size_mb"] > 50 for r in inspection["rate_file_analysis"]):
            rules["processing_requirements"].append("streaming_processing")
        
        if any(r["compression"] == "gzip" for r in inspection["rate_file_analysis"]):
            rules["processing_requirements"].append("gzip_decompression")
        
        # Convert sets to lists for JSON serialization
        rules["required_fields"] = list(rules["required_fields"])
        rules["optional_fields"] = list(rules["optional_fields"])
        for key in rules["data_patterns"]:
            rules["data_patterns"][key] = list(rules["data_patterns"][key])
        
        return rules
    
    def generate_overall_recommendations(self, inspection: Dict[str, Any]) -> List[str]:
        """Generate overall recommendations based on complete inspection."""
        recommendations = []
        
        # Overall compatibility assessment
        if inspection["overall_score"] >= 80:
            recommendations.append("✅ HIGH COMPATIBILITY - Ready for production pipeline")
        elif inspection["overall_score"] >= 60:
            recommendations.append("⚠️ MEDIUM COMPATIBILITY - May need minor adjustments")
        else:
            recommendations.append("❌ LOW COMPATIBILITY - Needs custom handler development")
        
        # Specific recommendations based on analysis
        if inspection["rate_file_analysis"]:
            avg_file_size = sum(r["file_size_mb"] for r in inspection["rate_file_analysis"]) / len(inspection["rate_file_analysis"])
            if avg_file_size > 100:
                recommendations.append("📁 Large files detected - ensure memory-efficient processing")
            
            compression_types = set(r["compression"] for r in inspection["rate_file_analysis"])
            if "gzip" in compression_types:
                recommendations.append("🗜️ Gzip compression detected - ensure decompression handling")
        
        # Missing critical fields
        rules = inspection["compatibility_rules"]
        critical_fields = ["billing_code", "negotiated_rates"]
        missing_critical = [field for field in critical_fields if field not in rules["required_fields"]]
        if missing_critical:
            recommendations.append(f"🚨 Missing critical fields: {', '.join(missing_critical)}")
        
        return recommendations
    
    def print_inspection_report(self, inspection: Dict[str, Any]):
        """Print comprehensive inspection report."""
        print("=" * 80)
        print("🔍 COMPREHENSIVE MRF INSPECTION REPORT")
        print("=" * 80)
        
        print(f"\n📋 PAYER DETAILS:")
        print(f"   Name: {inspection['payer_name']}")
        print(f"   Index URL: {inspection['index_url']}")
        print(f"   Inspection Time: {inspection['inspection_time']}")
        
        print(f"\n📊 INDEX ANALYSIS:")
        index_analysis = inspection["index_analysis"]
        print(f"   Structure Type: {index_analysis.get('structure_pattern', 'unknown')}")
        print(f"   File Types: {', '.join(index_analysis.get('file_types', []))}")
        print(f"   Total Keys: {index_analysis.get('key_count', 0)}")
        print(f"   Sample URLs: {len(index_analysis.get('sample_urls', []))} found")
        
        print(f"\n🔍 RATE FILE ANALYSIS:")
        rate_analyses = inspection["rate_file_analysis"]
        print(f"   Files Analyzed: {len(rate_analyses)}")
        
        for i, analysis in enumerate(rate_analyses, 1):
            print(f"   {i}. {analysis['url'][:60]}...")
            print(f"      Size: {analysis['file_size_mb']:.1f} MB")
            print(f"      Compression: {analysis['compression']}")
            print(f"      Compatibility Score: {analysis['compatibility_score']:.1f}%")
            print(f"      Required Fields: {len(analysis['required_fields'])}")
            print(f"      Issues: {len(analysis['issues'])}")
            
            if analysis['recommendations']:
                for rec in analysis['recommendations']:
                    print(f"      💡 {rec}")
            print()
        
        print(f"\n📋 COMPATIBILITY RULES:")
        rules = inspection["compatibility_rules"]
        print(f"   Required Fields: {', '.join(rules.get('required_fields', []))}")
        print(f"   Optional Fields: {', '.join(rules.get('optional_fields', []))}")
        print(f"   Processing Requirements: {', '.join(rules.get('processing_requirements', []))}")
        
        print(f"\n📊 OVERALL ASSESSMENT:")
        print(f"   Overall Compatibility Score: {inspection['overall_score']:.1f}%")
        
        print(f"\n🎯 RECOMMENDATIONS:")
        for rec in inspection["recommendations"]:
            print(f"   {rec}")
        
        print("\n" + "=" * 80)

def main():
    parser = argparse.ArgumentParser(description="Comprehensive MRF Inspector")
    parser.add_argument("payer_name", help="Name of the payer to inspect")
    parser.add_argument("index_url", help="URL or path to the MRF index file")
    parser.add_argument("--sample-files", type=int, default=3, help="Number of rate files to sample")
    parser.add_argument("--output", help="Output file for detailed results (JSON)")
    
    args = parser.parse_args()
    
    inspector = ComprehensiveMRFInspector()
    
    # Perform inspection
    inspection = inspector.inspect_payer(args.payer_name, args.index_url, args.sample_files)
    
    # Print report
    inspector.print_inspection_report(inspection)
    
    # Save detailed results if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(inspection, f, indent=2, default=str)
        print(f"\n📁 Detailed results saved to: {args.output}")

if __name__ == "__main__":
    main() 