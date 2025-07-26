#!/usr/bin/env python3
"""Analyze large MRF files with streaming and sampling for memory efficiency."""

import json
import gzip
import requests
import ijson
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional, Iterator
from collections import defaultdict
import argparse
import sys
import os

def detect_gzip_compression(file_path: str, is_url: bool = False) -> bool:
    """Detect if a file is gzip compressed by checking magic number."""
    if file_path.endswith('.gz'):
        return True
    
    try:
        if is_url:
            resp = requests.get(file_path, stream=True)
            chunk = resp.raw.read(10)
            resp.raw.seek(0)
            return len(chunk) >= 2 and chunk[0] == 0x1f and chunk[1] == 0x8b
        else:
            with open(file_path, 'rb') as f:
                chunk = f.read(10)
                return len(chunk) >= 2 and chunk[0] == 0x1f and chunk[1] == 0x8b
    except:
        return False

def get_file_object(file_path: str, is_url: bool = False):
    """Get a file object with proper compression handling."""
    if is_url:
        # For URLs, we need to download the content to check compression
        resp = requests.get(file_path, stream=True)
        # Read the entire content into memory (for small files this is OK)
        content = resp.content
        
        # Check for gzip compression
        if len(content) >= 2 and content[0] == 0x1f and content[1] == 0x8b:
            from io import BytesIO
            return gzip.GzipFile(fileobj=BytesIO(content))
        else:
            from io import BytesIO
            return BytesIO(content)
    else:
        # Check for gzip compression
        with open(file_path, 'rb') as f:
            chunk = f.read(10)
            if len(chunk) >= 2 and chunk[0] == 0x1f and chunk[1] == 0x8b:
                return gzip.open(file_path, 'rb')
            else:
                return open(file_path, 'rb')

def stream_json_array(file_obj, array_path: str, max_items: Optional[int] = None) -> Iterator[Dict]:
    """Stream items from a JSON array without loading entire file."""
    parser = ijson.items(file_obj, f'{array_path}.item')
    count = 0
    for item in parser:
        yield item
        count += 1
        if max_items and count >= max_items:
            break

def analyze_file_structure(file_path: str, is_url: bool = False) -> Dict[str, Any]:
    """Analyze structure with minimal memory usage."""
    print(f"\n[*] Analyzing file structure...")
    
    analysis = {
        "source": file_path,
        "is_url": is_url,
        "file_size_mb": 0,
        "compression": "none",
        "structure_type": "unknown",
        "top_level_keys": [],
        "statistics": {},
        "samples": {}
    }
    
    try:
        # Get file size and detect compression
        if is_url:
            try:
                resp = requests.head(file_path, timeout=10)
                if 'content-length' in resp.headers:
                    analysis["file_size_mb"] = int(resp.headers['content-length']) / 1024 / 1024
            except:
                pass
        else:
            analysis["file_size_mb"] = Path(file_path).stat().st_size / 1024 / 1024
        
        # Detect compression and get file object
        file_obj = get_file_object(file_path, is_url)
        
        # Determine compression type for reporting
        if file_path.endswith('.gz'):
            analysis["compression"] = "gzip"
        elif hasattr(file_obj, 'read'):
            # Check if it's a gzip file by trying to read a small chunk
            try:
                pos = file_obj.tell()
                chunk = file_obj.read(10)
                file_obj.seek(pos)
                if len(chunk) >= 2 and chunk[0] == 0x1f and chunk[1] == 0x8b:
                    analysis["compression"] = "gzip"
            except:
                pass
        
        print(f"   File size: {analysis['file_size_mb']:.2f} MB")
        print(f"   Compression: {analysis['compression']}")
        
        # Peek at structure to determine type
        parser = ijson.parse(file_obj)
        depth = 0
        current_path = []
        
        for prefix, event, value in parser:
            if event == 'start_map':
                depth += 1
            elif event == 'end_map':
                depth -= 1
            elif event == 'map_key' and depth == 1:
                # Top level keys
                if value not in analysis["top_level_keys"]:
                    analysis["top_level_keys"].append(value)
            
            # Stop after getting basic structure
            if len(analysis["top_level_keys"]) >= 10 and depth == 0:
                break
        
        file_obj.close()
        
        # Determine structure type
        if "reporting_structure" in analysis["top_level_keys"]:
            analysis["structure_type"] = "table_of_contents"
        elif "in_network" in analysis["top_level_keys"]:
            analysis["structure_type"] = "in_network_rates"
        elif "allowed_amounts" in analysis["top_level_keys"]:
            analysis["structure_type"] = "allowed_amounts"
        elif "provider_references" in analysis["top_level_keys"]:
            analysis["structure_type"] = "provider_references"
        
        print(f"   Structure type: {analysis['structure_type']}")
        print(f"   Top keys: {', '.join(analysis['top_level_keys'][:5])}")
        
        return analysis
        
    except Exception as e:
        analysis["error"] = str(e)
        print(f"   [X] Error: {str(e)}")
        return analysis
    finally:
        if 'file_obj' in locals():
            file_obj.close()

def analyze_table_of_contents(file_path: str, is_url: bool = False, sample_size: int = 10) -> Dict[str, Any]:
    """Analyze a Table of Contents file with streaming."""
    print(f"\n[TOC] Analyzing Table of Contents")
    
    analysis = {
        "total_reporting_structures": 0,
        "file_counts": defaultdict(int),
        "plan_samples": [],
        "url_patterns": set()
    }
    
    try:
        # Open file with proper compression detection
        file_obj = get_file_object(file_path, is_url)
        
        # Stream reporting structures
        print("   Streaming reporting structures...")
        for i, rs in enumerate(stream_json_array(file_obj, 'reporting_structure', max_items=sample_size*10)):
            analysis["total_reporting_structures"] += 1
            
            # Count file types
            if "in_network_files" in rs:
                analysis["file_counts"]["in_network"] += len(rs["in_network_files"])
                
                # Extract URL patterns
                for f in rs["in_network_files"][:2]:
                    if "location" in f:
                        # Get domain and path pattern
                        url = f["location"]
                        if "://" in url:
                            parts = url.split("/")
                            pattern = f"{parts[2]}/{parts[3] if len(parts) > 3 else '...'}"
                            analysis["url_patterns"].add(pattern)
            
            if "allowed_amount_file" in rs:
                analysis["file_counts"]["allowed_amounts"] += 1
            
            if "provider_references" in rs:
                analysis["file_counts"]["provider_references"] += len(rs.get("provider_references", []))
            
            # Sample plans
            if i < sample_size:
                analysis["plan_samples"].append({
                    "plan_name": rs.get("plan_name", ""),
                    "plan_id": rs.get("plan_id", ""),
                    "plan_market_type": rs.get("plan_market_type", ""),
                    "in_network_files": len(rs.get("in_network_files", [])),
                    "keys": list(rs.keys())
                })
            
            # Progress indicator
            if (i + 1) % 100 == 0:
                print(f"   Processed {i + 1} reporting structures...")
        
        file_obj.close()
        
        # Convert sets to lists for JSON serialization
        analysis["url_patterns"] = list(analysis["url_patterns"])
        
        print(f"   Total reporting structures: {analysis['total_reporting_structures']}")
        print(f"   In-network files: {analysis['file_counts']['in_network']}")
        print(f"   URL patterns: {', '.join(analysis['url_patterns'][:3])}")
        
        return analysis
        
    except Exception as e:
        analysis["error"] = str(e)
        print(f"   [X] Error: {str(e)}")
        return analysis

def analyze_in_network_rates(file_path: str, is_url: bool = False, sample_size: int = 1000) -> Dict[str, Any]:
    """Analyze in-network rates file with streaming."""
    print(f"\n[MRF] Analyzing In-Network Rates")
    
    analysis = {
        "total_items": 0,
        "billing_code_types": defaultdict(int),
        "key_patterns": defaultdict(set),
        "rate_structures": [],
        "provider_structures": [],
        "unique_fields": set()
    }
    
    try:
        # Open file with proper compression detection
        file_obj = get_file_object(file_path, is_url)
        
        # Stream in_network items
        print(f"   Streaming in_network items (sampling {sample_size})...")
        for i, item in enumerate(stream_json_array(file_obj, 'in_network', max_items=sample_size)):
            analysis["total_items"] += 1
            
            # Track billing code types
            if "billing_code_type" in item:
                analysis["billing_code_types"][item["billing_code_type"]] += 1
            
            # Track unique fields
            for key in item.keys():
                analysis["unique_fields"].add(key)
            
            # Analyze rate structure (first 10 items)
            if i < 10 and "negotiated_rates" in item and item["negotiated_rates"]:
                rate = item["negotiated_rates"][0]
                rate_structure = {
                    "keys": list(rate.keys()),
                    "has_provider_groups": "provider_groups" in rate,
                    "has_provider_references": "provider_references" in rate
                }
                
                # Check provider structure
                if "provider_groups" in rate and rate["provider_groups"]:
                    pg = rate["provider_groups"][0]
                    rate_structure["provider_group_keys"] = list(pg.keys())
                    rate_structure["provider_location"] = "direct" if "npi" in pg else "nested"
                
                # Check price structure
                if "negotiated_prices" in rate and rate["negotiated_prices"]:
                    price = rate["negotiated_prices"][0]
                    rate_structure["price_keys"] = list(price.keys())
                    rate_structure["rate_field"] = "negotiated_rate" if "negotiated_rate" in price else "negotiated_price"
                
                analysis["rate_structures"].append(rate_structure)
            
            # Progress indicator
            if (i + 1) % 100 == 0:
                print(f"   Processed {i + 1} items...")
        
        file_obj.close()
        
        # Convert sets to lists
        analysis["unique_fields"] = list(analysis["unique_fields"])
        
        print(f"   Total items sampled: {analysis['total_items']}")
        print(f"   Billing code types: {dict(analysis['billing_code_types'])}")
        print(f"   Unique fields: {', '.join(sorted(analysis['unique_fields'])[:10])}")
        
        return analysis
        
    except Exception as e:
        analysis["error"] = str(e)
        print(f"   [X] Error: {str(e)}")
        return analysis

def save_analysis(analysis: Dict[str, Any], output_path: str):
    """Save analysis to JSON file."""
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, default=str)
    print(f"\n[+] Analysis saved to: {output_path}")

def main():
    print("DEBUG: Script starting...")
    parser = argparse.ArgumentParser(description="Analyze large MRF files efficiently")
    parser.add_argument("file_path", help="Path to file (local) or URL")
    parser.add_argument("--sample-size", type=int, default=1000, help="Number of items to sample")
    parser.add_argument("--output", help="Output JSON file path")
    parser.add_argument("--type", choices=["auto", "toc", "rates"], default="auto", help="File type")
    args = parser.parse_args()
    
    print(f"DEBUG: File path: {args.file_path}")
    print(f"DEBUG: Sample size: {args.sample_size}")
    
    # Determine if URL or local file
    is_url = args.file_path.startswith(('http://', 'https://'))
    print(f"DEBUG: Is URL: {is_url}")
    
    # Check if file exists
    if not is_url:
        import os
        if not os.path.exists(args.file_path):
            print(f"ERROR: File does not exist: {args.file_path}")
            return
        print(f"DEBUG: File exists, size: {os.path.getsize(args.file_path)} bytes")
    
    # Auto-detect file type if needed
    if args.type == "auto":
        print("[*] Auto-detecting file type...")
        structure = analyze_file_structure(args.file_path, is_url)
        
        if structure.get("error"):
            print(f"[X] Failed to analyze structure: {structure['error']}")
            return
        
        file_type = structure["structure_type"]
        if file_type == "table_of_contents":
            args.type = "toc"
        elif file_type == "in_network_rates":
            args.type = "rates"
        else:
            print(f"[X] Unknown file type: {file_type}")
            return
    
    # Perform analysis
    analysis = {
        "file_path": args.file_path,
        "analysis_time": datetime.now().isoformat(),
        "sample_size": args.sample_size
    }
    
    # Add structure analysis
    analysis["structure"] = analyze_file_structure(args.file_path, is_url)
    
    # Type-specific analysis
    if args.type == "toc":
        analysis["table_of_contents"] = analyze_table_of_contents(
            args.file_path, is_url, sample_size=args.sample_size
        )
    elif args.type == "rates":
        analysis["in_network_rates"] = analyze_in_network_rates(
            args.file_path, is_url, sample_size=args.sample_size
        )
    
    # Generate output filename if not specified
    if not args.output:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if is_url:
            # Extract payer name from URL
            parts = args.file_path.split("/")
            filename = parts[-1].replace(".json.gz", "").replace(".json", "")
            args.output = f"{filename}_analysis_{timestamp}.json"
        else:
            filename = Path(args.file_path).stem
            args.output = f"{filename}_analysis_{timestamp}.json"
    
    # Save analysis
    save_analysis(analysis, args.output)
    
    print("\n[DONE] Analysis complete!")
    print(f"File type: {args.type}")
    print(f"File size: {analysis['structure']['file_size_mb']:.2f} MB")
    if args.type == "toc":
        toc = analysis.get("table_of_contents", {})
        print(f"Reporting structures: {toc.get('total_reporting_structures', 0)}")
        print(f"In-network files: {toc.get('file_counts', {}).get('in_network', 0)}")
    elif args.type == "rates":
        rates = analysis.get("in_network_rates", {})
        print(f"Items sampled: {rates.get('total_items', 0)}")

if __name__ == "__main__":
    main()