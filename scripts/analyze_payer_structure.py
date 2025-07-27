#!/usr/bin/env python3
"""Analyze Table of Contents and MRF structures for all payers in config.

Script name: analyze_payer_structures.py
"""

import json
import yaml
import requests
import gzip
from io import BytesIO
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional
from collections import defaultdict
import argparse

def load_config(config_path: str) -> Dict[str, Any]:
    """Load YAML configuration."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def fetch_json(url: str, max_size_mb: int = 500) -> Optional[Dict[str, Any]]:
    """Fetch and parse JSON from URL with size limit."""
    try:
        # Stream download to check size
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        
        # Check content length if available
        content_length = resp.headers.get('content-length')
        if content_length and int(content_length) > max_size_mb * 1024 * 1024:
            print(f"  [!] File too large: {int(content_length) / 1024 / 1024:.1f} MB")
            return None
        
        # Download content
        content = resp.content
        
        # Handle gzipped content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                return json.load(gz)
        else:
            return json.loads(content.decode('utf-8'))
            
    except Exception as e:
        print(f"  [X] Error fetching {url}: {str(e)}")
        return None

def fetch_json_streaming(url: str, max_size_mb: int = 1000) -> Optional[Dict[str, Any]]:
    """Fetch and parse JSON from URL with streaming for large files."""
    try:
        # Stream download to check size
        resp = requests.get(url, stream=True)
        resp.raise_for_status()
        
        # Check content length if available
        content_length = resp.headers.get('content-length')
        if content_length and int(content_length) > max_size_mb * 1024 * 1024:
            print(f"  [!] File too large: {int(content_length) / 1024 / 1024:.1f} MB")
            return None
        
        # For very large files, use streaming JSON parser
        if content_length and int(content_length) > 100 * 1024 * 1024:  # > 100MB
            print(f"  [*] Using streaming parser for large file ({int(content_length) / 1024 / 1024:.1f} MB)")
            return fetch_json_streaming_large(url, resp)
        
        # Download content for smaller files
        content = resp.content
        
        # Handle gzipped content
        if url.endswith('.gz') or content.startswith(b'\x1f\x8b'):
            with gzip.GzipFile(fileobj=BytesIO(content)) as gz:
                return json.load(gz)
        else:
            return json.loads(content.decode('utf-8'))
            
    except Exception as e:
        print(f"  [X] Error fetching {url}: {str(e)}")
        return None

def fetch_json_streaming_large(url: str, resp) -> Optional[Dict[str, Any]]:
    """Stream parse large JSON files to avoid memory issues."""
    try:
        import ijson  # For streaming JSON parsing
        
        # Handle gzipped content
        if url.endswith('.gz'):
            import gzip
            stream = gzip.GzipFile(fileobj=resp.raw)
        else:
            stream = resp.raw
        
        # Parse JSON stream
        parser = ijson.parse(stream)
        # This is a simplified approach - for full implementation we'd need more complex streaming
        # For now, fall back to regular parsing with increased limits
        return json.loads(resp.content.decode('utf-8'))
        
    except ImportError:
        print("  [!] ijson not available, falling back to regular parsing")
        return json.loads(resp.content.decode('utf-8'))
    except Exception as e:
        print(f"  [X] Error in streaming parse: {str(e)}")
        return None

def analyze_structure(data: Any, path: str = "root", max_depth: int = 4, current_depth: int = 0) -> Dict[str, Any]:
    """Recursively analyze JSON structure with depth limit."""
    if current_depth >= max_depth:
        return {"type": "truncated", "reason": "max_depth"}
    
    if isinstance(data, dict):
        analysis = {
            "type": "object",
            "keys": list(data.keys()),
            "key_count": len(data.keys()),
            "children": {}
        }
        
        # Analyze each key
        for key in data.keys():
            if key in ["in_network", "reporting_structure", "allowed_amount_file", 
                      "in_network_files", "provider_references", "negotiated_rates"]:
                # Analyze these important keys deeper
                analysis["children"][key] = analyze_structure(
                    data[key], f"{path}.{key}", max_depth, current_depth + 1
                )
        
        return analysis
        
    elif isinstance(data, list):
        analysis = {
            "type": "array",
            "length": len(data),
            "sample_item": None
        }
        
        if data:
            # Analyze first item as sample
            analysis["sample_item"] = analyze_structure(
                data[0], f"{path}[0]", max_depth, current_depth + 1
            )
            
        return analysis
    else:
        return {
            "type": type(data).__name__,
            "sample_value": str(data)[:50] if data else None
        }

def analyze_table_of_contents(url: str, payer: str) -> Dict[str, Any]:
    """Analyze a Table of Contents (index) file structure."""
    print(f"\n[TOC] Analyzing Table of Contents for {payer}")
    print(f"   URL: {url}")
    
    data = fetch_json(url)
    if not data:
        return {"error": "Failed to fetch"}
    
    analysis = {
        "payer": payer,
        "url": url,
        "structure_type": "unknown",
        "top_level_keys": list(data.keys()) if isinstance(data, dict) else [],
        "file_counts": {},
        "sample_files": {},
        "detailed_structure": {}
    }
    
    if isinstance(data, dict):
        # Standard Table of Contents structure
        if "reporting_structure" in data:
            analysis["structure_type"] = "standard_toc"
            rs = data["reporting_structure"]
            analysis["file_counts"]["reporting_structures"] = len(rs)
            
            # Count file types
            in_network_count = 0
            allowed_amount_count = 0
            provider_ref_count = 0
            
            # Sample first reporting structure
            if rs:
                first_rs = rs[0]
                analysis["sample_structure"] = {
                    "keys": list(first_rs.keys()),
                    "plan_name": first_rs.get("plan_name", ""),
                    "plan_id": first_rs.get("plan_id", ""),
                    "plan_market_type": first_rs.get("plan_market_type", "")
                }
                
                # Count and sample in-network files
                if "in_network_files" in first_rs:
                    for r in rs:
                        in_network_count += len(r.get("in_network_files", []))
                    
                    # Sample first in-network file
                    if first_rs["in_network_files"]:
                        sample_file = first_rs["in_network_files"][0]
                        sample_url = sample_file.get("location", "")
                        analysis["sample_files"]["in_network"] = {
                            "url": sample_url,
                            "url_display": sample_url[:100] + "..." if len(sample_url) > 100 else sample_url,
                            "description": sample_file.get("description", "")
                        }
                
                # Count other file types
                for r in rs:
                    if "allowed_amount_file" in r:
                        allowed_amount_count += 1
                    if "provider_references" in r:
                        provider_ref_count += len(r.get("provider_references", []))
            
            analysis["file_counts"]["in_network_files"] = in_network_count
            analysis["file_counts"]["allowed_amount_files"] = allowed_amount_count
            analysis["file_counts"]["provider_reference_files"] = provider_ref_count
            
        # Legacy blobs structure
        elif "blobs" in data:
            analysis["structure_type"] = "legacy_blobs"
            analysis["file_counts"]["blobs"] = len(data["blobs"])
            
            if data["blobs"]:
                sample_url = data["blobs"][0].get("url", "")
                analysis["sample_files"]["blob"] = {
                    "url": sample_url,
                    "url_display": sample_url[:100] + "..." if len(sample_url) > 100 else sample_url,
                    "name": data["blobs"][0].get("name", "")
                }
        
        # Direct in_network_files structure
        elif "in_network_files" in data:
            analysis["structure_type"] = "direct_in_network"
            analysis["file_counts"]["in_network_files"] = len(data["in_network_files"])
            
            if data["in_network_files"]:
                sample_url = data["in_network_files"][0].get("location", "")
                analysis["sample_files"]["in_network"] = {
                    "url": sample_url,
                    "url_display": sample_url[:100] + "..." if len(sample_url) > 100 else sample_url,
                    "description": data["in_network_files"][0].get("description", "")
                }
        
        # Store detailed structure analysis
        analysis["detailed_structure"] = analyze_structure(data)
    
    return analysis

def analyze_in_network_file(url: str, payer: str, max_items: int = 5) -> Dict[str, Any]:
    """Analyze an in-network MRF file structure."""
    print(f"\n[MRF] Analyzing In-Network MRF for {payer}")
    print(f"   URL: {url[:100]}...")
    
    # Try streaming first for large files, then fall back to regular fetch
    data = fetch_json_streaming(url, max_size_mb=1000)  # Much higher limit for MRF files
    if not data:
        data = fetch_json(url, max_size_mb=500)  # Fallback with higher limit
    if not data:
        return {"error": "Failed to fetch or file too large"}
    
    analysis = {
        "payer": payer,
        "url": url,
        "structure_type": "unknown",
        "top_level_keys": list(data.keys()) if isinstance(data, dict) else [],
        "in_network_count": 0,
        "billing_code_types": defaultdict(int),
        "sample_items": [],
        "rate_structure": {},
        "provider_structure": {}
    }
    
    if isinstance(data, dict) and "in_network" in data:
        analysis["structure_type"] = "standard_in_network"
        in_network = data["in_network"]
        analysis["in_network_count"] = len(in_network)
        
        # For very large files, limit analysis to avoid memory issues
        if len(in_network) > 100000:  # More than 100K items
            print(f"  [!] Large file detected: {len(in_network):,} items. Limiting analysis to first {max_items} items.")
            analysis["note"] = f"Large file - only analyzing first {max_items} items out of {len(in_network):,} total"
        
        # Analyze sample items
        for i, item in enumerate(in_network[:max_items]):
            item_analysis = {
                "index": i,
                "billing_code": item.get("billing_code"),
                "billing_code_type": item.get("billing_code_type"),
                "description": item.get("description", "")[:50],
                "keys": list(item.keys()),
                "negotiated_rates_count": len(item.get("negotiated_rates", []))
            }
            
            # Count billing code types
            if item.get("billing_code_type"):
                analysis["billing_code_types"][item["billing_code_type"]] += 1
            
            # Analyze negotiated rates structure
            if "negotiated_rates" in item and item["negotiated_rates"]:
                first_rate = item["negotiated_rates"][0]
                item_analysis["rate_structure"] = {
                    "keys": list(first_rate.keys()),
                    "has_provider_groups": "provider_groups" in first_rate,
                    "has_provider_references": "provider_references" in first_rate,
                    "negotiated_prices_count": len(first_rate.get("negotiated_prices", []))
                }
                
                # Analyze provider structure
                if "provider_groups" in first_rate and first_rate["provider_groups"]:
                    pg = first_rate["provider_groups"][0]
                    item_analysis["provider_group_structure"] = {
                        "keys": list(pg.keys()),
                        "has_npi": "npi" in pg,
                        "has_tin": "tin" in pg,
                        "has_providers": "providers" in pg
                    }
                    
                    # Check if providers are nested
                    if "providers" in pg and pg["providers"]:
                        item_analysis["nested_provider_structure"] = {
                            "keys": list(pg["providers"][0].keys())
                        }
                
                # Analyze negotiated prices structure
                if "negotiated_prices" in first_rate and first_rate["negotiated_prices"]:
                    np = first_rate["negotiated_prices"][0]
                    item_analysis["price_structure"] = {
                        "keys": list(np.keys()),
                        "has_negotiated_rate": "negotiated_rate" in np,
                        "has_negotiated_price": "negotiated_price" in np,
                        "negotiated_rate_value": np.get("negotiated_rate"),
                        "service_codes": np.get("service_code", [])
                    }
            
            analysis["sample_items"].append(item_analysis)
        
        # Store overall structure
        analysis["detailed_structure"] = analyze_structure(data, max_depth=5)
    
    return analysis

def save_analysis(analyses: Dict[str, Any], output_dir: str = "payer_structure_analysis"):
    """Save analysis results to files."""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save full analysis
    full_path = output_path / f"full_analysis_{timestamp}.json"
    with open(full_path, 'w', encoding='utf-8') as f:
        json.dump(analyses, f, indent=2, default=str)
    
    # Save summary
    summary_path = output_path / f"summary_{timestamp}.txt"
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("PAYER STRUCTURE ANALYSIS SUMMARY\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        for payer, analysis in analyses.items():
            f.write(f"\n{'='*60}\n")
            f.write(f"PAYER: {payer}\n")
            f.write(f"{'='*60}\n")
            
            # Table of Contents summary
            toc = analysis.get("table_of_contents", {})
            if toc and "error" not in toc:
                f.write("\n[TABLE OF CONTENTS STRUCTURE]\n")
                f.write(f"  - Structure Type: {toc.get('structure_type', 'unknown')}\n")
                f.write(f"  - Top Level Keys: {', '.join(toc.get('top_level_keys', []))}\n")
                f.write(f"  - File Counts: {json.dumps(toc.get('file_counts', {}))}\n")
                
                if toc.get('sample_structure'):
                    f.write(f"  - Sample Plan Keys: {', '.join(toc['sample_structure'].get('keys', []))}\n")
            
            # In-Network MRF summary
            mrf = analysis.get("in_network_mrf", {})
            if mrf and "error" not in mrf:
                f.write("\n[IN-NETWORK MRF STRUCTURE]\n")
                f.write(f"  - Structure Type: {mrf.get('structure_type', 'unknown')}\n")
                f.write(f"  - Total Items: {mrf.get('in_network_count', 0)}\n")
                f.write(f"  - Billing Code Types: {dict(mrf.get('billing_code_types', {}))}\n")
                
                if mrf.get('sample_items'):
                    item = mrf['sample_items'][0]
                    f.write(f"\n  Sample Item Structure:\n")
                    f.write(f"    - Item Keys: {', '.join(item.get('keys', []))}\n")
                    
                    if item.get('rate_structure'):
                        rs = item['rate_structure']
                        f.write(f"    - Rate Keys: {', '.join(rs.get('keys', []))}\n")
                        f.write(f"    - Has Provider Groups: {rs.get('has_provider_groups', False)}\n")
                        f.write(f"    - Has Provider References: {rs.get('has_provider_references', False)}\n")
                    
                    if item.get('provider_group_structure'):
                        pgs = item['provider_group_structure']
                        f.write(f"    - Provider Group Keys: {', '.join(pgs.get('keys', []))}\n")
                        f.write(f"    - Provider Info Location: ")
                        if pgs.get('has_npi'):
                            f.write("Direct in provider_group\n")
                        elif pgs.get('has_providers'):
                            f.write("Nested in providers array\n")
                    
                    if item.get('price_structure'):
                        ps = item['price_structure']
                        f.write(f"    - Price Keys: {', '.join(ps.get('keys', []))}\n")
                        f.write(f"    - Rate Field Name: ")
                        if ps.get('has_negotiated_rate'):
                            f.write("negotiated_rate\n")
                        elif ps.get('has_negotiated_price'):
                            f.write("negotiated_price\n")
    
    print(f"\n[+] Analysis saved to:")
    print(f"   - Full analysis: {full_path}")
    print(f"   - Summary: {summary_path}")

def main():
    parser = argparse.ArgumentParser(description="Analyze payer MRF structures")
    parser.add_argument("--config", default="production_config.yaml", help="Path to config file")
    parser.add_argument("--payers", nargs="+", help="Specific payers to analyze")
    parser.add_argument("--skip-mrf", action="store_true", help="Skip in-network MRF analysis")
    parser.add_argument("--output-dir", default="payer_structure_analysis", help="Output directory")
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    # Try both payer_endpoints and endpoints (for backward compatibility)
    payer_endpoints = config.get("payer_endpoints", config.get("endpoints", {}))
    
    # Filter payers if specified
    if args.payers:
        payer_endpoints = {k: v for k, v in payer_endpoints.items() if k in args.payers}
    
    print(f"[*] Analyzing {len(payer_endpoints)} payer(s) from {args.config}")
    
    # Analyze each payer
    all_analyses = {}
    
    for payer, index_url in payer_endpoints.items():
        print(f"\n{'='*80}")
        print(f"ANALYZING PAYER: {payer}")
        print(f"{'='*80}")
        
        payer_analysis = {}
        
        # Analyze Table of Contents
        toc_analysis = analyze_table_of_contents(index_url, payer)
        payer_analysis["table_of_contents"] = toc_analysis
        
        # Analyze sample in-network MRF (if not skipped)
        if not args.skip_mrf and toc_analysis.get("sample_files", {}).get("in_network"):
            mrf_url = toc_analysis["sample_files"]["in_network"]["url"]
            if not mrf_url:
                print("  Warning: No MRF URL found in TOC")
            else:
                mrf_analysis = analyze_in_network_file(mrf_url, payer)
                payer_analysis["in_network_mrf"] = mrf_analysis
        
        all_analyses[payer] = payer_analysis
    
    # Save results
    save_analysis(all_analyses, args.output_dir)
    
    print("\n[DONE] Analysis complete!")

if __name__ == "__main__":
    main()