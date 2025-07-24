#!/usr/bin/env python3
"""Quick comparison of payer formats to identify patterns and differences."""

import json
import yaml
from pathlib import Path
from typing import Dict, Any, List
import argparse
from collections import defaultdict

def load_analysis(analysis_file: str) -> Dict[str, Any]:
    """Load a previous analysis file."""
    with open(analysis_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def compare_toc_structures(analyses: Dict[str, Any]) -> Dict[str, Any]:
    """Compare Table of Contents structures across payers."""
    comparison = {
        "structure_types": defaultdict(list),
        "top_level_keys": defaultdict(list),
        "file_types_available": defaultdict(list),
        "unique_patterns": {}
    }
    
    for payer, analysis in analyses.items():
        toc = analysis.get("table_of_contents", {})
        if "error" in toc:
            continue
            
        # Group by structure type
        structure_type = toc.get("structure_type", "unknown")
        comparison["structure_types"][structure_type].append(payer)
        
        # Track top-level keys
        for key in toc.get("top_level_keys", []):
            comparison["top_level_keys"][key].append(payer)
        
        # Track available file types
        file_counts = toc.get("file_counts", {})
        for file_type, count in file_counts.items():
            if count > 0:
                comparison["file_types_available"][file_type].append(payer)
    
    return comparison

def compare_mrf_structures(analyses: Dict[str, Any]) -> Dict[str, Any]:
    """Compare in-network MRF structures across payers."""
    comparison = {
        "billing_code_types": defaultdict(list),
        "provider_info_location": defaultdict(list),
        "rate_field_names": defaultdict(list),
        "unique_fields": defaultdict(list),
        "structural_patterns": []
    }
    
    for payer, analysis in analyses.items():
        mrf = analysis.get("in_network_mrf", {})
        if "error" in mrf or not mrf.get("sample_items"):
            continue
        
        # Billing code types used
        for code_type in mrf.get("billing_code_types", {}).keys():
            comparison["billing_code_types"][code_type].append(payer)
        
        # Analyze first sample item
        sample = mrf["sample_items"][0]
        
        # Provider info location
        if sample.get("provider_group_structure"):
            pgs = sample["provider_group_structure"]
            if pgs.get("has_npi"):
                comparison["provider_info_location"]["direct_in_provider_group"].append(payer)
            elif pgs.get("has_providers"):
                comparison["provider_info_location"]["nested_in_providers_array"].append(payer)
        
        # Rate field names
        if sample.get("price_structure"):
            ps = sample["price_structure"]
            if ps.get("has_negotiated_rate"):
                comparison["rate_field_names"]["negotiated_rate"].append(payer)
            elif ps.get("has_negotiated_price"):
                comparison["rate_field_names"]["negotiated_price"].append(payer)
        
        # Unique fields in the item structure
        for field in sample.get("keys", []):
            if field not in ["billing_code", "billing_code_type", "description", "negotiated_rates"]:
                comparison["unique_fields"][field].append(payer)
    
    return comparison

def generate_payer_profile(payer: str, analysis: Dict[str, Any]) -> List[str]:
    """Generate a profile summary for a specific payer."""
    profile = []
    
    # TOC profile
    toc = analysis.get("table_of_contents", {})
    if toc and "error" not in toc:
        profile.append(f"TOC Structure: {toc.get('structure_type', 'unknown')}")
        file_counts = toc.get("file_counts", {})
        profile.append(f"Files: {json.dumps(file_counts)}")
    
    # MRF profile
    mrf = analysis.get("in_network_mrf", {})
    if mrf and "error" not in mrf and mrf.get("sample_items"):
        sample = mrf["sample_items"][0]
        
        # Provider location
        if sample.get("provider_group_structure", {}).get("has_npi"):
            profile.append("Provider Info: Direct in provider_group")
        elif sample.get("provider_group_structure", {}).get("has_providers"):
            profile.append("Provider Info: Nested in providers array")
        
        # Rate field
        if sample.get("price_structure", {}).get("has_negotiated_rate"):
            profile.append("Rate Field: negotiated_rate")
        elif sample.get("price_structure", {}).get("has_negotiated_price"):
            profile.append("Rate Field: negotiated_price")
        
        # Billing code types
        code_types = list(mrf.get("billing_code_types", {}).keys())
        if code_types:
            profile.append(f"Code Types: {', '.join(code_types)}")
    
    return profile

def print_comparison_report(analyses: Dict[str, Any]):
    """Print a formatted comparison report."""
    print("=" * 80)
    print("PAYER FORMAT COMPARISON REPORT")
    print("=" * 80)
    
    # TOC Comparison
    print("\n[TOC] TABLE OF CONTENTS PATTERNS")
    print("-" * 40)
    toc_comparison = compare_toc_structures(analyses)
    
    print("\nStructure Types:")
    for stype, payers in toc_comparison["structure_types"].items():
        print(f"  {stype}: {', '.join(payers)}")
    
    print("\nFile Types Available:")
    for ftype, payers in toc_comparison["file_types_available"].items():
        print(f"  {ftype}: {len(payers)} payers")
    
    # MRF Comparison
    print("\n\n[MRF] IN-NETWORK MRF PATTERNS")
    print("-" * 40)
    mrf_comparison = compare_mrf_structures(analyses)
    
    print("\nProvider Info Location:")
    for location, payers in mrf_comparison["provider_info_location"].items():
        print(f"  {location}: {', '.join(payers)}")
    
    print("\nRate Field Names:")
    for field, payers in mrf_comparison["rate_field_names"].items():
        print(f"  {field}: {', '.join(payers)}")
    
    print("\nBilling Code Types:")
    for code_type, payers in mrf_comparison["billing_code_types"].items():
        print(f"  {code_type}: {len(payers)} payers")
    
    # Individual Payer Profiles
    print("\n\n[PROFILES] INDIVIDUAL PAYER PROFILES")
    print("-" * 40)
    
    for payer, analysis in analyses.items():
        profile = generate_payer_profile(payer, analysis)
        print(f"\n{payer}:")
        for item in profile:
            print(f"  - {item}")

def generate_etl_recommendations(analyses: Dict[str, Any]) -> List[str]:
    """Generate ETL modification recommendations based on analysis."""
    recommendations = []
    
    # Check for non-standard TOC structures
    toc_comparison = compare_toc_structures(analyses)
    non_standard = []
    for stype, payers in toc_comparison["structure_types"].items():
        if stype not in ["standard_toc"]:
            non_standard.extend(payers)
    
    if non_standard:
        recommendations.append(
            f"[!] Non-standard TOC structures found for: {', '.join(non_standard)}. "
            "Consider creating custom handlers for these payers."
        )
    
    # Check for different provider info locations
    mrf_comparison = compare_mrf_structures(analyses)
    if len(mrf_comparison["provider_info_location"]) > 1:
        recommendations.append(
            "[!] Multiple provider info locations detected. Your parser should handle both:\n"
            "   - Direct provider info in provider_groups\n"
            "   - Nested provider info in provider_groups.providers array"
        )
    
    # Check for different rate field names
    if len(mrf_comparison["rate_field_names"]) > 1:
        recommendations.append(
            "[!] Multiple rate field names detected. Ensure your parser checks for both:\n"
            "   - negotiated_rate\n"
            "   - negotiated_price"
        )
    
    # Check for unique fields that might contain important data
    important_unique_fields = ["additional_information", "modifiers", "derived_amount"]
    found_unique = []
    for field, payers in mrf_comparison["unique_fields"].items():
        if field in important_unique_fields:
            found_unique.append(f"{field} ({', '.join(payers)})")
    
    if found_unique:
        recommendations.append(
            f"[+] Consider extracting these additional fields: {', '.join(found_unique)}"
        )
    
    return recommendations

def main():
    parser = argparse.ArgumentParser(description="Compare payer format structures")
    parser.add_argument("analysis_file", help="Path to full_analysis JSON file")
    parser.add_argument("--output", help="Output comparison to file")
    args = parser.parse_args()
    
    # Load analysis
    analyses = load_analysis(args.analysis_file)
    
    # Print comparison report
    print_comparison_report(analyses)
    
    # Generate recommendations
    print("\n\n[RECOMMENDATIONS] ETL RECOMMENDATIONS")
    print("-" * 40)
    recommendations = generate_etl_recommendations(analyses)
    for i, rec in enumerate(recommendations, 1):
        print(f"\n{i}. {rec}")
    
    # Save to file if requested
    if args.output:
        output_path = Path(args.output)
        with open(output_path, 'w') as f:
            # Recreate the report for file output
            import sys
            original_stdout = sys.stdout
            sys.stdout = f
            print_comparison_report(analyses)
            print("\n\n[RECOMMENDATIONS] ETL RECOMMENDATIONS")
            print("-" * 40)
            for i, rec in enumerate(recommendations, 1):
                print(f"\n{i}. {rec}")
            sys.stdout = original_stdout
        print(f"\n[+] Comparison saved to: {output_path}")

if __name__ == "__main__":
    main()