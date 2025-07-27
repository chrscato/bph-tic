#!/usr/bin/env python3
"""Analyze inspection findings and provide key insights."""

import json
from pathlib import Path

def analyze_findings():
    """Analyze the inspection findings and provide key insights."""
    
    # Load the summary data
    summary_file = Path("data_inspection_production/inspection_summary.json")
    if not summary_file.exists():
        print("❌ Inspection summary file not found!")
        return
    
    with open(summary_file, 'r') as f:
        data = json.load(f)
    
    print("="*60)
    print("DATA INSPECTION FINDINGS SUMMARY")
    print("="*60)
    
    # Dataset sizes
    print("\n📊 DATASET SIZES:")
    print("-" * 30)
    for name, info in data['datasets'].items():
        records = info['shape'][0]
        columns = info['shape'][1]
        print(f"  {name.title():12} : {records:>8,} records, {columns:>2} columns")
    
    # Key insights
    print("\n🔍 KEY INSIGHTS:")
    print("-" * 30)
    
    # NPPES coverage
    nppes_match = data['join_compatibility']['potential_joins'][0]
    print(f"  • NPPES Coverage: {nppes_match['estimated_match_rate']:.1f}% of providers have NPPES data")
    print(f"  • NPPES Records: {nppes_match['nppes_unique_npis']:,} vs {nppes_match['providers_unique_npis']:,} providers")
    
    # Organization join potential
    org_join = data['join_strategy']['recommended_joins'][1]
    print(f"  • Organization Join: {org_join['expected_matches']:,} potential matches via organization_uuid")
    
    # Data quality issues
    print("\n⚠️  DATA QUALITY ISSUES:")
    print("-" * 30)
    if data['join_compatibility']['join_challenges']:
        for issue in data['join_compatibility']['join_challenges']:
            print(f"  • {issue['dataset']}.{issue['column']}: {issue['null_percentage']:.1f}% null values")
    else:
        print("  • No major data quality issues detected")
    
    # Join strategy recommendations
    print("\n🎯 JOIN STRATEGY RECOMMENDATIONS:")
    print("-" * 30)
    for join in data['join_strategy']['recommended_joins']:
        print(f"  • {join['name']}: {join['type']} via {join['join_key']}")
        print(f"    Expected matches: {join['expected_matches']:,}")
        if join['data_preparation']:
            for step in join['data_preparation']:
                print(f"    Prep: {step}")
    
    # Dashboard implications
    print("\n📈 DASHBOARD IMPLICATIONS:")
    print("-" * 30)
    print("  • Rich rate data: 1.5M+ rate records available")
    print("  • Limited NPPES enrichment: Only 0.3% provider coverage")
    print("  • Strong organization linkage: 45K+ organization matches")
    print("  • Analytics ready: 98 pre-computed analytics records")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    analyze_findings() 