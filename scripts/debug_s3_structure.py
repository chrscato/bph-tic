#!/usr/bin/env python3
"""Debug S3 Structure - Helper script to explore S3 bucket contents."""

import os
import boto3
import json
from typing import Dict, List, Any

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Loaded environment variables from .env file")
except ImportError:
    print("python-dotenv not installed, using system environment variables")
except Exception as e:
    print(f"Warning: Could not load .env file: {e}")

def debug_s3_structure():
    """Debug the S3 bucket structure to understand what's there."""
    
    # Configuration
    s3_bucket = os.getenv("S3_BUCKET", "commercial-rates")
    s3_prefix = os.getenv("S3_PREFIX", "commercial-rates/tic-mrf")
    
    print(f"S3 Bucket: {s3_bucket}")
    print(f"S3 Prefix: {s3_prefix}")
    print("=" * 50)
    
    try:
        s3_client = boto3.client('s3')
        
        # Test basic S3 access
        print("Testing S3 access...")
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            MaxKeys=1
        )
        print(f"✓ S3 access successful. Bucket exists.")
        
        # List all objects with the prefix
        print(f"\nListing objects with prefix: {s3_prefix}")
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=s3_bucket,
            Prefix=s3_prefix
        )
        
        all_objects = []
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    all_objects.append(obj['Key'])
        
        print(f"Found {len(all_objects)} objects with prefix '{s3_prefix}'")
        
        if all_objects:
            print("\nFirst 10 objects:")
            for i, key in enumerate(all_objects[:10]):
                print(f"  {i+1}. {key}")
            
            if len(all_objects) > 10:
                print(f"  ... and {len(all_objects) - 10} more")
        
        # Analyze structure
        print("\nAnalyzing structure...")
        structure = analyze_s3_structure(all_objects, s3_prefix)
        
        print("\nStructure analysis:")
        print(json.dumps(structure, indent=2))
        
        # Check for specific patterns
        print("\nChecking for batch files...")
        batch_files = [obj for obj in all_objects if 'batch_' in obj and obj.endswith('.parquet')]
        print(f"Found {len(batch_files)} batch files")
        
        if batch_files:
            print("\nSample batch files:")
            for i, key in enumerate(batch_files[:5]):
                print(f"  {i+1}. {key}")
        
        # Check for payer patterns
        print("\nChecking for payer patterns...")
        payers_found = set()
        for obj in all_objects:
            if 'payer=' in obj:
                # Extract payer name from path like "commercial-rates/tic-mrf/rates/payer=bcbs_il/"
                parts = obj.split('payer=')
                if len(parts) > 1:
                    payer_part = parts[1].split('/')[0]
                    payers_found.add(payer_part)
        
        print(f"Found payers: {sorted(list(payers_found))}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()

def analyze_s3_structure(objects: List[str], prefix: str) -> Dict[str, Any]:
    """Analyze the S3 object structure."""
    structure = {
        "total_objects": len(objects),
        "data_types": {},
        "payers": set(),
        "date_patterns": set(),
        "file_patterns": {}
    }
    
    for obj in objects:
        # Remove the prefix to get relative path
        relative_path = obj[len(prefix):].lstrip('/')
        parts = relative_path.split('/')
        
        if len(parts) >= 2:
            data_type = parts[0]  # rates, organizations, providers
            if data_type not in structure["data_types"]:
                structure["data_types"][data_type] = {
                    "total_files": 0,
                    "payers": set(),
                    "sample_files": []
                }
            
            structure["data_types"][data_type]["total_files"] += 1
            
            # Extract payer from path like "payer=bcbs_il/date=2025-07-29/"
            for part in parts:
                if part.startswith('payer='):
                    payer = part.split('=')[1]
                    structure["payers"].add(payer)
                    structure["data_types"][data_type]["payers"].add(payer)
                elif part.startswith('date='):
                    date = part.split('=')[1]
                    structure["date_patterns"].add(date)
            
            # Track file patterns
            if obj.endswith('.parquet'):
                if 'batch_' in obj:
                    pattern = 'batch_files'
                elif 'final' in obj:
                    pattern = 'final_files'
                else:
                    pattern = 'other_parquet'
                
                if pattern not in structure["file_patterns"]:
                    structure["file_patterns"][pattern] = []
                
                if len(structure["file_patterns"][pattern]) < 3:  # Keep sample
                    structure["file_patterns"][pattern].append(obj)
    
    # Convert sets to lists for JSON serialization
    structure["payers"] = sorted(list(structure["payers"]))
    structure["date_patterns"] = sorted(list(structure["date_patterns"]))
    
    for data_type in structure["data_types"]:
        structure["data_types"][data_type]["payers"] = sorted(list(structure["data_types"][data_type]["payers"]))
    
    return structure

if __name__ == "__main__":
    debug_s3_structure() 