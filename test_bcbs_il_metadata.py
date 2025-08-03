#!/usr/bin/env python3
"""
Test to examine BCBS IL file metadata structure.
"""

import sys
import json
import logging
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.payers.bcbs_il import Bcbs_IlHandler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bcbs_il_metadata():
    """Examine BCBS IL file metadata structure."""
    
    print("="*60)
    print("BCBS IL METADATA EXAMINATION")
    print("="*60)
    
    handler = Bcbs_IlHandler()
    bcbs_il_index_url = "https://app0004702110a5prdnc868.blob.core.windows.net/toc/2025-07-23_Blue-Cross-and-Blue-Shield-of-Illinois_index.json"
    
    print("\n1. Examining file metadata structure...")
    try:
        files = list(handler.list_mrf_files(bcbs_il_index_url))
        if files:
            print(f"Found {len(files)} files")
            
            # Examine first few files
            for i, file_meta in enumerate(files[:5]):
                print(f"\nFile {i+1}:")
                print(f"  All keys: {list(file_meta.keys())}")
                print(f"  Description: {file_meta.get('description', 'No description')}")
                print(f"  Location: {file_meta.get('location', 'No location')}")
                print(f"  URL: {file_meta.get('url', 'No URL')}")
                print(f"  Full metadata: {json.dumps(file_meta, indent=2)}")
                
                # Look for any URL-like fields
                for key, value in file_meta.items():
                    if isinstance(value, str) and ('http' in value or 'blob' in value):
                        print(f"  Found URL-like field '{key}': {value}")
            
            # Check if there are any files with location URLs
            files_with_location = [f for f in files if f.get('location')]
            files_with_url = [f for f in files if f.get('url')]
            
            print(f"\nSummary:")
            print(f"  Files with 'location': {len(files_with_location)}")
            print(f"  Files with 'url': {len(files_with_url)}")
            
            if files_with_location:
                print(f"  First file with location: {files_with_location[0].get('description')}")
                print(f"  Location URL: {files_with_location[0].get('location')}")
            elif files_with_url:
                print(f"  First file with url: {files_with_url[0].get('description')}")
                print(f"  URL: {files_with_url[0].get('url')}")
            else:
                print(f"  ❌ No files have location or url fields!")
                print(f"  This explains why we can't access individual MRF files.")
                
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_bcbs_il_metadata() 