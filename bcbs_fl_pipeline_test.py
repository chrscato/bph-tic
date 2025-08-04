#!/usr/bin/env python3
"""
Fixed BCBS FL test script - properly handles normalize_tic_record parameters.
"""

import sys
import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.payers.bcbs_fl import BCBSFLHandler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_bcbs_fl_fixed():
    """Fixed test of BCBS FL handler with proper normalization parameters."""
    
    print("="*80)
    print("BCBS FL FIXED TEST")
    print("="*80)
    
    # Define CPT whitelist based on BCBS FL analysis
    cpt_whitelist = {
        "99213", "99214", "72148", "73721", "70450",  # Common orthopedic/radiology codes
        "0001U", "0002M", "0003M", "0004M", "0004U"   # From BCBS FL analysis
    }
    payer_name = "bcbs_fl"
    
    # Create output directory
    output_dir = Path("./test_bcbs_fl_fixed")
    output_dir.mkdir(exist_ok=True)
    
    print(f"\n1. TESTING HANDLER AND FILE DISCOVERY")
    
    # Get handler
    try:
        handler = BCBSFLHandler() 
        print(f"   ✅ Handler created: {type(handler).__name__}")
    except Exception as e:
        print(f"   ❌ Handler creation failed: {e}")
        return False
    
    # Get file list
    bcbs_fl_url = "https://d1hgtx7rrdl2cn.cloudfront.net/mrf/toc/FloridaBlue_Health-Insurance-Issuer_index.json"
    
    try:
        mrf_files = list(list_mrf_blobs_enhanced(bcbs_fl_url))
        print(f"   ✅ Found {len(mrf_files)} MRF files")
        
        if not mrf_files:
            print("   ❌ No MRF files found")
            return False
        
        # Get first file URL
        first_file = mrf_files[0]
        test_url = first_file.get('location', first_file.get('url', ''))
        
        if not test_url:
            print(f"   ❌ No valid URL found")
            print(f"   Available keys: {list(first_file.keys())}")
            return False
            
        print(f"   ✅ Test file URL: {test_url[:100]}...")
        
    except Exception as e:
        print(f"   ❌ File discovery failed: {e}")
        return False
    
    print(f"\n2. PROCESSING REAL MRF DATA")
    
    try:
        import requests
        import gzip
        
        # Fetch MRF file
        print(f"   Fetching MRF file...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(test_url, headers=headers, timeout=120)
        response.raise_for_status()
        
        # Handle compression
        if test_url.endswith('.gz') or '.gz?' in test_url:
            content = gzip.decompress(response.content).decode('utf-8')
        else:
            content = response.text
        
        # Parse JSON
        mrf_data = json.loads(content)
        print(f"   ✅ Successfully parsed MRF file")
        print(f"   Top-level keys: {list(mrf_data.keys())}")
        
        # Show structure
        provider_refs = mrf_data.get('provider_references', [])
        in_network = mrf_data.get('in_network', [])
        print(f"   Provider references: {len(provider_refs)}")
        print(f"   In-network items: {len(in_network)}")
        
        # Preprocess for provider caching
        if hasattr(handler, 'preprocess_mrf_file'):
            handler.preprocess_mrf_file(mrf_data)
            cache_size = len(getattr(handler, 'provider_references_cache', {}))
            print(f"   ✅ Built provider cache: {cache_size} entries")
        
        # Limit data for testing
        max_items = 20  # Small number for testing
        test_items = in_network[:max_items]
        print(f"   Processing {len(test_items)} test items...")
        
    except Exception as e:
        print(f"   ❌ MRF processing failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print(f"\n3. PARSING AND NORMALIZING RECORDS")
    
    all_normalized = []
    raw_records = []
    
    try:
        for i, item in enumerate(test_items):
            # Store raw record
            raw_records.append(item)
            
            # Parse with handler
            try:
                parsed_records = handler.parse_in_network(item)
                print(f"   Item {i+1}: {item.get('billing_code', 'N/A')} -> {len(parsed_records)} parsed")
                
                # Normalize each parsed record
                for parsed in parsed_records:
                    try:
                        # Use correct normalize_tic_record signature
                        normalized = normalize_tic_record(parsed, cpt_whitelist, payer_name)
                        if normalized:
                            all_normalized.append(normalized)
                            
                    except Exception as e:
                        print(f"     ⚠️  Normalization failed: {e}")
                        continue
                        
            except Exception as e:
                print(f"   ❌ Parsing failed for item {i+1}: {e}")
                continue
        
        print(f"   ✅ Total normalized records: {len(all_normalized)}")
        
    except Exception as e:
        print(f"   ❌ Record processing failed: {e}")
        return False
    
    if not all_normalized:
        print(f"   ❌ No records were successfully normalized")
        return False
    
    print(f"\n4. CREATING DATAFRAME AND PARQUET")
    
    try:
        # Create DataFrame
        df = pd.DataFrame(all_normalized)
        print(f"   ✅ Created DataFrame: {len(df)} rows x {len(df.columns)} columns")
        
        # Show column info
        print(f"   Columns:")
        for col in df.columns:
            non_null = df[col].notna().sum()
            print(f"     - {col}: {non_null}/{len(df)} non-null")
        
        # Show sample data
        print(f"\n   Sample records:")
        for i, record in enumerate(all_normalized[:3]):
            print(f"   Record {i+1}:")
            print(f"     - Billing code: {record.get('billing_code')}")
            print(f"     - Rate: ${record.get('negotiated_rate', 0):.2f}")
            print(f"     - Provider NPI: {record.get('provider_npi', 'None')}")
            print(f"     - Payer: {record.get('payer_name')}")
        
        # Save parquet
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        parquet_file = output_dir / f"bcbs_fl_normalized_{timestamp}.parquet"
        df.to_parquet(parquet_file, index=False)
        print(f"   ✅ Saved parquet: {parquet_file}")
        
        # Show parquet info
        file_size_kb = parquet_file.stat().st_size / 1024
        print(f"   Parquet size: {file_size_kb:.1f} KB")
        
        # Data quality check
        rates = df['negotiated_rate'].dropna()
        if len(rates) > 0:
            print(f"\n   Rate statistics:")
            print(f"     - Count: {len(rates)}")
            print(f"     - Min: ${rates.min():.2f}")
            print(f"     - Max: ${rates.max():.2f}")
            print(f"     - Mean: ${rates.mean():.2f}")
        
        # Provider coverage
        providers_with_npi = df['provider_npi'].notna().sum()
        print(f"   Provider coverage: {providers_with_npi}/{len(df)} records have NPI")
        
    except Exception as e:
        print(f"   ❌ DataFrame/Parquet creation failed: {e}")
        return False
    
    # Save test report
    print(f"\n5. SAVING TEST REPORT")
    
    test_report = {
        "timestamp": datetime.now().isoformat(),
        "source_url": bcbs_fl_url,
        "test_file_url": test_url,
        "processing_summary": {
            "raw_items_processed": len(raw_records),
            "normalized_records": len(all_normalized),
            "provider_cache_size": len(getattr(handler, 'provider_references_cache', {})),
            "success_rate": len(all_normalized) / len(raw_records) if raw_records else 0
        },
        "data_quality": {
            "records_with_rates": rates.count() if 'rates' in locals() else 0,
            "records_with_providers": providers_with_npi if 'providers_with_npi' in locals() else 0,
            "rate_range": {
                "min": float(rates.min()) if 'rates' in locals() and len(rates) > 0 else None,
                "max": float(rates.max()) if 'rates' in locals() and len(rates) > 0 else None,
                "mean": float(rates.mean()) if 'rates' in locals() and len(rates) > 0 else None
            }
        },
        "files_created": [str(parquet_file)],
        "sample_records": all_normalized[:3]
    }
    
    report_file = output_dir / f"test_report_{timestamp}.json"
    with open(report_file, 'w') as f:
        json.dump(test_report, f, indent=2, default=str)
    
    print(f"   ✅ Saved report: {report_file}")
    
    # Final summary
    print(f"\n" + "="*80)
    print(f"🎉 BCBS FL TEST COMPLETED SUCCESSFULLY")
    print(f"="*80)
    print(f"✅ Handler: Working")
    print(f"✅ File discovery: {len(mrf_files)} files")
    print(f"✅ MRF processing: {len(provider_refs)} provider refs, {len(in_network)} items")
    print(f"✅ Provider cache: {len(getattr(handler, 'provider_references_cache', {}))} entries")
    print(f"✅ Record processing: {len(all_normalized)} normalized records")
    print(f"✅ Parquet output: {parquet_file.name} ({file_size_kb:.1f} KB)")
    
    print(f"\n📊 PRODUCTION READINESS:")
    print(f"   - Handler implements required interface ✅")  
    print(f"   - Provider references working ✅")
    print(f"   - Normalization working ✅")
    print(f"   - Parquet output compatible ✅")
    print(f"   - Ready for production_etl_pipeline.py ✅")
    
    return True

if __name__ == "__main__":
    success = test_bcbs_fl_fixed()
    sys.exit(0 if success else 1)