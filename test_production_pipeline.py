#!/usr/bin/env python3
"""Simple test of the production pipeline with minimal configuration."""

import os
import sys
from pathlib import Path

# Add src to path so we can import tic_mrf_scraper
sys.path.insert(0, str(Path(__file__).parent / "src"))

from production_etl_pipeline import ProductionETLPipeline, ETLConfig

def test_production_pipeline():
    """Test the production pipeline with minimal configuration."""
    
    print("🧪 Testing Production ETL Pipeline")
    print("=" * 50)
    
    # Minimal test configuration
    config = ETLConfig(
        payer_endpoints={
            "centene_fidelis": "http://centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-04-29_centene-management-company-llc_fidelis-ex_in-network.json"
        },
        cpt_whitelist=[
            "0240U", "0241U",  # Codes we know work
            "99213", "99214",  # Common office visits
        ],
        max_files_per_payer=1,          # Just one file for testing
        max_records_per_file=1000,      # Small sample
        batch_size=100,                 # Small batches
        parallel_workers=1,             # Single-threaded for testing
        local_output_dir="test_production_data",
        s3_bucket=None,                 # No S3 for testing
        schema_version="v2.1.0-test",
        processing_version="tic-etl-test"
    )
    
    print(f"📋 Test Configuration:")
    print(f"   - Payers: {len(config.payer_endpoints)}")
    print(f"   - CPT codes: {len(config.cpt_whitelist)}")
    print(f"   - Max records: {config.max_records_per_file}")
    print(f"   - Output: {config.local_output_dir}")
    print()
    
    try:
        # Create and run pipeline
        pipeline = ProductionETLPipeline(config)
        pipeline.process_all_payers()
        
        print("✅ Production pipeline test completed!")
        
        # Check output files
        output_dir = Path(config.local_output_dir)
        if output_dir.exists():
            print(f"\n📁 Output files created:")
            for table_dir in ["rates", "organizations", "providers", "payers", "analytics"]:
                table_path = output_dir / table_dir
                if table_path.exists():
                    files = list(table_path.glob("*.parquet"))
                    if files:
                        print(f"   {table_dir}: {len(files)} files")
                        # Show file sizes
                        for file in files:
                            size_mb = file.stat().st_size / 1024 / 1024
                            print(f"     - {file.name}: {size_mb:.2f} MB")
                    else:
                        print(f"   {table_dir}: No files")
        
        print(f"\n🎉 Test successful! Check {config.local_output_dir}/ for results")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        print("\nFull error:")
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    # Test if we can import the required modules
    try:
        from tic_mrf_scraper.stream.parser import stream_parse_enhanced
        from tic_mrf_scraper.transform.normalize import normalize_tic_record
        print("✅ tic_mrf_scraper modules imported successfully")
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("\nMake sure you're running from the project root directory")
        print("and that src/tic_mrf_scraper is available")
        sys.exit(1)
    
    # Run the test
    success = test_production_pipeline()
    sys.exit(0 if success else 1) 