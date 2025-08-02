#!/usr/bin/env python3
"""Local test script for small amount of data on a specific payer endpoint."""

import os
import sys
import yaml
import tempfile
from pathlib import Path
from datetime import datetime

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from production_etl_pipeline import ProductionETLPipeline, ETLConfig

def create_test_config(payer_name: str, max_files: int = 2, max_records: int = 1000) -> ETLConfig:
    """Create a test configuration for a specific payer with limited data."""
    
    # Load base config
    with open('production_config.yaml', 'r') as f:
        base_config = yaml.safe_load(f)
    
    # Get the specific payer endpoint
    payer_endpoints = base_config.get('payer_endpoints', {})
    if payer_name not in payer_endpoints:
        available_payers = list(payer_endpoints.keys())
        raise ValueError(f"Payer '{payer_name}' not found. Available payers: {available_payers}")
    
    # Create test output directory
    test_output_dir = f"test_output_{payer_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    return ETLConfig(
        payer_endpoints={payer_name: payer_endpoints[payer_name]},
        cpt_whitelist=base_config.get('cpt_whitelist', []),
        batch_size=100,  # Smaller batch size for testing
        parallel_workers=1,  # Single worker for testing
        max_files_per_payer=max_files,  # Limit files
        max_records_per_file=max_records,  # Limit records per file
        local_output_dir=test_output_dir,
        s3_bucket=None,  # Disable S3 for local testing
        s3_prefix="test-prefix",
        schema_version="v2.1.0-test",
        processing_version="tic-etl-test-v1.0",
        min_completeness_pct=50.0,
        min_accuracy_score=0.7
    )

def test_single_payer(payer_name: str, max_files: int = 2, max_records: int = 1000):
    """Test a single payer with limited data."""
    
    print(f"=== TESTING PAYER: {payer_name} ===")
    print(f"Max files: {max_files}")
    print(f"Max records per file: {max_records}")
    print(f"Batch size: 100")
    print(f"Workers: 1")
    print("=" * 50)
    
    try:
        # Create test configuration
        config = create_test_config(payer_name, max_files, max_records)
        
        # Initialize pipeline
        pipeline = ProductionETLPipeline(config)
        
        # Process the payer
        pipeline.process_all_payers()
        
        # Print results
        print("\n=== TEST RESULTS ===")
        print(f"Output directory: {config.local_output_dir}")
        print(f"Files processed: {pipeline.stats['files_processed']}")
        print(f"Files succeeded: {pipeline.stats['files_succeeded']}")
        print(f"Files failed: {pipeline.stats['files_failed']}")
        print(f"Records extracted: {pipeline.stats['records_extracted']:,}")
        print(f"Records validated: {pipeline.stats['records_validated']:,}")
        
        if pipeline.stats['errors']:
            print(f"\nErrors encountered:")
            for error in pipeline.stats['errors']:
                print(f"  - {error}")
        
        # Check output files
        output_dir = Path(config.local_output_dir)
        if output_dir.exists():
            print(f"\nOutput files created:")
            for subdir in ['rates', 'organizations', 'providers', 'analytics']:
                subdir_path = output_dir / subdir
                if subdir_path.exists():
                    files = list(subdir_path.glob("*.parquet"))
                    print(f"  {subdir}: {len(files)} files")
                    for file in files:
                        print(f"    - {file.name}")
        
        print(f"\n✅ Test completed successfully!")
        print(f"📁 Check output in: {config.local_output_dir}")
        
    except Exception as e:
        print(f"\n❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    """Main entry point for local testing."""
    
    # Available payers from config
    with open('production_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    available_payers = list(config.get('payer_endpoints', {}).keys())
    
    print("Available payers for testing:")
    for i, payer in enumerate(available_payers, 1):
        print(f"  {i}. {payer}")
    
    print("\nUsage examples:")
    print("  python test_local_small.py bcbs_fl")
    print("  python test_local_small.py bcbs_fl --max-files 1 --max-records 500")
    
    if len(sys.argv) < 2:
        print(f"\nPlease specify a payer name. Example:")
        print(f"  python test_local_small.py {available_payers[0]}")
        return
    
    payer_name = sys.argv[1]
    
    # Parse optional arguments
    max_files = 2
    max_records = 1000
    
    for i, arg in enumerate(sys.argv[2:], 2):
        if arg == "--max-files" and i + 1 < len(sys.argv):
            max_files = int(sys.argv[i + 1])
        elif arg == "--max-records" and i + 1 < len(sys.argv):
            max_records = int(sys.argv[i + 1])
    
    test_single_payer(payer_name, max_files, max_records)

if __name__ == "__main__":
    main() 