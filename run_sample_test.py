#!/usr/bin/env python3
"""Run sample ETL test with conservative settings."""

import os
import sys
import shutil
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def run_sample_test():
    """Run a conservative sample test of the ETL pipeline."""
    
    print("🧪 Running Sample ETL Test")
    print("=" * 50)
    print("Configuration:")
    print("  - Aetna Florida only")
    print("  - 5 CPT codes only")
    print("  - Max 2 files")
    print("  - Max 10K records per file")
    print("  - 1000-record batches")
    print("  - Single worker")
    print("  - Output: sample_data/")
    print("=" * 50)
    
    # Backup current configs
    if os.path.exists("production_config.yaml"):
        shutil.copy("production_config.yaml", "production_config_backup.yaml")
        print("✅ Backed up production_config.yaml")
    
    if os.path.exists("config.yaml"):
        shutil.copy("config.yaml", "config_backup.yaml")
        print("✅ Backed up config.yaml")
    
    # Use sample config for both files
    shutil.copy("production_config_sample.yaml", "production_config.yaml")
    shutil.copy("production_config_sample.yaml", "config.yaml")
    print("✅ Using sample configuration for both config files")
    
    # Create sample data directory
    os.makedirs("sample_data", exist_ok=True)
    print("✅ Created sample_data directory")
    
    # Run the pipeline
    print("\n🚀 Starting sample ETL pipeline...")
    try:
        from tic_mrf_scraper.__main__ import main
        main()
        print("\n✅ Sample ETL completed successfully!")
        
        # Show results
        print("\n📊 Sample Results:")
        print("=" * 30)
        
        sample_data_dir = Path("sample_data")
        if sample_data_dir.exists():
            for subdir in ["rates", "providers", "organizations", "payers"]:
                subdir_path = sample_data_dir / subdir
                if subdir_path.exists():
                    parquet_files = list(subdir_path.glob("*.parquet"))
                    if parquet_files:
                        print(f"  {subdir}: {len(parquet_files)} files")
                    else:
                        print(f"  {subdir}: No data")
                else:
                    print(f"  {subdir}: No directory")
        
        # Quick data check
        rates_file = sample_data_dir / "rates" / "rates_final.parquet"
        if rates_file.exists():
            import pandas as pd
            df = pd.read_parquet(rates_file)
            print(f"\n📈 Sample Data Summary:")
            print(f"  Total records: {len(df)}")
            print(f"  CPT codes found: {df['billing_code'].nunique()}")
            print(f"  Sample CPT codes: {sorted(df['billing_code'].unique()[:5])}")
        
    except Exception as e:
        print(f"\n❌ Sample ETL failed: {e}")
        return False
    
    finally:
        # Restore original configs
        if os.path.exists("production_config_backup.yaml"):
            shutil.copy("production_config_backup.yaml", "production_config.yaml")
            print("\n✅ Restored production_config.yaml")
        
        if os.path.exists("config_backup.yaml"):
            shutil.copy("config_backup.yaml", "config.yaml")
            print("✅ Restored config.yaml")
    
    print("\n🎉 Sample test complete!")
    print("Check 'sample_data/' directory for results")
    return True

if __name__ == "__main__":
    run_sample_test() 