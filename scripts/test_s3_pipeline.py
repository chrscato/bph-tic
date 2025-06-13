#!/usr/bin/env python3
"""Test the S3 pipeline with a small sample before processing full index."""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from production_etl_pipeline import ProductionETLPipeline, ETLConfig

def test_s3_pipeline():
    """Test the S3 pipeline with minimal configuration."""
    
    print("🧪 Testing S3 Pipeline Configuration")
    print("=" * 50)
    
    # Check environment variables
    s3_bucket = os.getenv('S3_BUCKET')
    s3_prefix = os.getenv('S3_PREFIX', 'tic-mrf')
    aws_region = os.getenv('AWS_DEFAULT_REGION')
    
    print(f"📋 S3 Configuration:")
    print(f"   Bucket: {s3_bucket}")
    print(f"   Prefix: {s3_prefix}")
    print(f"   Region: {aws_region}")
    print()
    
    if not s3_bucket:
        print("❌ S3_BUCKET not found in environment variables")
        print("Make sure your .env file is loaded correctly")
        return False
    
    # Test configuration - process just 1 file for testing
    config = ETLConfig(
        payer_endpoints={
            "centene_fidelis": "https://www.centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-04-29_fidelis_index.json"
        },
        cpt_whitelist=[
            "0240U", "0241U",  # Codes we know work
            "99213", "99214",  # Common office visits
            "70450", "72148",  # Imaging
        ],
        batch_size=1000,            # Small batches for testing
        parallel_workers=1,         # Single-threaded
        s3_bucket=s3_bucket,
        s3_prefix=f"{s3_prefix}/test",  # Use test subfolder
        schema_version="v2.1.0-test",
        processing_version="tic-etl-test"
    )
    
    print(f"🔧 Test Configuration:")
    print(f"   Target codes: {len(config.cpt_whitelist)}")
    print(f"   Batch size: {config.batch_size}")
    print(f"   S3 path: s3://{config.s3_bucket}/{config.s3_prefix}/")
    print()
    
    try:
        # Create pipeline
        pipeline = ProductionETLPipeline(config)
        
        # Test S3 connectivity
        if pipeline.s3_client:
            print("✅ S3 client initialized successfully")
            
            # Test bucket access
            try:
                pipeline.s3_client.head_bucket(Bucket=config.s3_bucket)
                print("✅ S3 bucket access confirmed")
            except Exception as e:
                print(f"❌ S3 bucket access failed: {e}")
                return False
        else:
            print("❌ S3 client failed to initialize")
            return False
        
        print()
        print("🚀 Starting test processing...")
        print("   This will process a small sample and upload to S3")
        print("   Check your S3 bucket for test files")
        print()
        
        # Run the pipeline
        pipeline.process_all_payers()
        
        print("✅ Test completed successfully!")
        print()
        print(f"📁 Check your S3 bucket for files:")
        print(f"   s3://{config.s3_bucket}/{config.s3_prefix}/")
        print()
        print("🎉 Ready for full production run!")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        print("\nFull error:")
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_s3_pipeline()
    
    if success:
        print()
        print("Next steps:")
        print("1. Check your S3 bucket for the test files")
        print("2. If everything looks good, run the full pipeline:")
        print("   python scripts/production_etl_pipeline.py")
        print("3. Monitor the logs for progress")
    else:
        print()
        print("Fix the issues above before running the full pipeline")
    
    sys.exit(0 if success else 1)