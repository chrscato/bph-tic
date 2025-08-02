#!/usr/bin/env python3
"""Test memory management with small data subset."""

import os
import sys
import yaml
import logging
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent))

from production_etl_pipeline import ProductionETLPipeline, ETLConfig, log_memory_usage, check_memory_pressure, force_memory_cleanup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_test_config():
    """Create a test configuration with very conservative settings."""
    
    # Read the production config
    with open('production_config_comprehensive.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Create test config with very conservative settings
    test_config = ETLConfig(
        payer_endpoints={
            # Test with just one payer
            'bcbs_il': config['endpoints']['bcbs_il']
        },
        cpt_whitelist=config['cpt_whitelist'][:10],  # Only first 10 CPT codes
        batch_size=100,  # Very small batch size
        parallel_workers=1,
        max_files_per_payer=1,  # Only process 1 file
        max_records_per_file=1000,  # Only process 1000 records
        safety_limit_records_per_file=1000,  # Very conservative limit
        local_output_dir="test_memory_output",
        s3_bucket=None,  # Use local storage for testing
        s3_prefix="test",
        schema_version="v2.1.0",
        processing_version="test-memory-v1.0",
        min_completeness_pct=30.0,
        min_accuracy_score=0.5
    )
    
    return test_config

def test_memory_management():
    """Test memory management with conservative settings."""
    
    logger.info("Starting memory management test...")
    
    # Log initial memory
    log_memory_usage("test_start")
    
    try:
        # Create test configuration
        config = create_test_config()
        
        # Initialize pipeline
        pipeline = ProductionETLPipeline(config)
        
        # Process with memory monitoring
        pipeline.process_all_payers()
        
        logger.info("Memory management test completed successfully!")
        
    except Exception as e:
        logger.error(f"Memory management test failed: {str(e)}")
        raise
    finally:
        # Log final memory
        log_memory_usage("test_end")
        
        # Check for memory pressure
        if check_memory_pressure():
            logger.warning("Memory pressure detected at end of test")
            force_memory_cleanup()

def main():
    """Main test function."""
    logger.info("Memory Management Test")
    logger.info("=" * 50)
    
    # Check initial system memory
    import psutil
    memory = psutil.virtual_memory()
    logger.info(f"System Memory: {memory.total / 1024 / 1024:.1f}MB total")
    logger.info(f"Available Memory: {memory.available / 1024 / 1024:.1f}MB")
    
    # Run test
    test_memory_management()
    
    logger.info("Test completed!")

if __name__ == "__main__":
    main() 