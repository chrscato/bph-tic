#!/usr/bin/env python3
"""
Cleanup script for temporary files left by the memory-efficient fact table creation process.
"""

import shutil
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def cleanup_temp_files():
    """Clean up temporary files from the fact table creation process."""
    
    # Path to the temp_chunks directory
    temp_dir = Path("dashboard_data/temp_chunks")
    
    if not temp_dir.exists():
        logger.info("No temporary directory found to clean up.")
        return
    
    try:
        # Remove the entire temp_chunks directory and all its contents
        shutil.rmtree(temp_dir)
        logger.info(f"Successfully cleaned up temporary directory: {temp_dir}")
    except Exception as e:
        logger.error(f"Failed to clean up temporary directory {temp_dir}: {e}")
        logger.info("You may need to manually delete the files or close any applications that might be using them.")

if __name__ == "__main__":
    cleanup_temp_files() 