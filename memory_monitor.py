#!/usr/bin/env python3
"""Memory monitoring utility for ETL pipeline."""

import psutil
import time
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MemoryMonitor:
    """Monitor memory usage during ETL processing."""
    
    def __init__(self, threshold_mb=2000):
        self.threshold_mb = threshold_mb
        self.start_time = time.time()
        self.peak_memory = 0
        self.warnings = 0
        
    def get_memory_usage(self):
        """Get current memory usage in MB."""
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    
    def check_memory(self, stage=""):
        """Check current memory usage and log if above threshold."""
        memory_mb = self.get_memory_usage()
        self.peak_memory = max(self.peak_memory, memory_mb)
        
        if memory_mb > self.threshold_mb:
            self.warnings += 1
            logger.warning(f"MEMORY PRESSURE: {memory_mb:.1f}MB at {stage}")
            return True
        else:
            logger.info(f"Memory OK: {memory_mb:.1f}MB at {stage}")
            return False
    
    def get_system_memory(self):
        """Get system memory information."""
        memory = psutil.virtual_memory()
        return {
            "total_mb": memory.total / 1024 / 1024,
            "available_mb": memory.available / 1024 / 1024,
            "percent_used": memory.percent,
            "process_mb": self.get_memory_usage()
        }
    
    def log_system_status(self):
        """Log comprehensive system memory status."""
        system_mem = self.get_system_memory()
        logger.info(f"System Memory: {system_mem['total_mb']:.1f}MB total, "
                   f"{system_mem['available_mb']:.1f}MB available, "
                   f"{system_mem['percent_used']:.1f}% used")
        logger.info(f"Process Memory: {system_mem['process_mb']:.1f}MB")
        logger.info(f"Peak Memory: {self.peak_memory:.1f}MB")
        logger.info(f"Memory Warnings: {self.warnings}")

def monitor_memory_continuously(interval=30):
    """Continuously monitor memory usage."""
    monitor = MemoryMonitor()
    logger.info("Starting continuous memory monitoring...")
    
    try:
        while True:
            monitor.check_memory("continuous_monitoring")
            monitor.log_system_status()
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Memory monitoring stopped.")
        monitor.log_system_status()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "continuous":
        monitor_memory_continuously()
    else:
        # One-time memory check
        monitor = MemoryMonitor()
        monitor.check_memory("manual_check")
        monitor.log_system_status() 