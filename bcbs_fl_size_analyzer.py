#!/usr/bin/env python3
"""
Analyzes BCBS FL file sizes and processes the 10 smallest files.
"""

import os
import json
import requests
import structlog
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any
from dotenv import load_dotenv
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger()

def get_file_size(url: str) -> float:
    """Get file size in MB."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, application/octet-stream'
        }
        response = requests.head(url, headers=headers, timeout=30)
        size_mb = int(response.headers.get('content-length', 0)) / (1024 * 1024)
        return size_mb
    except Exception as e:
        logger.error("size_check_failed", url=url, error=str(e))
        return float('inf')  # Return infinity so it goes to the end of the sorted list

def analyze_files():
    """Analyze file sizes and return sorted list."""
    bcbs_fl_url = "https://d1hgtx7rrdl2cn.cloudfront.net/mrf/toc/FloridaBlue_Health-Insurance-Issuer_index.json"
    mrf_files = list(list_mrf_blobs_enhanced(bcbs_fl_url))
    logger.info("files_discovered", count=len(mrf_files))

    # Get sizes
    file_sizes = []
    for idx, file_info in enumerate(mrf_files):
        size_mb = get_file_size(file_info["url"])
        file_sizes.append({
            "url": file_info["url"],
            "size_mb": size_mb,
            "size_gb": size_mb / 1024,
            "original_info": file_info
        })
        logger.info("checked_file",
                   file_num=idx + 1,
                   total_files=len(mrf_files),
                   size_gb=f"{size_mb/1024:.2f}GB")

    # Sort by size
    sorted_files = sorted(file_sizes, key=lambda x: x["size_mb"])

    # Calculate distribution
    size_ranges = {
        "0-1GB": 0,
        "1-2GB": 0,
        "2-4GB": 0,
        "4-6GB": 0,
        "6GB+": 0
    }

    for file in sorted_files:
        size_gb = file["size_gb"]
        if size_gb <= 1:
            size_ranges["0-1GB"] += 1
        elif size_gb <= 2:
            size_ranges["1-2GB"] += 1
        elif size_gb <= 4:
            size_ranges["2-4GB"] += 1
        elif size_gb <= 6:
            size_ranges["4-6GB"] += 1
        else:
            size_ranges["6GB+"] += 1

    # Generate report
    report = {
        "total_files": len(sorted_files),
        "size_distribution": size_ranges,
        "smallest_10": [
            {
                "url": f["url"],
                "size_gb": f"{f['size_gb']:.2f}GB"
            }
            for f in sorted_files[:10]
        ],
        "largest_10": [
            {
                "url": f["url"],
                "size_gb": f"{f['size_gb']:.2f}GB"
            }
            for f in sorted_files[-10:]
        ],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # Save report
    report_file = Path("bcbs_fl_size_analysis.json")
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)

    logger.info("analysis_complete", 
                total_files=len(sorted_files),
                distribution=size_ranges,
                report_file=str(report_file))

    # Return the 10 smallest files with their full info
    return [f["original_info"] for f in sorted_files[:10]]

if __name__ == "__main__":
    smallest_10 = analyze_files()
    
    # Save the 10 smallest files for processing
    with open("smallest_10_files.json", 'w') as f:
        json.dump(smallest_10, f, indent=2)
    
    logger.info("saved_smallest_files", 
                count=len(smallest_10),
                output_file="smallest_10_files.json")