"""Module for writing MRF records to parquet files."""

import os
from pathlib import Path
from typing import Dict, Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

class ParquetWriter:
    """Writer for MRF records to parquet files."""
    
    def __init__(self, output_path: str):
        """Initialize writer.
        
        Args:
            output_path: Path to output parquet file
        """
        self.output_path = output_path
        self.records = []
        
        # Create output directory if needed
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
    @staticmethod
    def local_path(blob_url: str, cpt_whitelist: list) -> str:
        """Get local path for parquet file.
        
        Args:
            blob_url: URL to MRF blob
            cpt_whitelist: List of allowed CPT codes
            
        Returns:
            Local path for parquet file
        """
        # Extract filename from URL
        filename = os.path.basename(blob_url)
        # Remove .json.gz extension
        base = os.path.splitext(os.path.splitext(filename)[0])[0]
        # Add .parquet extension
        return f"output/{base}.parquet"
        
    def write(self, record: Dict[str, Any]):
        """Write a single record.
        
        Args:
            record: Record to write
        """
        self.records.append(record)
        
    def close(self):
        """Write records to parquet file and close."""
        if not self.records:
            logger.warning("no_records", path=self.output_path)
            return
            
        # Convert to pyarrow table
        table = pa.Table.from_pylist(self.records)
        
        # Write to parquet
        logger.info("writing_parquet", path=self.output_path, count=len(self.records))
        pq.write_table(table, self.output_path)
