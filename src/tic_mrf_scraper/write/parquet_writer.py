"""Module for writing MRF records to parquet files."""

import os
from pathlib import Path
from typing import Dict, Any, List
import pyarrow as pa
import pyarrow.parquet as pq
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

class ParquetWriter:
    """Writer for MRF records to parquet files."""
    
    def __init__(self, output_path: str, batch_size: int = 1000):
        """Initialize writer.
        
        Args:
            output_path: Path to output parquet file
            batch_size: Number of records per batch
        """
        self.output_path = Path(output_path)
        self.batch_size = batch_size
        self.records: List[Dict[str, Any]] = []
        self.file_counter = 0
        
        # Create output directory if needed
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
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
        
        # Write batch if full
        if len(self.records) >= self.batch_size:
            self._write_batch()
            
    def close(self):
        """Write remaining records and close."""
        if self.records:
            self._write_batch()
            
    def _write_batch(self):
        """Write current batch to file."""
        if not self.records:
            return
            
        # Create filename for this batch
        if self.file_counter == 0:
            batch_path = self.output_path
        else:
            stem = self.output_path.stem
            suffix = self.output_path.suffix
            batch_path = self.output_path.parent / f"{stem}_{self.file_counter:04d}{suffix}"
        
        # Convert to pyarrow table and write
        table = pa.Table.from_pylist(self.records)
        pq.write_table(table, batch_path)
        
        logger.info("wrote_batch", 
                   path=str(batch_path), 
                   records=len(self.records))
        
        # Reset for next batch
        self.records = []
        self.file_counter += 1
