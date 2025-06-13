"""Module for writing MRF records to parquet files with direct S3 upload."""

import os
import boto3
import tempfile
from pathlib import Path
from typing import Dict, Any, List, Optional
import pyarrow as pa
import pyarrow.parquet as pq
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

class ParquetWriter:
    """Writer for MRF records to parquet files with direct S3 upload."""
    
    def __init__(self,
                 local_path: str,
                 batch_size: int = 1000,
                 s3_bucket: Optional[str] = None,
                 s3_prefix: Optional[str] = None):
        """Initialize writer.

        Args:
            local_path: Local path for temp files (or final path if no S3)
            batch_size: Number of records per batch
            s3_bucket: S3 bucket name (if None, uses local storage only)
            s3_prefix: S3 prefix/folder path
        """
        self.output_path = Path(local_path)
        self.batch_size = batch_size
        self.records: List[Dict[str, Any]] = []
        self.file_counter = 0
        
        # S3 configuration
        self.s3_bucket = s3_bucket or os.getenv('S3_BUCKET')
        self.s3_prefix = s3_prefix or os.getenv('S3_PREFIX', 'tic-mrf')
        self.s3_client = boto3.client('s3') if self.s3_bucket else None
        
        # Create local temp directory if using S3
        if self.s3_client:
            self.temp_dir = tempfile.mkdtemp(prefix="parquet_writer_")
            logger.info("created_temp_dir", temp_dir=self.temp_dir)
        else:
            # Create output directory if needed for local-only mode
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.temp_dir = None
        
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
        
        # Cleanup temp directory if using S3
        if self.temp_dir:
            import shutil
            shutil.rmtree(self.temp_dir, ignore_errors=True)
            logger.info("cleaned_temp_dir", temp_dir=self.temp_dir)
    
    def _write_batch(self):
        """Write current batch to file and upload to S3 if configured."""
        if not self.records:
            return
        
        # Determine file path
        if self.s3_client:
            # Use temp file for S3 upload
            filename = f"batch_{self.file_counter:04d}.parquet"
            local_path = Path(self.temp_dir) / filename
        else:
            # Use regular output path for local storage
            if self.file_counter == 0:
                local_path = self.output_path
            else:
                stem = self.output_path.stem
                suffix = self.output_path.suffix
                local_path = self.output_path.parent / f"{stem}_{self.file_counter:04d}{suffix}"
        
        # Convert to pyarrow table and write locally
        table = pa.Table.from_pylist(self.records)
        pq.write_table(table, local_path)
        
        logger.info("wrote_local_batch", 
                   path=str(local_path), 
                   records=len(self.records))
        
        # Upload to S3 if configured
        if self.s3_client:
            success = self._upload_to_s3(local_path)
            if success:
                # Delete local temp file after successful upload
                local_path.unlink()
                logger.info("deleted_temp_file", path=str(local_path))
            else:
                logger.error("keeping_temp_file_due_to_upload_failure", path=str(local_path))
        
        # Reset for next batch
        self.records = []
        self.file_counter += 1
    
    def _upload_to_s3(self, local_path: Path) -> bool:
        """Upload file to S3.
        
        Args:
            local_path: Local file path to upload
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Construct S3 key
            filename = local_path.name
            s3_key = f"{self.s3_prefix}/{filename}"
            
            # Upload file
            self.s3_client.upload_file(str(local_path), self.s3_bucket, s3_key)
            
            logger.info("uploaded_to_s3", 
                       local_path=str(local_path),
                       s3_bucket=self.s3_bucket,
                       s3_key=s3_key,
                       file_size_mb=local_path.stat().st_size / 1024 / 1024)
            
            return True
            
        except Exception as e:
            logger.error("s3_upload_failed", 
                        local_path=str(local_path),
                        s3_bucket=self.s3_bucket,
                        error=str(e))
            return False
    
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