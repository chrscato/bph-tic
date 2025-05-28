"""Module for uploading files to S3."""

import os
import s3fs
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

def upload_to_s3(local_path: str, bucket: str = None, prefix: str = None):
    """Upload a file to S3.
    
    Args:
        local_path: Path to local file
        bucket: S3 bucket name (defaults to S3_BUCKET env var)
        prefix: S3 key prefix (defaults to S3_PREFIX env var)
    """
    # Get S3 config from env vars if not provided
    bucket = bucket or os.getenv('S3_BUCKET')
    prefix = prefix or os.getenv('S3_PREFIX', 'tic-mrf')
    
    if not bucket:
        raise ValueError("S3 bucket must be provided or set via S3_BUCKET env var")
        
    # Initialize S3 filesystem
    fs = s3fs.S3FileSystem(
        key=os.getenv('AWS_ACCESS_KEY_ID'),
        secret=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )
    
    # Construct destination path
    filename = os.path.basename(local_path)
    dest_path = f"{bucket}/{prefix}/{filename}"
    
    # Upload file
    logger.info("uploading_to_s3", local_path=local_path, dest_path=dest_path)
    fs.put(local_path, dest_path)
