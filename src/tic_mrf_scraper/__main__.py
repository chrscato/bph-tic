import argparse
import os
import yaml
from pathlib import Path
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs
from tic_mrf_scraper.stream.parser import stream_parse
from tic_mrf_scraper.transform.normalize import normalize_record
from tic_mrf_scraper.write.parquet_writer import ParquetWriter
from tic_mrf_scraper.write.s3_uploader import upload_to_s3
from tic_mrf_scraper.utils.backoff_logger import setup_logging

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Write only locally")
    args = parser.parse_args()

    # Get the project root directory (2 levels up from this file)
    root_dir = Path(__file__).parent.parent.parent
    config_path = root_dir / args.config
    
    cfg = yaml.safe_load(open(config_path))
    setup_logging(cfg["logging"]["level"])

    # Get S3 configuration from environment or config
    s3_bucket = os.getenv('S3_BUCKET') or cfg["aws"]["s3_bucket"]
    s3_prefix = os.getenv('S3_PREFIX') or cfg["aws"]["s3_prefix"]

    for payer, idx_url in cfg["endpoints"].items():
        for blob_url in list_mrf_blobs(idx_url):
            local_path = ParquetWriter.local_path(blob_url, cfg["cpt_whitelist"])
            writer = ParquetWriter(local_path)
            for raw in stream_parse(blob_url):
                rec = normalize_record(raw, cfg["cpt_whitelist"], payer)
                if rec:
                    writer.write(rec)
            writer.close()

            if not args.dry_run:
                upload_to_s3(local_path, s3_bucket, s3_prefix)

if __name__ == "__main__":
    main()
