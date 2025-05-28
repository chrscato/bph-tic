import argparse
import os
import yaml
from pathlib import Path
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs
from tic_mrf_scraper.stream.parser import stream_parse
from tic_mrf_scraper.transform.normalize import normalize_record
from tic_mrf_scraper.write.parquet_writer import ParquetWriter
from tic_mrf_scraper.write.s3_uploader import upload_to_s3
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

# Load .env if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = get_logger(__name__)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--output", default="output", help="Output directory")
    parser.add_argument("--upload", action="store_true", help="Upload to S3")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    args = parser.parse_args()

    # Load config
    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)
    
    setup_logging(cfg["logging"]["level"])
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    # Convert CPT whitelist to set
    cpt_whitelist = set(cfg["cpt_whitelist"])
    
    # Process each endpoint
    for payer_name, index_url in cfg["endpoints"].items():
        logger.info("processing_payer", payer=payer_name, url=index_url)
        try:
            # Get blob URLs
            blob_urls = list_mrf_blobs(index_url)
            for blob_url in blob_urls:
                # Create local path
                filename = os.path.basename(blob_url).replace('.json.gz', '.parquet')
                local_path = os.path.join(args.output, f"{payer_name}_{filename}")
                # Process blob
                writer = ParquetWriter(local_path)
                record_count = 0
                try:
                    for raw_record in stream_parse(blob_url):
                        normalized = normalize_record(raw_record, cpt_whitelist, payer_name)
                        if normalized:
                            writer.write(normalized)
                            record_count += 1
                    writer.close()
                    logger.info("processed_blob", blob=blob_url, records=record_count, output=local_path)
                    # Upload to S3 if requested
                    if args.upload and not args.dry_run:
                        upload_to_s3(local_path)
                except Exception as e:
                    logger.error("blob_processing_failed", blob=blob_url, error=str(e))
        except Exception as e:
            logger.error("payer_processing_failed", payer=payer_name, error=str(e))

if __name__ == "__main__":
    main()
