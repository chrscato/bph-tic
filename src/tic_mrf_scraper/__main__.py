"""Enhanced main module with proper TiC MRF structure handling."""

import argparse
import os
import yaml
from pathlib import Path
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced, analyze_index_structure
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
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

def analyze_endpoint(index_url: str, payer_name: str):
    """Analyze an endpoint structure before processing."""
    logger.info("analyzing_endpoint", payer=payer_name, url=index_url)
    
    # Analyze index structure
    index_analysis = analyze_index_structure(index_url)
    logger.info("index_analysis", payer=payer_name, analysis=index_analysis)
    
    # Get detailed MRF information
    try:
        mrfs = list_mrf_blobs_enhanced(index_url)
        logger.info("found_mrfs", payer=payer_name, count=len(mrfs))
        
        for i, mrf in enumerate(mrfs[:3]):  # Sample first 3
            logger.info("mrf_sample", 
                       index=i,
                       type=mrf["type"],
                       plan_name=mrf["plan_name"],
                       url=mrf["url"][:100] + "..." if len(mrf["url"]) > 100 else mrf["url"])
                       
    except Exception as e:
        logger.error("mrf_analysis_failed", payer=payer_name, error=str(e))

def process_mrf_file(mrf_info: dict, 
                    cpt_whitelist: set, 
                    payer_name: str, 
                    output_dir: str,
                    args) -> dict:
    """Process a single MRF file with enhanced parsing.
    
    Returns:
        Processing statistics
    """
    stats = {
        "url": mrf_info["url"],
        "type": mrf_info["type"],
        "plan_name": mrf_info["plan_name"],
        "records_processed": 0,
        "records_written": 0,
        "status": "started",
        "error": None
    }
    
    try:
        # Create output filename
        plan_safe_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in mrf_info["plan_name"])
        filename_base = f"{payer_name}_{plan_safe_name}_{mrf_info['type']}"
        if mrf_info["plan_id"]:
            filename_base += f"_{mrf_info['plan_id']}"
        
        local_path = os.path.join(output_dir, f"{filename_base}.parquet")
        
        # Initialize writer
        writer = ParquetWriter(local_path)
        
        logger.info("processing_mrf_file", 
                   url=mrf_info["url"][:100] + "..." if len(mrf_info["url"]) > 100 else mrf_info["url"],
                   type=mrf_info["type"],
                   plan=mrf_info["plan_name"])
        
        # Process with enhanced parser
        provider_ref_url = mrf_info.get("provider_reference_url")
        
        for raw_record in stream_parse_enhanced(
            mrf_info["url"], 
            payer_name,
            provider_ref_url
        ):
            stats["records_processed"] += 1
            
            # Normalize the record
            normalized = normalize_tic_record(raw_record, cpt_whitelist, payer_name)
            if normalized:
                writer.write(normalized)
                stats["records_written"] += 1
                
            # Log progress for large files
            if stats["records_processed"] % 10000 == 0:
                logger.info("processing_progress",
                           processed=stats["records_processed"],
                           written=stats["records_written"],
                           plan=mrf_info["plan_name"])
        
        writer.close()
        stats["status"] = "completed"
        
        logger.info("completed_mrf_processing", 
                   plan=mrf_info["plan_name"],
                   processed=stats["records_processed"],
                   written=stats["records_written"],
                   output=local_path)
        
        # Upload to S3 if requested and we have data
        if args.upload and not args.dry_run and stats["records_written"] > 0:
            try:
                upload_to_s3(local_path)
                logger.info("uploaded_to_s3", file=local_path)
            except Exception as e:
                logger.error("s3_upload_failed", file=local_path, error=str(e))
        
        return stats
        
    except Exception as e:
        stats["status"] = "failed"
        stats["error"] = str(e)
        logger.error("mrf_processing_failed", 
                    url=mrf_info["url"][:100] + "...",
                    plan=mrf_info["plan_name"],
                    error=str(e))
        return stats

def main():
    parser = argparse.ArgumentParser(description="Enhanced TiC MRF Scraper")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--output", default="output", help="Output directory")
    parser.add_argument("--upload", action="store_true", help="Upload to S3")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    parser.add_argument("--analyze-only", action="store_true", help="Only analyze endpoints, don't process")
    parser.add_argument("--max-files", type=int, help="Maximum number of files to process per payer")
    parser.add_argument("--file-types", nargs="+", 
                       choices=["in_network_rates", "allowed_amounts", "provider_references"],
                       default=["in_network_rates"],
                       help="Types of MRF files to process")
    args = parser.parse_args()

    # Load config
    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)
    
    setup_logging(cfg["logging"]["level"])
    logger.info("starting_enhanced_scraper", args=vars(args))
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    # Convert CPT whitelist to set
    cpt_whitelist = set(cfg["cpt_whitelist"])
    logger.info("loaded_cpt_whitelist", count=len(cpt_whitelist))
    
    # Overall statistics
    overall_stats = {
        "payers_processed": 0,
        "files_processed": 0,
        "files_succeeded": 0,
        "files_failed": 0,
        "total_records_written": 0
    }
    
    # Process each endpoint
    for payer_name, index_url in cfg["endpoints"].items():
        logger.info("processing_payer", payer=payer_name, url=index_url)
        overall_stats["payers_processed"] += 1
        
        try:
            # Analyze endpoint structure first
            if args.analyze_only:
                analyze_endpoint(index_url, payer_name)
                continue
            
            # Get MRF files with metadata
            mrfs = list_mrf_blobs_enhanced(index_url)
            
            # Filter by file types
            filtered_mrfs = [mrf for mrf in mrfs if mrf["type"] in args.file_types]
            
            # Limit number of files if specified
            if args.max_files:
                filtered_mrfs = filtered_mrfs[:args.max_files]
            
            logger.info("found_filtered_mrfs", 
                       payer=payer_name,
                       total=len(mrfs),
                       filtered=len(filtered_mrfs),
                       types=args.file_types)
            
            # Process each MRF file
            for mrf_info in filtered_mrfs:
                overall_stats["files_processed"] += 1
                
                # Skip non-rate files for now unless specifically requested
                if mrf_info["type"] not in args.file_types:
                    continue
                
                file_stats = process_mrf_file(
                    mrf_info, 
                    cpt_whitelist, 
                    payer_name, 
                    args.output,
                    args
                )
                
                if file_stats["status"] == "completed":
                    overall_stats["files_succeeded"] += 1
                    overall_stats["total_records_written"] += file_stats["records_written"]
                else:
                    overall_stats["files_failed"] += 1
                    
        except Exception as e:
            logger.error("payer_processing_failed", payer=payer_name, error=str(e))
            overall_stats["files_failed"] += 1
    
    # Log final statistics
    logger.info("scraping_completed", stats=overall_stats)
    
    if not args.analyze_only:
        print(f"""
Enhanced TiC MRF Scraper Results:
=================================
Payers processed: {overall_stats['payers_processed']}
Files processed: {overall_stats['files_processed']}
Files succeeded: {overall_stats['files_succeeded']}
Files failed: {overall_stats['files_failed']}
Total records written: {overall_stats['total_records_written']}
Output directory: {args.output}
""")

if __name__ == "__main__":
    main()
