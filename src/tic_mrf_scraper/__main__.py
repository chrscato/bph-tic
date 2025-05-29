"""Enhanced main module for processing complete index files with S3 upload."""

import argparse
import os
import yaml
from pathlib import Path
from datetime import datetime
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced, analyze_index_structure
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.write.parquet_writer import ParquetWriter
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
        
        # Analyze file types
        file_types = {}
        for mrf in mrfs:
            mrf_type = mrf["type"]
            file_types[mrf_type] = file_types.get(mrf_type, 0) + 1
        
        logger.info("mrf_file_types", payer=payer_name, types=file_types)
        
        # Sample first few files
        for i, mrf in enumerate(mrfs[:5]):
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
                    s3_bucket: str = None,
                    s3_prefix: str = None,
                    args=None) -> dict:
    """Process a single MRF file with enhanced parsing and direct S3 upload.
    
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
        "error": None,
        "start_time": datetime.now(),
        "s3_uploads": 0
    }
    
    try:
        # Create output filename for S3
        plan_safe_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in mrf_info["plan_name"])
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename_base = f"{payer_name}_{plan_safe_name}_{mrf_info['type']}_{timestamp}"
        if mrf_info["plan_id"]:
            filename_base += f"_{mrf_info['plan_id']}"
        
        # For S3: use a more organized path structure
        if s3_bucket:
            s3_file_prefix = f"{s3_prefix}/payer={payer_name}/type={mrf_info['type']}/date={datetime.now().strftime('%Y-%m-%d')}/{filename_base}"
        else:
            s3_file_prefix = None
        
        # Initialize writer with S3 configuration
        local_path = f"temp_{filename_base}.parquet"  # Temp filename
        writer = ParquetWriter(
            local_path, 
            batch_size=5000,  # Larger batches for better S3 efficiency
            s3_bucket=s3_bucket,
            s3_prefix=s3_file_prefix
        )
        
        logger.info("processing_mrf_file", 
                   url=mrf_info["url"][:100] + "..." if len(mrf_info["url"]) > 100 else mrf_info["url"],
                   type=mrf_info["type"],
                   plan=mrf_info["plan_name"],
                   s3_bucket=s3_bucket,
                   s3_prefix=s3_file_prefix)
        
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
            if stats["records_processed"] % 50000 == 0:
                logger.info("processing_progress",
                           processed=stats["records_processed"],
                           written=stats["records_written"],
                           plan=mrf_info["plan_name"],
                           progress_pct=f"{(stats['records_written']/max(stats['records_processed'], 1)*100):.1f}%")
        
        # Close writer (this will upload final batch to S3)
        writer.close()
        stats["status"] = "completed"
        stats["end_time"] = datetime.now()
        stats["processing_time_seconds"] = (stats["end_time"] - stats["start_time"]).total_seconds()
        
        logger.info("completed_mrf_processing", 
                   plan=mrf_info["plan_name"],
                   processed=stats["records_processed"],
                   written=stats["records_written"],
                   processing_time=f"{stats['processing_time_seconds']:.1f}s",
                   records_per_second=f"{stats['records_written']/max(stats['processing_time_seconds'], 1):.1f}",
                   s3_bucket=s3_bucket)
        
        return stats
        
    except Exception as e:
        stats["status"] = "failed"
        stats["error"] = str(e)
        stats["end_time"] = datetime.now()
        logger.error("mrf_processing_failed", 
                    url=mrf_info["url"][:100] + "...",
                    plan=mrf_info["plan_name"],
                    error=str(e))
        return stats

def main():
    parser = argparse.ArgumentParser(description="Enhanced TiC MRF Scraper - Full Index Processing")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--s3-bucket", help="S3 bucket name (overrides config)")
    parser.add_argument("--s3-prefix", default="healthcare-rates", help="S3 prefix/folder")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode - analyze only")
    parser.add_argument("--analyze-only", action="store_true", help="Only analyze endpoints, don't process")
    parser.add_argument("--file-types", nargs="+", 
                       choices=["in_network_rates", "allowed_amounts", "provider_references"],
                       default=["in_network_rates"],
                       help="Types of MRF files to process")
    parser.add_argument("--skip-failed", action="store_true", 
                       help="Continue processing other files if one fails")
    args = parser.parse_args()

    # Load config
    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)
    
    setup_logging(cfg["logging"]["level"])
    
    # S3 configuration
    s3_bucket = args.s3_bucket or os.getenv('S3_BUCKET')
    s3_prefix = args.s3_prefix
    
    if s3_bucket:
        logger.info("s3_upload_enabled", bucket=s3_bucket, prefix=s3_prefix)
    else:
        logger.info("s3_upload_disabled", reason="no_bucket_configured")
    
    logger.info("starting_enhanced_scraper", 
               args=vars(args),
               s3_enabled=bool(s3_bucket))
    
    # Convert CPT whitelist to set
    cpt_whitelist = set(cfg["cpt_whitelist"])
    logger.info("loaded_cpt_whitelist", count=len(cpt_whitelist), codes=sorted(list(cpt_whitelist)))
    
    # Overall statistics
    overall_stats = {
        "payers_processed": 0,
        "total_files_found": 0,
        "files_processed": 0,
        "files_succeeded": 0,
        "files_failed": 0,
        "total_records_written": 0,
        "failed_files": [],
        "start_time": datetime.now()
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
            
            # Get ALL MRF files from index (no limits!)
            logger.info("fetching_full_index", payer=payer_name)
            mrfs = list_mrf_blobs_enhanced(index_url)
            
            # Filter by file types
            filtered_mrfs = [mrf for mrf in mrfs if mrf["type"] in args.file_types]
            overall_stats["total_files_found"] += len(filtered_mrfs)
            
            logger.info("found_mrf_files", 
                       payer=payer_name,
                       total_in_index=len(mrfs),
                       filtered_count=len(filtered_mrfs),
                       types_processing=args.file_types)
            
            # Process ALL filtered files (this is the key change!)
            payer_success_count = 0
            payer_fail_count = 0
            
            for i, mrf_info in enumerate(filtered_mrfs, 1):
                overall_stats["files_processed"] += 1
                
                logger.info("starting_file_processing",
                           payer=payer_name,
                           file_number=f"{i}/{len(filtered_mrfs)}",
                           plan=mrf_info["plan_name"],
                           type=mrf_info["type"])
                
                file_stats = process_mrf_file(
                    mrf_info, 
                    cpt_whitelist, 
                    payer_name,
                    s3_bucket,
                    s3_prefix,
                    args
                )
                
                if file_stats["status"] == "completed":
                    overall_stats["files_succeeded"] += 1
                    overall_stats["total_records_written"] += file_stats["records_written"]
                    payer_success_count += 1
                    
                    logger.info("file_completed_successfully",
                               payer=payer_name,
                               file_number=f"{i}/{len(filtered_mrfs)}",
                               records_written=file_stats["records_written"],
                               processing_time=f"{file_stats.get('processing_time_seconds', 0):.1f}s")
                else:
                    overall_stats["files_failed"] += 1
                    overall_stats["failed_files"].append({
                        "payer": payer_name,
                        "plan": mrf_info["plan_name"],
                        "url": mrf_info["url"],
                        "error": file_stats["error"]
                    })
                    payer_fail_count += 1
                    
                    logger.error("file_processing_failed",
                                payer=payer_name,
                                file_number=f"{i}/{len(filtered_mrfs)}",
                                plan=mrf_info["plan_name"],
                                error=file_stats["error"])
                    
                    # Stop processing this payer if skip_failed is False
                    if not args.skip_failed:
                        logger.error("stopping_payer_processing_due_to_failure", payer=payer_name)
                        break
            
            logger.info("completed_payer_processing",
                       payer=payer_name,
                       files_attempted=len(filtered_mrfs),
                       files_succeeded=payer_success_count,
                       files_failed=payer_fail_count)
                    
        except Exception as e:
            logger.error("payer_processing_failed", payer=payer_name, error=str(e))
            overall_stats["failed_files"].append({
                "payer": payer_name,
                "plan": "INDEX_PROCESSING",
                "url": index_url,
                "error": str(e)
            })
    
    # Calculate final timing
    overall_stats["end_time"] = datetime.now()
    overall_stats["total_processing_time"] = (overall_stats["end_time"] - overall_stats["start_time"]).total_seconds()
    
    # Log final statistics
    logger.info("scraping_completed", stats=overall_stats)
    
    if not args.analyze_only:
        print(f"""
🎉 Enhanced TiC MRF Scraper Results:
===================================
Payers processed: {overall_stats['payers_processed']}
Total files found: {overall_stats['total_files_found']}
Files processed: {overall_stats['files_processed']}
Files succeeded: {overall_stats['files_succeeded']}
Files failed: {overall_stats['files_failed']}
Total records written: {overall_stats['total_records_written']:,}
Total processing time: {overall_stats['total_processing_time']/60:.1f} minutes
S3 bucket: {s3_bucket or 'Not configured'}
S3 prefix: {s3_prefix}

{f"❌ Failed files: {len(overall_stats['failed_files'])}" if overall_stats['failed_files'] else "✅ All files processed successfully!"}
""")
        
        # Show failed files if any
        if overall_stats['failed_files']:
            print("Failed Files:")
            for failed in overall_stats['failed_files'][:10]:  # Show first 10
                print(f"  - {failed['payer']}: {failed['plan']} - {failed['error'][:100]}")
            if len(overall_stats['failed_files']) > 10:
                print(f"  ... and {len(overall_stats['failed_files']) - 10} more")

if __name__ == "__main__":
    main()