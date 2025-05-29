#!/usr/bin/env python3
"""Process a single Centene MRF file to Parquet format."""

import os
import time
from pathlib import Path
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.write.parquet_writer import ParquetWriter
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger

def process_single_file():
    """Process the Centene file with your target CPT codes."""
    
    # Setup logging
    setup_logging("INFO")
    logger = get_logger(__name__)
    
    # Configuration
    url = "http://centene.com/content/dam/centene/Centene%20Corporate/json/DOCUMENT/2025-04-29_centene-management-company-llc_fidelis-ex_in-network.json"
    
    # YOUR TARGET CPT/HCPCS CODES - EDIT THIS LIST
    cpt_whitelist = {
        # Codes that definitely work (from your debug)
        "0240U",  # NFCT DS RNA 3 TARGETS 
        "0241U",  # NFCT DS RNA 4 TARGETS
        
        # Additional codes found in the data
        "10005",  # FINE NEEDLE ASPIRATION BX
        "10006",  # FINE NEEDLE ASPIRATION BX EA ADDL
        "10040",  # ACNE SURGERY
        "70450",  # CT HEAD/BRAIN
        "72148",  # MRI SPINAL
        "99213",  # OFFICE VISIT
        "99214",  # OFFICE VISIT
        "99215",  # OFFICE VISIT
        
        # Add any other codes you're interested in here
    }
    
    # Output setup
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    output_file = output_dir / "centene_fidelis_rates.parquet"
    
    # Initialize writer
    writer = ParquetWriter(str(output_file), batch_size=1000)
    
    # Processing stats
    stats = {
        "records_processed": 0,
        "records_written": 0,
        "unique_codes_found": set(),
        "rates_by_code": {},
        "start_time": time.time()
    }
    
    logger.info("Starting single file processing...")
    logger.info(f"URL: {url}")
    logger.info(f"Target codes: {sorted(cpt_whitelist)}")
    logger.info(f"Output: {output_file}")
    
    try:
        # Stream and process records
        for raw_record in stream_parse_enhanced(url, payer="centene_fidelis"):
            stats["records_processed"] += 1
            
            # Track what codes we're seeing
            billing_code = raw_record.get("billing_code")
            if billing_code:
                stats["unique_codes_found"].add(billing_code)
            
            # Normalize the record
            normalized = normalize_tic_record(raw_record, cpt_whitelist, "centene_fidelis")
            if normalized:
                writer.write(normalized)
                stats["records_written"] += 1
                
                # Track rates by code
                code = normalized["service_code"]
                rate = normalized["negotiated_rate"]
                if code not in stats["rates_by_code"]:
                    stats["rates_by_code"][code] = []
                stats["rates_by_code"][code].append(rate)
                
                # Log progress for successful writes
                if stats["records_written"] % 100 == 0:
                    logger.info(f"Written {stats['records_written']} records...")
            
            # Log progress for processing
            if stats["records_processed"] % 10000 == 0:
                elapsed = time.time() - stats["start_time"]
                logger.info(f"Processed {stats['records_processed']} records in {elapsed:.1f}s")
                logger.info(f"Found codes: {sorted(list(stats['unique_codes_found']))[:10]}...")
        
        # Close writer
        writer.close()
        
        # Final statistics
        elapsed = time.time() - stats["start_time"]
        logger.info("Processing complete!")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info(f"Records processed: {stats['records_processed']:,}")
        logger.info(f"Records written: {stats['records_written']:,}")
        logger.info(f"Unique codes seen: {len(stats['unique_codes_found']):,}")
        
        # Rate summary by code
        if stats["rates_by_code"]:
            logger.info("\nRate summary by code:")
            for code, rates in stats["rates_by_code"].items():
                min_rate = min(rates)
                max_rate = max(rates)
                avg_rate = sum(rates) / len(rates)
                logger.info(f"  {code}: {len(rates)} rates, ${min_rate:.2f}-${max_rate:.2f} (avg: ${avg_rate:.2f})")
        
        # File info
        if output_file.exists():
            file_size = output_file.stat().st_size / 1024 / 1024  # MB
            logger.info(f"\nOutput file: {output_file}")
            logger.info(f"File size: {file_size:.1f} MB")
            
            # Quick validation
            import pandas as pd
            df = pd.read_parquet(output_file)
            logger.info(f"Parquet validation: {len(df)} rows, {len(df.columns)} columns")
            logger.info(f"Service codes in output: {sorted(df['service_code'].unique())}")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        writer.close()  # Ensure file is closed even on error

if __name__ == "__main__":
    print("🚀 Processing single Centene MRF file...")
    print("📋 Edit the cpt_whitelist in this script to target specific codes")
    print("⏱️  This may take 2-5 minutes for the full file...")
    print()
    
    process_single_file()
    
    print("\n✅ Done! Check the output/ directory for your Parquet file.")