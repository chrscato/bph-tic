# NPPES Provider Information Backfill Guide

This guide explains how to use the updated `backfill_provider_info.py` script to fetch provider information from the NPI Registry API.

## Overview

The script can work in two modes:
1. **Local Mode**: Extract NPIs from local provider files
2. **S3 Mode**: Extract NPIs from S3 provider files with partitioned structure

## Prerequisites

### For Local Mode
- Provider files in the expected directory structure:
  ```
  ortho_radiology_data_bcbs_il/providers/*.parquet
  ortho_radiology_data_bcbs_la/providers/*.parquet
  ortho_radiology_data_centene_fidelis/providers/*.parquet
  ```

### For S3 Mode
- AWS credentials configured
- S3_BUCKET environment variable set
- Provider files in S3 with partitioned structure:
  ```
  s3://{bucket}/{prefix}/providers/payer=bcbs_il/date=2025-07-29/*.parquet
  s3://{bucket}/{prefix}/providers/payer=bcbs_la/date=2025-07-29/*.parquet
  ```

## Environment Variables

Set these in your `.env` file or environment:

```bash
# Required for S3 mode
S3_BUCKET=your-bucket-name

# Optional
S3_PREFIX=your-prefix-path
LOCAL_DATA_DIR=.
LOCAL_BASE_PATTERN=ortho_radiology_data_
NPPES_DATA_DIR=nppes_data
BATCH_SIZE=100
MAX_WORKERS=5
REQUEST_DELAY=0.1
MAX_RETRIES=3
```

## Usage Examples

### Local Mode (Default)

```bash
# Basic run with local data
python scripts/backfill_provider_info.py --limit 500

# Test with small sample
python scripts/backfill_provider_info.py --limit 50

# Custom local directory
python scripts/backfill_provider_info.py --local-data-dir /path/to/data --limit 100
```

### S3 Mode

```bash
# Using S3_BUCKET from environment
python scripts/backfill_provider_info.py --s3-prefix my-data/providers --limit 500

# Custom bucket and prefix
python scripts/backfill_provider_info.py --s3-bucket my-bucket --s3-prefix my-data/providers --limit 500

# Test with small sample
python scripts/backfill_provider_info.py --s3-prefix test-data/providers --limit 50
```

### Advanced Options

```bash
# Custom API settings
python scripts/backfill_provider_info.py --s3-prefix production/providers \
  --limit 1000 \
  --request-delay 0.2 \
  --max-retries 5 \
  --batch-size 200 \
  --max-workers 10
```

## Output

The script will:
1. Extract unique NPIs from provider files
2. Fetch provider information from NPI Registry API
3. Store results in `nppes_data/nppes_providers.parquet`
4. Generate statistics in `nppes_data/nppes_statistics.json`
5. Log failed NPIs in `nppes_data/failed_npis.json`

## S3 Structure Expected

The script expects S3 files to be organized as:
```
s3://{bucket}/{prefix}/
├── providers/
│   ├── payer=bcbs_il/
│   │   └── date=2025-07-29/
│   │       ├── file1.parquet
│   │       └── file2.parquet
│   ├── payer=bcbs_la/
│   │   └── date=2025-07-29/
│   │       ├── file1.parquet
│   │       └── file2.parquet
│   └── payer=centene_fidelis/
│       └── date=2025-07-29/
│           ├── file1.parquet
│           └── file2.parquet
```

## Troubleshooting

### Common Issues

1. **S3_BUCKET not set**: Set the environment variable or use `--s3-bucket`
2. **No provider files found**: Check the S3 prefix or local directory structure
3. **API rate limiting**: Increase `--request-delay` to be more respectful
4. **Memory issues**: Reduce `--batch-size` or `--max-workers`

### Testing Configuration

Run the test script to verify your setup:
```bash
python test_backfill_config.py
```

## Data Quality

The script includes quality controls:
- Minimum success rate threshold (95% by default)
- Failed NPI logging
- Comprehensive error handling
- Progress tracking with tqdm

## Performance Tips

- Start with small limits (`--limit 50`) for testing
- Use appropriate request delays to avoid API rate limiting
- Monitor memory usage with large datasets
- Consider running during off-peak hours for large datasets 