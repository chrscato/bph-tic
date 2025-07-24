# TiC Scripts Guide: NPPES Backfill & Fact Table Creation

This guide explains how to use the scripts for enriching provider data with NPPES information and creating comprehensive fact tables from S3 chunked parquet files.

## 📋 Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Scripts Overview](#scripts-overview)
- [NPPES Provider Backfill](#nppes-provider-backfill)
- [Fact Table Creation](#fact-table-creation)
- [Testing & Validation](#testing--validation)
- [Advanced Usage](#advanced-usage)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

The TiC project includes two main data processing workflows:

1. **NPPES Provider Backfill**: Enriches provider data with NPPES registry information
2. **Fact Table Creation**: Creates comprehensive fact tables from chunked S3 parquet files

Both workflows support S3 integration and can handle large datasets efficiently.

## 🔧 Prerequisites

### Required Python Packages
```bash
pip install pandas boto3 requests pyyaml tqdm pyarrow
```

### AWS Configuration
Ensure you have AWS credentials configured for S3 access:
```bash
# Option 1: AWS CLI configuration
aws configure

# Option 2: Environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### S3 Bucket Access
- Access to `s3://commercial-rates/tic-mrf/test/` bucket
- Read permissions for parquet files
- Write permissions for output files

## 📁 Scripts Overview

### Core Scripts
- `backfill_provider_info.py` - NPPES provider enrichment
- `create_memory_efficient_fact_table.py` - Fact table creation
- `test_s3_connection.py` - S3 connectivity testing
- `test_fact_table_s3.py` - Fact table creation testing

### Shell Scripts
- `run_backfill.sh` - NPPES backfill runner
- `run_fact_table.sh` - Fact table creation runner

## 🏥 NPPES Provider Backfill

The NPPES backfill script enriches provider data with information from the NPI Registry API.

### Quick Start

#### Test S3 Connection
```bash
python scripts/test_s3_connection.py
```

#### Small Test (50 NPIs)
```bash
python scripts/backfill_provider_info.py --limit 50
```

#### Full Backfill
```bash
python scripts/backfill_provider_info.py
```

### Using the Shell Script

```bash
# Test S3 connection
./scripts/run_backfill.sh test

# Small test (50 NPIs)
./scripts/run_backfill.sh small

# Medium test (500 NPIs)
./scripts/run_backfill.sh medium

# Large test (1000 NPIs)
./scripts/run_backfill.sh large

# Full backfill (all NPIs)
./scripts/run_backfill.sh full

# Custom limit
./scripts/run_backfill.sh custom 250
```

### Command Line Options

```bash
python scripts/backfill_provider_info.py [OPTIONS]

Options:
  --limit INT           Limit number of NPIs to process (for testing)
  --request-delay FLOAT Delay between API requests (default: 0.1s)
  --max-retries INT     Maximum retries for failed requests (default: 3)
  --batch-size INT      Batch size for processing (default: 100)
  --max-workers INT     Maximum worker threads (default: 5)
  --s3-bucket STR       S3 bucket name (default: commercial-rates)
  --s3-prefix STR       S3 prefix/path (default: tic-mrf/test)
```

### Output Files

```
nppes_data/
├── nppes_providers.parquet          # Enriched provider data
├── nppes_statistics.json            # Summary statistics
└── failed_npis.json                 # NPIs that failed to fetch
```

## 📊 Fact Table Creation

The fact table creation script processes chunked S3 parquet files to create comprehensive fact tables.

### Quick Start

#### Test S3 Connection
```bash
python scripts/test_fact_table_s3.py
```

#### Small Test (100 records)
```bash
python scripts/create_memory_efficient_fact_table.py --test --sample-size 100
```

#### Full Fact Table Creation
```bash
python scripts/create_memory_efficient_fact_table.py
```

### Using the Shell Script

```bash
# Test S3 connection
./scripts/run_fact_table.sh test

# Small test (100 records)
./scripts/run_fact_table.sh small

# Medium test (1000 records)
./scripts/run_fact_table.sh medium

# Large test (10000 records)
./scripts/run_fact_table.sh large

# Full fact table creation
./scripts/run_fact_table.sh full

# Custom sample size
./scripts/run_fact_table.sh custom 5000

# Local files only
./scripts/run_fact_table.sh local

# S3 input, local output only
./scripts/run_fact_table.sh local-only
```

### Command Line Options

```bash
python scripts/create_memory_efficient_fact_table.py [OPTIONS]

Options:
  --test                    Run in test mode with small sample
  --sample-size INT         Sample size for test mode (default: 1000)
  --nppes-inner-join        Use inner join for NPPES data (only keep enriched records)
  --chunk-size INT          Chunk size for processing (default: 50000)
  --s3-bucket STR           S3 bucket name (default: commercial-rates)
  --s3-prefix STR           S3 prefix/path (default: tic-mrf/test)
  --local                   Use local files instead of S3
  --no-upload               Skip uploading results to S3 (save locally only)
```

### NPPES Join Options

#### Left Join (Default)
```bash
# Keeps all records, even without NPPES enrichment
python scripts/create_memory_efficient_fact_table.py --test
```

#### Inner Join
```bash
# Only keeps records with NPPES enrichment
python scripts/create_memory_efficient_fact_table.py --test --nppes-inner-join
```

### Output Files

#### Local Output
```
dashboard_data/
├── memory_efficient_fact_table.parquet          # Main fact table
├── memory_efficient_fact_table_summary.json     # Summary statistics
├── fact_table_s3_location.json                  # S3 metadata (if uploaded)
└── temp_chunks/                                 # Temporary files (auto-cleaned)
```

#### S3 Output (when uploaded)
```
s3://commercial-rates/tic-mrf/test/
├── fact_tables/
│   └── memory_efficient_fact_table_20250129_143022.parquet
└── fact_table_summary.json
```

## 🧪 Testing & Validation

### S3 Connection Testing

```bash
# Test NPPES backfill S3 connection
python scripts/test_s3_connection.py

# Test fact table S3 connection
python scripts/test_fact_table_s3.py
```

### Expected Test Output

```
✅ S3 bucket access successful
📊 File Summary:
   Rates: 5 files
   Organizations: 5 files
   Providers: 5 files
✅ Successfully read rates file
   Shape: (1234, 15)
   Columns: ['rate_uuid', 'organization_uuid', 'provider_network', ...]
```

### Data Validation

Check the output files for:
- Record counts in summary files
- Column structure in parquet files
- S3 upload success in metadata files

## 🔧 Advanced Usage

### Custom S3 Configuration

```bash
# Custom S3 bucket and prefix
python scripts/backfill_provider_info.py \
  --s3-bucket my-custom-bucket \
  --s3-prefix my-data/providers

python scripts/create_memory_efficient_fact_table.py \
  --s3-bucket my-custom-bucket \
  --s3-prefix my-data/rates
```

### Performance Tuning

```bash
# Adjust chunk size for memory usage
python scripts/create_memory_efficient_fact_table.py \
  --chunk-size 25000

# Adjust API request rate
python scripts/backfill_provider_info.py \
  --request-delay 0.2 \
  --max-workers 3
```

### Local-Only Processing

```bash
# Use local files instead of S3
python scripts/create_memory_efficient_fact_table.py \
  --local --test

python scripts/backfill_provider_info.py \
  --local
```

### S3 Input, Local Output

```bash
# Read from S3, save locally only
python scripts/create_memory_efficient_fact_table.py \
  --test --no-upload
```

## 📈 Workflow Examples

### Complete Data Pipeline

```bash
# 1. Test S3 connections
./scripts/run_backfill.sh test
./scripts/run_fact_table.sh test

# 2. Run NPPES backfill
./scripts/run_backfill.sh medium

# 3. Create fact table with NPPES enrichment
./scripts/run_fact_table.sh medium --nppes-inner-join

# 4. Create full fact table
./scripts/run_fact_table.sh full
```

### Development Workflow

```bash
# 1. Quick validation
./scripts/run_backfill.sh small
./scripts/run_fact_table.sh small

# 2. Medium testing
./scripts/run_backfill.sh medium
./scripts/run_fact_table.sh medium

# 3. Production run
./scripts/run_backfill.sh full
./scripts/run_fact_table.sh full
```

## 🚨 Troubleshooting

### Common Issues

#### S3 Connection Errors
```bash
# Check AWS credentials
aws sts get-caller-identity

# Test S3 access
aws s3 ls s3://commercial-rates/tic-mrf/test/
```

#### Memory Issues
```bash
# Reduce chunk size
python scripts/create_memory_efficient_fact_table.py \
  --chunk-size 10000 --test
```

#### API Rate Limiting
```bash
# Increase request delay
python scripts/backfill_provider_info.py \
  --request-delay 0.5 --limit 100
```

#### File Not Found Errors
```bash
# Check S3 file structure
python scripts/test_s3_connection.py
```

### Log Files

Check the console output for detailed logging. The scripts provide comprehensive logging including:
- S3 connection status
- File discovery results
- Processing progress
- Error details
- Upload confirmations

### Debug Mode

For more detailed logging, you can modify the logging level in the scripts:
```python
logging.basicConfig(level=logging.DEBUG)
```

## 📊 Output Data Structure

### NPPES Provider Data
```json
{
  "npi": "1234567890",
  "provider_name": {
    "first": "John",
    "last": "Doe",
    "middle": "A",
    "suffix": ""
  },
  "primary_specialty": "Orthopedic Surgery",
  "addresses": [...],
  "credentials": ["MD"],
  "provider_type": "Individual"
}
```

### Fact Table Structure
```json
{
  "rate_uuid": "uuid-123",
  "organization_uuid": "org-456",
  "npi": "1234567890",
  "negotiated_rate": 150.00,
  "service_code": "99213",
  "nppes_primary_specialty": "Orthopedic Surgery",
  "nppes_city": "Chicago",
  "nppes_state": "IL",
  "rate_category": "$100-500",
  "service_category": "Evaluation & Management"
}
```

## 🔄 Data Flow

1. **S3 Input**: Chunked parquet files in `s3://commercial-rates/tic-mrf/test/`
2. **NPPES Enrichment**: API calls to NPI Registry
3. **Processing**: Memory-efficient chunked processing
4. **Joins**: Rates + Organizations + NPPES data
5. **Output**: Comprehensive fact table (local + S3)

This workflow ensures efficient processing of large healthcare datasets while maintaining data quality and providing comprehensive provider enrichment. 