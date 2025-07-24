# Production ETL Pipeline Guide: Healthcare Rates Data Processing

This comprehensive guide explains the production ETL pipeline system for processing healthcare rates data from Machine Readable Files (MRFs). The system is designed to handle large-scale data processing with orthopedic and radiology focus, supporting both local and cloud-based workflows.

## 📋 Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Execution Modes](#execution-modes)
- [Data Processing Workflows](#data-processing-workflows)
- [Output Formats](#output-formats)
- [Monitoring & Progress Tracking](#monitoring--progress-tracking)
- [Quality Assurance](#quality-assurance)
- [Performance Optimization](#performance-optimization)
- [Troubleshooting](#troubleshooting)
- [Advanced Usage](#advanced-usage)
- [Integration Patterns](#integration-patterns)

## 🎯 Overview

The Production ETL Pipeline is a comprehensive system for processing healthcare rates data from Machine Readable Files (MRFs). It's designed with orthopedic and radiology specialties as the primary focus, but can be configured for any medical specialty.

### Key Features

- **Multi-Payer Support**: Process data from multiple insurance payers
- **Specialty Focus**: Optimized for orthopedic and radiology procedures
- **Scalable Processing**: Handles millions of records efficiently
- **Quality Validation**: Built-in data quality checks and validation
- **Progress Tracking**: Real-time progress monitoring with ETA
- **Cloud Integration**: S3 upload capabilities for cloud storage
- **Flexible Output**: Multiple output formats (Parquet, JSON, CSV)
- **Error Handling**: Robust error handling and recovery

### Data Flow

```
MRF Index Files → Payer Handlers → Stream Processing → Normalization → 
Quality Validation → Batch Writing → Aggregation → S3 Upload
```

## 🏗️ System Architecture

### Core Components

1. **Configuration Management** (`production_config.yaml`)
   - Payer endpoints and authentication
   - CPT code whitelists
   - Processing parameters
   - Output configuration

2. **Pipeline Orchestration** (`run_production_etl.sh`)
   - Shell script wrapper with progress tracking
   - Environment setup and validation
   - Error handling and reporting

3. **Data Processing Engine** (`production_etl_pipeline.py`)
   - Multi-threaded processing
   - Memory-efficient streaming
   - Quality validation
   - Batch writing

4. **Progress Monitoring** (`production_etl_pipeline_quiet.py`)
   - Real-time progress tracking
   - Performance metrics
   - Resource monitoring

### File Structure

```
tic/
├── run_production_etl.sh              # Main execution script
├── production_etl_pipeline.py         # Full-featured ETL pipeline
├── production_etl_pipeline_quiet.py   # Quiet mode with progress tracking
├── production_config.yaml             # Configuration file
├── production_data/                   # Output directory
│   ├── rates/                        # Rate data
│   ├── organizations/                 # Organization data
│   ├── providers/                    # Provider data
│   ├── payers/                       # Payer metadata
│   └── analytics/                    # Aggregated analytics
└── logs/                             # Processing logs
```

## 🔧 Prerequisites

### System Requirements

- **Python 3.8+** with pip
- **8GB+ RAM** (16GB recommended for large datasets)
- **50GB+ disk space** for processing and output
- **Internet connection** for MRF downloads
- **AWS CLI** (optional, for S3 integration)

### Python Dependencies

```bash
pip install pandas pyarrow boto3 pyyaml tqdm structlog psutil
```

### AWS Configuration (Optional)

For S3 integration, configure AWS credentials:

```bash
# Option 1: AWS CLI configuration
aws configure

# Option 2: Environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
export S3_BUCKET=your-bucket-name
```

## 🚀 Quick Start

### Basic Execution

```bash
# Run with default configuration
./run_production_etl.sh

# Run in quiet mode (minimal output)
./run_production_etl.sh --quiet

# Run with custom configuration
ETL_CONFIG=my_config.yaml ./run_production_etl.sh
```

### Test Run

```bash
# Test with limited data
python production_etl_pipeline.py --test-mode --max-files 5
```

### Monitor Progress

```bash
# Run with progress tracking
./run_production_etl.sh

# Check logs in real-time
tail -f logs/etl_$(date +%Y%m%d)*.log
```

## ⚙️ Configuration

### Configuration File Structure

The `production_config.yaml` file controls all aspects of the ETL pipeline:

```yaml
# Payer endpoints
payer_endpoints:
  bcbsil: "https://app0004702110a5prdnc868.blob.core.windows.net/toc/2025-05-20_Blue-Cross-and-Blue-Shield-of-Illinois_index.json"
  # Add more payers as needed

# CPT code whitelist (orthopedic/radiology focus)
cpt_whitelist:
  - "99202"  # New patient office visit
  - "72148"  # MRI lumbar spine
  - "27130"  # Total hip replacement
  # ... more codes

# Processing configuration
processing:
  max_files_per_payer: 50
  max_records_per_file: 500000
  batch_size: 10000
  parallel_workers: 4

# Output configuration
output:
  local_directory: "production_data"
  s3:
    bucket: "${S3_BUCKET}"
    prefix: "healthcare-rates-ortho-radiology"
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_BUCKET` | S3 bucket for uploads | None |
| `ETL_LOG_LEVEL` | Logging level | INFO |
| `ETL_START_TIME` | Pipeline start time | Auto-set |
| `ETL_SHOW_PROGRESS` | Show progress bars | true |

### Configuration Overrides

```bash
# Override configuration values
ETL_MAX_FILES=10 ETL_BATCH_SIZE=5000 ./run_production_etl.sh

# Use custom config file
ETL_CONFIG=my_config.yaml ./run_production_etl.sh
```

## 🎛️ Execution Modes

### Standard Mode

Full-featured execution with progress tracking and comprehensive logging:

```bash
./run_production_etl.sh
```

**Features:**
- Real-time progress bars
- Detailed logging
- Performance metrics
- Error reporting
- S3 upload (if configured)

### Quiet Mode

Minimal output for automated/background execution:

```bash
./run_production_etl.sh --quiet
```

**Features:**
- Suppressed progress bars
- Essential logging only
- Background processing
- File-based progress tracking

### Test Mode

Limited processing for testing and validation:

```bash
python production_etl_pipeline.py --test-mode --max-files 5 --max-records 10000
```

**Features:**
- Limited file processing
- Sample data extraction
- Validation testing
- Performance benchmarking

## 🔄 Data Processing Workflows

### 1. Index Analysis

The pipeline starts by analyzing MRF index files to understand the data structure:

```python
# Analyze index structure
index_info = analyze_index_structure(index_url)
file_list = list_mrf_blobs_enhanced(index_info)
```

### 2. Payer-Specific Processing

Each payer has a custom handler for their specific MRF format:

```python
# Get payer-specific handler
handler = get_handler(payer_name)

# Process files with handler
for file_info in file_list:
    records = process_mrf_file_enhanced(payer_uuid, payer_name, file_info, handler)
```

### 3. Stream Processing

Large files are processed in streaming mode to manage memory:

```python
# Stream parse large files
for record in stream_parse_enhanced(file_url, handler):
    normalized = normalize_tic_record(record)
    validated = validate_rate_record(normalized)
    yield validated
```

### 4. Quality Validation

Each record undergoes quality validation:

```python
# Validate rate record
def validate_rate_record(record):
    # Check required fields
    # Validate rate ranges
    # Check data types
    # Apply business rules
    return validated_record
```

### 5. Batch Writing

Validated records are written in batches for efficiency:

```python
# Write batches to parquet
def write_batches_local(rate_batch, org_batch, provider_batch):
    append_to_parquet(rate_file, rate_batch)
    append_to_parquet(org_file, org_batch)
    append_to_parquet(provider_file, provider_batch)
```

## 📊 Output Formats

### Parquet Files

The primary output format is Apache Parquet for efficient storage and querying:

```
production_data/
├── rates/
│   ├── rates_batch_001.parquet
│   ├── rates_batch_002.parquet
│   └── rates_final.parquet
├── organizations/
│   ├── organizations_batch_001.parquet
│   └── organizations_final.parquet
├── providers/
│   ├── providers_batch_001.parquet
│   └── providers_final.parquet
└── analytics/
    └── analytics_final.parquet
```

### Data Schema

#### Rates Table
```python
{
    "rate_uuid": "string",
    "payer_uuid": "string", 
    "organization_uuid": "string",
    "service_code": "string",
    "negotiated_rate": "float",
    "effective_date": "string",
    "expiration_date": "string",
    "billing_class": "string",
    "tin_type": "string",
    "npi_numbers": "array",
    "provider_references": "array"
}
```

#### Organizations Table
```python
{
    "organization_uuid": "string",
    "tin": "string",
    "organization_name": "string",
    "address_line_1": "string",
    "city": "string",
    "state": "string",
    "zip_code": "string",
    "phone": "string"
}
```

#### Providers Table
```python
{
    "provider_uuid": "string",
    "npi": "string",
    "organization_uuid": "string",
    "provider_name": "string",
    "specialty": "string",
    "address_line_1": "string",
    "city": "string",
    "state": "string",
    "zip_code": "string"
}
```

### Analytics Aggregations

The pipeline generates analytics tables with pre-computed aggregations:

```python
# Rate statistics by procedure
analytics = {
    "procedure_code": "string",
    "procedure_name": "string", 
    "avg_rate": "float",
    "median_rate": "float",
    "min_rate": "float",
    "max_rate": "float",
    "rate_count": "integer",
    "payer_count": "integer"
}
```

## 📈 Monitoring & Progress Tracking

### Real-Time Progress

The pipeline provides real-time progress tracking:

```
[██████████████████████████████████████████████████] 100% | BCBSIL | 1,234,567 records | 1,234.5 rec/sec
```

### Progress File

Progress is written to a temporary file for external monitoring:

```bash
# Monitor progress externally
tail -f /tmp/progress_*.csv
```

### Performance Metrics

The pipeline tracks comprehensive performance metrics:

```json
{
    "processing_time_seconds": 3600.5,
    "records_processed": 5000000,
    "files_processed": 150,
    "payers_processed": 3,
    "processing_rate_per_second": 1388.9,
    "memory_usage_mb": 2048.5,
    "errors": [],
    "warnings": []
}
```

### Log Files

Comprehensive logging is available:

```bash
# View latest log
tail -f logs/etl_$(date +%Y%m%d)*.log

# Search for errors
grep "ERROR" logs/etl_*.log

# Check processing statistics
cat production_data/processing_statistics.json
```

## ✅ Quality Assurance

### Data Validation Rules

The pipeline implements comprehensive data validation:

```python
# Rate validation
def validate_rate_record(record):
    # Required fields
    required_fields = ["service_code", "negotiated_rate", "payer_uuid"]
    
    # Rate range validation
    if record["negotiated_rate"] <= 0:
        return None
    
    # CPT code validation
    if record["service_code"] not in cpt_whitelist:
        return None
    
    # Date validation
    if not is_valid_date(record["effective_date"]):
        return None
    
    return record
```

### Quality Thresholds

Configurable quality thresholds ensure data integrity:

```yaml
quality_rules:
  rates:
    min_rate: 0.01
    max_rate: 100000.00
    required_fields: ["service_code", "negotiated_rate", "payer_uuid"]
  
  high_cost_procedures:
    max_reasonable_rates:
      "27130": 75000.00  # Total hip replacement
      "27447": 70000.00  # Total knee replacement
      "72148": 5000.00   # MRI lumbar spine
```

### Completeness Checks

The pipeline tracks data completeness:

```python
# Calculate completeness percentage
completeness = (valid_records / total_records) * 100

# Alert if below threshold
if completeness < min_completeness_pct:
    logger.warning(f"Data completeness below threshold: {completeness}%")
```

## ⚡ Performance Optimization

### Memory Management

The pipeline uses memory-efficient processing:

```python
# Stream processing for large files
def stream_parse_enhanced(file_url, handler):
    for chunk in stream_file(file_url):
        for record in parse_chunk(chunk, handler):
            yield record

# Batch writing to manage memory
def write_batches_local(batches):
    for batch in batches:
        if len(batch) >= batch_size:
            write_batch_to_parquet(batch)
            batch.clear()
```

### Parallel Processing

Multi-threaded processing for improved performance:

```python
# Parallel file processing
with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
    futures = []
    for file_info in file_list:
        future = executor.submit(process_file, file_info)
        futures.append(future)
    
    for future in as_completed(futures):
        result = future.result()
```

### Caching Strategy

Intelligent caching reduces redundant processing:

```python
# Cache parsed files
cache_key = hashlib.md5(file_url.encode()).hexdigest()
if cache_key in processed_files:
    continue

# Cache validation results
validation_cache = {}
```

### Compression

Efficient compression for storage and transfer:

```python
# Parquet compression
pq.write_table(table, file_path, compression='snappy')

# S3 compression
s3_client.upload_fileobj(
    compressed_data,
    bucket,
    key,
    ExtraArgs={'ContentEncoding': 'gzip'}
)
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Memory Errors

**Symptoms:** `MemoryError` or high memory usage

**Solutions:**
```bash
# Reduce batch size
ETL_BATCH_SIZE=5000 ./run_production_etl.sh

# Reduce parallel workers
ETL_PARALLEL_WORKERS=2 ./run_production_etl.sh

# Increase system memory or use swap
```

#### 2. Network Timeouts

**Symptoms:** Connection errors or slow downloads

**Solutions:**
```bash
# Increase timeout values
export ETL_TIMEOUT=300

# Use retry logic
export ETL_MAX_RETRIES=5

# Check network connectivity
curl -I https://mrfdata.hmhs.com/
```

#### 3. S3 Upload Failures

**Symptoms:** S3 upload errors or missing files

**Solutions:**
```bash
# Verify AWS credentials
aws sts get-caller-identity

# Check S3 permissions
aws s3 ls s3://your-bucket/

# Test S3 upload
python -c "import boto3; s3 = boto3.client('s3'); s3.upload_file('test.txt', 'bucket', 'test.txt')"
```

#### 4. Data Quality Issues

**Symptoms:** Low completeness scores or validation errors

**Solutions:**
```bash
# Review validation logs
grep "VALIDATION" logs/etl_*.log

# Check data quality report
cat production_data/quality_report.json

# Adjust quality thresholds
ETL_MIN_COMPLETENESS=70 ETL_MIN_ACCURACY=0.75 ./run_production_etl.sh
```

### Debug Mode

Enable debug mode for detailed troubleshooting:

```bash
# Enable debug logging
ETL_LOG_LEVEL=DEBUG ./run_production_etl.sh

# Run with debug output
python production_etl_pipeline.py --debug --verbose

# Check debug logs
tail -f logs/debug_*.log
```

### Performance Diagnostics

Monitor system performance during processing:

```bash
# Monitor memory usage
watch -n 1 'ps aux | grep python'

# Monitor disk usage
watch -n 1 'df -h'

# Monitor network activity
iftop -i eth0
```

## 🚀 Advanced Usage

### Custom Payer Handlers

Create custom handlers for new payers:

```python
# Custom payer handler
class CustomPayerHandler:
    def parse_index(self, index_data):
        # Custom index parsing logic
        return file_list
    
    def parse_file(self, file_data):
        # Custom file parsing logic
        return records
    
    def normalize_record(self, record):
        # Custom normalization logic
        return normalized_record

# Register handler
register_handler("custom_payer", CustomPayerHandler())
```

### Custom Quality Rules

Implement custom quality validation:

```python
# Custom quality validator
class CustomQualityValidator:
    def validate_rate(self, rate_record):
        # Custom rate validation
        if rate_record["negotiated_rate"] > 1000000:
            return False
        return True
    
    def validate_provider(self, provider_record):
        # Custom provider validation
        if not provider_record["npi"].isdigit():
            return False
        return True
```

### Custom Analytics

Add custom analytics aggregations:

```python
# Custom analytics processor
def generate_custom_analytics(rate_data):
    analytics = {}
    
    # Geographic analysis
    geo_stats = rate_data.groupby("state").agg({
        "negotiated_rate": ["mean", "median", "count"]
    })
    
    # Specialty analysis
    specialty_stats = rate_data.groupby("specialty").agg({
        "negotiated_rate": ["mean", "median", "count"]
    })
    
    return {
        "geographic_analysis": geo_stats,
        "specialty_analysis": specialty_stats
    }
```

### Automated Scheduling

Set up automated processing with cron:

```bash
# Daily processing at 2 AM
0 2 * * * /path/to/tic/run_production_etl.sh --quiet

# Weekly processing on Sundays
0 3 * * 0 /path/to/tic/run_production_etl.sh --quiet

# Monthly processing on 1st of month
0 4 1 * * /path/to/tic/run_production_etl.sh --quiet
```

### Docker Integration

Containerize the ETL pipeline:

```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN chmod +x run_production_etl.sh

CMD ["./run_production_etl.sh"]
```

## 🔗 Integration Patterns

### Data Warehouse Integration

Load processed data into a data warehouse:

```python
# Snowflake integration
import snowflake.connector

def load_to_snowflake(parquet_file, table_name):
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="COMPUTE_WH",
        database="HEALTHCARE_RATES"
    )
    
    cursor = conn.cursor()
    cursor.execute(f"COPY INTO {table_name} FROM @my_stage/{parquet_file}")
    conn.close()
```

### API Integration

Expose processed data via API:

```python
# FastAPI integration
from fastapi import FastAPI
import pandas as pd

app = FastAPI()

@app.get("/rates/{procedure_code}")
async def get_rates(procedure_code: str):
    df = pd.read_parquet("production_data/rates/rates_final.parquet")
    rates = df[df["service_code"] == procedure_code]
    return rates.to_dict("records")
```

### Monitoring Integration

Integrate with monitoring systems:

```python
# Prometheus metrics
from prometheus_client import Counter, Histogram, Gauge

# Metrics
records_processed = Counter('etl_records_processed_total', 'Total records processed')
processing_time = Histogram('etl_processing_duration_seconds', 'Processing time')
memory_usage = Gauge('etl_memory_usage_bytes', 'Memory usage')

# Update metrics during processing
records_processed.inc(len(batch))
processing_time.observe(time.time() - start_time)
memory_usage.set(psutil.Process().memory_info().rss)
```

### Alerting Integration

Set up alerting for pipeline issues:

```python
# Slack integration
import requests

def send_slack_alert(message, channel="#etl-alerts"):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    payload = {
        "text": f"ETL Pipeline Alert: {message}",
        "channel": channel
    }
    requests.post(webhook_url, json=payload)

# Alert on errors
if error_count > error_threshold:
    send_slack_alert(f"ETL pipeline has {error_count} errors")
```

## 📚 Additional Resources

### Documentation

- [TiC Scripts Guide](./SCRIPTS_GUIDE.md) - NPPES backfill and fact table creation
- [API Documentation](./docs/api.md) - REST API documentation
- [Configuration Reference](./docs/config.md) - Configuration options

### Examples

- [Simple Run Example](./examples/simple_run.sh) - Basic execution example
- [Advanced Configuration](./examples/advanced_config.yaml) - Advanced configuration
- [Custom Handlers](./examples/custom_handlers.py) - Custom payer handlers

### Support

- **Issues**: GitHub Issues for bug reports
- **Discussions**: GitHub Discussions for questions
- **Wiki**: Project wiki for additional documentation

---

*This guide covers the comprehensive production ETL pipeline system. For specific questions or advanced usage patterns, please refer to the additional resources or create an issue for support.* 