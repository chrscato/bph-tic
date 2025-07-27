# MRF Payer Structure Analysis Tools

This repository contains tools for analyzing Machine Readable Files (MRFs) from healthcare payers and processing them through a comprehensive ETL pipeline.

## Overview

The system provides:
- **Automated payer integration** using intelligent analysis and handler generation
- **Production ETL pipeline** for processing complete MRF datasets with S3 upload
- **Memory-efficient streaming** for large healthcare rate files
- **Standardized data normalization** across multiple payer formats

## Prerequisites

```bash
# Install required dependencies
pip install -r requirements.txt

# Additional dependencies for analysis
pip install ijson requests pyyaml boto3 pandas pyarrow
```

---

# Part 1: New Payer Integration

The smart payer integration system automatically analyzes new payers and generates appropriate handlers. For detailed integration workflows, see [SMART_PAYER_INTEGRATION_GUIDE.md](SMART_PAYER_INTEGRATION_GUIDE.md).

## Quick Integration Workflow

### Step 1: Analyze New Payer Structure

```bash
# Analyze a new payer's MRF structure
python scripts/analyze_payer_structure.py --payers "new_payer_name" --config production_config.yaml
```

This generates analysis files in `payer_structure_analysis/` with detailed structure information.

### Step 2: Run Intelligent Integration

```bash
# Use intelligent integration to auto-generate handlers
python scripts/intelligent_payer_integration.py \
  --analysis-file payer_structure_analysis/full_analysis_YYYYMMDD_HHMMSS.json \
  --payer-name "new_payer_name" \
  --index-url "https://example.com/mrf_index.json"
```

### Step 3: Complete Automated Workflow (Recommended)

```bash
# Run complete automated workflow
python scripts/smart_payer_workflow.py \
  --payer-name "new_payer_name" \
  --index-url "https://example.com/mrf_index.json" \
  --auto-deploy
```

## Integration Components

### Handler Generation

The system intelligently generates handlers based on structure analysis:

#### Standard Handlers
```python
@register_handler("standard_payer")
class StandardPayerHandler(PayerHandler):
    """Handler for standard MRF format."""
    
    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [record]
```

#### Complex Handlers with Custom Logic
```python
@register_handler("nested_provider_payer")
class NestedProviderPayerHandler(PayerHandler):
    """Handler for payer with nested provider structures."""
    
    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "negotiated_rates" in record:
            for rate_group in record.get("negotiated_rates", []):
                if "provider_groups" in rate_group:
                    normalized_groups = []
                    for pg in rate_group["provider_groups"]:
                        if "providers" in pg and pg["providers"]:
                            # Extract NPIs from nested providers
                            npis = [provider["npi"] for provider in pg["providers"] if "npi" in provider]
                            normalized_groups.append({
                                "npi": npis[0] if npis else "",
                                "tin": pg.get("tin", ""),
                                "npi_list": npis
                            })
                        else:
                            normalized_groups.append(pg)
                    rate_group["provider_groups"] = normalized_groups
        return [record]
```

### Configuration Updates

The system automatically updates `production_config.yaml`:

```yaml
payer_endpoints:
  new_payer_name: "https://example.com/mrf_index.json"
```

### Testing Integration

```bash
# Test handler import
python -c "from tic_mrf_scraper.payers import get_handler; handler = get_handler('new_payer_name')"

# Test with production pipeline
python production_etl_pipeline.py --payers new_payer_name --max-files 1
```

## Integration Monitoring

Check integration success:

```bash
# View handler registration
python -c "from tic_mrf_scraper.payers import get_handler; print(get_handler('new_payer_name').__class__.__name__)"

# Test with sample data
python scripts/quick_payer_test.py \
  --payer-name "new_payer_name" \
  --index-url "https://example.com/mrf_index.json" \
  --max-files 1 \
  --max-records 10
```

---

# Part 2: Production ETL Pipeline

The `production_etl_pipeline.py` script provides a comprehensive ETL system for processing complete MRF datasets with memory-efficient streaming and S3 upload capabilities.

## Pipeline Architecture

### Core Components

#### 1. ETL Configuration (`ETLConfig`)
```python
@dataclass
class ETLConfig:
    # Input sources
    payer_endpoints: Dict[str, str]
    cpt_whitelist: List[str]
    
    # Processing configuration
    batch_size: int = 10000
    parallel_workers: int = 2
    max_files_per_payer: Optional[int] = None
    max_records_per_file: Optional[int] = None
    
    # Output configuration
    local_output_dir: str = "ortho_radiology_data_default"
    s3_bucket: Optional[str] = None
    s3_prefix: str = "healthcare-rates-ortho-radiology"
    
    # Data versioning
    schema_version: str = "v2.1.0"
    processing_version: str = "tic-etl-v1.0"
    
    # Quality thresholds
    min_completeness_pct: float = 80.0
    min_accuracy_score: float = 0.85
```

#### 2. UUID Generation (`UUIDGenerator`)
```python
class UUIDGenerator:
    @staticmethod
    def payer_uuid(payer_name: str, parent_org: str = "") -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"payer:{payer_name}:{parent_org}"))
    
    @staticmethod
    def organization_uuid(tin: str, org_name: str = "") -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"org:{tin}:{org_name}"))
    
    @staticmethod
    def provider_uuid(npi: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"provider:{npi}"))
    
    @staticmethod
    def rate_uuid(payer_uuid: str, org_uuid: str, service_code: str, 
                  rate: float, effective_date: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, 
                             f"rate:{payer_uuid}:{org_uuid}:{service_code}:{rate}:{effective_date}"))
```

#### 3. Data Quality Validation (`DataQualityValidator`)
```python
class DataQualityValidator:
    @staticmethod
    def validate_rate_record(record: Dict[str, Any]) -> Dict[str, Any]:
        # Validate required fields
        required_fields = ["service_code", "negotiated_rate", "payer"]
        for field in required_fields:
            if not record.get(field):
                return None
        
        # Validate data types
        if not isinstance(record["negotiated_rate"], (int, float)):
            return None
        
        # Add quality scores
        record["quality_score"] = 0.95  # Example score
        record["completeness_pct"] = 90.0  # Example completeness
        
        return record
```

### Processing Pipeline

#### 1. Index Analysis (`analyze_index_structure`)
```python
from tic_mrf_scraper.fetch.blobs import analyze_index_structure

# Analyze payer index structure
index_info = analyze_index_structure(index_url)
# Returns: structure type, file counts, URL patterns, compression info
```

#### 2. MRF File Discovery (`list_mrf_blobs_enhanced`)
```python
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced

# Get all MRF files from index
mrf_files = list_mrf_blobs_enhanced(index_url)
# Returns: List of file metadata with URLs, types, plan info
```

#### 3. Streaming Parser (`stream_parse_enhanced`)
```python
from tic_mrf_scraper.stream.parser import stream_parse_enhanced

# Process records with memory-efficient streaming
for raw_record in stream_parse_enhanced(
    file_info["url"],
    payer_name,
    file_info.get("provider_reference_url"),
    handler
):
    # Process each record individually
    pass
```

#### 4. Record Normalization (`normalize_tic_record`)
```python
from tic_mrf_scraper.transform.normalize import normalize_tic_record

# Normalize raw records to standard format
normalized = normalize_tic_record(
    raw_record, 
    set(cpt_whitelist), 
    payer_name
)
# Returns: Standardized record with all required fields
```

### Data Flow

#### 1. Payer Processing
```python
def process_payer(self, payer_name: str, index_url: str) -> Dict[str, Any]:
    # Create payer record
    payer_uuid = self.create_payer_record(payer_name, index_url)
    
    # Analyze index structure
    index_info = identify_index(index_url)
    
    # Get all MRF files using handler
    handler = get_handler(payer_name)
    mrf_files = handler.list_mrf_files(index_url)
    
    # Filter to in-network rates files
    rate_files = [f for f in mrf_files if f["type"] == "in_network_rates"]
    
    # Process all rate files
    for file_index, file_info in enumerate(rate_files, 1):
        file_stats = self.process_mrf_file_enhanced(
            payer_uuid, payer_name, file_info, handler, file_index, len(rate_files)
        )
```

#### 2. File Processing
```python
def process_mrf_file_enhanced(self, payer_uuid: str, payer_name: str,
                            file_info: Dict[str, Any], handler, 
                            file_index: int, total_files: int) -> Dict[str, Any]:
    
    # Batch collectors for S3 upload
    rate_batch = []
    org_batch = []
    provider_batch = []
    
    # Process records with streaming parser
    for raw_record in stream_parse_enhanced(
        file_info["url"],
        payer_name,
        file_info.get("provider_reference_url"),
        handler
    ):
        # Normalize and validate
        normalized = normalize_tic_record(
            raw_record, 
            set(self.config.cpt_whitelist), 
            payer_name
        )
        
        if not normalized:
            continue
        
        # Create structured records
        rate_record = self.create_rate_record(payer_uuid, normalized, file_info, raw_record)
        org_record = self.create_organization_record(normalized, raw_record)
        provider_records = self.create_provider_records(normalized, raw_record)
        
        # Add to batches
        rate_batch.append(rate_record)
        org_batch.append(org_record)
        provider_batch.extend(provider_records)
        
        # Upload batches when full
        if len(rate_batch) >= self.config.batch_size:
            self.write_batches_to_s3(rate_batch, org_batch, provider_batch, 
                                   payer_name, filename_base, batch_number)
```

#### 3. Record Creation
```python
def create_rate_record(self, payer_uuid: str, normalized: Dict[str, Any], 
                      file_info: Dict[str, Any], raw_record: Dict[str, Any]) -> Dict[str, Any]:
    
    # Generate UUIDs
    org_uuid = self.uuid_gen.organization_uuid(
        normalized.get("provider_tin", ""), 
        normalized.get("provider_name", "")
    )
    
    service_code = normalized.get("service_code", "")
    rate_uuid = self.uuid_gen.rate_uuid(
        payer_uuid, org_uuid, service_code,
        normalized["negotiated_rate"],
        normalized.get("expiration_date", "")
    )
    
    return {
        "rate_uuid": rate_uuid,
        "payer_uuid": payer_uuid,
        "organization_uuid": org_uuid,
        "service_code": service_code,
        "negotiated_rate": normalized["negotiated_rate"],
        "billing_code_type": normalized.get("billing_code_type", ""),
        "description": normalized.get("description", ""),
        "expiration_date": normalized.get("expiration_date", ""),
        "effective_date": file_info.get("effective_date", ""),
        "plan_name": file_info.get("plan_name", ""),
        "plan_id": file_info.get("plan_id", ""),
        "created_at": datetime.now(timezone.utc),
        "data_source": "TiC_MRF"
    }
```

### Output Handling

#### S3 Upload
```python
def write_batches_to_s3(self, rate_batch: List[Dict], org_batch: List[Dict], 
                       provider_batch: List[Dict], payer_name: str, 
                       filename_base: str, batch_number: int) -> Dict[str, Any]:
    
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    
    # Upload rate records
    if rate_batch:
        s3_key = f"{self.config.s3_prefix}/rates/{payer_name}/{filename_base}_batch_{batch_number}_{timestamp}.parquet"
        self.upload_batch_to_s3(rate_batch, s3_key, "rates")
    
    # Upload organization records
    if org_batch:
        s3_key = f"{self.config.s3_prefix}/organizations/{payer_name}/{filename_base}_batch_{batch_number}_{timestamp}.parquet"
        self.upload_batch_to_s3(org_batch, s3_key, "organizations")
    
    # Upload provider records
    if provider_batch:
        s3_key = f"{self.config.s3_prefix}/providers/{payer_name}/{filename_base}_batch_{batch_number}_{timestamp}.parquet"
        self.upload_batch_to_s3(provider_batch, s3_key, "providers")
```

#### Local Output
```python
def write_batches_local(self, rate_batch: List[Dict], org_batch: List[Dict], 
                       provider_batch: List[Dict], payer_name: str) -> Dict[str, Any]:
    
    base_dir = Path(self.config.local_output_dir)
    
    # Write rate records
    if rate_batch:
        rate_file = base_dir / "rates" / f"{payer_name}_rates.parquet"
        self.append_to_parquet(rate_file, rate_batch)
    
    # Write organization records
    if org_batch:
        org_file = base_dir / "organizations" / f"{payer_name}_organizations.parquet"
        self.append_to_parquet(org_file, org_batch)
    
    # Write provider records
    if provider_batch:
        provider_file = base_dir / "providers" / f"{payer_name}_providers.parquet"
        self.append_to_parquet(provider_file, provider_batch)
```

## Usage

### Basic Pipeline Execution
```bash
# Run with production config
python production_etl_pipeline.py

# Run with custom config
python production_etl_pipeline.py --config custom_config.yaml

# Process specific payers
python production_etl_pipeline.py --payers "Blue Cross Blue Shield" "Aetna"
```

### Configuration
```yaml
# production_config.yaml
payer_endpoints:
  "Blue Cross Blue Shield": "https://example.com/bcbs_index.json"
  "Aetna": "https://example.com/aetna_index.json"

cpt_whitelist:
  - "99213"  # Office visit
  - "99214"  # Office visit
  - "70450"  # CT head/brain
  - "72148"  # MRI spinal

# Processing limits
batch_size: 10000
parallel_workers: 2
max_files_per_payer: null  # Process all files
max_records_per_file: null  # Process all records

# Output configuration
local_output_dir: "ortho_radiology_data_default"
s3_bucket: "my-healthcare-data-bucket"
s3_prefix: "healthcare-rates-ortho-radiology"
```

### Progress Tracking
```python
# Progress tracking with tqdm
progress = ProgressTracker()
progress.update_progress(
    payer=payer_name,
    files_completed=file_index,
    total_files=len(rate_files),
    records_processed=payer_stats["records_extracted"]
)
```

### Error Handling
```python
# Comprehensive error tracking
self.stats = {
    "payers_processed": 0,
    "total_files_found": 0,
    "files_processed": 0,
    "files_succeeded": 0,
    "files_failed": 0,
    "records_extracted": 0,
    "records_validated": 0,
    "s3_uploads": 0,
    "processing_start": datetime.now(timezone.utc),
    "errors": []
}
```

## Monitoring and Logging

### Structured Logging
```python
# Configure structlog for JSON logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
```

### Performance Monitoring
```python
# Track processing statistics
logger.info("processing_progress",
           processed=stats["records_processed"],
           written=stats["records_written"],
           plan=file_info["plan_name"],
           progress_pct=f"{(stats['records_written']/max(stats['records_processed'], 1)*100):.1f}%")
```

## Integration with New Payers

Once a new payer is integrated using Part 1, it automatically works with the production pipeline:

```bash
# New payer will be processed automatically
python production_etl_pipeline.py
```

The pipeline will:
1. Load the new payer from `production_config.yaml`
2. Use the generated handler for processing
3. Apply the same normalization and validation as existing payers
4. Output to the configured storage (local or S3)

## Best Practices

1. **Always test new payers** with sample data before full processing
2. **Monitor memory usage** for large files (>1GB)
3. **Use S3 for production** to handle large datasets efficiently
4. **Review generated handlers** for complex payers
5. **Keep analysis reports** for future reference
6. **Use version control** for handler changes

## Support

For issues with the production pipeline:
1. Check the structured logs for detailed error information
2. Review the payer analysis reports
3. Test individual components manually
4. Monitor system resources during processing 