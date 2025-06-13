# Healthcare Rates ETL Pipeline

A production-grade ETL pipeline for processing healthcare rates data from Machine Readable Files (MRFs) with full index processing and S3 integration.

## Overview

This ETL pipeline processes healthcare rates data from various payer Machine Readable Files (MRFs), normalizes the data, performs quality validation, and stores the results in either S3 or local storage. The pipeline is designed to handle large-scale data processing with memory efficiency and robust error handling.

## Features

- Full index processing of MRF files
- Memory-efficient streaming processing
- Data quality validation and scoring
- Deterministic UUID generation for entity identification
- S3 integration with organized partitioning
- Local storage fallback
- Comprehensive logging and progress tracking
- Analytics table generation
- Configurable processing parameters
- Optional limits to stop after a set number of files or records
- Parallel processing support

## Prerequisites

- Python 3.6+
- AWS credentials (for S3 integration)
- Required Python packages (see `requirements.txt`)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd <repository-directory>
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials (if using S3):
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export S3_BUCKET=your_bucket_name
```

## Repository Structure

- `src/tic_mrf_scraper/` - core Python package with the ETL modules
- `scripts/` - helper scripts for debugging and validation
- `tests/` - pytest suite verifying parser and writer logic

## Configuration

The pipeline is configured using a YAML file (`production_config.yaml`). Here's the structure:

```yaml
payer_endpoints:
  payer_name: index_url

cpt_whitelist:
  - code1
  - code2

processing:
  batch_size: 10000
  parallel_workers: 2
  max_files_per_payer: 5
  max_records_per_file: 100000
  min_completeness_pct: 80.0
  min_accuracy_score: 0.85

output:
  local_directory: "production_data"
  s3:
    prefix: "healthcare-rates-v2"

versioning:
  schema_version: "v2.1.0"
  processing_version: "tic-etl-v1.0"
```

`max_files_per_payer` and `max_records_per_file` can be used to limit how much
data is processed during a run. When either limit is reached the pipeline stops
processing additional files or records for that payer.

## Usage

Run the pipeline:

```bash
python production_etl_pipeline.py
```

## Data Model

The pipeline processes and generates the following data types:

### Rates
- Rate UUID
- Payer UUID
- Organization UUID
- Service code and description
- Negotiated rate
- Billing class and type
- Plan details
- Contract period
- Provider network
- Geographic scope
- Data lineage

### Organizations
- Organization UUID
- TIN
- Organization name
- Organization type
- NPI count
- Facility status
- Address information
- Service areas
- Data quality metrics

### Providers
- Provider UUID
- NPI
- Organization UUID
- Provider details
- Credentials
- Specialties
- Addresses
- Status information

### Analytics
- Analytics UUID
- Service code
- Geographic scope
- Market statistics
- Payer analysis
- Trend analysis
- Computation metadata

## Output Structure

### S3 Storage
```
s3://<bucket>/<prefix>/
├── rates/
│   └── payer=<payer_name>/
│       └── date=<YYYY-MM-DD>/
├── organizations/
│   └── payer=<payer_name>/
│       └── date=<YYYY-MM-DD>/
├── providers/
│   └── payer=<payer_name>/
│       └── date=<YYYY-MM-DD>/
└── processing_statistics/
    └── <YYYY-MM-DD>/
```

### Local Storage
```
production_data/
├── rates/
├── organizations/
├── providers/
├── analytics/
└── processing_statistics.json
```

## Data Quality

The pipeline implements several data quality checks:

- Required field validation
- Rate reasonableness checks
- NPI validation
- Completeness scoring
- Confidence scoring
- Data lineage tracking

## Logging

The pipeline uses structured logging with the following features:

- JSON-formatted logs
- Timestamp and log level
- Contextual information
- Error tracking
- Processing statistics

## Error Handling

The pipeline implements robust error handling:

- Graceful failure recovery
- Detailed error logging
- Failed file tracking
- Processing statistics collection
- Temporary file cleanup

## Performance Considerations

- Memory-efficient streaming processing
- Batch processing for S3 uploads
- Configurable batch sizes
- Parallel processing support
- Progress tracking and ETA calculation

## Payer Handler Plugins

The pipeline supports a pluggable handler system for payer-specific logic. Each handler subclassing `BasePayerHandler` can modify records before they are written. Handlers are discovered via the `tic_mrf_scraper.payer_handlers` entry point group.

To add your own handler:
1. Create `src/tic_mrf_scraper/handlers/my_handler.py` and define `class MyHandler(BasePayerHandler)`.
2. Register it in `pyproject.toml`:

```toml
[tool.poetry.plugins."tic_mrf_scraper.payer_handlers"]
my_payer = "tic_mrf_scraper.handlers.my_handler:MyHandler"
```

3. Reinstall the package with `pip install -e .` to load the plugin.

### Built-in payer modules

Handlers included with the project live in `src/tic_mrf_scraper/payers`. The file name of the module
is used as a **format identifier**. This identifier appears in `production_config.yaml` under
`payer_endpoints` and determines which handler processes an index. Each module should declare a
subclass of `PayerHandler` decorated with `@register_handler("<identifier>")`. Override
`parse_in_network` whenever the payer's MRF format deviates from the default structure. See
`src/tic_mrf_scraper/payers/example.py` for a minimal template.
## Running Tests

1. Install development dependencies with `pip install -r requirements.txt` (or `poetry install`).
2. From the repository root run:
```bash
pytest
```


## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes and run `pytest`
4. Commit the updates
5. Push the branch and open a Pull Request

## License

[Specify your license here]

## Support

For support, please [specify contact information or support channels] 