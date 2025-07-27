# TIC MRF Scraper Utilities

This directory contains utility functions for the TIC MRF Scraper that can be easily used in notebooks and other Python scripts.

## Available Utilities

### 1. Fact Table Builder (`fact_table_builder.py`)

Creates memory-efficient fact tables from healthcare rate data.

#### Quick Usage Examples:

```python
# Import the utilities
from tic_mrf_scraper.utils.fact_table_builder import (
    create_fact_table_from_local_data,
    create_fact_table_from_s3
)

# Create fact table from local data (e.g., ortho_radiology_data_bcbs_la)
fact_table_path = create_fact_table_from_local_data(
    data_dir="ortho_radiology_data_bcbs_la",
    test_mode=True,  # Use small sample for testing
    sample_size=1000,
    nppes_inner_join=False,
    chunk_size=50000,
    output_dir="."  # Output to current directory (default)
)

# Create fact table from S3 data
fact_table_path = create_fact_table_from_s3(
    s3_bucket="commercial-rates",
    s3_prefix="tic-mrf/test",
    test_mode=True,
    sample_size=1000,
    nppes_inner_join=False,
    chunk_size=50000,
    upload_to_s3=False,
    output_dir="."  # Output to current directory (default)
)
```

#### Advanced Usage:

```python
from tic_mrf_scraper.utils.fact_table_builder import FactTableBuilder

# Create builder with custom configuration
builder = FactTableBuilder(
    data_dir="ortho_radiology_data_bcbs_la",
    use_s3=False,
    test_mode=True,
    sample_size=1000,
    nppes_inner_join=False,
    chunk_size=50000,
    upload_to_s3=False
)

# Run the fact table creation
fact_table_path = builder.run_fact_table_creation()
```

### 2. NPPES Backfiller (`nppes_backfiller.py`)

Fetches and manages NPPES (National Provider Identifier) provider information from the NPI Registry API.

#### Quick Usage Examples:

```python
# Import the utilities
from tic_mrf_scraper.utils.nppes_backfiller import (
    backfill_nppes_from_local_data,
    backfill_nppes_from_s3
)

# Backfill NPPES data from local provider data
nppes_manager = backfill_nppes_from_local_data(
    data_dir="ortho_radiology_data_bcbs_la",
    limit=100,  # Only process 100 NPIs for testing
    request_delay=0.1,  # 100ms delay between API requests
    max_retries=3
)

# Backfill NPPES data from S3 provider data
nppes_manager = backfill_nppes_from_s3(
    s3_bucket="commercial-rates",
    s3_prefix="tic-mrf/test",
    limit=100,
    request_delay=0.1,
    max_retries=3
)
```

#### Advanced Usage:

```python
from tic_mrf_scraper.utils.nppes_backfiller import (
    NPPESDataManager,
    create_nppes_config
)

# Create custom NPPES configuration
config = create_nppes_config(
    limit=50,
    request_delay=0.2,
    max_retries=5,
    batch_size=50,
    max_workers=3,
    local_data_dir="ortho_radiology_data_bcbs_la",
    nppes_data_dir="nppes_data"
)

# Create NPPES manager
nppes_manager = NPPESDataManager(config)

# Run the update process
nppes_manager.run_nppes_update(data_dir="ortho_radiology_data_bcbs_la")
```

## Notebook Usage

You can use these utilities directly in Jupyter notebooks. See the example notebook at `examples/fact_table_notebook_example.ipynb` for detailed examples.

### Basic Notebook Setup:

```python
import sys
sys.path.append('../src')

from tic_mrf_scraper.utils.fact_table_builder import create_fact_table_from_local_data
from tic_mrf_scraper.utils.nppes_backfiller import backfill_nppes_from_local_data

# Create fact table from your data directory
fact_table_path = create_fact_table_from_local_data(
    data_dir="ortho_radiology_data_bcbs_la",
    test_mode=True,
    sample_size=1000,
    output_dir="."  # Output to current directory (default)
)

# Backfill NPPES data
nppes_manager = backfill_nppes_from_local_data(
    data_dir="ortho_radiology_data_bcbs_la",
    limit=100
)
```

## Configuration Options

### Fact Table Builder Options:

- `data_dir`: Path to local data directory (e.g., "ortho_radiology_data_bcbs_la")
- `s3_bucket`: S3 bucket name (default: "commercial-rates")
- `s3_prefix`: S3 prefix/path (default: "tic-mrf/test")
- `use_s3`: Whether to use S3 as data source (default: False)
- `test_mode`: Run in test mode with small sample (default: False)
- `sample_size`: Sample size for test mode (default: 1000)
- `nppes_inner_join`: Use inner join for NPPES data (default: False)
- `chunk_size`: Chunk size for processing (default: 50000)
- `upload_to_s3`: Upload results to S3 (default: False)
- `output_dir`: Output directory (defaults to current working directory)

### NPPES Backfiller Options:

- `data_dir`: Path to local data directory
- `s3_bucket`: S3 bucket name (default: "commercial-rates")
- `s3_prefix`: S3 prefix/path (default: "tic-mrf/test")
- `limit`: Limit number of NPIs to process (useful for testing)
- `request_delay`: Delay between API requests in seconds (default: 0.1)
- `max_retries`: Maximum number of retries for failed API requests (default: 3)
- `batch_size`: Batch size for processing (default: 100)
- `max_workers`: Maximum number of worker threads (default: 5)

## Output Files

### Fact Table Builder Output:

- `./memory_efficient_fact_table.parquet`: Main fact table file (in current directory)
- `./memory_efficient_fact_table_summary.json`: Summary statistics
- `./fact_table_s3_location.json`: S3 location metadata (if uploaded)
- `./test/memory_efficient_fact_table.parquet`: Fact table file (when in test mode)

### NPPES Backfiller Output:

- `nppes_data/nppes_providers.parquet`: NPPES provider data
- `nppes_data/nppes_statistics.json`: NPPES dataset statistics
- `nppes_data/failed_npis.json`: List of NPIs that failed to fetch

## Data Directory Structure

For local data, the utilities expect the following structure:

```
ortho_radiology_data_bcbs_la/
├── rates/
│   └── rates_final.parquet
├── organizations/
│   └── organizations_final.parquet
└── providers/
    └── providers_final.parquet
```

## Error Handling

Both utilities include comprehensive error handling and logging:

- API rate limiting for NPPES data fetching
- Memory management for large datasets
- Temporary file cleanup
- Detailed logging of operations
- Graceful handling of missing data

## Performance Tips

1. **Use test mode** for initial testing with small samples
2. **Adjust chunk size** based on available memory
3. **Use appropriate request delays** for API calls to avoid rate limiting
4. **Monitor memory usage** when processing large datasets
5. **Use S3 for large datasets** to avoid local storage limitations 