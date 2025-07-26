# MRF Payer Structure Analysis Tools

This repository contains tools for analyzing Machine Readable Files (MRFs) from healthcare payers to understand their structure, scale, and processing requirements.

## Overview

The analysis tools help you:
- **Understand file structures** before processing
- **Estimate processing requirements** and resource needs
- **Identify scaling challenges** and optimization opportunities
- **Validate payer compatibility** with your processing pipeline

## Tools

### 1. `analyze_large_mrfs.py` - Individual File Analysis

Analyzes individual MRF files (Table of Contents or In-Network Rates) with memory-efficient streaming.

### 2. `analyze_payer_structure.py` - Comprehensive Payer Analysis

Analyzes all payers in your configuration to understand their complete structure and scale.

## Prerequisites

```bash
# Install required dependencies
pip install -r requirements.txt

# Additional dependencies for analysis
pip install ijson requests pyyaml
```

## Quick Start

### 1. Analyze a Single Large MRF File

```bash
# Analyze a Table of Contents file
python scripts/analyze_large_mrfs.py https://example.com/index.json.gz --type toc

# Analyze an In-Network Rates file
python scripts/analyze_large_mrfs.py https://example.com/in_network.json.gz --type rates

# Auto-detect file type
python scripts/analyze_large_mrfs.py https://example.com/file.json.gz --type auto
```

### 2. Analyze All Payers in Configuration

```bash
# Analyze all payers in production_config.yaml
python scripts/analyze_payer_structure.py

# Analyze specific payers
python scripts/analyze_payer_structure.py --payers "Blue Cross Blue Shield" "Aetna"

# Skip in-network MRF analysis (faster)
python scripts/analyze_payer_structure.py --skip-mrf
```

## Detailed Usage

### Individual File Analysis (`analyze_large_mrfs.py`)

#### Command Line Options

```bash
python scripts/analyze_large_mrfs.py <file_path_or_url> [options]

Options:
  --sample-size INT     Number of items to sample (default: 1000)
  --output PATH         Output JSON file path
  --type {auto,toc,rates}  File type (default: auto)
```

#### Examples

```bash
# Analyze local file
python scripts/analyze_large_mrfs.py ./data/index.json --type toc --sample-size 500

# Analyze remote file with custom output
python scripts/analyze_large_mrfs.py https://payer.com/index.json.gz \
  --type auto \
  --output ./analysis/payer_analysis.json

# Quick analysis with small sample
python scripts/analyze_large_mrfs.py https://payer.com/rates.json.gz \
  --type rates \
  --sample-size 100
```

#### Output Structure

The script generates a JSON file with:

```json
{
  "file_path": "https://example.com/file.json.gz",
  "analysis_time": "2024-01-15T10:30:00",
  "sample_size": 1000,
  "structure": {
    "file_size_mb": 245.67,
    "compression": "gzip",
    "structure_type": "table_of_contents",
    "top_level_keys": ["reporting_structure", "version"]
  },
  "table_of_contents": {
    "total_reporting_structures": 15000,
    "file_counts": {
      "in_network": 45000,
      "allowed_amounts": 15000
    },
    "url_patterns": ["cdn.payer.com/mrf/2024/", "s3.amazonaws.com/payer-data/"]
  }
}
```

### Comprehensive Payer Analysis (`analyze_payer_structure.py`)

#### Command Line Options

```bash
python scripts/analyze_payer_structure.py [options]

Options:
  --config PATH         Path to config file (default: production_config.yaml)
  --payers PAYER [PAYER ...]  Specific payers to analyze
  --skip-mrf           Skip in-network MRF analysis
  --output-dir PATH    Output directory (default: payer_structure_analysis)
```

#### Examples

```bash
# Full analysis of all payers
python scripts/analyze_payer_structure.py

# Quick analysis (skip MRF files)
python scripts/analyze_payer_structure.py --skip-mrf

# Analyze specific payers
python scripts/analyze_payer_structure.py \
  --payers "Blue Cross Blue Shield" "Aetna" "UnitedHealth"

# Use custom config
python scripts/analyze_payer_structure.py \
  --config custom_config.yaml \
  --output-dir ./custom_analysis
```

#### Output Structure

The script creates two files:

1. **Full Analysis JSON** (`full_analysis_YYYYMMDD_HHMMSS.json`)
2. **Summary Text** (`summary_YYYYMMDD_HHMMSS.txt`)

Example summary output:
```
================================================================================
PAYER STRUCTURE ANALYSIS SUMMARY
Generated: 2024-01-15 10:30:00
================================================================================

============================================================
PAYER: Blue Cross Blue Shield
============================================================

[TABLE OF CONTENTS STRUCTURE]
  - Structure Type: standard_toc
  - Top Level Keys: reporting_structure, version
  - File Counts: {"reporting_structures": 15000, "in_network_files": 45000}
  - Sample Plan Keys: plan_name, plan_id, in_network_files

[IN-NETWORK MRF STRUCTURE]
  - Structure Type: standard_in_network
  - Total Items: 2500000
  - Billing Code Types: {"CPT": 1800000, "HCPCS": 700000}

  Sample Item Structure:
    - Item Keys: billing_code, billing_code_type, negotiated_rates
    - Rate Keys: provider_groups, negotiated_prices
    - Has Provider Groups: True
    - Provider Group Keys: npi, tin, providers
    - Provider Info Location: Direct in provider_group
    - Price Keys: negotiated_rate, service_code
    - Rate Field Name: negotiated_rate
```

## Understanding the Analysis Results

### File Structure Types

#### Table of Contents Files
- **`standard_toc`**: Standard MRF index with `reporting_structure` array
- **`legacy_blobs`**: Legacy format with `blobs` array
- **`direct_in_network`**: Direct `in_network_files` array

#### In-Network Rate Files
- **`standard_in_network`**: Standard format with `in_network` array
- **`custom_format`**: Non-standard structure

### Key Metrics to Monitor

#### Scale Indicators
- **File Size**: >100MB indicates large datasets
- **Reporting Structures**: >10,000 suggests complex payer organization
- **In-Network Items**: >1M items requires streaming processing
- **File Counts**: High counts indicate distributed data

#### Structure Complexity
- **Provider References**: External provider files add complexity
- **Nested Providers**: Providers in arrays vs. direct NPIs
- **Rate Variations**: Different rate field names across payers
- **Billing Code Types**: Mix of CPT, HCPCS, ICD codes

### Scaling Considerations

#### Memory Requirements
```bash
# Estimate memory needs
file_size_mb * 3-5x = estimated_processing_memory_mb

# Example: 500MB file ≈ 2-2.5GB processing memory
```

#### Processing Time Estimates
```bash
# Rough time estimates
items_per_second = 1000-5000 (depending on complexity)
estimated_time = total_items / items_per_second

# Example: 2M items ≈ 7-33 minutes
```

#### Storage Requirements
```bash
# Parquet compression ratios
json_size * 0.1-0.3 = estimated_parquet_size

# Example: 500MB JSON ≈ 50-150MB Parquet
```

## Best Practices

### 1. Start with Small Samples
```bash
# Quick structure analysis
python scripts/analyze_large_mrfs.py <url> --sample-size 100 --type auto
```

### 2. Validate Payer Compatibility
```bash
# Check if payer uses standard format
python scripts/analyze_payer_structure.py --payers <payer_name>
```

### 3. Monitor Resource Usage
```bash
# Use system monitoring during analysis
htop  # or top
iotop # for I/O monitoring
```

### 4. Plan for Large Files
```bash
# For files >1GB, consider:
# - Streaming processing only
# - Chunked downloads
# - Distributed processing
```

## Troubleshooting

### Common Issues

#### Memory Errors
```bash
# Reduce sample size
python scripts/analyze_large_mrfs.py <url> --sample-size 100

# Use skip-mrf for TOC analysis only
python scripts/analyze_payer_structure.py --skip-mrf
```

#### Network Timeouts
```bash
# Large files may timeout - check file size first
curl -I <url>  # Check Content-Length header
```

#### Unsupported Formats
```bash
# Check structure type in output
# Non-standard formats may need custom handlers
```

### Error Messages

- **"File too large"**: Reduce `max_size_mb` parameter or use streaming
- **"Failed to fetch"**: Check URL accessibility and network connection
- **"Unknown file type"**: File may use non-standard MRF format

## Integration with Processing Pipeline

### Pre-Processing Validation
```bash
# Validate before processing
python scripts/analyze_payer_structure.py --payers <payer>
# Check output for compatibility issues
```

### Resource Planning
```bash
# Estimate resource needs
python scripts/analyze_large_mrfs.py <url> --type auto
# Use file_size_mb and item counts for capacity planning
```

### Monitoring Processing
```bash
# Compare expected vs actual processing times
# Monitor memory usage during processing
# Validate output structure matches analysis
```

## Advanced Usage

### Custom Analysis Scripts
```python
# Use analysis functions in your own scripts
from scripts.analyze_large_mrfs import analyze_file_structure
from scripts.analyze_payer_structure import analyze_table_of_contents

# Analyze structure programmatically
structure = analyze_file_structure("https://example.com/file.json.gz")
```

### Batch Processing
```bash
# Analyze multiple files
for url in $(cat urls.txt); do
    python scripts/analyze_large_mrfs.py "$url" --type auto
done
```

### Integration with CI/CD
```bash
# Add to deployment pipeline
python scripts/analyze_payer_structure.py --skip-mrf
# Fail deployment if incompatible payers detected
```

## Contributing

When adding new payers or formats:

1. **Analyze structure first** using these tools
2. **Document format variations** in payer-specific handlers
3. **Test with sample data** before full processing
4. **Update analysis tools** if new patterns discovered

## Support

For issues or questions:
1. Check the analysis output for error details
2. Review the troubleshooting section
3. Examine the payer's MRF documentation
4. Contact the development team with analysis results 