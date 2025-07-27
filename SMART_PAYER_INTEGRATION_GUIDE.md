# Smart Payer Integration Guide

This guide explains how to use the intelligent payer integration system to automatically add new payers to your production ETL pipeline.

## Overview

The smart payer integration system provides a complete automated workflow for:

1. **Analyzing** new payer MRF structures using `analyze_payer_structure.py`
2. **Intelligently generating** appropriate handlers based on structure patterns
3. **Testing** the integration with sample data
4. **Deploying** to production with configuration updates

## Quick Start

### Step 1: Analyze a New Payer

First, analyze the new payer's structure:

```bash
python scripts/analyze_payer_structure.py --payers "new_payer_name" --config production_config.yaml
```

This will generate analysis files in `payer_structure_analysis/`.

### Step 2: Run Intelligent Integration

Use the intelligent integration script to automatically generate handlers and update configuration:

```bash
python scripts/intelligent_payer_integration.py \
  --analysis-file payer_structure_analysis/full_analysis_YYYYMMDD_HHMMSS.json \
  --payer-name "new_payer_name" \
  --index-url "https://example.com/mrf_index.json"
```

### Step 3: Run Complete Workflow (Recommended)

For the most comprehensive approach, use the smart workflow:

```bash
python scripts/smart_payer_workflow.py \
  --payer-name "new_payer_name" \
  --index-url "https://example.com/mrf_index.json" \
  --auto-deploy
```

## Detailed Workflow

### Phase 1: Analysis

The analysis phase examines:

- **Table of Contents Structure**: Standard vs legacy blobs format
- **MRF File Structure**: Provider groups, rate formats, service codes
- **Compression**: Gzip vs uncompressed files
- **Field Patterns**: Custom field names and structures

### Phase 2: Intelligent Handler Generation

Based on the analysis, the system generates handlers for:

#### Standard Handlers
- No custom requirements
- Uses base `PayerHandler` functionality

#### Moderate Complexity Handlers
- Custom provider group structures
- Rate field mapping (negotiated_price → negotiated_rate)
- Service codes array handling

#### Complex Handlers
- Nested provider structures
- Legacy blob formats
- Multiple custom requirements

### Phase 3: Testing & Validation

The system runs comprehensive tests:

1. **Handler Import Test**: Ensures handler can be loaded
2. **File Listing Test**: Verifies MRF files can be discovered
3. **Sample Processing Test**: Tests record parsing and normalization
4. **Configuration Test**: Validates production config updates

### Phase 4: Production Deployment

Automatic deployment includes:

- Handler file creation in `src/tic_mrf_scraper/payers/`
- Production config updates in `production_config.yaml`
- Integration testing with sample data

## Handler Patterns

### Standard Pattern
```python
@register_handler("standard_payer")
class StandardPayerHandler(PayerHandler):
    """Handler for standard MRF format."""
    
    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [record]
```

### Provider Group Customization
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

### Rate Field Mapping
```python
@register_handler("custom_rate_payer")
class CustomRatePayerHandler(PayerHandler):
    """Handler for payer with custom rate field names."""
    
    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "negotiated_rates" in record:
            for rate_group in record.get("negotiated_rates", []):
                if "negotiated_prices" in rate_group:
                    for price in rate_group["negotiated_prices"]:
                        if "negotiated_price" in price and "negotiated_rate" not in price:
                            price["negotiated_rate"] = price["negotiated_price"]
        return [record]
```

## Configuration Updates

The system automatically updates `production_config.yaml`:

```yaml
payer_endpoints:
  new_payer_name: "https://example.com/mrf_index.json"
```

## Testing Your Integration

### Manual Testing
```bash
# Test handler import
python -c "from tic_mrf_scraper.payers import get_handler; handler = get_handler('new_payer_name')"

# Test file listing
python -c "from tic_mrf_scraper.payers import get_handler; handler = get_handler('new_payer_name'); files = handler.list_mrf_files('https://example.com/mrf_index.json'); print(f'Found {len(files)} files')"
```

### Production Testing
```bash
# Test with production pipeline
python production_etl_pipeline.py --payers new_payer_name --max-files 1
```

## Troubleshooting

### Common Issues

1. **Handler Import Fails**
   - Check that handler file was created in `src/tic_mrf_scraper/payers/`
   - Verify handler class name matches file name
   - Check for syntax errors in generated handler

2. **No Files Found**
   - Verify index URL is accessible
   - Check if payer uses legacy blob structure
   - Review analysis output for file discovery issues

3. **Normalization Fails**
   - Check field mappings in generated handler
   - Review sample data structure
   - Verify CPT whitelist configuration

4. **Configuration Update Fails**
   - Ensure `production_config.yaml` exists
   - Check file permissions
   - Verify YAML syntax

### Debug Mode

Enable detailed logging:

```bash
python scripts/smart_payer_workflow.py \
  --payer-name "new_payer_name" \
  --index-url "https://example.com/mrf_index.json" \
  --work-dir "debug_workflow"
```

Check the work directory for detailed logs and intermediate files.

## Advanced Usage

### Custom Handler Templates

For complex payers, you can create custom handler templates:

```bash
# Create custom template
python scripts/payer_development_workflow.py \
  --payer-name "complex_payer" \
  --index-url "https://example.com/mrf_index.json" \
  --workflow create-handler

# Edit the generated handler
# Then run integration
python scripts/intelligent_payer_integration.py \
  --analysis-file analysis_file.json \
  --payer-name "complex_payer" \
  --index-url "https://example.com/mrf_index.json"
```

### Batch Processing

For multiple payers:

```bash
# Analyze multiple payers
python scripts/analyze_payer_structure.py \
  --payers "payer1" "payer2" "payer3" \
  --config production_config.yaml

# Process each payer
for payer in payer1 payer2 payer3; do
  python scripts/smart_payer_workflow.py \
    --payer-name "$payer" \
    --index-url "https://example.com/${payer}_index.json"
done
```

## Integration with Production Pipeline

Once integrated, the new payer will automatically be processed by:

```bash
python production_etl_pipeline.py
```

The pipeline will:
1. Load the new payer from `production_config.yaml`
2. Use the generated handler for processing
3. Apply the same normalization and validation as existing payers
4. Output to the configured storage (local or S3)

## Monitoring Integration

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

## Best Practices

1. **Always test with sample data** before full integration
2. **Review generated handlers** for complex payers
3. **Monitor processing statistics** after integration
4. **Keep analysis reports** for future reference
5. **Use version control** for handler changes

## Support

For issues with the smart integration system:

1. Check the analysis reports in `smart_payer_workflow/reports/`
2. Review handler generation logs
3. Test individual components manually
4. Consult the troubleshooting section above 