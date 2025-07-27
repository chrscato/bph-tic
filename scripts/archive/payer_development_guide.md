# Payer Development Guide

This guide provides a systematic approach to adding new payers to the healthcare rates ETL pipeline.

## Overview

The ETL pipeline is designed to handle different MRF (Machine Readable File) formats from various payers. Each payer may have slightly different structures, so we use a modular handler system to accommodate these differences.

## Development Workflow

### Step 1: Analyze the New Payer

First, analyze the new payer's MRF structure to understand its format:

```bash
python scripts/payer_development_workflow.py \
  --payer-name "new_payer" \
  --index-url "https://example.com/mrf_index.json" \
  --workflow analyze
```

This will:
- Download and analyze sample MRF files
- Identify structure patterns
- Generate recommendations
- Create a detailed analysis report

### Step 2: Create a Custom Handler

Based on the analysis, create a custom handler:

```bash
python scripts/payer_development_workflow.py \
  --payer-name "new_payer" \
  --index-url "https://example.com/mrf_index.json" \
  --workflow create-handler
```

This generates a template handler file in `payer_development/handlers/new_payer_handler.py`.

### Step 3: Customize the Handler

Edit the generated handler to handle payer-specific requirements:

```python
@register_handler("new_payer")
class NewPayerHandler(PayerHandler):
    """Handler for New Payer MRF files."""
    
    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Custom parsing logic here
        return [record]
```

Common customizations:
- **Provider group structure**: Handle non-standard provider group formats
- **Rate formatting**: Normalize different rate representations
- **Field mappings**: Map payer-specific fields to standard format
- **Compression**: Handle special compression formats

### Step 4: Test the Handler

Test your handler with sample data:

```bash
python scripts/payer_development_workflow.py \
  --payer-name "new_payer" \
  --index-url "https://example.com/mrf_index.json" \
  --workflow test
```

This will:
- Test the handler with sample files
- Validate output structure
- Generate test reports
- Calculate success rates

### Step 5: Integrate to Production

Once testing is successful, integrate to production:

```bash
python scripts/payer_development_workflow.py \
  --payer-name "new_payer" \
  --index-url "https://example.com/mrf_index.json" \
  --workflow integrate
```

This will:
- Copy the handler to `src/tic_mrf_scraper/payers/`
- Update `production_config.yaml` with the new payer
- Test the integration

## Complete Workflow

Run the entire workflow in one command:

```bash
python scripts/payer_development_workflow.py \
  --payer-name "new_payer" \
  --index-url "https://example.com/mrf_index.json" \
  --workflow full \
  --auto-integrate
```

## Handler Development Patterns

### Standard Handler (No Customization Needed)

If the payer follows standard MRF format:

```python
@register_handler("standard_payer")
class StandardPayerHandler(PayerHandler):
    """Handler for standard MRF format."""
    
    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        # No customization needed
        return [record]
```

### Custom Provider Group Structure

If the payer has non-standard provider groups:

```python
@register_handler("custom_provider_payer")
class CustomProviderPayerHandler(PayerHandler):
    """Handler for payer with custom provider group structure."""
    
    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "in_network" in record:
            for item in record["in_network"]:
                if "provider_groups" in item:
                    normalized_groups = []
                    for pg in item["provider_groups"]:
                        # Custom provider group processing
                        if "custom_field" in pg:
                            normalized_groups.append({
                                "npi": pg["custom_field"],
                                "tin": pg.get("tin", "")
                            })
                        else:
                            normalized_groups.append(pg)
                    item["provider_groups"] = normalized_groups
        return [record]
```

### Custom Rate Structure

If the payer has non-standard rate formats:

```python
@register_handler("custom_rate_payer")
class CustomRatePayerHandler(PayerHandler):
    """Handler for payer with custom rate structure."""
    
    def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        if "in_network" in record:
            for item in record["in_network"]:
                if "negotiated_rates" in item:
                    for rate in item["negotiated_rates"]:
                        # Custom rate processing
                        if "custom_rate_field" in rate:
                            rate["negotiated_rate"] = rate["custom_rate_field"]
                        if "custom_service_code" in rate:
                            rate["service_code"] = rate["custom_service_code"]
        return [record]
```

## Testing Your Handler

### Manual Testing

Test your handler manually:

```python
from tic_mrf_scraper.payers import get_handler
from tic_mrf_scraper.stream.parser import stream_parse_enhanced

# Get your handler
handler = get_handler("new_payer")

# Test with a sample file
for record in stream_parse_enhanced("sample_file_url", max_records=5):
    processed = handler.parse_in_network(record)
    print(f"Processed {len(processed)} records")
```

### Automated Testing

Use the workflow testing:

```bash
python scripts/payer_development_workflow.py \
  --payer-name "new_payer" \
  --index-url "https://example.com/mrf_index.json" \
  --workflow test \
  --test-size 10
```

## Common Issues and Solutions

### Issue: Handler Not Found

**Solution**: Ensure the handler is properly registered:

```python
@register_handler("payer_name")  # Must match config
class PayerHandler(PayerHandler):
    pass
```

### Issue: Parsing Errors

**Solution**: Add error handling to your handler:

```python
def parse_in_network(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        # Your parsing logic
        return [record]
    except Exception as e:
        logger.warning(f"Parsing error: {e}")
        return [record]  # Return original if parsing fails
```

### Issue: Non-Standard Compression

**Solution**: Override the file handling in your handler:

```python
def list_mrf_files(self, index_url: str) -> List[Dict[str, Any]]:
    # Custom file listing logic
    return custom_list_mrf_files(index_url)
```

## Production Integration Checklist

Before integrating to production:

- [ ] Handler passes all tests
- [ ] Success rate > 95%
- [ ] No critical errors in test reports
- [ ] Handler follows naming conventions
- [ ] Config file updated with payer endpoint
- [ ] Documentation updated

## Monitoring and Maintenance

After integration:

1. **Monitor the first production run**:
   ```bash
   python production_etl_pipeline.py
   ```

2. **Check processing statistics**:
   ```bash
   cat production_data/processing_statistics.json
   ```

3. **Review error logs**:
   ```bash
   tail -f logs/etl_*.log
   ```

## Best Practices

1. **Start with analysis**: Always analyze the payer's structure first
2. **Test thoroughly**: Use multiple sample files for testing
3. **Handle errors gracefully**: Don't let parsing errors crash the pipeline
4. **Document customizations**: Comment your handler code clearly
5. **Follow naming conventions**: Use consistent payer names
6. **Monitor performance**: Watch for processing rate drops

## Troubleshooting

### Handler Not Loading

Check that:
- Handler file is in `src/tic_mrf_scraper/payers/`
- Handler class is properly decorated with `@register_handler`
- Payer name in config matches handler registration

### Parsing Failures

Common causes:
- Non-standard JSON structure
- Missing required fields
- Custom compression format
- Large file sizes

### Performance Issues

Solutions:
- Reduce batch sizes
- Add more error handling
- Optimize parsing logic
- Use streaming for large files

## Support

For issues with payer development:

1. Check the analysis reports in `payer_development/reports/`
2. Review test results in `payer_development/tests/`
3. Examine error logs in `logs/`
4. Consult the existing handler examples in `src/tic_mrf_scraper/payers/` 