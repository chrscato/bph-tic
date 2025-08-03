# BCBS MI Handler Guide

## Overview

The BCBS MI handler is based on the Centene handler but adapted for Blue Cross Blue Shield of Michigan's specific MRF structure. The key difference is that BCBS MI uses `provider_references` instead of embedded `provider_groups`.

## Key Differences from Centene

### Centene Structure
```json
{
  "negotiated_rates": [
    {
      "negotiated_prices": [...],
      "provider_groups": [  // Embedded provider groups
        {
          "npi": ["1234567890"],
          "tin": {"value": "12-3456789"},
          "name": "Provider Name"
        }
      ]
    }
  ]
}
```

### BCBS MI Structure
```json
{
  "negotiated_rates": [
    {
      "negotiated_prices": [...],
      "provider_references": ["provider_group_001"]  // Reference IDs
    }
  ],
  "provider_references": [  // Separate section at top level
    {
      "provider_group_id": "provider_group_001",
      "provider_groups": [
        {
          "npi": ["1234567890"],
          "tin": {"value": "12-3456789"},
          "name": "Provider Name"
        }
      ]
    }
  ]
}
```

## How to Use the BCBS MI Handler

### 1. Basic Usage

```python
from src.tic_mrf_scraper.payers.bcbs_mi import BCBSMIHandler

# Create handler
handler = BCBSMIHandler()

# Preprocess MRF file to build provider references cache
handler.preprocess_mrf_file(mrf_data)

# Parse in-network records
for record in mrf_data["in_network"]:
    results = handler.parse_in_network(record)
    # Process results...
```

### 2. Provider References Processing

The handler automatically:
1. **Preprocesses** the MRF file to extract provider information from the `provider_references` section
2. **Caches** provider information for fast lookup during record processing
3. **Maps** provider reference IDs to actual provider details (NPI, name, TIN)

### 3. Integration with Pipeline

To integrate with the existing pipeline:

1. **Add BCBS MI to config**:
```yaml
payer_endpoints:
  bcbs_mi: https://bcbsm.sapphiremrfhub.com/tocs/current/blue_cross_blue_shield_of_michigan
```

2. **Register the handler** (already done in the handler file):
```python
@register_handler("bcbs_mi")
@register_handler("bcbsm")
class BCBSMIHandler(PayerHandler):
```

3. **Use in pipeline**:
```python
# The pipeline will automatically use the BCBS MI handler
# when processing bcbs_mi payer data
```

## Structure Analysis Results

From the analysis of BCBS MI:
- **File Counts**: 669 in-network files, 7 allowed amount files
- **Structure**: Standard TOC with `in_network_files` (unlike Cigna)
- **Provider References**: 1,048 provider references in sample file
- **Billing Codes**: Uses HCPCS codes
- **Rate Structure**: Uses `negotiated_rate` (not `negotiated_price`)

## Comparison with Other Payers

| Payer | In-Network Files | Provider Structure | Handler Type |
|-------|------------------|-------------------|--------------|
| **BCBS MI** | ✅ 669 files | `provider_references` | Standard |
| **Centene** | ✅ Multiple files | `embedded provider_groups` | Embedded |
| **Cigna** | ❌ 0 files | `allowed_amount_file` only | Custom needed |

## Testing

Use the provided test script:
```bash
python test_bcbs_mi_handler.py
```

This will test the handler with sample data based on the actual BCBS MI structure.

## Key Features

1. **Provider Reference Caching**: Efficiently caches provider information for fast lookup
2. **Standard MRF Compatibility**: Works with standard MRF structure
3. **Rate Processing**: Handles both direct rates and complex negotiated rates
4. **Service Code Support**: Properly processes service codes and billing classes
5. **Error Handling**: Graceful handling of missing or malformed data

## Next Steps

1. **Test with real BCBS MI data** to validate the handler
2. **Add BCBS MI to production config** for processing
3. **Monitor performance** with large BCBS MI files
4. **Consider optimizations** for memory usage with 669+ files 