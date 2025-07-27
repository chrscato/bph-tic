#!/bin/bash
# Example Smart Payer Integration - Complete workflow demonstration

set -e  # Exit on any error

# Configuration
PAYER_NAME="example_smart_payer"
INDEX_URL="https://example.com/mrf_index.json"
WORK_DIR="example_smart_integration"

echo "🚀 Smart Payer Integration Example"
echo "=================================="
echo "Payer: $PAYER_NAME"
echo "Index URL: $INDEX_URL"
echo "Work directory: $WORK_DIR"
echo ""

# Step 1: Run complete smart workflow
echo "📋 Step 1: Running Smart Payer Workflow..."
python scripts/smart_payer_workflow.py \
  --payer-name "$PAYER_NAME" \
  --index-url "$INDEX_URL" \
  --work-dir "$WORK_DIR" \
  --auto-deploy

echo "✅ Smart workflow complete!"
echo ""

# Step 2: Verify integration
echo "🔍 Step 2: Verifying Integration..."
echo ""

# Test handler import
echo "Testing handler import..."
python -c "
from tic_mrf_scraper.payers import get_handler
try:
    handler = get_handler('$PAYER_NAME')
    print(f'✅ Handler imported successfully: {handler.__class__.__name__}')
except Exception as e:
    print(f'❌ Handler import failed: {e}')
"

echo ""

# Test file listing
echo "Testing file listing..."
python -c "
from tic_mrf_scraper.payers import get_handler
try:
    handler = get_handler('$PAYER_NAME')
    files = handler.list_mrf_files('$INDEX_URL')
    rate_files = [f for f in files if f['type'] == 'in_network_rates']
    print(f'✅ File listing successful: {len(rate_files)} rate files found')
except Exception as e:
    print(f'❌ File listing failed: {e}')
"

echo ""

# Test production config
echo "Testing production config..."
python -c "
import yaml
try:
    with open('production_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    if '$PAYER_NAME' in config.get('payer_endpoints', {}):
        print(f'✅ Payer found in production config')
    else:
        print(f'❌ Payer not found in production config')
except Exception as e:
    print(f'❌ Config check failed: {e}')
"

echo ""

# Step 3: Show results
echo "📊 Step 3: Integration Results"
echo "================================"

if [ -d "$WORK_DIR/reports" ]; then
    echo "Reports available in: $WORK_DIR/reports/"
    ls -la "$WORK_DIR/reports/"
    echo ""
    
    # Show final report if available
    FINAL_REPORT="$WORK_DIR/reports/${PAYER_NAME}_final_report.json"
    if [ -f "$FINAL_REPORT" ]; then
        echo "Final Report Summary:"
        python -c "
import json
with open('$FINAL_REPORT', 'r') as f:
    report = json.load(f)
summary = report.get('summary', {})
print(f'Analysis success: {\"✅\" if summary.get(\"analysis_success\") else \"❌\"}')
print(f'Integration success: {\"✅\" if summary.get(\"integration_success\") else \"❌\"}')
print(f'Validation success: {\"✅\" if summary.get(\"validation_success\") else \"❌\"}')
print(f'Overall success: {\"✅\" if summary.get(\"overall_success\") else \"❌\"}')
"
    fi
fi

echo ""
echo "🎉 Smart Payer Integration Example Complete!"
echo ""
echo "Next Steps:"
echo "1. Review the generated handler in src/tic_mrf_scraper/payers/$PAYER_NAME.py"
echo "2. Test with production pipeline: python production_etl_pipeline.py"
echo "3. Monitor processing results in the configured output directory"
echo ""
echo "For troubleshooting, check:"
echo "- $WORK_DIR/reports/ for detailed analysis"
echo "- src/tic_mrf_scraper/payers/$PAYER_NAME.py for generated handler"
echo "- production_config.yaml for configuration updates" 