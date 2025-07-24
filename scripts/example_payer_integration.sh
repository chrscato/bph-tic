#!/bin/bash
# Example Payer Integration - Complete workflow for adding a new payer

set -e  # Exit on any error

# Configuration
PAYER_NAME="example_payer"
INDEX_URL="https://example.com/mrf_index.json"
WORK_DIR="payer_development"

echo "🚀 Starting Payer Integration Workflow"
echo "====================================="
echo "Payer: $PAYER_NAME"
echo "Index URL: $INDEX_URL"
echo ""

# Step 1: Analyze the new payer
echo "📋 Step 1: Analyzing payer structure..."
python scripts/payer_development_workflow.py \
  --payer-name "$PAYER_NAME" \
  --index-url "$INDEX_URL" \
  --workflow analyze \
  --sample-size 3

echo "✅ Analysis complete. Check $WORK_DIR/samples/$PAYER_NAME/"
echo ""

# Step 2: Create handler template
echo "📝 Step 2: Creating handler template..."
python scripts/payer_development_workflow.py \
  --payer-name "$PAYER_NAME" \
  --index-url "$INDEX_URL" \
  --workflow create-handler

echo "✅ Handler template created. Edit $WORK_DIR/handlers/${PAYER_NAME}_handler.py"
echo ""

# Step 3: Quick test before customization
echo "🧪 Step 3: Quick test of basic handler..."
python scripts/quick_payer_test.py \
  --payer-name "$PAYER_NAME" \
  --index-url "$INDEX_URL" \
  --test-type handler \
  --max-files 1 \
  --max-records 20

echo "✅ Quick test complete."
echo ""

# Step 4: Instructions for customization
echo "📝 Step 4: Customize the handler"
echo "================================="
echo "1. Edit the handler file: $WORK_DIR/handlers/${PAYER_NAME}_handler.py"
echo "2. Add custom parsing logic based on the analysis report"
echo "3. Test your changes with:"
echo "   python scripts/quick_payer_test.py --payer-name \"$PAYER_NAME\" --index-url \"$INDEX_URL\""
echo ""

# Step 5: Full testing (commented out - uncomment when ready)
echo "🧪 Step 5: Full testing (uncomment when ready to test)"
echo "# python scripts/payer_development_workflow.py \\"
echo "#   --payer-name \"$PAYER_NAME\" \\"
echo "#   --index-url \"$INDEX_URL\" \\"
echo "#   --workflow test"
echo ""

# Step 6: Integration (commented out - uncomment when ready)
echo "🚀 Step 6: Production integration (uncomment when ready)"
echo "# python scripts/payer_development_workflow.py \\"
echo "#   --payer-name \"$PAYER_NAME\" \\"
echo "#   --index-url \"$INDEX_URL\" \\"
echo "#   --workflow integrate"
echo ""

# Step 7: Production run (commented out - uncomment when ready)
echo "🏭 Step 7: Production run (uncomment when ready)"
echo "# python production_etl_pipeline.py"
echo ""

echo "📋 Next Steps:"
echo "=============="
echo "1. Review the analysis report in $WORK_DIR/samples/$PAYER_NAME/"
echo "2. Customize the handler based on the recommendations"
echo "3. Test your changes with the quick test script"
echo "4. Run full testing when satisfied"
echo "5. Integrate to production when ready"
echo "6. Monitor the first production run"
echo ""

echo "📁 Generated Files:"
echo "==================="
echo "- Analysis report: $WORK_DIR/samples/$PAYER_NAME/${PAYER_NAME}_analysis.json"
echo "- Handler template: $WORK_DIR/handlers/${PAYER_NAME}_handler.py"
echo "- Test results: quick_test_${PAYER_NAME}_*.json"
echo ""

echo "🎉 Workflow setup complete!" 