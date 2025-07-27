#!/bin/bash
# Repository Cleanup Script
# This script safely removes deprecated files and organizes the repository

set -e  # Exit on any error

echo "🧹 TiC Repository Cleanup"
echo "========================="
echo ""

# Create backup directory
BACKUP_DIR="cleanup_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

echo "📦 Creating backup in: $BACKUP_DIR"
echo ""

# Function to safely move files to backup
backup_and_remove() {
    local source="$1"
    local description="$2"
    
    if [ -e "$source" ]; then
        echo "🗑️  Removing: $source ($description)"
        mkdir -p "$BACKUP_DIR/$(dirname "$source")"
        mv "$source" "$BACKUP_DIR/$source"
    else
        echo "⚠️  Not found: $source"
    fi
}

# Function to safely remove directories
backup_and_remove_dir() {
    local source="$1"
    local description="$2"
    
    if [ -d "$source" ]; then
        echo "🗑️  Removing directory: $source ($description)"
        mkdir -p "$BACKUP_DIR/$(dirname "$source")"
        mv "$source" "$BACKUP_DIR/$source"
    else
        echo "⚠️  Directory not found: $source"
    fi
}

echo "🔍 Phase 1: Removing deprecated test files"
echo "-------------------------------------------"

# Remove deprecated test files
backup_and_remove "test_mvp.py" "Old MVP test"
backup_and_remove "test_handlers.py" "Old handler test"
backup_and_remove "test_s3_pipeline.py" "Old S3 test"
backup_and_remove "test_pipeline_mini.py" "Mini pipeline test"
backup_and_remove "test_cpt_matching.py" "CPT matching test"
backup_and_remove "test_fact_table_s3.py" "Fact table S3 test"
backup_and_remove "test_s3_connection.py" "S3 connection test"

echo ""
echo "🔍 Phase 2: Removing old configuration files"
echo "---------------------------------------------"

# Remove old configuration files
backup_and_remove "config.yaml" "Old config"
backup_and_remove "config_backup.yaml" "Config backup"
backup_and_remove "production_config_backup.yaml" "Production config backup"
backup_and_remove "production_config_sample.yaml" "Production config sample"

echo ""
echo "🔍 Phase 3: Removing generated output directories"
echo "------------------------------------------------"

# Remove generated output directories
backup_and_remove_dir "ortho_radiology_data_default" "Ortho radiology data"
backup_and_remove_dir "centene_fidelis" "Centene data"
backup_and_remove_dir "debug_output" "Debug output"
backup_and_remove_dir "json_analysis" "JSON analysis"
backup_and_remove_dir "mrf_analysis" "MRF analysis"
backup_and_remove_dir "structure_analysis" "Structure analysis"
backup_and_remove_dir "payer_structure_analysis" "Payer structure analysis"
backup_and_remove_dir "data_inspection" "Data inspection"
backup_and_remove_dir "data_inspection_production" "Production data inspection"
backup_and_remove_dir "enriched_provider_data" "Enriched provider data"
backup_and_remove_dir "nppes_data" "NPPES data"
backup_and_remove_dir "ortho_radiology_data" "Ortho radiology data"
backup_and_remove_dir "output" "General output"
backup_and_remove_dir "production_data" "Production data"
backup_and_remove_dir "sample_data" "Sample data"
backup_and_remove_dir "temp_s3_downloads" "Temporary S3 downloads"
backup_and_remove_dir "test_output" "Test output"

echo ""
echo "🔍 Phase 4: Removing log files"
echo "-------------------------------"

# Remove log files
backup_and_remove_dir "logs" "Log directory"
find . -name "*.log" -type f -exec echo "🗑️  Removing log file: {}" \; -exec mv {} "$BACKUP_DIR/" \;

echo ""
echo "🔍 Phase 5: Removing analysis reports"
echo "-------------------------------------"

# Remove analysis reports
find . -name "*_analysis_*.json" -type f -exec echo "🗑️  Removing analysis JSON: {}" \; -exec mv {} "$BACKUP_DIR/" \;
find . -name "*_analysis_*.txt" -type f -exec echo "🗑️  Removing analysis TXT: {}" \; -exec mv {} "$BACKUP_DIR/" \;
find . -name "quick_test_*.json" -type f -exec echo "🗑️  Removing quick test: {}" \; -exec mv {} "$BACKUP_DIR/" \;

echo ""
echo "🔍 Phase 6: Removing temporary files"
echo "------------------------------------"

# Remove temporary files
backup_and_remove "et --soft 7b65119" "Git temporary file"
backup_and_remove "parq_inspect.py" "Temporary inspection script"

echo ""
echo "🔍 Phase 7: Removing legacy scripts (with confirmation)"
echo "-------------------------------------------------------"

# List legacy scripts for confirmation
LEGACY_SCRIPTS=(
    "scripts/analyze_inspection_findings.py"
    "scripts/analyze_nppes_data.py"
    "scripts/analyze_rate_bucketing.py"
    "scripts/backfill_provider_info.py"
    "scripts/check_nppes_data.py"
    "scripts/compare_payer_formats.py"
    "scripts/comprehensive_mrf_inspector.py"
    "scripts/create_fact_table_parquet.py"
    "scripts/create_joined_analysis_parquet.py"
    "scripts/create_memory_efficient_fact_table.py"
    "scripts/create_s3_streaming_fact_table.py"
    "scripts/create_simple_fact_table.py"
    "scripts/debug_nppes_join.py"
    "scripts/example_payer_integration.sh"
    "scripts/inspect_actual_mrf.py"
    "scripts/inspect_comprehensive_analysis.py"
    "scripts/inspect_core_parquets.py"
    "scripts/inspect_org_parquets.py"
    "scripts/inspect_ortho_radiology_data.py"
    "scripts/inspect_payer_compatibility.py"
    "scripts/inspect_payer_parquets.py"
    "scripts/inspect_structure.py"
    "scripts/join_nppes_data.py"
    "scripts/nppes_fact_table_mapping.py"
    "scripts/ortho_radiology_nppes_joiner.py"
    "scripts/payer_development_guide.md"
    "scripts/quick_payer_test.py"
    "scripts/review_s3_streaming_results.py"
    "scripts/run_backfill.sh"
    "scripts/run_fact_table.sh"
    "scripts/test_cpt_matching.py"
    "scripts/test_fact_table_s3.py"
    "scripts/test_s3_connection.py"
    "scripts/validate_enhancement.py"
)

echo "Found ${#LEGACY_SCRIPTS[@]} legacy scripts:"
for script in "${LEGACY_SCRIPTS[@]}"; do
    if [ -e "$script" ]; then
        echo "  - $script"
    fi
done

echo ""
read -p "Remove legacy scripts? (y/N): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Removing legacy scripts..."
    for script in "${LEGACY_SCRIPTS[@]}"; do
        backup_and_remove "$script" "Legacy script"
    done
else
    echo "Skipping legacy script removal"
fi

echo ""
echo "🔍 Phase 8: Cleaning up any remaining generated files"
echo "-----------------------------------------------------"

# Remove any remaining generated files
find . -name "*.parquet" -type f -not -path "./tests/*" -exec echo "🗑️  Removing parquet file: {}" \; -exec mv {} "$BACKUP_DIR/" \;
find . -name "*.json.gz" -type f -not -path "./tests/*" -exec echo "🗑️  Removing gzipped JSON: {}" \; -exec mv {} "$BACKUP_DIR/" \;

echo ""
echo "🔍 Phase 9: Final cleanup and verification"
echo "------------------------------------------"

# Remove empty directories
find . -type d -empty -delete 2>/dev/null || true

# Count remaining files
REMAINING_FILES=$(find . -type f -name "*.py" -o -name "*.yaml" -o -name "*.md" -o -name "*.sh" | wc -l)
REMAINING_DIRS=$(find . -type d | wc -l)

echo ""
echo "📊 Cleanup Summary"
echo "=================="
echo "Backup created in: $BACKUP_DIR"
echo "Remaining files: $REMAINING_FILES"
echo "Remaining directories: $REMAINING_DIRS"
echo ""

echo "✅ Cleanup completed!"
echo ""
echo "🔍 Verification Steps:"
echo "1. Test production pipeline: python production_etl_pipeline.py --test-mode"
echo "2. Test smart integration: python scripts/smart_payer_workflow.py --payer-name test --index-url https://example.com"
echo "3. Run tests: python -m pytest tests/"
echo "4. Check imports: python -c \"from tic_mrf_scraper.payers import get_handler; print('✅ Imports work')\""
echo ""
echo "💡 If anything breaks, restore from: $BACKUP_DIR"
echo "💡 To permanently delete backup: rm -rf $BACKUP_DIR" 