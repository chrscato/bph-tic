# Repository Cleanup Guide

This guide identifies the core files used in the production ETL pipeline and provides recommendations for cleaning up the repository.

## 🎯 Core Production Files (KEEP)

### **Essential Production Pipeline**
These files are actively used in the production ETL workflow:

#### **Main Pipeline Files**
- ✅ `production_etl_pipeline.py` - **CRITICAL** - Main production ETL pipeline
- ✅ `production_etl_pipeline_quiet.py` - **CRITICAL** - Quiet mode production pipeline
- ✅ `run_production_etl.sh` - **CRITICAL** - Production execution script
- ✅ `production_config.yaml` - **CRITICAL** - Production configuration
- ✅ `requirements.txt` - **CRITICAL** - Python dependencies
- ✅ `pyproject.toml` - **CRITICAL** - Project configuration
- ✅ `setup.py` - **CRITICAL** - Package setup

#### **Core Source Code** (`src/tic_mrf_scraper/`)
- ✅ `src/tic_mrf_scraper/__init__.py` - **CRITICAL** - Package initialization
- ✅ `src/tic_mrf_scraper/__main__.py` - **CRITICAL** - CLI entry point
- ✅ `src/tic_mrf_scraper/diagnostics.py` - **CRITICAL** - File diagnostics
- ✅ `src/tic_mrf_scraper/payers/__init__.py` - **CRITICAL** - Payer handler system
- ✅ `src/tic_mrf_scraper/payers/centene.py` - **CRITICAL** - Centene handler
- ✅ `src/tic_mrf_scraper/payers/aetna.py` - **CRITICAL** - Aetna handler
- ✅ `src/tic_mrf_scraper/payers/horizon.py` - **CRITICAL** - Horizon handler
- ✅ `src/tic_mrf_scraper/payers/bcbsil.py` - **CRITICAL** - BCBSIL handler
- ✅ `src/tic_mrf_scraper/fetch/blobs.py` - **CRITICAL** - MRF file fetching
- ✅ `src/tic_mrf_scraper/stream/parser.py` - **CRITICAL** - Streaming parser
- ✅ `src/tic_mrf_scraper/transform/normalize.py` - **CRITICAL** - Data normalization
- ✅ `src/tic_mrf_scraper/write/parquet_writer.py` - **CRITICAL** - Parquet writing
- ✅ `src/tic_mrf_scraper/write/s3_uploader.py` - **CRITICAL** - S3 upload
- ✅ `src/tic_mrf_scraper/utils/backoff_logger.py` - **CRITICAL** - Logging utilities
- ✅ `src/tic_mrf_scraper/utils/format_identifier.py` - **CRITICAL** - Format detection

#### **Smart Integration System** (NEW)
- ✅ `scripts/intelligent_payer_integration.py` - **CRITICAL** - Smart payer integration
- ✅ `scripts/smart_payer_workflow.py` - **CRITICAL** - Complete workflow automation
- ✅ `SMART_PAYER_INTEGRATION_GUIDE.md` - **CRITICAL** - Integration documentation
- ✅ `scripts/example_smart_integration.sh` - **CRITICAL** - Example usage

#### **Documentation**
- ✅ `PRODUCTION_ETL_GUIDE.md` - **CRITICAL** - Production pipeline documentation
- ✅ `README.md` - **CRITICAL** - Main project documentation

#### **Testing**
- ✅ `tests/` - **CRITICAL** - All test files
- ✅ `test_production_pipeline.py` - **CRITICAL** - Production pipeline testing

## 🔧 Development & Analysis Files (KEEP)

### **Payer Analysis & Development**
- ✅ `scripts/analyze_payer_structure.py` - **IMPORTANT** - Payer structure analysis
- ✅ `scripts/analyze_large_mrfs.py` - **IMPORTANT** - Large MRF analysis
- ✅ `scripts/payer_development_workflow.py` - **IMPORTANT** - Payer development
- ✅ `scripts/payer_development_guide.md` - **IMPORTANT** - Development guide
- ✅ `scripts/quick_payer_test.py` - **IMPORTANT** - Quick payer testing
- ✅ `scripts/validate_enhancement.py` - **IMPORTANT** - Validation testing

### **Debugging & Inspection**
- ✅ `scripts/debug_parsing.py` - **USEFUL** - Debug parsing issues
- ✅ `scripts/inspect_json_structure.py` - **USEFUL** - JSON structure inspection
- ✅ `scripts/identify_format.py` - **USEFUL** - Format identification

## 🗑️ Files Safe to Remove (CLEANUP)

### **Legacy/Deprecated Files**
- ❌ `test_mvp.py` - **DEPRECATED** - Old MVP test, replaced by production pipeline
- ❌ `test_handlers.py` - **DEPRECATED** - Old handler test, replaced by comprehensive tests
- ❌ `test_s3_pipeline.py` - **DEPRECATED** - Old S3 test, functionality integrated into main pipeline
- ❌ `test_pipeline_mini.py` - **DEPRECATED** - Mini pipeline test, no longer needed
- ❌ `test_cpt_matching.py` - **DEPRECATED** - CPT matching test, integrated into normalization
- ❌ `test_fact_table_s3.py` - **DEPRECATED** - Old fact table test
- ❌ `test_s3_connection.py` - **DEPRECATED** - Old S3 connection test

### **Old Configuration Files**
- ❌ `config.yaml` - **DEPRECATED** - Old config, replaced by production_config.yaml
- ❌ `config_backup.yaml` - **DEPRECATED** - Backup of old config
- ❌ `production_config_backup.yaml` - **DEPRECATED** - Backup of production config
- ❌ `production_config_sample.yaml` - **DEPRECATED** - Sample config, no longer needed

### **Legacy Scripts**
- ❌ `scripts/analyze_inspection_findings.py` - **DEPRECATED** - Old analysis script
- ❌ `scripts/analyze_nppes_data.py` - **DEPRECATED** - NPPES analysis, not used in current pipeline
- ❌ `scripts/analyze_payer_structure.py` - **DEPRECATED** - Old payer structure analysis
- ❌ `scripts/analyze_rate_bucketing.py` - **DEPRECATED** - Rate bucketing analysis
- ❌ `scripts/backfill_provider_info.py` - **DEPRECATED** - Provider backfill, not in current pipeline
- ❌ `scripts/check_nppes_data.py` - **DEPRECATED** - NPPES checking
- ❌ `scripts/compare_payer_formats.py` - **DEPRECATED** - Payer format comparison
- ❌ `scripts/comprehensive_mrf_inspector.py` - **DEPRECATED** - MRF inspection
- ❌ `scripts/create_fact_table_parquet.py` - **DEPRECATED** - Fact table creation
- ❌ `scripts/create_joined_analysis_parquet.py` - **DEPRECATED** - Joined analysis
- ❌ `scripts/create_memory_efficient_fact_table.py` - **DEPRECATED** - Memory efficient fact table
- ❌ `scripts/create_s3_streaming_fact_table.py` - **DEPRECATED** - S3 streaming fact table
- ❌ `scripts/create_simple_fact_table.py` - **DEPRECATED** - Simple fact table
- ❌ `scripts/debug_nppes_join.py` - **DEPRECATED** - NPPES join debugging
- ❌ `scripts/debug_parsing.py` - **DEPRECATED** - Old parsing debug
- ❌ `scripts/example_payer_integration.sh` - **DEPRECATED** - Old integration example
- ❌ `scripts/inspect_actual_mrf.py` - **DEPRECATED** - MRF inspection
- ❌ `scripts/inspect_comprehensive_analysis.py` - **DEPRECATED** - Comprehensive analysis
- ❌ `scripts/inspect_core_parquets.py` - **DEPRECATED** - Core parquet inspection
- ❌ `scripts/inspect_org_parquets.py` - **DEPRECATED** - Organization parquet inspection
- ❌ `scripts/inspect_ortho_radiology_data.py` - **DEPRECATED** - Ortho radiology inspection
- ❌ `scripts/inspect_payer_compatibility.py` - **DEPRECATED** - Payer compatibility
- ❌ `scripts/inspect_payer_parquets.py` - **DEPRECATED** - Payer parquet inspection
- ❌ `scripts/inspect_structure.py` - **DEPRECATED** - Structure inspection
- ❌ `scripts/join_nppes_data.py` - **DEPRECATED** - NPPES data joining
- ❌ `scripts/nppes_fact_table_mapping.py` - **DEPRECATED** - NPPES fact table mapping
- ❌ `scripts/ortho_radiology_nppes_joiner.py` - **DEPRECATED** - Ortho radiology NPPES joining
- ❌ `scripts/payer_development_guide.md` - **DEPRECATED** - Old development guide
- ❌ `scripts/payer_development_workflow.py` - **DEPRECATED** - Old development workflow
- ❌ `scripts/quick_payer_test.py` - **DEPRECATED** - Old quick test
- ❌ `scripts/review_s3_streaming_results.py` - **DEPRECATED** - S3 streaming review
- ❌ `scripts/run_backfill.sh` - **DEPRECATED** - Backfill runner
- ❌ `scripts/run_fact_table.sh` - **DEPRECATED** - Fact table runner
- ❌ `scripts/test_cpt_matching.py` - **DEPRECATED** - CPT matching test
- ❌ `scripts/test_fact_table_s3.py` - **DEPRECATED** - Fact table S3 test
- ❌ `scripts/test_s3_connection.py` - **DEPRECATED** - S3 connection test
- ❌ `scripts/validate_enhancement.py` - **DEPRECATED** - Enhancement validation

### **Generated Output Directories**
- ❌ `ortho_radiology_data_*/` - **GENERATED** - All ortho radiology data directories
- ❌ `centene_fidelis/` - **GENERATED** - Centene data output
- ❌ `debug_output/` - **GENERATED** - Debug output
- ❌ `json_analysis/` - **GENERATED** - JSON analysis output
- ❌ `mrf_analysis/` - **GENERATED** - MRF analysis output
- ❌ `structure_analysis/` - **GENERATED** - Structure analysis output
- ❌ `payer_structure_analysis/` - **GENERATED** - Payer structure analysis
- ❌ `data_inspection/` - **GENERATED** - Data inspection output
- ❌ `data_inspection_production/` - **GENERATED** - Production data inspection
- ❌ `enriched_provider_data/` - **GENERATED** - Enriched provider data
- ❌ `nppes_data/` - **GENERATED** - NPPES data
- ❌ `ortho_radiology_data/` - **GENERATED** - Ortho radiology data
- ❌ `output/` - **GENERATED** - General output
- ❌ `production_data/` - **GENERATED** - Production data
- ❌ `sample_data/` - **GENERATED** - Sample data
- ❌ `temp_s3_downloads/` - **GENERATED** - Temporary S3 downloads
- ❌ `test_output/` - **GENERATED** - Test output

### **Log Files**
- ❌ `logs/` - **GENERATED** - All log files
- ❌ `*.log` - **GENERATED** - All log files

### **Analysis Reports**
- ❌ `*_analysis_*.json` - **GENERATED** - Analysis report files
- ❌ `*_analysis_*.txt` - **GENERATED** - Analysis text files
- ❌ `quick_test_*.json` - **GENERATED** - Quick test results

### **Temporary Files**
- ❌ `et --soft 7b65119` - **TEMPORARY** - Git temporary file
- ❌ `parq_inspect.py` - **TEMPORARY** - Temporary inspection script

## 📊 File Usage Analysis

### **Production Dependencies**
The production pipeline depends on these core modules:

```python
# Core imports in production_etl_pipeline.py
from tic_mrf_scraper.fetch.blobs import analyze_index_structure, list_mrf_blobs_enhanced
from tic_mrf_scraper.payers import get_handler
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
from tic_mrf_scraper.utils.backoff_logger import setup_logging, get_logger
from tic_mrf_scraper.diagnostics import identify_index, detect_compression, identify_in_network
```

### **Smart Integration Dependencies**
The smart integration system uses:

```python
# Core imports in smart_payer_workflow.py
from tic_mrf_scraper.payers import get_handler, PayerHandler
from tic_mrf_scraper.fetch.blobs import list_mrf_blobs_enhanced
from tic_mrf_scraper.stream.parser import stream_parse_enhanced
from tic_mrf_scraper.transform.normalize import normalize_tic_record
```

## 🧹 Cleanup Recommendations

### **Immediate Cleanup (Safe to Delete)**
```bash
# Remove deprecated test files
rm test_mvp.py test_handlers.py test_s3_pipeline.py test_pipeline_mini.py
rm test_cpt_matching.py test_fact_table_s3.py test_s3_connection.py

# Remove old configuration files
rm config.yaml config_backup.yaml production_config_backup.yaml production_config_sample.yaml

# Remove generated output directories
rm -rf ortho_radiology_data_*/ centene_fidelis/ debug_output/ json_analysis/
rm -rf mrf_analysis/ structure_analysis/ payer_structure_analysis/
rm -rf data_inspection/ data_inspection_production/ enriched_provider_data/
rm -rf nppes_data/ ortho_radiology_data/ output/ production_data/
rm -rf sample_data/ temp_s3_downloads/ test_output/

# Remove log files
rm -rf logs/ *.log

# Remove analysis reports
rm *_analysis_*.json *_analysis_*.txt quick_test_*.json

# Remove temporary files
rm "et --soft 7b65119" parq_inspect.py
```

### **Legacy Scripts Cleanup**
```bash
# Remove deprecated scripts (after confirming they're not needed)
rm scripts/analyze_inspection_findings.py scripts/analyze_nppes_data.py
rm scripts/analyze_payer_structure.py scripts/analyze_rate_bucketing.py
rm scripts/backfill_provider_info.py scripts/check_nppes_data.py
rm scripts/compare_payer_formats.py scripts/comprehensive_mrf_inspector.py
rm scripts/create_fact_table_parquet.py scripts/create_joined_analysis_parquet.py
rm scripts/create_memory_efficient_fact_table.py scripts/create_s3_streaming_fact_table.py
rm scripts/create_simple_fact_table.py scripts/debug_nppes_join.py
rm scripts/example_payer_integration.sh scripts/inspect_actual_mrf.py
rm scripts/inspect_comprehensive_analysis.py scripts/inspect_core_parquets.py
rm scripts/inspect_org_parquets.py scripts/inspect_ortho_radiology_data.py
rm scripts/inspect_payer_compatibility.py scripts/inspect_payer_parquets.py
rm scripts/inspect_structure.py scripts/join_nppes_data.py
rm scripts/nppes_fact_table_mapping.py scripts/ortho_radiology_nppes_joiner.py
rm scripts/payer_development_guide.md scripts/payer_development_workflow.py
rm scripts/quick_payer_test.py scripts/review_s3_streaming_results.py
rm scripts/run_backfill.sh scripts/run_fact_table.sh
rm scripts/test_cpt_matching.py scripts/test_fact_table_s3.py
rm scripts/test_s3_connection.py scripts/validate_enhancement.py
```

### **Archive Instead of Delete**
For important historical files, consider archiving:

```bash
# Create archive directory
mkdir -p archive/legacy_scripts
mkdir -p archive/old_configs
mkdir -p archive/generated_outputs

# Move files to archive instead of deleting
mv scripts/analyze_*.py archive/legacy_scripts/
mv scripts/create_*.py archive/legacy_scripts/
mv scripts/inspect_*.py archive/legacy_scripts/
mv config*.yaml archive/old_configs/
mv ortho_radiology_data_*/ archive/generated_outputs/
```

## 📈 Repository Size Impact

### **Before Cleanup**
- **Total Files**: ~200+ files
- **Generated Data**: ~500MB+ (mostly parquet files)
- **Legacy Scripts**: ~50+ deprecated scripts
- **Log Files**: ~100MB+ of logs

### **After Cleanup**
- **Core Files**: ~50 essential files
- **Generated Data**: 0MB (removed)
- **Legacy Scripts**: 0 (removed/archived)
- **Log Files**: 0MB (removed)

### **Estimated Space Savings**
- **Total Savings**: ~600MB+ and 150+ files
- **Repository Size**: Reduced by ~80%
- **Maintenance**: Significantly easier with focused codebase

## 🔍 Verification Steps

After cleanup, verify the production pipeline still works:

```bash
# Test production pipeline
python production_etl_pipeline.py --test-mode

# Test smart integration
python scripts/smart_payer_workflow.py --payer-name "test" --index-url "https://example.com"

# Run all tests
python -m pytest tests/

# Check imports work
python -c "from tic_mrf_scraper.payers import get_handler; print('✅ Imports work')"
```

## 📝 Post-Cleanup Structure

After cleanup, your repository will have this clean structure:

```
tic/
├── src/tic_mrf_scraper/           # Core source code
│   ├── payers/                    # Payer handlers
│   ├── fetch/                     # File fetching
│   ├── stream/                    # Streaming parser
│   ├── transform/                 # Data transformation
│   ├── write/                     # Output writing
│   └── utils/                     # Utilities
├── scripts/                       # Active scripts only
│   ├── intelligent_payer_integration.py
│   ├── smart_payer_workflow.py
│   ├── analyze_payer_structure.py
│   └── quick_payer_test.py
├── tests/                         # All test files
├── production_etl_pipeline.py     # Main pipeline
├── production_config.yaml         # Configuration
├── requirements.txt               # Dependencies
└── README.md                     # Documentation
```

This cleanup will make your repository much more maintainable and focused on the core production functionality. 