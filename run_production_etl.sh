#!/bin/bash
# run_production_etl.sh - Execute the production ETL pipeline

set -e  # Exit on any error

# Parse command line arguments
QUIET_MODE=false
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -q|--quiet) QUIET_MODE=true ;;
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Progress bar function
progress_bar() {
    local current=$1
    local total=$2
    local width=50
    local percentage=$((current * 100 / total))
    local completed=$((width * current / total))
    local remaining=$((width - completed))
    
    printf "\r["
    printf "%${completed}s" | tr " " "█"
    printf "%${remaining}s" | tr " " "░"
    printf "] %d%%" $percentage
}

# Clean output function
clean_output() {
    if [ "$QUIET_MODE" = true ]; then
        "$@" > /dev/null 2>&1
    else
        "$@"
    fi
}

# Setup
if [ "$QUIET_MODE" = false ]; then
    echo "🚀 Starting Healthcare Rates ETL Pipeline"
    echo "=========================================="
fi

# Check requirements
if [ "$QUIET_MODE" = false ]; then
    echo "📋 Checking requirements..."
fi

# Check Python dependencies
python -c "import pandas, pyarrow, boto3, tqdm" || {
    if [ "$QUIET_MODE" = false ]; then
        echo "❌ Installing required packages..."
    fi
    pip install pandas pyarrow boto3 pyyaml tqdm > /dev/null 2>&1
}

# Check AWS credentials (if using S3)
if [ ! -z "$S3_BUCKET" ]; then
    if [ "$QUIET_MODE" = false ]; then
        echo "☁️ Verifying AWS credentials..."
    fi
    aws sts get-caller-identity > /dev/null || {
        echo "❌ AWS credentials not configured"
        echo "💡 Run: aws configure"
        exit 1
    }
fi

# Create output directories
mkdir -p production_data/{payers,organizations,providers,rates,analytics}
mkdir -p logs

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
export ETL_LOG_LEVEL="${ETL_LOG_LEVEL:-INFO}"
export ETL_START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
export ETL_SHOW_PROGRESS="true"

# Create a temporary file for progress monitoring
PROGRESS_FILE=$(mktemp)
trap 'rm -f $PROGRESS_FILE' EXIT

# Start progress monitor in background
if [ "$QUIET_MODE" = false ]; then
    (
        while true; do
            if [ -f "$PROGRESS_FILE" ]; then
                current=$(tail -n 1 "$PROGRESS_FILE" 2>/dev/null | cut -d',' -f1)
                total=$(tail -n 1 "$PROGRESS_FILE" 2>/dev/null | cut -d',' -f2)
                payer=$(tail -n 1 "$PROGRESS_FILE" 2>/dev/null | cut -d',' -f3)
                records=$(tail -n 1 "$PROGRESS_FILE" 2>/dev/null | cut -d',' -f4)
                rate=$(tail -n 1 "$PROGRESS_FILE" 2>/dev/null | cut -d',' -f5)
                
                if [ ! -z "$current" ] && [ ! -z "$total" ]; then
                    printf "\r\033[K"  # Clear line
                    progress_bar $current $total
                    printf " | %s | %d records | %.1f rec/sec" "$payer" $records $rate
                fi
            fi
            sleep 1
        done
    ) &
    PROGRESS_PID=$!
fi

# Run the ETL pipeline with progress monitoring
python scripts/production_etl_pipeline.py 2>&1 | tee "logs/etl_$(date +%Y%m%d_%H%M%S).log"

ETL_EXIT_CODE=$?

# Kill progress monitor
if [ "$QUIET_MODE" = false ]; then
    kill $PROGRESS_PID 2>/dev/null || true
    echo  # New line after progress bar
fi

# Check results
if [ $ETL_EXIT_CODE -eq 0 ]; then
    if [ "$QUIET_MODE" = false ]; then
        echo "✅ ETL Pipeline completed successfully!"
        
        # Display results summary
        echo ""
        echo "📊 Results Summary:"
        echo "=================="
        
        # Count records in each table
        for table in rates organizations providers payers analytics; do
            final_file="production_data/${table}/${table}_final.parquet"
            if [ -f "$final_file" ]; then
                record_count=$(python -c "
import pandas as pd
try:
    df = pd.read_parquet('$final_file')
    print(f'{len(df):,}')
except:
    print('0')
")
                echo "   ${table^}: ${record_count} records"
            else
                echo "   ${table^}: No data"
            fi
        done
        
        # Show output files
        echo ""
        echo "📁 Output Files:"
        echo "==============="
        find production_data -name "*_final.parquet" -exec ls -lh {} \; | \
            awk '{print "   " $9 " (" $5 ")"}'
        
        # Show S3 uploads (if configured)
        if [ ! -z "$S3_BUCKET" ]; then
            echo ""
            echo "☁️ S3 Uploads:"
            echo "============="
            aws s3 ls "s3://$S3_BUCKET/healthcare-rates-v2/" --recursive --human-readable | \
                tail -10 | awk '{print "   " $0}'
        fi
        
        # Display statistics
        if [ -f "production_data/processing_statistics.json" ]; then
            echo ""
            echo "📈 Processing Statistics:"
            echo "======================="
            python -c "
import json
with open('production_data/processing_statistics.json') as f:
    stats = json.load(f)
print(f'   Payers processed: {stats.get(\"payers_processed\", 0)}')
print(f'   Files processed: {stats.get(\"files_processed\", 0)}')
print(f'   Records extracted: {stats.get(\"records_extracted\", 0):,}')
print(f'   Processing time: {stats.get(\"processing_time_seconds\", 0):.1f} seconds')
print(f'   Rate per second: {stats.get(\"processing_rate_per_second\", 0):.1f} records/sec')
if stats.get('errors'):
    print(f'   Errors: {len(stats[\"errors\"])}')
"
        fi
        
        echo ""
        echo "🎉 Next Steps:"
        echo "============="
        echo "1. Review data quality in production_data/processing_statistics.json"
        echo "2. Test queries on the parquet files"
        echo "3. Set up automated scheduling (cron, Airflow, etc.)"
        echo "4. Add more payers to the configuration"
        echo "5. Integrate NPPES NPI registry data"
    fi
else
    echo "❌ ETL Pipeline failed with exit code $ETL_EXIT_CODE"
    echo ""
    echo "🔍 Check the logs for details:"
    echo "   - Latest log: logs/etl_$(date +%Y%m%d)*.log"
    echo "   - Error details in the log output above"
    
    exit $ETL_EXIT_CODE
fi

if [ "$QUIET_MODE" = false ]; then
    echo ""
    echo "📋 Pipeline Summary:"
    echo "   Start time: $ETL_START_TIME"
    echo "   End time: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "   Data location: $(pwd)/production_data/"
    echo "   Log location: $(pwd)/logs/"
fi 