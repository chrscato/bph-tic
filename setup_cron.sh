#!/bin/bash

# Setup Cron Jobs for TIC MRF Scraper Cost Optimization
# Optimized for 32GB RAM, 4 CPU Digital Ocean droplet

set -e

# Configuration
CRON_USER=$(whoami)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Create necessary directories
setup_directories() {
    log "Creating necessary directories..."
    mkdir -p "$LOG_DIR"
    log "Directories created"
}

# Create monitoring script
create_monitoring_script() {
    log "Creating monitoring script..."
    
    cat > "$SCRIPT_DIR/cron_monitor.sh" << 'EOF'
#!/bin/bash

# Automated monitoring script for cron jobs
# Optimized for 32GB RAM droplet
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/logs/cron_monitor.log"

# Log function
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

# Check if application is running
check_app_status() {
    if docker ps | grep -q "tic-mrf-scraper"; then
        log "Application is running"
        return 0
    else
        log "Application is not running"
        return 1
    fi
}

# Check memory usage (optimized for 32GB)
check_memory() {
    MEMORY_USAGE=$(free -m | awk 'NR==2{printf "%.0f", $3}')
    MEMORY_TOTAL=$(free -m | awk 'NR==2{printf "%.0f", $2}')
    MEMORY_PERCENT=$((MEMORY_USAGE * 100 / MEMORY_TOTAL))
    
    log "Memory usage: ${MEMORY_USAGE}MB / ${MEMORY_TOTAL}MB (${MEMORY_PERCENT}%)"
    
    if [ "$MEMORY_PERCENT" -gt 85 ]; then
        log "WARNING: High memory usage detected"
        return 1
    fi
    return 0
}

# Check disk usage
check_disk() {
    DISK_USAGE=$(df / | awk 'NR==2{print $5}' | sed 's/%//')
    log "Disk usage: ${DISK_USAGE}%"
    
    if [ "$DISK_USAGE" -gt 85 ]; then
        log "WARNING: High disk usage detected"
        return 1
    fi
    return 0
}

# Calculate uptime and cost (32GB droplet pricing)
calculate_cost() {
    UPTIME_HOURS=$(awk '{print $1/3600}' /proc/uptime)
    COST_PER_HOUR=0.24
    ESTIMATED_COST=$(echo "$UPTIME_HOURS * $COST_PER_HOUR" | bc -l)
    
    log "Droplet uptime: ${UPTIME_HOURS%.*} hours"
    log "Estimated cost: \$${ESTIMATED_COST%.2f}"
}

# Main monitoring logic
main() {
    log "Starting automated monitoring check"
    
    check_app_status
    check_memory
    check_disk
    calculate_cost
    
    log "Monitoring check completed"
}

main "$@"
EOF

    chmod +x "$SCRIPT_DIR/cron_monitor.sh"
    log "Monitoring script created"
}

# Create cost optimization script
create_cost_optimization_script() {
    log "Creating cost optimization script..."
    
    cat > "$SCRIPT_DIR/cron_cost_optimize.sh" << 'EOF'
#!/bin/bash

# Automated cost optimization script for cron jobs
# Optimized for 32GB RAM droplet
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$SCRIPT_DIR/logs/cron_cost_optimize.log"

# Log function
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

# Check if application has been running too long (12 hours for 32GB)
check_runtime() {
    if docker ps | grep -q "tic-mrf-scraper"; then
        # Get container start time
        START_TIME=$(docker inspect --format='{{.State.StartedAt}}' tic-mrf-scraper 2>/dev/null)
        if [ -n "$START_TIME" ]; then
            START_EPOCH=$(date -d "$START_TIME" +%s)
            CURRENT_EPOCH=$(date +%s)
            RUNTIME_HOURS=$(( (CURRENT_EPOCH - START_EPOCH) / 3600 ))
            
            log "Application has been running for ${RUNTIME_HOURS} hours"
            
            # Stop if running for more than 12 hours (optimized for 32GB)
            if [ "$RUNTIME_HOURS" -gt 12 ]; then
                log "Stopping application due to long runtime (${RUNTIME_HOURS} hours)"
                cd "$SCRIPT_DIR" && ./deploy.sh stop
                return 0
            fi
        fi
    fi
    return 1
}

# Check for idle time (no recent log activity)
check_idle_time() {
    LOG_FILE="$SCRIPT_DIR/logs/etl_pipeline.log"
    if [ -f "$LOG_FILE" ]; then
        LAST_ACTIVITY=$(stat -c %Y "$LOG_FILE")
        CURRENT_TIME=$(date +%s)
        IDLE_MINUTES=$(( (CURRENT_TIME - LAST_ACTIVITY) / 60 ))
        
        log "Application idle for ${IDLE_MINUTES} minutes"
        
        # Stop if idle for more than 45 minutes (longer for 32GB)
        if [ "$IDLE_MINUTES" -gt 45 ]; then
            log "Stopping application due to inactivity (${IDLE_MINUTES} minutes)"
            cd "$SCRIPT_DIR" && ./deploy.sh stop
            return 0
        fi
    fi
    return 1
}

# Clean up old data (optimized for 32GB droplet)
cleanup_old_data() {
    log "Checking for old data to clean up..."
    
    # Remove backups older than 14 days (more storage on 32GB droplet)
    if [ -d "$SCRIPT_DIR/backups" ]; then
        find "$SCRIPT_DIR/backups" -name "backup_*.tar.gz" -mtime +14 -delete
        log "Cleaned up old backups"
    fi
    
    # Remove logs older than 45 days (more storage on 32GB droplet)
    if [ -d "$SCRIPT_DIR/logs" ]; then
        find "$SCRIPT_DIR/logs" -name "*.log" -mtime +45 -delete
        log "Cleaned up old logs"
    fi
}

# Main cost optimization logic
main() {
    log "Starting cost optimization check"
    
    check_runtime
    check_idle_time
    cleanup_old_data
    
    log "Cost optimization check completed"
}

main "$@"
EOF

    chmod +x "$SCRIPT_DIR/cron_cost_optimize.sh"
    log "Cost optimization script created"
}

# Setup cron jobs
setup_cron_jobs() {
    log "Setting up cron jobs..."
    
    # Create temporary cron file
    TEMP_CRON=$(mktemp)
    
    # Get existing cron jobs
    crontab -l 2>/dev/null > "$TEMP_CRON" || true
    
    # Add monitoring job (every 5 minutes)
    echo "# TIC MRF Scraper - Resource monitoring (every 5 minutes)" >> "$TEMP_CRON"
    echo "*/5 * * * * $SCRIPT_DIR/cron_monitor.sh >> $LOG_DIR/cron_monitor.log 2>&1" >> "$TEMP_CRON"
    
    # Add cost optimization job (every 15 minutes)
    echo "# TIC MRF Scraper - Cost optimization (every 15 minutes)" >> "$TEMP_CRON"
    echo "*/15 * * * * $SCRIPT_DIR/cron_cost_optimize.sh >> $LOG_DIR/cron_cost_optimize.log 2>&1" >> "$TEMP_CRON"
    
    # Add daily cleanup job (at 2 AM)
    echo "# TIC MRF Scraper - Daily cleanup (2 AM daily)" >> "$TEMP_CRON"
    echo "0 2 * * * $SCRIPT_DIR/deploy.sh cleanup >> $LOG_DIR/cron_cleanup.log 2>&1" >> "$TEMP_CRON"
    
    # Add weekly cost report (every Sunday at 6 AM)
    echo "# TIC MRF Scraper - Weekly cost report (Sunday 6 AM)" >> "$TEMP_CRON"
    echo "0 6 * * 0 $SCRIPT_DIR/monitor_resources.sh report >> $LOG_DIR/weekly_cost_report.log 2>&1" >> "$TEMP_CRON"
    
    # Install the new cron jobs
    crontab "$TEMP_CRON"
    rm "$TEMP_CRON"
    
    log "Cron jobs installed successfully"
}

# Show current cron jobs
show_cron_jobs() {
    log "Current cron jobs:"
    crontab -l | grep -E "(TIC MRF|monitor|optimize|cleanup|cost)" || echo "No TIC MRF cron jobs found"
}

# Remove cron jobs
remove_cron_jobs() {
    log "Removing TIC MRF cron jobs..."
    
    # Create temporary cron file
    TEMP_CRON=$(mktemp)
    
    # Get existing cron jobs and filter out TIC MRF jobs
    crontab -l 2>/dev/null | grep -v -E "(TIC MRF|monitor|optimize|cleanup|cost)" > "$TEMP_CRON" || true
    
    # Install the filtered cron jobs
    crontab "$TEMP_CRON"
    rm "$TEMP_CRON"
    
    log "TIC MRF cron jobs removed"
}

# Test monitoring script
test_monitoring() {
    log "Testing monitoring script..."
    "$SCRIPT_DIR/cron_monitor.sh"
    log "Monitoring test completed"
}

# Show help
show_help() {
    echo "TIC MRF Scraper - Cron Job Setup Script (32GB RAM Optimized)"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  setup     - Setup cron jobs for monitoring and cost optimization"
    echo "  remove    - Remove TIC MRF cron jobs"
    echo "  show      - Show current cron jobs"
    echo "  test      - Test monitoring script"
    echo "  help      - Show this help message"
    echo ""
    echo "Cron Jobs Created:"
    echo "  - Resource monitoring (every 5 minutes)"
    echo "  - Cost optimization (every 15 minutes)"
    echo "  - Daily cleanup (2 AM daily)"
    echo "  - Weekly cost report (Sunday 6 AM)"
    echo ""
    echo "Optimized for 32GB RAM droplet:"
    echo "  - 12-hour runtime limit"
    echo "  - 45-minute idle timeout"
    echo "  - 14-day backup retention"
    echo "  - 45-day log retention"
    echo ""
    echo "Log Files:"
    echo "  - $LOG_DIR/cron_monitor.log"
    echo "  - $LOG_DIR/cron_cost_optimize.log"
    echo "  - $LOG_DIR/cron_cleanup.log"
    echo "  - $LOG_DIR/weekly_cost_report.log"
}

# Main script logic
case "${1:-setup}" in
    setup)
        setup_directories
        create_monitoring_script
        create_cost_optimization_script
        setup_cron_jobs
        show_cron_jobs
        log "Cron job setup completed successfully"
        ;;
    remove)
        remove_cron_jobs
        ;;
    show)
        show_cron_jobs
        ;;
    test)
        test_monitoring
        ;;
    help|*)
        show_help
        ;;
esac 