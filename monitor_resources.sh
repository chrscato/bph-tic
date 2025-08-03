#!/bin/bash

# Resource Monitoring Script for TIC MRF Scraper
# Optimized for 32GB RAM, 4 CPU Digital Ocean droplet

set -e

# Configuration
LOG_FILE="./logs/resource_monitor.log"
ALERT_THRESHOLD_MB=24576  # 24GB alert threshold (75% of 32GB)
COST_PER_HOUR=0.24  # Approximate cost per hour for 32GB droplet

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

# Get system information
get_system_info() {
    echo "=== System Information ==="
    echo "Hostname: $(hostname)"
    echo "Uptime: $(uptime)"
    echo "OS: $(lsb_release -d | cut -f2)"
    echo "Kernel: $(uname -r)"
    echo "Droplet Specs: 32GB RAM, 4 vCPU, 100GB NVMe SSD"
    echo ""
}

# Get memory usage
get_memory_usage() {
    echo "=== Memory Usage ==="
    free -h
    echo ""
    
    # Get memory usage in MB
    MEMORY_USAGE=$(free -m | awk 'NR==2{printf "%.0f", $3}')
    MEMORY_TOTAL=$(free -m | awk 'NR==2{printf "%.0f", $2}')
    MEMORY_PERCENT=$((MEMORY_USAGE * 100 / MEMORY_TOTAL))
    
    echo "Memory Usage: ${MEMORY_USAGE}MB / ${MEMORY_TOTAL}MB (${MEMORY_PERCENT}%)"
    
    if [ "$MEMORY_USAGE" -gt "$ALERT_THRESHOLD_MB" ]; then
        warning "High memory usage detected: ${MEMORY_USAGE}MB"
    fi
    echo ""
}

# Get disk usage
get_disk_usage() {
    echo "=== Disk Usage ==="
    df -h
    echo ""
    
    # Check specific directories
    echo "=== Application Directories ==="
    for dir in data logs temp backups; do
        if [ -d "$dir" ]; then
            SIZE=$(du -sh "$dir" 2>/dev/null | cut -f1)
            echo "$dir: $SIZE"
        fi
    done
    echo ""
}

# Get CPU usage
get_cpu_usage() {
    echo "=== CPU Usage ==="
    top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print "CPU Usage: " 100 - $1 "%"}'
    echo ""
}

# Get Docker container status
get_docker_status() {
    echo "=== Docker Containers ==="
    if command -v docker &> /dev/null; then
        docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
        echo ""
        
        # Get container resource usage
        echo "=== Container Resource Usage ==="
        docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}"
        echo ""
    else
        echo "Docker not installed or not running"
        echo ""
    fi
}

# Calculate estimated costs
calculate_costs() {
    echo "=== Cost Estimation ==="
    
    # Get uptime in hours
    UPTIME_HOURS=$(awk '{print $1/3600}' /proc/uptime)
    
    # Calculate estimated cost
    ESTIMATED_COST=$(echo "$UPTIME_HOURS * $COST_PER_HOUR" | bc -l)
    
    echo "Droplet Uptime: ${UPTIME_HOURS%.*} hours"
    echo "Estimated Cost: \$${ESTIMATED_COST%.2f}"
    echo "Cost per Hour: \$$COST_PER_HOUR"
    echo "Monthly Cost (if running 24/7): \$$(echo "$COST_PER_HOUR * 24 * 30" | bc -l)"
    echo ""
}

# Get network usage
get_network_usage() {
    echo "=== Network Usage ==="
    if command -v iftop &> /dev/null; then
        echo "Network monitoring available (iftop installed)"
    else
        echo "Network monitoring not available (install iftop for detailed stats)"
    fi
    
    # Show basic network info
    echo "Active connections:"
    ss -tuln | grep LISTEN | head -10
    echo ""
}

# Get application logs summary
get_app_logs() {
    echo "=== Application Logs Summary ==="
    
    if [ -f "./logs/etl_pipeline.log" ]; then
        echo "Recent log entries:"
        tail -5 "./logs/etl_pipeline.log" 2>/dev/null || echo "No recent log entries"
    else
        echo "No application log file found"
    fi
    echo ""
}

# Check for alerts
check_alerts() {
    echo "=== Alerts ==="
    
    # Memory alert
    MEMORY_USAGE=$(free -m | awk 'NR==2{printf "%.0f", $3}')
    if [ "$MEMORY_USAGE" -gt "$ALERT_THRESHOLD_MB" ]; then
        warning "ALERT: High memory usage (${MEMORY_USAGE}MB)"
    fi
    
    # Disk alert
    DISK_USAGE=$(df / | awk 'NR==2{print $5}' | sed 's/%//')
    if [ "$DISK_USAGE" -gt 80 ]; then
        warning "ALERT: High disk usage (${DISK_USAGE}%)"
    fi
    
    # Container alert
    if command -v docker &> /dev/null; then
        CONTAINER_COUNT=$(docker ps -q | wc -l)
        if [ "$CONTAINER_COUNT" -eq 0 ]; then
            warning "ALERT: No containers running"
        fi
    fi
    
    echo ""
}

# Generate report
generate_report() {
    log "Starting resource monitoring report"
    
    get_system_info
    get_memory_usage
    get_disk_usage
    get_cpu_usage
    get_docker_status
    calculate_costs
    get_network_usage
    get_app_logs
    check_alerts
    
    log "Resource monitoring report completed"
}

# Continuous monitoring
continuous_monitor() {
    log "Starting continuous monitoring (press Ctrl+C to stop)"
    
    while true; do
        clear
        generate_report
        echo "Monitoring... Press Ctrl+C to stop"
        sleep 30
    done
}

# Main script logic
case "${1:-report}" in
    report)
        generate_report
        ;;
    monitor)
        continuous_monitor
        ;;
    help)
        echo "Resource Monitoring Script (32GB RAM Optimized)"
        echo ""
        echo "Usage: $0 [COMMAND]"
        echo ""
        echo "Commands:"
        echo "  report   - Generate one-time resource report (default)"
        echo "  monitor  - Start continuous monitoring"
        echo "  help     - Show this help message"
        echo ""
        echo "The script monitors:"
        echo "  - Memory usage (32GB total)"
        echo "  - Disk usage (100GB NVMe SSD)"
        echo "  - CPU usage (4 vCPU)"
        echo "  - Docker container status"
        echo "  - Estimated costs (~$0.24/hour)"
        echo "  - Application logs"
        echo "  - System alerts"
        ;;
    *)
        generate_report
        ;;
esac 