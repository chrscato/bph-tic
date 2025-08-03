#!/bin/bash

# TIC MRF Scraper Deployment Script for Digital Ocean Droplet
# Optimized for 32GB RAM, 4 CPU droplet with cost optimization

set -e

# Configuration
PROJECT_NAME="tic-mrf-scraper"
DOCKER_COMPOSE_FILE="docker-compose.yml"
ENV_FILE="env.production"
BACKUP_DIR="./backups"
LOG_DIR="./logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Check if Docker and Docker Compose are installed
check_dependencies() {
    log "Checking dependencies..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    log "Dependencies check passed"
}

# Create necessary directories
setup_directories() {
    log "Setting up directories..."
    mkdir -p $BACKUP_DIR $LOG_DIR data temp
    log "Directories created"
}

# Backup current data
backup_data() {
    if [ -d "data" ] && [ "$(ls -A data)" ]; then
        log "Creating backup of current data..."
        timestamp=$(date +%Y%m%d_%H%M%S)
        tar -czf "$BACKUP_DIR/backup_$timestamp.tar.gz" data/
        log "Backup created: backup_$timestamp.tar.gz"
    fi
}

# Check environment file
check_env() {
    if [ ! -f "$ENV_FILE" ]; then
        error "Environment file $ENV_FILE not found. Please create it from env.example"
        exit 1
    fi
    
    # Check for required AWS credentials
    if grep -q "your_aws_access_key_here" "$ENV_FILE"; then
        warning "Please update AWS credentials in $ENV_FILE before deployment"
    fi
}

# Build and start the application
start() {
    log "Starting $PROJECT_NAME..."
    
    check_dependencies
    setup_directories
    check_env
    
    # Build the image
    log "Building Docker image..."
    docker-compose -f $DOCKER_COMPOSE_FILE build
    
    # Start the services
    log "Starting services..."
    docker-compose -f $DOCKER_COMPOSE_FILE up -d
    
    log "Application started successfully!"
    log "Check logs with: ./deploy.sh logs"
    log "Monitor with: ./deploy.sh status"
}

# Stop the application
stop() {
    log "Stopping $PROJECT_NAME..."
    
    # Backup data before stopping
    backup_data
    
    # Stop services
    docker-compose -f $DOCKER_COMPOSE_FILE down
    
    log "Application stopped successfully!"
}

# Restart the application
restart() {
    log "Restarting $PROJECT_NAME..."
    stop
    sleep 5
    start
}

# Show application status
status() {
    log "Checking application status..."
    docker-compose -f $DOCKER_COMPOSE_FILE ps
    
    echo ""
    log "Container logs (last 20 lines):"
    docker-compose -f $DOCKER_COMPOSE_FILE logs --tail=20
}

# Show logs
logs() {
    log "Showing logs..."
    docker-compose -f $DOCKER_COMPOSE_FILE logs -f
}

# Clean up old data and containers
cleanup() {
    log "Cleaning up old data and containers..."
    
    # Stop and remove containers
    docker-compose -f $DOCKER_COMPOSE_FILE down -v
    
    # Remove old images
    docker image prune -f
    
    # Clean up old backups (keep last 10 for 32GB droplet)
    if [ -d "$BACKUP_DIR" ]; then
        cd $BACKUP_DIR
        ls -t backup_*.tar.gz | tail -n +11 | xargs -r rm
        cd ..
    fi
    
    log "Cleanup completed"
}

# Monitor resource usage
monitor() {
    log "Monitoring resource usage..."
    
    echo "=== Container Resource Usage ==="
    docker stats --no-stream
    
    echo ""
    echo "=== Disk Usage ==="
    du -sh data/ logs/ temp/ 2>/dev/null || echo "Directories not found"
    
    echo ""
    echo "=== Memory Usage ==="
    free -h
    
    echo ""
    echo "=== Cost Estimation ==="
    UPTIME_HOURS=$(awk '{print $1/3600}' /proc/uptime)
    COST_PER_HOUR=0.24
    ESTIMATED_COST=$(echo "$UPTIME_HOURS * $COST_PER_HOUR" | bc -l)
    echo "Droplet uptime: ${UPTIME_HOURS%.*} hours"
    echo "Estimated cost: \$${ESTIMATED_COST%.2f}"
    echo "Cost per hour: \$$COST_PER_HOUR"
}

# Quick start for cost optimization (runs for limited time)
quick_run() {
    log "Starting quick run (12 hours max for 32GB droplet)..."
    
    # Set environment variable for time limit
    export MAX_PROCESSING_TIME_HOURS=12
    
    start
    
    # Schedule stop after 12 hours
    log "Scheduling stop after 12 hours..."
    (sleep 43200 && ./deploy.sh stop && log "Quick run completed and stopped") &
    
    log "Quick run started. Will auto-stop after 12 hours."
    log "To stop manually: ./deploy.sh stop"
}

# High-performance run (uses full 32GB RAM)
high_performance_run() {
    log "Starting high-performance run (uses full 32GB RAM)..."
    
    # Update environment for high performance
    export BATCH_SIZE=50000
    export MAX_WORKERS=8
    export MEMORY_THRESHOLD_MB=28672
    
    start
    
    log "High-performance run started with optimized settings:"
    log "  - Batch size: 50,000"
    log "  - Max workers: 8"
    log "  - Memory threshold: 28GB"
    log "To stop: ./deploy.sh stop"
}

# Show help
help() {
    echo "TIC MRF Scraper Deployment Script (32GB RAM Optimized)"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start              - Build and start the application"
    echo "  stop               - Stop the application and backup data"
    echo "  restart            - Restart the application"
    echo "  status             - Show application status and recent logs"
    echo "  logs               - Show live logs"
    echo "  monitor            - Monitor resource usage and costs"
    echo "  cleanup            - Clean up old containers and data"
    echo "  quick_run          - Start for 12 hours then auto-stop (cost optimization)"
    echo "  high_performance   - Start with full 32GB RAM optimization"
    echo "  help               - Show this help message"
    echo ""
    echo "Droplet Specs: 32GB RAM, 4 vCPU, 100GB NVMe SSD"
    echo "Cost: ~$0.24/hour (~$172/month if running 24/7)"
    echo ""
    echo "Cost Optimization Tips:"
    echo "  - Use 'quick_run' for time-limited processing"
    echo "  - Use 'high_performance' for maximum throughput"
    echo "  - Monitor resource usage with 'monitor'"
    echo "  - Stop when not in use to save costs"
    echo "  - Use 'cleanup' to free up disk space"
}

# Main script logic
case "${1:-help}" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    logs)
        logs
        ;;
    monitor)
        monitor
        ;;
    cleanup)
        cleanup
        ;;
    quick_run)
        quick_run
        ;;
    high_performance)
        high_performance_run
        ;;
    help|*)
        help
        ;;
esac 