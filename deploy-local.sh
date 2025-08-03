#!/bin/bash

# TIC MRF Scraper - Local Deployment Script
# Optimized for small local runs with minimal resource usage

set -e

# Configuration
PROJECT_NAME="tic-mrf-scraper-local"
DOCKER_COMPOSE_FILE="docker-compose.local.yml"
ENV_FILE="env.production"
BACKUP_DIR="./backups"
LOG_DIR="./logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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
    mkdir -p $BACKUP_DIR $LOG_DIR data temp sample_data
    log "Directories created"
}

# Backup current data
backup_data() {
    if [ -d "data" ] && [ "$(ls -A data)" ]; then
        log "Creating backup of current data..."
        timestamp=$(date +%Y%m%d_%H%M%S)
        tar -czf "$BACKUP_DIR/local_backup_$timestamp.tar.gz" data/
        log "Backup created: local_backup_$timestamp.tar.gz"
    fi
}

# Check environment file
check_env() {
    if [ ! -f "$ENV_FILE" ]; then
        error "Environment file $ENV_FILE not found. Please create it from env.example"
        exit 1
    fi
    
    # Check for required AWS credentials (optional for local runs)
    if grep -q "your_aws_access_key_here" "$ENV_FILE"; then
        warning "AWS credentials not configured. S3 upload will be disabled for local testing."
    fi
}

# Build and start the application
start() {
    log "Starting $PROJECT_NAME (local mode)..."
    
    check_dependencies
    setup_directories
    check_env
    
    # Build the image
    log "Building Docker image..."
    docker-compose -f $DOCKER_COMPOSE_FILE build
    
    # Start the services
    log "Starting services..."
    docker-compose -f $DOCKER_COMPOSE_FILE up -d
    
    log "Local application started successfully!"
    log "Check logs with: ./deploy-local.sh logs"
    log "Monitor with: ./deploy-local.sh status"
}

# Stop the application
stop() {
    log "Stopping $PROJECT_NAME..."
    
    # Backup data before stopping
    backup_data
    
    # Stop services
    docker-compose -f $DOCKER_COMPOSE_FILE down
    
    log "Local application stopped successfully!"
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
    
    # Clean up old backups (keep last 3 for local)
    if [ -d "$BACKUP_DIR" ]; then
        cd $BACKUP_DIR
        ls -t local_backup_*.tar.gz | tail -n +4 | xargs -r rm
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
    du -sh data/ logs/ temp/ sample_data/ 2>/dev/null || echo "Directories not found"
    
    echo ""
    echo "=== Memory Usage ==="
    free -h
}

# Quick test run (very conservative)
quick_test() {
    log "Starting quick test run (conservative settings)..."
    
    # Update config for minimal testing
    export MAX_FILES_PER_PAYER=1
    export MAX_RECORDS_PER_FILE=1000
    export BATCH_SIZE=500
    
    start
    
    log "Quick test started with minimal settings:"
    log "  - Max files per payer: 1"
    log "  - Max records per file: 1,000"
    log "  - Batch size: 500"
    log "To stop: ./deploy-local.sh stop"
}

# Show help
help() {
    echo "TIC MRF Scraper - Local Deployment Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start       - Build and start the application (local mode)"
    echo "  stop        - Stop the application and backup data"
    echo "  restart     - Restart the application"
    echo "  status      - Show application status and recent logs"
    echo "  logs        - Show live logs"
    echo "  monitor     - Monitor resource usage"
    echo "  cleanup     - Clean up old containers and data"
    echo "  quick_test  - Start with minimal settings for testing"
    echo "  help        - Show this help message"
    echo ""
    echo "Local Configuration:"
    echo "  - Uses config.yaml (conservative settings)"
    echo "  - Memory limit: 4GB"
    echo "  - CPU limit: 2 cores"
    echo "  - Small batches for testing"
    echo "  - Minimal file processing"
    echo ""
    echo "For production deployment, use:"
    echo "  ./deploy.sh (for 32GB droplet)"
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
    quick_test)
        quick_test
        ;;
    help|*)
        help
        ;;
esac 