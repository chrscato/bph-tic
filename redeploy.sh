#!/bin/bash

# TIC MRF Pipeline Complete Redeployment Script
# Optimized for 32GB RAM VM with clean rebuild

set -e

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

# Configuration
PROJECT_NAME="tic-mrf-scraper"
DOCKER_COMPOSE_FILE="docker-compose.yml"
BACKUP_DIR="./backups"
LOG_DIR="./logs"

echo "=========================================="
echo "TIC MRF Pipeline Complete Redeployment"
echo "32GB RAM VM - Clean Rebuild"
echo "=========================================="

# Step 1: Stop current deployment
log "Step 1: Stopping current deployment..."
if [ -f "./deploy.sh" ]; then
    chmod +x deploy.sh
    ./deploy.sh stop || true
else
    warning "deploy.sh not found, stopping Docker directly..."
fi

# Stop Docker containers
log "Stopping Docker containers..."
docker-compose down -v || true
docker stop $(docker ps -q --filter "name=tic-mrf") || true

# Step 2: Clean Docker cache and images
log "Step 2: Cleaning Docker cache and images..."
docker system prune -f
docker image prune -f
docker volume prune -f
docker network prune -f

# Remove old images
log "Removing old images..."
docker rmi $(docker images -q tic-mrf-scraper) || true

# Step 3: Git operations
log "Step 3: Updating repository..."
cd ~/bph-tic

# Stash any local changes
git stash || true

# Fetch and reset to origin/master
log "Pulling latest from master..."
git fetch origin
git reset --hard origin/master
git clean -fd

# Step 4: Fix permissions
log "Step 4: Fixing script permissions..."
chmod +x deploy.sh quick_setup.sh monitor_resources.sh || true

# Step 5: Verify environment
log "Step 5: Checking environment configuration..."
if [ -f ".env" ]; then
    info "✅ .env file exists"
    S3_BUCKET=$(grep S3_BUCKET .env | cut -d'=' -f2)
    info "S3_BUCKET: $S3_BUCKET"
else
    warning "❌ .env file missing - copying from env.production"
    cp env.production .env
fi

# Step 6: Clean old data (optional)
read -p "Do you want to clear old data? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    log "Clearing old data..."
    rm -rf data/* logs/* temp/* 2>/dev/null || true
    info "✅ Old data cleared"
else
    info "Keeping existing data"
fi

# Step 7: Rebuild and start
log "Step 6: Rebuilding and starting deployment..."

# Build without cache
log "Building Docker image without cache..."
docker-compose build --no-cache

# Start deployment
log "Starting deployment..."
./deploy.sh start

# Step 8: Wait and check status
log "Step 7: Waiting for startup..."
sleep 30

# Check status
log "Step 8: Checking deployment status..."
./deploy.sh status

# Show recent logs
log "Step 9: Recent logs:"
./deploy.sh logs --tail=10

echo "=========================================="
echo "✅ Redeployment Complete!"
echo "=========================================="
echo ""
echo "📊 Monitor Commands:"
echo "  ./deploy.sh logs          # View live logs"
echo "  ./deploy.sh logs | grep -i s3  # Watch S3 uploads"
echo "  ./deploy.sh monitor       # Monitor resources"
echo "  ./deploy.sh status        # Check status"
echo ""
echo "🚀 Expected Performance:"
echo "  - Batch Size: 25,000 records"
echo "  - Memory Threshold: 24GB"
echo "  - Parallel Workers: 6"
echo "  - S3 Uploads: Every 100K records"
echo ""
echo "⏱️  Estimated Timeline:"
echo "  - 669 files to process"
echo "  - ~12-16 minutes total"
echo "  - S3 uploads start after ~10K records"
echo ""
echo "💡 Next Steps:"
echo "  1. Monitor logs for S3 uploads"
echo "  2. Check S3 bucket: commercial-rates"
echo "  3. Verify data quality in logs"
echo "" 