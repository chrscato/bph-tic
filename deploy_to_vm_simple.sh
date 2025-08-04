#!/bin/bash

# Simple VM Deployment Script (No sshpass required)
# Handles git push and provides manual deployment instructions

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

# Load environment variables from .env file
if [ -f ".env" ]; then
    log "Loading environment variables from .env file"
    export $(cat .env | grep -v '^#' | xargs)
    
    # Extract VM credentials from .env
    if [ -n "$VM_IP" ]; then
        log "Found VM_IP in .env: $VM_IP"
    else
        error "VM_IP not found in .env file"
        exit 1
    fi
else
    error ".env file not found. Please create one with VM_IP"
    exit 1
fi

# Function to check if we're in the right directory
check_directory() {
    if [ ! -f "production_etl_pipeline.py" ]; then
        error "This script must be run from the bph-tic repository root"
        exit 1
    fi
    log "✓ Repository directory confirmed"
}

# Function to check git status
check_git_status() {
    log "Checking git status..."
    
    # Check if we have uncommitted changes
    if [ -n "$(git status --porcelain)" ]; then
        warning "You have uncommitted changes:"
        git status --short
        read -p "Do you want to commit these changes? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            read -p "Enter commit message: " commit_message
            git add .
            git commit -m "$commit_message"
            log "✓ Changes committed"
        else
            error "Please commit or stash your changes before deploying"
            exit 1
        fi
    else
        log "✓ No uncommitted changes"
    fi
}

# Function to push to git
push_to_git() {
    log "Pushing to git repository..."
    
    # Check if we're on the right branch
    current_branch=$(git branch --show-current)
    if [ "$current_branch" != "master" ]; then
        warning "You're on branch '$current_branch', but deploying to 'master'"
        read -p "Continue anyway? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    # Push to remote
    if git push origin master; then
        log "✓ Successfully pushed to origin/master"
    else
        error "Failed to push to git"
        exit 1
    fi
}

# Function to provide manual deployment instructions
provide_manual_instructions() {
    echo ""
    echo "=========================================="
    echo "MANUAL DEPLOYMENT INSTRUCTIONS"
    echo "=========================================="
    echo ""
    echo "Since sshpass is not available, please manually deploy on your VM:"
    echo ""
    echo "1. SSH to your VM:"
    echo "   ssh root@$VM_IP"
    echo ""
    echo "2. Navigate to the project directory:"
    echo "   cd ~/bph-tic"
    echo ""
    echo "3. Run the deployment script:"
    echo "   ./redeploy.sh"
    echo ""
    echo "4. Monitor the deployment:"
    echo "   ./deploy.sh logs"
    echo ""
    echo "5. Check S3 uploads:"
    echo "   ./deploy.sh logs | grep -i s3"
    echo ""
    echo "=========================================="
    echo "DEPLOYMENT COMMANDS FOR VM"
    echo "=========================================="
    echo ""
    echo "Copy and paste these commands on your VM:"
    echo ""
    echo "cd ~/bph-tic"
    echo "git fetch origin"
    echo "git reset --hard origin/master"
    echo "git clean -fd"
    echo "chmod +x deploy.sh redeploy.sh"
    echo "./deploy.sh stop"
    echo "docker-compose down -v"
    echo "docker system prune -f"
    echo "./deploy.sh start"
    echo "./deploy.sh logs"
    echo ""
}

# Main execution
main() {
    echo "=========================================="
    echo "TIC MRF Pipeline - Simple Deployment"
    echo "=========================================="
    
    # Check prerequisites
    check_directory
    check_git_status
    
    # Push to git
    push_to_git
    
    # Provide manual instructions
    provide_manual_instructions
    
    echo ""
    echo "✓ Git push completed successfully!"
    echo "✓ Your changes are now available on the VM"
    echo "✓ Please follow the manual deployment instructions above"
    echo ""
}

# Run main function
main "$@" 