#!/bin/bash

# Complete VM Deployment Script
# Handles git push, SSH connection, and automated redeployment

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
REPO_NAME="bph-tic"
VM_USER="root"
VM_IP=""
VM_PASSWORD=""
GIT_REMOTE="origin"
GIT_BRANCH="master"

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
    
    if [ -n "$VM_PASSWORD" ]; then
        log "Found VM_PASSWORD in .env"
    else
        error "VM_PASSWORD not found in .env file"
        exit 1
    fi
else
    error ".env file not found. Please create one with VM_IP and VM_PASSWORD"
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
    if [ "$current_branch" != "$GIT_BRANCH" ]; then
        warning "You're on branch '$current_branch', but deploying to '$GIT_BRANCH'"
        read -p "Continue anyway? (y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    # Push to remote
    if git push $GIT_REMOTE $GIT_BRANCH; then
        log "✓ Successfully pushed to $GIT_REMOTE/$GIT_BRANCH"
    else
        error "Failed to push to git"
        exit 1
    fi
}

# Function to deploy on VM
deploy_on_vm() {
    log "Connecting to VM and deploying..."
    
    # Create the deployment script for the VM
    cat > /tmp/vm_deploy_script.sh << 'EOF'
#!/bin/bash

# VM Deployment Script
set -e

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

echo "=========================================="
echo "TIC MRF Pipeline VM Deployment"
echo "=========================================="

# Navigate to project directory
cd ~/bph-tic

# Stop current deployment
log "Stopping current deployment..."
if [ -f "./deploy.sh" ]; then
    ./deploy.sh stop || true
fi

# Clean Docker
log "Cleaning Docker containers and images..."
docker-compose down -v || true
docker system prune -f || true
docker image prune -f || true

# Pull latest changes
log "Pulling latest changes from git..."
git fetch origin
git reset --hard origin/master
git clean -fd

# Fix permissions
log "Fixing script permissions..."
chmod +x deploy.sh || true
chmod +x redeploy.sh || true
chmod +x quick_setup.sh || true
chmod +x monitor_resources.sh || true

# Start deployment
log "Starting deployment..."
./deploy.sh start

# Wait a moment for startup
sleep 10

# Check deployment status
log "Checking deployment status..."
if docker-compose ps | grep -q "Up"; then
    log "✓ Deployment successful"
    
    # Show logs
    log "Recent logs:"
    ./deploy.sh logs --tail=20
    
    # Show container status
    log "Container status:"
    docker-compose ps
    
else
    error "Deployment failed"
    ./deploy.sh logs
    exit 1
fi

log "✓ VM deployment completed successfully"
EOF

    # Make the script executable
    chmod +x /tmp/vm_deploy_script.sh
    
    # Use sshpass to handle password authentication
    if command -v sshpass &> /dev/null; then
        log "Using sshpass for password authentication"
        
        # Copy script to VM and execute
        sshpass -p "$VM_PASSWORD" scp -o StrictHostKeyChecking=no /tmp/vm_deploy_script.sh $VM_USER@$VM_IP:/tmp/
        sshpass -p "$VM_PASSWORD" ssh -o StrictHostKeyChecking=no $VM_USER@$VM_IP "chmod +x /tmp/vm_deploy_script.sh && /tmp/vm_deploy_script.sh"
        
    else
        error "sshpass not installed. Please install it or use SSH keys"
        log "To install sshpass:"
        log "  Ubuntu/Debian: sudo apt-get install sshpass"
        log "  macOS: brew install sshpass"
        log "  CentOS/RHEL: sudo yum install sshpass"
        exit 1
    fi
    
    # Clean up
    rm -f /tmp/vm_deploy_script.sh
}

# Function to monitor deployment
monitor_deployment() {
    log "Monitoring deployment..."
    
    # Wait a moment for startup
    sleep 5
    
    # Check if we can connect and get logs
    if command -v sshpass &> /dev/null; then
        log "Fetching recent logs..."
        sshpass -p "$VM_PASSWORD" ssh -o StrictHostKeyChecking=no $VM_USER@$VM_IP "cd ~/bph-tic && ./deploy.sh logs --tail=10" || true
    fi
}

# Main execution
main() {
    echo "=========================================="
    echo "TIC MRF Pipeline - Complete VM Deployment"
    echo "=========================================="
    
    # Check prerequisites
    check_directory
    check_git_status
    
    # Push to git
    push_to_git
    
    # Deploy on VM
    deploy_on_vm
    
    # Monitor deployment
    monitor_deployment
    
    echo ""
    echo "=========================================="
    echo "✓ Deployment completed successfully!"
    echo "=========================================="
    echo ""
    echo "To monitor the deployment:"
    echo "  ssh $VM_USER@$VM_IP"
    echo "  cd ~/bph-tic"
    echo "  ./deploy.sh logs"
    echo ""
    echo "To check S3 uploads:"
    echo "  ./deploy.sh logs | grep -i s3"
    echo ""
}

# Run main function
main "$@" 