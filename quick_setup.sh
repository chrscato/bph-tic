#!/bin/bash

# Quick Setup Script for TIC MRF Scraper on Digital Ocean Droplet
# This script automates the entire deployment process

set -e

# Configuration
PROJECT_NAME="tic-mrf-scraper"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Check if running as root
check_root() {
    if [ "$EUID" -eq 0 ]; then
        warning "Running as root. Consider using a non-root user for security."
    fi
}

# Check system requirements
check_system_requirements() {
    log "Checking system requirements..."
    
    # Check OS
    if ! grep -q "Ubuntu" /etc/os-release; then
        warning "This script is optimized for Ubuntu. Other distributions may work but are not tested."
    fi
    
    # Check memory
    MEMORY_GB=$(free -g | awk 'NR==2{print $2}')
    if [ "$MEMORY_GB" -lt 8 ]; then
        error "Insufficient memory. At least 8GB RAM required. Current: ${MEMORY_GB}GB"
        exit 1
    fi
    
    # Check disk space
    DISK_GB=$(df -BG / | awk 'NR==2{print $4}' | sed 's/G//')
    if [ "$DISK_GB" -lt 50 ]; then
        error "Insufficient disk space. At least 50GB required. Current: ${DISK_GB}GB"
        exit 1
    fi
    
    log "System requirements check passed"
}

# Install Docker and Docker Compose
install_docker() {
    log "Installing Docker and Docker Compose..."
    
    # Update package list
    apt update
    
    # Install required packages
    apt install -y apt-transport-https ca-certificates curl gnupg lsb-release
    
    # Add Docker GPG key
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
    
    # Add Docker repository
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    
    # Install Docker
    apt update
    apt install -y docker-ce docker-ce-cli containerd.io
    
    # Install Docker Compose
    curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    chmod +x /usr/local/bin/docker-compose
    
    # Add user to docker group
    usermod -aG docker $USER
    
    log "Docker and Docker Compose installed successfully"
}

# Setup project structure
setup_project() {
    log "Setting up project structure..."
    
    # Create necessary directories
    mkdir -p data logs temp backups
    
    # Make scripts executable
    chmod +x deploy.sh
    chmod +x monitor_resources.sh
    chmod +x setup_cron.sh
    
    # Copy environment file if it doesn't exist
    if [ ! -f ".env" ]; then
        if [ -f "env.production" ]; then
            cp env.production .env
            warning "Please edit .env file with your AWS credentials and configuration"
        else
            error "No environment file found. Please create .env file from env.example"
            exit 1
        fi
    fi
    
    log "Project structure setup completed"
}

# Configure environment
configure_environment() {
    log "Configuring environment..."
    
    # Check if .env file exists and has required variables
    if [ ! -f ".env" ]; then
        error ".env file not found. Please create it from env.example"
        exit 1
    fi
    
    # Check for AWS credentials
    if grep -q "your_aws_access_key_here" .env; then
        warning "Please update AWS credentials in .env file before deployment"
        echo ""
        echo "Required AWS configuration:"
        echo "  S3_BUCKET=your-s3-bucket-name"
        echo "  AWS_ACCESS_KEY_ID=your-aws-access-key"
        echo "  AWS_SECRET_ACCESS_KEY=your-aws-secret-key"
        echo ""
        read -p "Press Enter to continue after updating .env file..."
    fi
    
    log "Environment configuration completed"
}

# Test Docker installation
test_docker() {
    log "Testing Docker installation..."
    
    # Test Docker
    if ! docker --version; then
        error "Docker installation failed"
        exit 1
    fi
    
    # Test Docker Compose
    if ! docker-compose --version; then
        error "Docker Compose installation failed"
        exit 1
    fi
    
    # Test Docker daemon
    if ! docker info > /dev/null 2>&1; then
        error "Docker daemon not running. Please start Docker service"
        exit 1
    fi
    
    log "Docker installation test passed"
}

# Build and test application
build_application() {
    log "Building application..."
    
    # Build Docker image
    if ! docker-compose build; then
        error "Docker build failed"
        exit 1
    fi
    
    log "Application build completed"
}

# Setup monitoring and cron jobs
setup_monitoring() {
    log "Setting up monitoring and cron jobs..."
    
    # Setup cron jobs
    ./setup_cron.sh setup
    
    log "Monitoring setup completed"
}

# Show final instructions
show_final_instructions() {
    echo ""
    echo "🎉 TIC MRF Scraper Setup Completed Successfully!"
    echo "================================================"
    echo ""
    echo "Next Steps:"
    echo "1. Configure your AWS credentials in .env file"
    echo "2. Start the application: ./deploy.sh start"
    echo "3. Monitor resources: ./deploy.sh monitor"
    echo "4. View logs: ./deploy.sh logs"
    echo "5. Stop when done: ./deploy.sh stop"
    echo ""
    echo "Cost Optimization:"
    echo "- Use 'quick_run' for time-limited processing: ./deploy.sh quick_run"
    echo "- Monitor costs: ./monitor_resources.sh"
    echo "- Clean up old data: ./deploy.sh cleanup"
    echo ""
    echo "Useful Commands:"
    echo "- Check status: ./deploy.sh status"
    echo "- View resource usage: ./monitor_resources.sh"
    echo "- Restart application: ./deploy.sh restart"
    echo ""
    echo "Log Files:"
    echo "- Application logs: ./logs/etl_pipeline.log"
    echo "- Monitoring logs: ./logs/cron_monitor.log"
    echo "- Cost optimization logs: ./logs/cron_cost_optimize.log"
    echo ""
    echo "For help: ./deploy.sh help"
    echo ""
}

# Main setup function
main_setup() {
    log "Starting TIC MRF Scraper setup..."
    
    check_root
    check_system_requirements
    install_docker
    setup_project
    configure_environment
    test_docker
    build_application
    setup_monitoring
    show_final_instructions
    
    log "Setup completed successfully!"
}

# Show help
show_help() {
    echo "TIC MRF Scraper - Quick Setup Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  setup     - Complete setup (install Docker, configure, build)"
    echo "  docker    - Install Docker and Docker Compose only"
    echo "  build     - Build application only"
    echo "  test      - Test Docker installation"
    echo "  help      - Show this help message"
    echo ""
    echo "Prerequisites:"
    echo "  - Ubuntu 20.04+ (recommended)"
    echo "  - At least 8GB RAM"
    echo "  - At least 50GB disk space"
    echo "  - Internet connection"
    echo ""
    echo "After setup:"
    echo "  - Configure AWS credentials in .env file"
    echo "  - Start application: ./deploy.sh start"
}

# Main script logic
case "${1:-setup}" in
    setup)
        main_setup
        ;;
    docker)
        check_system_requirements
        install_docker
        test_docker
        ;;
    build)
        test_docker
        build_application
        ;;
    test)
        test_docker
        ;;
    help|*)
        show_help
        ;;
esac 