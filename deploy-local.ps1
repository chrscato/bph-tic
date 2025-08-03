# TIC MRF Scraper - Local Deployment Script for Windows
# Optimized for small local runs with minimal resource usage

param(
    [string]$Command = "start"
)

# Configuration
$ProjectName = "tic-mrf-scraper-local"
$DockerComposeFile = "docker-compose.local.yml"
$EnvFile = "env.local"
$BackupDir = "backups"
$LogDir = "logs"

# Colors for output
$Green = "Green"
$Red = "Red"
$Yellow = "Yellow"
$Blue = "Blue"

function Write-Log {
    param([string]$Message, [string]$Color = "White")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$timestamp] $Message" -ForegroundColor $Color
}

function Write-Error {
    param([string]$Message)
    Write-Log "ERROR: $Message" -Color $Red
}

function Write-Warning {
    param([string]$Message)
    Write-Log "WARNING: $Message" -Color $Yellow
}

function Test-Docker {
    Write-Log "Checking Docker..." -Color $Blue
    try {
        docker version | Out-Null
        Write-Log "Docker is running" -Color $Green
        return $true
    }
    catch {
        Write-Error "Docker is not running or not installed. Please start Docker Desktop."
        return $false
    }
}

function Setup-Directories {
    Write-Log "Setting up directories..." -Color $Blue
    
    $directories = @($BackupDir, $LogDir, "data", "temp", "sample_data")
    foreach ($dir in $directories) {
        if (-not (Test-Path $dir)) {
            New-Item -ItemType Directory -Path $dir | Out-Null
        }
    }
    
    Write-Log "Directories created" -Color $Green
}

function Test-Environment {
    if (-not (Test-Path $EnvFile)) {
        Write-Error "Environment file $EnvFile not found. Please create it from env.local"
        return $false
    }
    
    # Check for AWS credentials
    $content = Get-Content $EnvFile
    if ($content -match "your_aws_access_key_here") {
        Write-Warning "AWS credentials not configured. S3 upload will be disabled for local testing."
    }
    
    return $true
}

function Start-Application {
    Write-Log "Starting $ProjectName (local mode)..." -Color $Blue
    
    if (-not (Test-Docker)) { return $false }
    Setup-Directories
    if (-not (Test-Environment)) { return $false }
    
    # Build the image
    Write-Log "Building Docker image..." -Color $Blue
    $buildResult = docker-compose -f $DockerComposeFile build
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Docker build failed"
        return $false
    }
    
    # Start the services
    Write-Log "Starting services..." -Color $Blue
    $startResult = docker-compose -f $DockerComposeFile up -d
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to start services"
        return $false
    }
    
    Write-Log "Local application started successfully!" -Color $Green
    Write-Log "Check logs with: docker-compose -f $DockerComposeFile logs" -Color $Blue
    Write-Log "Monitor with: docker-compose -f $DockerComposeFile ps" -Color $Blue
    Write-Log "To stop: docker-compose -f $DockerComposeFile down" -Color $Blue
    
    return $true
}

function Stop-Application {
    Write-Log "Stopping $ProjectName..." -Color $Blue
    
    # Backup data if exists
    if (Test-Path "data" -and (Get-ChildItem "data" | Measure-Object).Count -gt 0) {
        Write-Log "Creating backup of current data..." -Color $Blue
        $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $backupFile = "$BackupDir\local_backup_$timestamp.zip"
        Compress-Archive -Path "data\*" -DestinationPath $backupFile
        Write-Log "Backup created: local_backup_$timestamp.zip" -Color $Green
    }
    
    docker-compose -f $DockerComposeFile down
    Write-Log "Local application stopped successfully!" -Color $Green
}

function Show-Status {
    Write-Log "Checking application status..." -Color $Blue
    docker-compose -f $DockerComposeFile ps
    
    Write-Log "Container logs (last 20 lines):" -Color $Blue
    docker-compose -f $DockerComposeFile logs --tail=20
}

function Show-Logs {
    Write-Log "Showing logs..." -Color $Blue
    docker-compose -f $DockerComposeFile logs -f
}

function Cleanup-Application {
    Write-Log "Cleaning up old data and containers..." -Color $Blue
    docker-compose -f $DockerComposeFile down -v
    docker image prune -f
    Write-Log "Cleanup completed" -Color $Green
}

function Monitor-Resources {
    Write-Log "Monitoring resource usage..." -Color $Blue
    
    Write-Host "=== Container Resource Usage ===" -ForegroundColor $Blue
    docker stats --no-stream
    
    Write-Host "`n=== Disk Usage ===" -ForegroundColor $Blue
    $directories = @("data", "logs", "temp", "sample_data")
    foreach ($dir in $directories) {
        if (Test-Path $dir) {
            $size = (Get-ChildItem $dir -Recurse | Measure-Object -Property Length -Sum).Sum
            $sizeMB = [math]::Round($size / 1MB, 2)
            Write-Host "$dir`: $sizeMB MB" -ForegroundColor $Green
        } else {
            Write-Host "$dir`: Directory not found" -ForegroundColor $Yellow
        }
    }
    
    Write-Host "`n=== Memory Usage ===" -ForegroundColor $Blue
    $memory = Get-WmiObject -Class Win32_OperatingSystem
    $totalGB = [math]::Round($memory.TotalVisibleMemorySize / 1MB, 2)
    $freeGB = [math]::Round($memory.FreePhysicalMemory / 1MB, 2)
    $usedGB = $totalGB - $freeGB
    Write-Host "Total: $totalGB GB" -ForegroundColor $Green
    Write-Host "Used: $usedGB GB" -ForegroundColor $Yellow
    Write-Host "Free: $freeGB GB" -ForegroundColor $Green
}

function Start-QuickTest {
    Write-Log "Starting quick test run (conservative settings)..." -Color $Blue
    
    # Set environment variables for minimal testing
    $env:MAX_FILES_PER_PAYER = "1"
    $env:MAX_RECORDS_PER_FILE = "1000"
    $env:BATCH_SIZE = "500"
    
    $result = Start-Application
    if ($result) {
        Write-Log "Quick test started with minimal settings:" -Color $Green
        Write-Log "  - Max files per payer: 1" -Color $Blue
        Write-Log "  - Max records per file: 1,000" -Color $Blue
        Write-Log "  - Batch size: 500" -Color $Blue
        Write-Log "To stop: docker-compose -f $DockerComposeFile down" -Color $Blue
    }
}

function Show-Help {
    Write-Host "TIC MRF Scraper - Local Deployment Script for Windows" -ForegroundColor $Blue
    Write-Host ""
    Write-Host "Usage: .\deploy-local.ps1 [COMMAND]" -ForegroundColor $Blue
    Write-Host ""
    Write-Host "Commands:" -ForegroundColor $Blue
    Write-Host "  start       - Build and start the application (local mode)" -ForegroundColor $Green
    Write-Host "  stop        - Stop the application and backup data" -ForegroundColor $Green
    Write-Host "  status      - Show application status and recent logs" -ForegroundColor $Green
    Write-Host "  logs        - Show live logs" -ForegroundColor $Green
    Write-Host "  monitor     - Monitor resource usage" -ForegroundColor $Green
    Write-Host "  cleanup     - Clean up old containers and data" -ForegroundColor $Green
    Write-Host "  quick_test  - Start with minimal settings for testing" -ForegroundColor $Green
    Write-Host "  help        - Show this help message" -ForegroundColor $Green
    Write-Host ""
    Write-Host "Local Configuration:" -ForegroundColor $Blue
    Write-Host "  - Uses config.yaml (conservative settings)" -ForegroundColor $Yellow
    Write-Host "  - Memory limit: 4GB" -ForegroundColor $Yellow
    Write-Host "  - CPU limit: 2 cores" -ForegroundColor $Yellow
    Write-Host "  - Small batches for testing" -ForegroundColor $Yellow
    Write-Host "  - Minimal file processing" -ForegroundColor $Yellow
    Write-Host ""
    Write-Host "For production deployment, use:" -ForegroundColor $Blue
    Write-Host "  deploy.sh (for 32GB droplet)" -ForegroundColor $Yellow
}

# Main script logic
switch ($Command.ToLower()) {
    "start" { Start-Application }
    "stop" { Stop-Application }
    "status" { Show-Status }
    "logs" { Show-Logs }
    "monitor" { Monitor-Resources }
    "cleanup" { Cleanup-Application }
    "quick_test" { Start-QuickTest }
    "help" { Show-Help }
    default { 
        Write-Log "Unknown command: $Command" -Color $Red
        Show-Help
    }
} 