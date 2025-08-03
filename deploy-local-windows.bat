@echo off
REM TIC MRF Scraper - Local Deployment Script for Windows
REM Optimized for small local runs with minimal resource usage

setlocal enabledelayedexpansion

REM Configuration
set PROJECT_NAME=tic-mrf-scraper-local
set DOCKER_COMPOSE_FILE=docker-compose.local.yml
set ENV_FILE=env.local
set BACKUP_DIR=backups
set LOG_DIR=logs

REM Check if Docker is running
echo [%date% %time%] Checking Docker...
docker version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not running or not installed. Please start Docker Desktop.
    exit /b 1
)
echo [%date% %time%] Docker is running

REM Create necessary directories
echo [%date% %time%] Setting up directories...
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "data" mkdir "data"
if not exist "temp" mkdir "temp"
if not exist "sample_data" mkdir "sample_data"
echo [%date% %time%] Directories created

REM Check environment file
if not exist "%ENV_FILE%" (
    echo [ERROR] Environment file %ENV_FILE% not found. Please create it from env.local
    exit /b 1
)

REM Check for AWS credentials
findstr "your_aws_access_key_here" "%ENV_FILE%" >nul
if not errorlevel 1 (
    echo [WARNING] AWS credentials not configured. S3 upload will be disabled for local testing.
)

REM Build the image
echo [%date% %time%] Building Docker image...
docker-compose -f %DOCKER_COMPOSE_FILE% build
if errorlevel 1 (
    echo [ERROR] Docker build failed
    exit /b 1
)

REM Start the services
echo [%date% %time%] Starting services...
docker-compose -f %DOCKER_COMPOSE_FILE% up -d
if errorlevel 1 (
    echo [ERROR] Failed to start services
    exit /b 1
)

echo [%date% %time%] Local application started successfully!
echo [%date% %time%] Check logs with: docker-compose -f %DOCKER_COMPOSE_FILE% logs
echo [%date% %time%] Monitor with: docker-compose -f %DOCKER_COMPOSE_FILE% ps
echo [%date% %time%] To stop: docker-compose -f %DOCKER_COMPOSE_FILE% down 