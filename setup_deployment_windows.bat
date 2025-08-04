@echo off
echo ==========================================
echo TIC MRF Pipeline - Windows Setup
echo ==========================================

echo.
echo Checking prerequisites...

REM Check if Git is installed
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ERROR: Git is not installed or not in PATH
    echo Please install Git from: https://git-scm.com/
    pause
    exit /b 1
)
echo ✓ Git is installed

REM Check if .env file exists
if not exist ".env" (
    echo.
    echo Creating .env file template...
    echo # VM Connection Details > .env
    echo VM_IP=your_vm_ip_address >> .env
    echo VM_PASSWORD=your_vm_password >> .env
    echo. >> .env
    echo # S3 Configuration >> .env
    echo S3_BUCKET=commercial-rates >> .env
    echo S3_PREFIX=tic-mrf-data/ >> .env
    echo AWS_ACCESS_KEY_ID=your_access_key >> .env
    echo AWS_SECRET_ACCESS_KEY=your_secret_key >> .env
    echo AWS_DEFAULT_REGION=us-east-2 >> .env
    echo.
    echo Created .env file template. Please edit it with your actual values.
    echo.
)

REM Check if WSL is available for sshpass
wsl --version >nul 2>&1
if %errorlevel% equ 0 (
    echo ✓ WSL is available
    echo.
    echo To use the deployment script, you have two options:
    echo.
    echo Option 1: Use WSL (Recommended)
    echo   1. Open WSL terminal
    echo   2. Navigate to this directory: cd /mnt/c/Users/chris/OneDrive\ -\ clarity-dx.com/Desktop/bph/tic/bph-tic
    echo   3. Install sshpass: sudo apt-get install sshpass
    echo   4. Run: ./deploy_to_vm.sh
    echo.
    echo Option 2: Use Git Bash
    echo   1. Open Git Bash
    echo   2. Navigate to this directory
    echo   3. Install sshpass: brew install sshpass
    echo   4. Run: ./deploy_to_vm.sh
    echo.
) else (
    echo.
    echo WSL not detected. You can use Git Bash instead:
    echo   1. Open Git Bash
    echo   2. Navigate to this directory
    echo   3. Install sshpass: brew install sshpass
    echo   4. Run: ./deploy_to_vm.sh
    echo.
)

echo ==========================================
echo Setup Complete!
echo ==========================================
echo.
echo Next steps:
echo 1. Edit .env file with your VM credentials
echo 2. Open WSL or Git Bash
echo 3. Install sshpass
echo 4. Run: ./deploy_to_vm.sh
echo.
pause 