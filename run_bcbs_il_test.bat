@echo off
echo Starting BCBS IL Local Test...
echo.

REM Check if virtual environment is activated
if not defined VIRTUAL_ENV (
    echo Activating virtual environment...
    call venv\Scripts\activate.bat
)

REM Install package in development mode if not already installed
echo Installing package in development mode...
pip install -e .

REM Run the test
echo Running BCBS IL test...
python test_bcbs_il_local.py

echo.
echo Test completed! Check test_output directory for results.
pause 