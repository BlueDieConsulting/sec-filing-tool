#!/bin/bash

# SEC Filings Download Script
# Usage: run_edgartools.sh [test|full] [--no-archive]
#   test: Download filings for first 10 companies from sp500.json
#   full: Download filings for all companies in sp500.json
#   --no-archive: Skip archiving step (optional)

set -e  # Exit on any error

# Initialize variables
MODE=""
ARCHIVE_FLAG="true"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON_SCRIPT="$SCRIPT_DIR/edgartools_runner.py"

# Parse command line arguments
for arg in "$@"; do
    case $arg in
        --no-archive)
            ARCHIVE_FLAG="false"
            ;;
        test|full)
            if [ -z "$MODE" ]; then
                MODE="$arg"
            fi
            ;;
        *)
            if [ -z "$MODE" ]; then
                MODE="$arg"
            fi
            ;;
    esac
done

# Validate mode parameter
if [ -z "$MODE" ]; then
    echo "Usage: $0 [test|full] [--no-archive]"
    echo "  test: Download filings for first 10 companies from sp500.json"
    echo "  full: Download filings for all companies in sp500.json"
    echo "  --no-archive: Skip archiving step (optional)"
    exit 1
fi

if [ "$MODE" != "test" ] && [ "$MODE" != "full" ]; then
    echo "Error: Invalid mode '$MODE'. Use 'test' or 'full'."
    exit 1
fi

# Check if sp500.json exists
if [ ! -f "$ROOT_DIR/sp500.json" ]; then
    echo "Error: sp500.json not found in $ROOT_DIR"
    echo "Please ensure the sp500.json file exists before running this script."
    exit 1
fi

echo "Starting SEC filings download in $MODE mode..."
if [ "$ARCHIVE_FLAG" = "true" ]; then
    echo "Archiving will be performed after download completion."
else
    echo "Archiving is disabled."
fi

# Fix timezone issue if needed (Linux/macOS version)
if [ ! -d "/usr/share/zoneinfo" ] && [ ! -f "/etc/localtime" ]; then
    echo "Warning: System timezone data not found. Installing tzdata..."
    # For different package managers
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update && sudo apt-get install -y tzdata
    elif command -v yum >/dev/null 2>&1; then
        sudo yum install -y tzdata
    elif command -v brew >/dev/null 2>&1; then
        brew install tzdata
    else
        echo "Warning: Could not install tzdata automatically. You may encounter timezone errors."
    fi
fi

# Change to the root directory
cd "$ROOT_DIR"

# Run the Python script with the specified mode
echo "Running: python \"$PYTHON_SCRIPT\" $MODE"
python "$PYTHON_SCRIPT" "$MODE"

if [ $? -ne 0 ]; then
    echo "Error: Python script failed with exit code $?"
    exit $?
fi

# Archive the results if archiving is enabled
if [ "$ARCHIVE_FLAG" = "true" ]; then
    echo ""
    echo "Creating archive of downloaded files..."

    # Create timestamp for archive filename
    timestamp=$(date +"%Y%m%d_%H%M%S")
    ARCHIVE_NAME="sp500_filings_${MODE}_${timestamp}.tar"

    if [ -d "sp500_filings_data" ]; then
        if command -v tar >/dev/null 2>&1; then
            tar -cf "$ARCHIVE_NAME" sp500_filings_data/
            if [ $? -eq 0 ]; then
                echo "Archive created successfully: $ARCHIVE_NAME"
            else
                echo "Warning: Failed to create archive"
            fi
        else
            # Fallback to Python for creating tar archive
            python -c "
import tarfile
import sys
try:
    with tarfile.open('$ARCHIVE_NAME', 'w') as tar:
        tar.add('sp500_filings_data', arcname='sp500_filings_data')
    print('Successfully created archive: $ARCHIVE_NAME')
except Exception as e:
    print(f'Warning: Failed to create archive: {e}')
    sys.exit(1)
"
            if [ $? -eq 0 ]; then
                echo "Archive created successfully: $ARCHIVE_NAME"
            else
                echo "Warning: Failed to create archive"
            fi
        fi
    else
        echo "Warning: sp500_filings_data directory not found. Skipping archiving."
    fi
fi

echo ""
echo "SEC filings download completed successfully!"
exit 0