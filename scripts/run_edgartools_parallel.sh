#!/bin/bash

# run_edgartools_parallel.sh
# Shell script to run the parallel SEC filings downloader for WSL
#
# Usage examples:
#   ./run_edgartools_parallel.sh test
#   ./run_edgartools_parallel.sh sp500 8
#   ./run_edgartools_parallel.sh custom 4 my_companies.json

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [test|sp500|custom] [num_workers] [batch_size] [custom_file]"
    echo ""
    echo "Modes:"
    echo "  test    - Download filings for first 10 companies from sp500.json"
    echo "  sp500   - Download filings for all companies in sp500.json"
    echo "  custom  - Download filings for companies in custom JSON file"
    echo ""
    echo "Parameters:"
    echo "  num_workers  - Number of parallel workers (optional, default: CPU count)"
    echo "  batch_size   - Companies per archive batch (optional, default: 12)"
    echo "                 Use large number (e.g., 999999) to disable archiving"
    echo "  custom_file  - Path to custom JSON file (required if mode is 'custom')"
    echo ""
    echo "Examples:"
    echo "  $0 test"
    echo "  $0 sp500 8"
    echo "  $0 sp500 8 6"
    echo "  $0 sp500 8 999999          # Disable archiving"
    echo "  $0 custom 4 my_companies.json"
    echo "  $0 custom 4 12 my_companies.json"
}

# Check if script is run from WSL
check_wsl() {
    if [[ ! -f /proc/version ]] || ! grep -qi microsoft /proc/version; then
        print_warning "This script is designed for WSL (Windows Subsystem for Linux)"
        print_warning "It may work on native Linux but is optimized for WSL"
    else
        print_info "Running in WSL environment"
    fi
}

# Check if required files exist
check_dependencies() {
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local python_script="$script_dir/edgartools_runner_parallel.py"

    if [[ ! -f "$python_script" ]]; then
        print_error "Python script not found: $python_script"
        exit 1
    fi

    # Check if sp500.json exists (for test and sp500 modes)
    if [[ "$mode" != "custom" ]]; then
        local sp500_file="$(dirname "$script_dir")/sp500.json"
        if [[ ! -f "$sp500_file" ]]; then
            print_error "sp500.json not found: $sp500_file"
            print_error "Please ensure sp500.json exists in the project root directory"
            exit 1
        fi
        print_info "Found sp500.json: $sp500_file"
    fi

    print_success "All dependencies found"
}

# Check if Python and required packages are available
check_python() {
    if ! command -v python3 &> /dev/null; then
        print_error "python3 is not installed or not in PATH"
        exit 1
    fi

    print_info "Python version: $(python3 --version)"

    # Try to import required packages
    if ! python3 -c "import edgar, tzdata, json, threading, queue, concurrent.futures, multiprocessing" 2>/dev/null; then
        print_error "Required Python packages are missing"
        print_error "Please install: pip install edgartools tzdata"
        exit 1
    fi

    print_success "Python and required packages are available"
}

# Set timezone environment for WSL compatibility
set_timezone() {
    export TZ=UTC
    export PYTHONTZPATH="/usr/share/zoneinfo"

    print_info "Timezone environment set for WSL compatibility"
}

# Main execution function
run_parallel_downloader() {
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local python_script="$script_dir/edgartools_runner_parallel.py"
    local project_root="$(dirname "$script_dir")"

    # Change to project root directory
    cd "$project_root"
    print_info "Working directory: $(pwd)"

    # Build Python command
    local python_cmd="python3 \"$python_script\" \"$mode\""

    if [[ -n "$num_workers" ]]; then
        python_cmd="$python_cmd \"$num_workers\""
    fi

    if [[ -n "$batch_size" ]]; then
        python_cmd="$python_cmd \"$batch_size\""
    fi

    if [[ -n "$custom_file" ]]; then
        python_cmd="$python_cmd \"$custom_file\""
    fi

    print_info "Executing: $python_cmd"
    print_info "Starting parallel SEC filings download..."

    # Record start time
    start_time=$(date +%s)

    # Execute the Python script
    if eval "$python_cmd"; then
        # Calculate duration
        end_time=$(date +%s)
        duration=$((end_time - start_time))

        print_success "Parallel download completed successfully!"
        print_success "Total execution time: ${duration} seconds"
    else
        exit_code=$?
        print_error "Parallel download failed with exit code: $exit_code"
        exit $exit_code
    fi
}

# Parse command line arguments
if [[ $# -eq 0 ]]; then
    print_error "No arguments provided"
    show_usage
    exit 1
fi

mode="$1"

# Validate mode
if [[ "$mode" != "test" && "$mode" != "sp500" && "$mode" != "custom" ]]; then
    print_error "Invalid mode: $mode"
    show_usage
    exit 1
fi

# Parse optional arguments
num_workers=""
batch_size=""
custom_file=""

if [[ $# -ge 2 ]]; then
    num_workers="$2"

    # Validate num_workers is a positive integer
    if ! [[ "$num_workers" =~ ^[1-9][0-9]*$ ]]; then
        print_error "Number of workers must be a positive integer: $num_workers"
        exit 1
    fi
fi

if [[ $# -ge 3 ]]; then
    if [[ "$mode" == "custom" ]]; then
        # For custom mode, arg 3 could be batch_size (if 4 args) or custom_file (if 3 args)
        if [[ $# -ge 4 ]]; then
            # 4+ args: custom mode batch_size custom_file
            batch_size="$3"
            custom_file="$4"

            # Validate batch_size is a positive integer
            if ! [[ "$batch_size" =~ ^[1-9][0-9]*$ ]]; then
                print_error "Batch size must be a positive integer: $batch_size"
                exit 1
            fi
            print_info "Using batch size: $batch_size"
        else
            # 3 args: custom mode custom_file (default batch_size)
            custom_file="$3"
        fi

        if [[ ! -f "$custom_file" ]]; then
            print_error "Custom JSON file not found: $custom_file"
            exit 1
        fi
        print_info "Using custom file: $custom_file"
    else
        # Non-custom mode: arg 3 is batch_size
        batch_size="$3"

        # Validate batch_size is a positive integer
        if ! [[ "$batch_size" =~ ^[1-9][0-9]*$ ]]; then
            print_error "Batch size must be a positive integer: $batch_size"
            exit 1
        fi
        print_info "Using batch size: $batch_size"
    fi
fi

if [[ "$mode" == "custom" ]]; then
    if [[ $# -lt 3 ]]; then
        print_error "Custom mode requires a JSON file path"
        show_usage
        exit 1
    fi
fi

# Main execution
print_info "Starting parallel SEC filings downloader"
print_info "Mode: $mode"

if [[ -n "$num_workers" ]]; then
    print_info "Workers: $num_workers"
else
    print_info "Workers: Auto-detect (CPU count)"
fi

# Run all checks
check_wsl
check_dependencies
check_python
set_timezone

# Run the downloader
run_parallel_downloader

print_success "Script execution completed!"