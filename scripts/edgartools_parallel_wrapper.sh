#!/bin/bash

# edgartools_parallel_wrapper.sh
# Wrapper script for easy execution of the parallel SEC filings downloader
# This script provides convenient shortcuts and additional functionality

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
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

print_header() {
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

# Function to show usage
show_usage() {
    print_header "SEC Filings Parallel Downloader Wrapper"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "COMMANDS:"
    echo "  test [workers] [batch_size]           - Quick test with first 10 companies"
    echo "  sp500 [workers] [batch_size]          - Download all S&P 500 companies"
    echo "  custom <file> [workers] [batch_size]  - Download companies from custom JSON file"
    echo "  status                                - Show download status and statistics"
    echo "  clean                                 - Clean up temporary files and logs"
    echo "  help                                  - Show this help message"
    echo ""
    echo "OPTIONS:"
    echo "  workers                  - Number of parallel workers (default: CPU count)"
    echo "  batch_size               - Companies per archive batch (default: 12)"
    echo ""
    echo "EXAMPLES:"
    echo "  $0 test                    - Test with auto-detected workers and batch size"
    echo "  $0 test 4                  - Test with 4 workers, default batch size"
    echo "  $0 test 4 6                - Test with 4 workers, batch size 6"
    echo "  $0 sp500 8                 - Download all S&P 500 with 8 workers"
    echo "  $0 sp500 8 12              - Download with 8 workers, batch size 12"
    echo "  $0 custom my_list.json 6   - Custom list with 6 workers"
    echo "  $0 custom my_list.json 6 8 - Custom list with 6 workers, batch size 8"
    echo "  $0 status                  - Show current status"
    echo "  $0 clean                   - Clean up files"
    echo ""
    echo "LOG FILES:"
    echo "  Logs are saved to: logs/edgartools_parallel_YYYYMMDD_HHMMSS.log"
    echo ""
}

# Function to create log directory
setup_logging() {
    local log_dir="logs"
    if [[ ! -d "$log_dir" ]]; then
        mkdir -p "$log_dir"
        print_info "Created log directory: $log_dir"
    fi

    # Generate log filename with timestamp
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    LOG_FILE="$log_dir/edgartools_parallel_$timestamp.log"
    print_info "Log file: $LOG_FILE"
}

# Function to log output
log_output() {
    if [[ -n "$LOG_FILE" ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
    fi
}

# Function to check system resources
check_system_resources() {
    print_info "Checking system resources..."

    # Check available memory
    if command -v free &> /dev/null; then
        local mem_info=$(free -h | grep '^Mem:')
        print_info "Memory: $mem_info"
        log_output "Memory: $mem_info"
    fi

    # Check CPU count
    local cpu_count=$(nproc 2>/dev/null || echo "unknown")
    print_info "CPU cores: $cpu_count"
    log_output "CPU cores: $cpu_count"

    # Check disk space
    if command -v df &> /dev/null; then
        local disk_info=$(df -h . | tail -n 1)
        print_info "Disk space (current dir): $disk_info"
        log_output "Disk space: $disk_info"
    fi
}

# Function to show download status
show_status() {
    print_header "Download Status"

    local data_dir="sp500_filings_data"

    if [[ ! -d "$data_dir" ]]; then
        print_warning "No download directory found: $data_dir"
        return
    fi

    local total_companies=$(find "$data_dir" -maxdepth 1 -type d | wc -l)
    total_companies=$((total_companies - 1))  # Subtract the parent directory

    local total_files=$(find "$data_dir" -name "*.html" -type f | wc -l)
    local total_size=$(du -sh "$data_dir" 2>/dev/null | cut -f1)

    print_info "Companies with data: $total_companies"
    print_info "Total HTML files: $total_files"
    print_info "Total size: $total_size"

    # Show runs/archives information
    local runs_dir="runs"
    if [[ -d "$runs_dir" ]]; then
        print_info "Archive runs:"
        local run_count=0
        for run_dir in "$runs_dir"/run_*; do
            if [[ -d "$run_dir" ]]; then
                run_count=$((run_count + 1))
                local run_name=$(basename "$run_dir")
                local archive_count=$(find "$run_dir" -name "*.tar" -type f | wc -l)
                local archive_size=$(du -sh "$run_dir" 2>/dev/null | cut -f1)
                echo "  $run_name: $archive_count archives, $archive_size"
            fi
        done
        if [[ $run_count -eq 0 ]]; then
            print_info "No archive runs found"
        fi
    else
        print_info "No runs directory found"
    fi

    # Show recent activity
    print_info "Recent files (last 10):"
    find "$data_dir" -name "*.html" -type f -printf '%T@ %p\n' 2>/dev/null | \
        sort -n | tail -10 | \
        while read timestamp file; do
            local date_str=$(date -d "@$timestamp" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "unknown")
            echo "  $date_str - $(basename "$file")"
        done
}

# Function to clean up files
clean_files() {
    print_header "Cleaning Up Files"

    # Clean old log files (older than 7 days)
    if [[ -d "logs" ]]; then
        local old_logs=$(find logs -name "*.log" -type f -mtime +7 2>/dev/null | wc -l)
        if [[ $old_logs -gt 0 ]]; then
            print_info "Removing $old_logs old log files..."
            find logs -name "*.log" -type f -mtime +7 -delete 2>/dev/null || true
        fi
    fi

    # Clean Python cache
    find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find . -name "*.pyc" -type f -delete 2>/dev/null || true

    print_success "Cleanup completed"
}

# Function to validate JSON file
validate_json_file() {
    local json_file="$1"

    if [[ ! -f "$json_file" ]]; then
        print_error "JSON file not found: $json_file"
        return 1
    fi

    # Check if it's valid JSON
    if ! python3 -c "import json; json.load(open('$json_file'))" 2>/dev/null; then
        print_error "Invalid JSON file: $json_file"
        return 1
    fi

    # Check if it contains company data
    local company_count=$(python3 -c "import json; data=json.load(open('$json_file')); print(len(data))" 2>/dev/null || echo "0")
    print_info "Found $company_count companies in $json_file"

    return 0
}

# Function to run the main script
run_downloader() {
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local main_script="$script_dir/run_edgartools_parallel.sh"

    if [[ ! -f "$main_script" ]]; then
        print_error "Main script not found: $main_script"
        exit 1
    fi

    # Make sure the script is executable
    chmod +x "$main_script"

    print_info "Executing main downloader script..."
    print_info "Progress will be shown below. Detailed logs saved to: $LOG_FILE"
    log_output "Starting download with arguments: $*"

    # Run the main script and capture output
    # Use tee to both display and log, but allow progress bar to work properly
    if "$main_script" "$@" 2>&1 | tee -a "$LOG_FILE"; then
        print_success "Download completed successfully!"
        log_output "Download completed successfully"

        # Show final log location
        echo ""
        print_info "Detailed logs available at: $LOG_FILE"
    else
        local exit_code=$?
        print_error "Download failed with exit code: $exit_code"
        log_output "Download failed with exit code: $exit_code"
        print_info "Check detailed logs at: $LOG_FILE"
        exit $exit_code
    fi
}

# Main script execution
if [[ $# -eq 0 ]]; then
    show_usage
    exit 0
fi

command="$1"
shift  # Remove the command from arguments

case "$command" in
    "test")
        print_header "SEC Filings Test Download"
        setup_logging
        check_system_resources
        run_downloader "test" "$@"
        ;;

    "sp500")
        print_header "SEC Filings Full S&P 500 Download"
        setup_logging
        check_system_resources
        run_downloader "sp500" "$@"
        ;;

    "custom")
        if [[ $# -eq 0 ]]; then
            print_error "Custom mode requires a JSON file path"
            show_usage
            exit 1
        fi

        json_file="$1"
        shift

        print_header "SEC Filings Custom Download"

        if ! validate_json_file "$json_file"; then
            exit 1
        fi

        setup_logging
        check_system_resources
        run_downloader "custom" "$json_file" "$@"
        ;;

    "status")
        show_status
        ;;

    "clean")
        clean_files
        ;;

    "help"|"-h"|"--help")
        show_usage
        ;;

    *)
        print_error "Unknown command: $command"
        show_usage
        exit 1
        ;;
esac