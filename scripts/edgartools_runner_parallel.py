#!/usr/bin/env python3
"""
Multithreaded SEC Filings Download Runner Script for WSL

This script downloads SEC filings (10-K and 10-Q) for S&P 500 companies using multiple threads.
It supports three modes:
- test: Downloads filings for the first 10 companies
- sp500: Downloads filings for all companies in sp500.json
- custom: Downloads filings for companies specified in a custom JSON file

Key features:
- Queue-based multithreading for parallel downloads
- CPU count limiting with configurable worker count
- WSL compatibility with proper timezone handling
- Progress tracking and completion status marking
"""

import json
import sys
import os
import threading
import queue
import time
import tarfile
import shutil
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing


class ProgressBar:
    """Simple progress bar for tracking download progress."""

    def __init__(self, total, width=50, lock=None):
        self.total = total
        self.current = 0
        self.width = width
        self.lock = lock or threading.Lock()
        self.start_time = time.time()

    def update(self, completed_count=None, message=""):
        """Update progress bar."""
        with self.lock:
            if completed_count is not None:
                self.current = completed_count
            else:
                self.current += 1

            # Calculate progress
            progress = self.current / self.total if self.total > 0 else 0
            filled_length = int(self.width * progress)

            # Create progress bar
            bar = '█' * filled_length + '░' * (self.width - filled_length)

            # Calculate time estimates
            elapsed_time = time.time() - self.start_time
            if self.current > 0:
                avg_time_per_item = elapsed_time / self.current
                eta_seconds = avg_time_per_item * (self.total - self.current)
                eta_str = self._format_time(eta_seconds)
            else:
                eta_str = "calculating..."

            # Format percentage
            percentage = progress * 100

            # Print progress bar
            progress_line = f"\r[{bar}] {self.current}/{self.total} ({percentage:.1f}%) ETA: {eta_str}"
            if message:
                progress_line += f" | {message}"

            print(progress_line, end='', flush=True)

    def finish(self, message="Completed"):
        """Finish progress bar."""
        with self.lock:
            elapsed_time = time.time() - self.start_time
            elapsed_str = self._format_time(elapsed_time)
            print(f"\n{message} in {elapsed_str}")

    def _format_time(self, seconds):
        """Format time in human readable format."""
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"


class BatchArchiver:
    """Handles batched archiving of downloaded SEC filings data."""

    def __init__(self, batch_size=12, data_dir="sp500_filings_data", runs_dir="runs"):
        self.batch_size = batch_size
        self.data_dir = data_dir
        self.runs_dir = runs_dir
        self.batch_counter = 0
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Create runs directory if it doesn't exist
        os.makedirs(self.runs_dir, exist_ok=True)

        # Create run-specific directory
        self.run_dir = os.path.join(self.runs_dir, f"run_{self.timestamp}")
        os.makedirs(self.run_dir, exist_ok=True)

    def archive_batch(self, completed_companies):
        """
        Archive the current batch of companies to a tar file.

        Args:
            completed_companies (list): List of company tickers that have been processed

        Returns:
            str: Path to the created archive file
        """
        self.batch_counter += 1
        archive_name = f"sp500_filings_data_{self.batch_counter}.tar"
        archive_path = os.path.join(self.run_dir, archive_name)

        if not os.path.exists(self.data_dir):
            return None

        # Create tar archive
        with tarfile.open(archive_path, 'w') as tar:
            for ticker in completed_companies:
                ticker_dir = os.path.join(self.data_dir, ticker)
                if os.path.exists(ticker_dir):
                    # Add ticker directory to archive with relative path
                    tar.add(ticker_dir, arcname=f"sp500_filings_data/{ticker}")

        # Calculate archive size
        archive_size = os.path.getsize(archive_path) / (1024 * 1024)  # Size in MB

        return archive_path, archive_size

    def cleanup_archived_data(self, completed_companies):
        """
        Remove archived company directories from the data directory.

        Args:
            completed_companies (list): List of company tickers to clean up
        """
        for ticker in completed_companies:
            ticker_dir = os.path.join(self.data_dir, ticker)
            if os.path.exists(ticker_dir):
                shutil.rmtree(ticker_dir)

    def get_batch_info(self):
        """Get current batch information."""
        return {
            'batch_counter': self.batch_counter,
            'batch_size': self.batch_size,
            'run_dir': self.run_dir,
            'timestamp': self.timestamp
        }


# Fix timezone issues on Windows/WSL BEFORE importing Edgar
if os.name == 'nt':  # Windows/WSL
    try:
        import tzdata
        tzdata_path = os.path.dirname(tzdata.__file__)
        zoneinfo_path = os.path.join(tzdata_path, 'zoneinfo')

        if os.path.exists(zoneinfo_path):
            os.environ['PYTHONTZPATH'] = zoneinfo_path
        else:
            os.environ['PYTHONTZPATH'] = tzdata_path

        # Also set TZ for UTC fallback
        os.environ['TZ'] = 'UTC'

        print(f"Timezone fix applied - PYTHONTZPATH: {os.environ.get('PYTHONTZPATH')}")
    except ImportError:
        print("Warning: Could not import tzdata package. Timezone issues may occur.")
        pass

# Now import Edgar tools
try:
    from edgar import set_identity, Company
except Exception as e:
    print(f"Error importing Edgar tools: {e}")
    print("Please ensure edgartools and tzdata are properly installed.")
    sys.exit(1)


class CompanyDownloadWorker:
    """Worker class for downloading SEC filings for individual companies."""

    def __init__(self, worker_id, progress_lock, progress_logger=None):
        self.worker_id = worker_id
        self.progress_lock = progress_lock
        self.progress_logger = progress_logger
        self.forms = ["10-K", "10-Q"]
        self.years_to_fetch = 10
        self.identity = "Finapp User finapp@example.com"
        self.root_dir = "sp500_filings_data"

        # Ensure root directory exists
        if not os.path.exists(self.root_dir):
            os.makedirs(self.root_dir)

    def log_progress(self, message):
        """Log progress message to progress logger."""
        if self.progress_logger:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.progress_logger.write(f"[{timestamp}] [Worker {self.worker_id}] {message}\n")
            self.progress_logger.flush()

    def download_company_filings(self, company_data):
        """
        Downloads SEC filings for a single company.

        Args:
            company_data (dict): Company information with 'Ticker' and 'Company Name'

        Returns:
            tuple: (success: bool, ticker: str, message: str)
        """
        ticker = company_data.get('Ticker')
        company_name = company_data.get('Company Name', 'Unknown')

        if not ticker:
            return False, 'Unknown', "Missing ticker information"

        self.log_progress(f"Starting download for {ticker} - {company_name}")

        try:
            # Set identity for the SEC
            set_identity(self.identity)

            # Define the years to loop through
            current_year = datetime.now().year
            years = range(current_year - self.years_to_fetch + 1, current_year + 1)

            # Initialize the Company object
            try:
                company = Company(ticker)
            except Exception as e:
                error_msg = f"Error initializing company {ticker}: {e}"
                self.log_progress(error_msg)
                return False, ticker, error_msg

            # Loop through each year, get filings, and save them
            total_filings = 0
            for year in years:
                try:
                    # Get all filings for the specified year and forms
                    filings = company.get_filings(form=self.forms, year=year)

                    for filing in filings:
                        filing_year = filing.filing_date.year
                        # Check to ensure we only save filings from the target year
                        if filing_year != year:
                            continue

                        form_type = filing.form

                        # Create the directory structure: e.g., "sp500_filings_data/MSFT/2024/10-Q/"
                        dir_path = os.path.join(self.root_dir, ticker, str(filing_year), form_type)
                        os.makedirs(dir_path, exist_ok=True)

                        # Use the unique accession number for the filename
                        accession_no = filing.accession_number
                        file_path = os.path.join(dir_path, f"{accession_no}.html")

                        # Download and save the filing's HTML if it doesn't already exist
                        if not os.path.exists(file_path):
                            try:
                                html_content = filing.html()
                                with open(file_path, "w", encoding="utf-8") as f:
                                    f.write(html_content)
                                total_filings += 1
                            except Exception as e:
                                # Silent error handling to avoid interfering with progress bar
                                pass

                except Exception as e:
                    # Silent error handling to avoid interfering with progress bar
                    pass

            success_msg = f"Downloaded {total_filings} filings"
            self.log_progress(f"Completed {ticker}: {success_msg}")
            return True, ticker, success_msg

        except Exception as e:
            error_msg = f"Error processing {ticker}: {e}"
            self.log_progress(error_msg)
            return False, ticker, error_msg


def process_companies_parallel(companies_to_fetch, num_workers, batch_size=12):
    """
    Process companies in parallel using ThreadPoolExecutor with a queue-based approach.

    Args:
        companies_to_fetch (list): List of company dictionaries to process
        num_workers (int): Number of worker threads to use
        batch_size (int): Number of companies per archive batch

    Returns:
        tuple: (successful_downloads: int, failed_companies: list)
    """
    print(f"Starting parallel processing with {num_workers} workers...")

    # Thread-safe lock for progress reporting
    progress_lock = threading.Lock()

    # Initialize batch archiver
    archiver = BatchArchiver(batch_size=batch_size)
    batch_info = archiver.get_batch_info()

    # Create progress log file
    os.makedirs("logs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    progress_log_file = f"logs/download_progress_{timestamp}.log"

    # Initialize progress bar
    total_companies = len(companies_to_fetch)
    progress_bar = ProgressBar(total_companies, width=50, lock=progress_lock)

    successful_downloads = 0
    failed_companies = []
    completed_tickers = []  # Track completed companies for batching

    print(f"\nProcessing {total_companies} companies...")
    print(f"Batch size: {batch_size} companies per archive")
    print(f"Archives will be saved to: {batch_info['run_dir']}")
    print(f"Progress details logged to: {progress_log_file}")
    progress_bar.update(0, "Initializing workers...")

    # Use ThreadPoolExecutor to manage the worker threads
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Create progress logger
        with open(progress_log_file, 'w') as progress_logger:
            progress_logger.write(f"Download Progress Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            progress_logger.write(f"Total companies: {total_companies}\n")
            progress_logger.write(f"Workers: {num_workers}\n")
            progress_logger.write("="*80 + "\n\n")
            progress_logger.flush()

            # Create worker instances
            workers = [CompanyDownloadWorker(i+1, progress_lock, progress_logger) for i in range(num_workers)]

            # Submit all company download tasks
            future_to_company = {}
            for i, company in enumerate(companies_to_fetch):
                worker = workers[i % num_workers]  # Round-robin assignment
                future = executor.submit(worker.download_company_filings, company)
                future_to_company[future] = company

            # Process completed tasks as they finish
            completed = 0

            for future in as_completed(future_to_company):
                completed += 1
                company = future_to_company[future]
                ticker = company.get('Ticker', 'Unknown')

                try:
                    success, result_ticker, message = future.result()
                    if success:
                        successful_downloads += 1
                        completed_tickers.append(result_ticker)
                        status_msg = f"✓ {result_ticker}"

                        # Check if we need to archive a batch
                        if len(completed_tickers) >= batch_size:
                            # Archive the current batch
                            batch_to_archive = completed_tickers[:batch_size]
                            remaining_tickers = completed_tickers[batch_size:]

                            try:
                                archive_path, archive_size = archiver.archive_batch(batch_to_archive)

                                # Update progress bar with archiving info
                                batch_num = archiver.get_batch_info()['batch_counter']
                                progress_bar.update(completed, f"📦 Archived batch {batch_num} ({archive_size:.1f}MB)")

                                # Log archiving
                                progress_logger.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ARCHIVED: Batch {batch_num} with {len(batch_to_archive)} companies ({archive_size:.1f}MB)\n")
                                progress_logger.write(f"  Archive: {archive_path}\n")
                                progress_logger.write(f"  Companies: {', '.join(batch_to_archive)}\n")
                                progress_logger.flush()

                                # Clean up archived data
                                archiver.cleanup_archived_data(batch_to_archive)

                                # Update completed tickers list
                                completed_tickers = remaining_tickers

                            except Exception as archive_error:
                                progress_logger.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ARCHIVE ERROR: {str(archive_error)}\n")
                                progress_logger.flush()

                    else:
                        failed_companies.append(result_ticker)
                        status_msg = f"✗ {result_ticker}"

                    # Update progress bar with current company (if not already updated by archiving)
                    if "📦" not in status_msg:
                        progress_bar.update(completed, status_msg)

                    # Log to progress file
                    progress_logger.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Completed {completed}/{total_companies}: {status_msg} - {message}\n")
                    progress_logger.flush()

                except Exception as e:
                    failed_companies.append(ticker)
                    progress_bar.update(completed, f"✗ {ticker} (Exception)")
                    progress_logger.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Exception {completed}/{total_companies}: ✗ {ticker} - {str(e)}\n")
                    progress_logger.flush()

            # Archive any remaining companies in final batch (remainder when total % batch_size != 0)
            if completed_tickers:
                try:
                    archive_path, archive_size = archiver.archive_batch(completed_tickers)
                    batch_num = archiver.get_batch_info()['batch_counter']

                    progress_logger.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] FINAL BATCH (REMAINDER): Batch {batch_num} with {len(completed_tickers)} companies ({archive_size:.1f}MB)\n")
                    progress_logger.write(f"  Archive: {archive_path}\n")
                    progress_logger.write(f"  Companies: {', '.join(completed_tickers)}\n")
                    progress_logger.write(f"  Note: This is the remainder batch ({successful_downloads} % {batch_size} = {successful_downloads % batch_size} companies)\n")

                    # Clean up final batch data
                    archiver.cleanup_archived_data(completed_tickers)

                    print(f"\n📦 Final remainder batch {batch_num} archived: {len(completed_tickers)} companies ({archive_size:.1f}MB)")

                except Exception as archive_error:
                    progress_logger.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] FINAL BATCH ERROR: {str(archive_error)}\n")

            # Final log entry
            progress_logger.write(f"\n{'='*80}\n")
            progress_logger.write(f"Download Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            progress_logger.write(f"Total companies: {total_companies}\n")
            progress_logger.write(f"Successful: {successful_downloads}\n")
            progress_logger.write(f"Failed: {len(failed_companies)}\n")
            progress_logger.write(f"Batches created: {archiver.get_batch_info()['batch_counter']}\n")
            progress_logger.write(f"Archive directory: {batch_info['run_dir']}\n")
            if failed_companies:
                progress_logger.write(f"Failed companies: {', '.join(failed_companies)}\n")

    # Finish progress bar
    progress_bar.finish(f"Parallel processing completed")

    return successful_downloads, failed_companies


def main():
    """Main function to run the parallel SEC filings download process."""

    if len(sys.argv) < 2:
        print("Usage: python edgartools_runner_parallel.py [test|sp500|custom] [num_workers] [batch_size] [custom_file]")
        print("  test: Download filings for first 10 companies from sp500.json")
        print("  sp500: Download filings for all companies in sp500.json")
        print("  custom: Download filings for companies in custom JSON file")
        print("  num_workers: Number of parallel workers (optional, default: CPU count)")
        print("  batch_size: Number of companies per archive batch (optional, default: 12)")
        print("  custom_file: Path to custom JSON file (required if mode is 'custom')")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode not in ['test', 'sp500', 'custom']:
        print("Error: Invalid mode. Use 'test', 'sp500', or 'custom'.")
        sys.exit(1)

    # Determine number of workers
    cpu_count = multiprocessing.cpu_count()
    if len(sys.argv) >= 3:
        try:
            num_workers = int(sys.argv[2])
            if num_workers <= 0:
                raise ValueError("Number of workers must be positive")
            if num_workers > cpu_count:
                print(f"Warning: Requested {num_workers} workers, but only {cpu_count} CPUs available")
        except ValueError as e:
            print(f"Error: Invalid number of workers. {e}")
            sys.exit(1)
    else:
        num_workers = cpu_count

    # Determine batch size
    batch_size = 12  # Default batch size
    if len(sys.argv) >= 4:
        try:
            batch_size = int(sys.argv[3])
            if batch_size <= 0:
                raise ValueError("Batch size must be positive")
        except ValueError as e:
            print(f"Error: Invalid batch size. {e}")
            sys.exit(1)

    print(f"Using {num_workers} workers (CPU count: {cpu_count})")
    print(f"Using batch size: {batch_size} companies per archive")

    # Load company data
    if mode == 'custom':
        if len(sys.argv) < 5:
            print("Error: Custom JSON file path required for custom mode")
            print("Usage for custom mode: python script.py custom [num_workers] [batch_size] <json_file>")
            sys.exit(1)
        json_file = sys.argv[4]
    else:
        json_file = 'sp500.json'

    try:
        with open(json_file, 'r') as f:
            sp500_list = json.load(f)
    except FileNotFoundError:
        print(f"Error: {json_file} not found. Please ensure the file exists in the current directory.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {json_file}. {e}")
        sys.exit(1)

    # Determine companies to fetch based on mode
    if mode == 'test':
        companies_to_fetch = sp500_list[:10]
        print(f"Test mode: Fetching filings for the first {len(companies_to_fetch)} companies...")
    elif mode == 'sp500':
        companies_to_fetch = sp500_list
        print(f"SP500 mode: Fetching filings for all {len(companies_to_fetch)} S&P 500 companies...")
    else:  # custom
        companies_to_fetch = sp500_list
        print(f"Custom mode: Fetching filings for {len(companies_to_fetch)} companies from {json_file}...")

    if not companies_to_fetch:
        print(f"No companies found in {json_file} or list is empty.")
        sys.exit(1)

    # Record start time
    start_time = time.time()

    # Process companies in parallel
    total_companies = len(companies_to_fetch)
    successful_downloads, failed_companies = process_companies_parallel(companies_to_fetch, num_workers, batch_size)

    # Record end time and calculate duration
    end_time = time.time()
    duration = end_time - start_time

    # Print summary
    print(f"\n{'='*60}")
    print("PARALLEL DOWNLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"Total companies processed: {total_companies}")
    print(f"Successful downloads: {successful_downloads}")
    print(f"Failed downloads: {len(failed_companies)}")
    print(f"Workers used: {num_workers}")
    print(f"Batch size: {batch_size}")
    print(f"Total duration: {duration:.2f} seconds")
    print(f"Average time per company: {duration/total_companies:.2f} seconds")

    # Show archive information
    print(f"\n📦 ARCHIVE SUMMARY:")
    runs_dir = "runs"
    if os.path.exists(runs_dir):
        run_dirs = [d for d in os.listdir(runs_dir) if d.startswith("run_")]
        if run_dirs:
            latest_run = sorted(run_dirs)[-1]
            run_path = os.path.join(runs_dir, latest_run)
            archives = [f for f in os.listdir(run_path) if f.endswith('.tar')]

            print(f"Run directory: {run_path}")
            print(f"Archives created: {len(archives)}")

            # Calculate batching info
            expected_full_batches = successful_downloads // batch_size
            remainder_companies = successful_downloads % batch_size

            print(f"Batch size: {batch_size} companies per archive")
            print(f"Full batches: {expected_full_batches} ({'×' + str(batch_size) + ' companies each' if expected_full_batches > 0 else 'none'})")
            if remainder_companies > 0:
                print(f"Remainder batch: 1 ({remainder_companies} companies)")
            else:
                print(f"Remainder batch: none (perfect fit)")

            total_archive_size = 0
            for i, archive in enumerate(sorted(archives), 1):
                archive_path = os.path.join(run_path, archive)
                size_mb = os.path.getsize(archive_path) / (1024 * 1024)
                total_archive_size += size_mb

                # Determine if this is a remainder batch
                if i == len(archives) and remainder_companies > 0:
                    print(f"  {archive}: {size_mb:.1f}MB (remainder batch - {remainder_companies} companies)")
                else:
                    print(f"  {archive}: {size_mb:.1f}MB")

            print(f"Total archive size: {total_archive_size:.1f}MB")

    if failed_companies:
        print(f"\n❌ Failed companies: {', '.join(failed_companies)}")

    print("\n✅ All SEC filings parallel download and archiving process completed!")

    # Exit with error code if there were failures
    if failed_companies:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()