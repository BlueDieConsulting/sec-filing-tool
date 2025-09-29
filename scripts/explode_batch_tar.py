#!/usr/bin/env python3
"""
Batch Tar File Exploder Script

Extracts batch-based tar archives from runs directories into a specified output directory.
Designed for the parallel edgartools runner archives (sp500_filings_data_X.tar format).

Usage:
    python explode_batch_tar.py --help
    python explode_batch_tar.py --run-dir runs/run_20250929_013730 --output extracted_run
    python explode_batch_tar.py --run-dir runs/run_20250929_013730 --batch 1 --output extracted_batch1
    python explode_batch_tar.py --run-dir runs/run_20250929_013730 --all --output extracted_all
"""

import argparse
import os
import tarfile
import logging
import shutil
from pathlib import Path
from typing import List, Optional


def setup_logging(verbose: bool = False):
    """Configure logging based on verbosity level."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def find_batch_archives(run_dir: str, batch_num: Optional[int] = None) -> List[str]:
    """
    Find batch archive files in the run directory.

    Args:
        run_dir: Directory containing batch archive files
        batch_num: Specific batch number to find, or None for all batches

    Returns:
        List of archive file paths sorted by batch number
    """
    archive_files = []
    run_path = Path(run_dir)

    if not run_path.exists():
        logging.error(f"Run directory does not exist: {run_dir}")
        return []

    # Look for sp500_filings_data_X.tar files
    pattern = 'sp500_filings_data_*.tar'

    for file_path in run_path.glob(pattern):
        filename = file_path.stem  # filename without extension

        # Extract batch number from filename like 'sp500_filings_data_1'
        if filename.startswith('sp500_filings_data_'):
            try:
                batch_str = filename.split('_')[-1]  # Get the last part after underscore
                file_batch = int(batch_str)

                if batch_num is None or file_batch == batch_num:
                    archive_files.append((file_batch, str(file_path)))
            except ValueError:
                logging.warning(f"Could not parse batch number from filename: {filename}")
                continue

    # Sort by batch number
    archive_files.sort(key=lambda x: x[0])
    return [path for _, path in archive_files]


def inspect_tar_file(tar_path: str) -> bool:
    """
    Inspect the contents of a tar file without extracting.

    Args:
        tar_path: Path to the tar file

    Returns:
        True if inspection succeeded
    """
    try:
        logging.info(f"Inspecting {os.path.basename(tar_path)}...")

        # Check file size
        file_size = os.path.getsize(tar_path)
        logging.info(f"  File size: {file_size:,} bytes ({file_size / (1024*1024):.1f} MB)")

        if file_size == 0:
            logging.warning(f"  Tar file is empty (0 bytes)")
            return False

        with tarfile.open(tar_path, 'r') as tar:
            # Get list of members
            members = tar.getmembers()
            total_members = len(members)
            total_files = len([m for m in members if m.isfile()])
            total_dirs = len([m for m in members if m.isdir()])

            logging.info(f"  Contents: {total_members} total members ({total_files} files, {total_dirs} directories)")

            # Show top-level directories (companies)
            top_level_dirs = set()
            for member in members:
                if member.isdir():
                    # Get first level directory
                    parts = member.name.split('/')
                    if len(parts) >= 2 and parts[0] == 'sp500_filings_data':
                        top_level_dirs.add(parts[1])

            if top_level_dirs:
                companies = sorted(list(top_level_dirs))
                logging.info(f"  Companies: {len(companies)} ({', '.join(companies[:5])}{'...' if len(companies) > 5 else ''})")

        return True

    except Exception as e:
        logging.error(f"Failed to inspect {tar_path}: {e}")
        return False


def extract_tar_file(tar_path: str, output_dir: str) -> bool:
    """
    Extract a tar file to the output directory.

    Args:
        tar_path: Path to the tar file
        output_dir: Directory to extract files to

    Returns:
        True if extraction succeeded
    """
    try:
        batch_name = Path(tar_path).stem  # Get filename without extension
        logging.info(f"Extracting {batch_name}...")

        # Create output directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        with tarfile.open(tar_path, 'r') as tar:
            # Extract all files
            tar.extractall(path=output_dir)

        logging.info(f"  Successfully extracted to {output_dir}")
        return True

    except Exception as e:
        logging.error(f"Failed to extract {tar_path}: {e}")
        return False


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Extract batch-based tar archives from runs directories',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract all batches from a run
  python explode_batch_tar.py --run-dir runs/run_20250929_013730 --all --output extracted_all

  # Extract specific batch
  python explode_batch_tar.py --run-dir runs/run_20250929_013730 --batch 1 --output extracted_batch1

  # Just inspect contents without extracting
  python explode_batch_tar.py --run-dir runs/run_20250929_013730 --inspect

  # Verbose output
  python explode_batch_tar.py --run-dir runs/run_20250929_013730 --all --output extracted --verbose
        """
    )

    parser.add_argument('--run-dir',
                       default='runs',
                       help='Directory containing batch archive files (default: runs)')

    parser.add_argument('--output',
                       help='Output directory for extracted files (required unless using --inspect)')

    # Mutually exclusive group for batch selection
    batch_group = parser.add_mutually_exclusive_group(required=True)
    batch_group.add_argument('--batch', type=int,
                           help='Specific batch number to extract (e.g., 1)')
    batch_group.add_argument('--all', action='store_true',
                           help='Extract all available batches')
    batch_group.add_argument('--inspect', action='store_true',
                           help='Just inspect archive contents without extracting')

    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose output')

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    # Validate arguments
    if not args.inspect and not args.output:
        parser.error("--output is required unless using --inspect")

    # Find archive files
    batch_num = args.batch if args.batch else None
    archive_files = find_batch_archives(args.run_dir, batch_num)

    if not archive_files:
        logging.warning(f"No batch archive files found in {args.run_dir}")
        if args.batch:
            logging.info(f"Looking for batch {args.batch} (sp500_filings_data_{args.batch}.tar)")
        else:
            logging.info("Looking for files matching pattern: sp500_filings_data_*.tar")
        return

    logging.info(f"Found {len(archive_files)} archive file(s)")

    # Process archives
    if args.inspect:
        # Just inspect
        print(f"\nInspecting {len(archive_files)} archive file(s):")
        print("=" * 60)

        for tar_path in archive_files:
            inspect_tar_file(tar_path)
            print()

    else:
        # Extract archives
        print(f"\nExtracting {len(archive_files)} archive file(s) to: {args.output}")
        print("=" * 60)

        extracted_count = 0
        failed_count = 0

        for tar_path in archive_files:
            if extract_tar_file(tar_path, args.output):
                extracted_count += 1
            else:
                failed_count += 1

        print(f"\nExtraction Summary:")
        print(f"  Successfully extracted: {extracted_count}")
        print(f"  Failed extractions: {failed_count}")
        print(f"  Output directory: {os.path.abspath(args.output)}")

        if extracted_count > 0:
            print(f"\nExtracted files are available in: {args.output}")


if __name__ == "__main__":
    main()