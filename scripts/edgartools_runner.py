#!/usr/bin/env python3
"""
SEC Filings Download Runner Script

This script downloads SEC filings (10-K and 10-Q) for S&P 500 companies.
It supports two modes:
- test: Downloads filings for the first 10 companies
- full: Downloads filings for all companies in sp500.json
"""

import json
import sys
import os
from datetime import datetime

# Fix timezone issues on Windows BEFORE importing Edgar
if os.name == 'nt':  # Windows
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


def download_sec_filings(ticker, forms, years_to_fetch, identity="Your Name your.email@example.com"):
    """
    Downloads SEC filings (10-K and 10-Q) for a given ticker and range of years.

    Args:
        ticker (str): The stock ticker symbol (e.g., "MSFT").
        forms (list): A list of forms to download (e.g., ["10-K", "10-Q"]).
        years_to_fetch (int): The number of historical years to fetch filings for.
        identity (str): Your identity for the SEC (required for EDGAR access).
                        Format: "Your Name your.email@example.com".
    """
    # Configuration
    FORMS = forms
    YEARS_TO_FETCH = years_to_fetch
    ROOT_DIR = "sp500_filings_data"  # Root directory for downloads

    # Create the root directory if it doesn't exist
    if not os.path.exists(ROOT_DIR):
        os.makedirs(ROOT_DIR)
        print(f"Created download directory: {os.path.abspath(ROOT_DIR)}")
    else:
        print(f"Using download directory: {os.path.abspath(ROOT_DIR)}")

    # Set identity for the SEC
    set_identity(identity)

    # Define the years to loop through
    current_year = datetime.now().year
    years = range(current_year - YEARS_TO_FETCH + 1, current_year + 1)

    # Initialize the Company object
    try:
        company = Company(ticker)
    except Exception as e:
        print(f"Error initializing company {ticker}: {e}")
        return False

    print(f"Fetching filings for {ticker} for the years: {list(years)}")

    # Loop through each year, get filings, and save them
    for year in years:
        print(f"\n--- Processing year: {year} ---")
        try:
            # Get all filings for the specified year and forms
            filings = company.get_filings(form=FORMS, year=year)
            print(f"Found {len(filings)} filings for {year}.")

            for filing in filings:
                filing_year = filing.filing_date.year
                # Check to ensure we only save filings from the target year
                if filing_year != year:
                    continue

                form_type = filing.form

                # Create the directory structure: e.g., "sp500_10y/MSFT/2024/10-Q/"
                dir_path = os.path.join(ROOT_DIR, ticker, str(filing_year), form_type)
                os.makedirs(dir_path, exist_ok=True)

                # Use the unique accession number for the filename
                accession_no = filing.accession_number
                file_path = os.path.join(dir_path, f"{accession_no}.html")

                # Download and save the filing's HTML if it doesn't already exist
                if not os.path.exists(file_path):
                    print(f"  -> Downloading {form_type} filed on {filing.filing_date.strftime('%Y-%m-%d')} to {file_path}")
                    try:
                        html_content = filing.html()
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(html_content)
                    except Exception as e:
                        print(f"      Could not download {file_path}. Error: {e}")
                else:
                    print(f"  -> Skipping {file_path} (already exists).")
        except Exception as e:
            print(f"Could not retrieve filings for {year}. Error: {e}")

    print(f"Completed downloading filings for {ticker}")
    return True


def main():
    """Main function to run the SEC filings download process."""

    if len(sys.argv) != 2:
        print("Usage: python edgartools_runner.py [test|full]")
        print("  test: Download filings for first 10 companies from sp500.json")
        print("  full: Download filings for all companies in sp500.json")
        sys.exit(1)

    mode = sys.argv[1].lower()

    if mode not in ['test', 'full']:
        print("Error: Invalid mode. Use 'test' or 'full'.")
        sys.exit(1)

    # Load the S&P 500 data from the JSON file
    try:
        with open('sp500.json', 'r') as f:
            sp500_list = json.load(f)
    except FileNotFoundError:
        print("Error: sp500.json not found. Please ensure the file exists in the current directory.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in sp500.json. {e}")
        sys.exit(1)

    # Determine companies to fetch based on mode
    if mode == 'test':
        companies_to_fetch = sp500_list[:10]
        print(f"Test mode: Fetching filings for the first {len(companies_to_fetch)} S&P 500 companies...")
    else:  # full mode
        companies_to_fetch = sp500_list
        print(f"Full mode: Fetching filings for all {len(companies_to_fetch)} S&P 500 companies...")

    if not companies_to_fetch:
        print("No companies found in sp500.json or list is empty.")
        sys.exit(1)

    # Process each company
    total_companies = len(companies_to_fetch)
    successful_downloads = 0
    failed_companies = []

    for i, company in enumerate(companies_to_fetch, 1):
        ticker = company.get('Ticker')
        company_name = company.get('Company Name', 'Unknown')

        if ticker:
            print(f"\n{'='*60}")
            print(f"Processing {i}/{total_companies}: {ticker} - {company_name}")
            print(f"{'='*60}")

            try:
                success = download_sec_filings(
                    ticker=ticker,
                    forms=["10-K", "10-Q"],
                    years_to_fetch=10,
                    identity="Finapp User finapp@example.com"
                )
                if success:
                    successful_downloads += 1
                else:
                    failed_companies.append(ticker)
            except Exception as e:
                print(f"Error processing {ticker}: {e}")
                failed_companies.append(ticker)
        else:
            print(f"Skipping company with missing Ticker information: {company}")
            failed_companies.append(f"Unknown_{i}")

    # Print summary
    print(f"\n{'='*60}")
    print("DOWNLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"Total companies processed: {total_companies}")
    print(f"Successful downloads: {successful_downloads}")
    print(f"Failed downloads: {len(failed_companies)}")

    if failed_companies:
        print(f"\nFailed companies: {', '.join(failed_companies)}")

    print("\nAll SEC filings download process completed!")

    # Exit with error code if there were failures
    if failed_companies:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()