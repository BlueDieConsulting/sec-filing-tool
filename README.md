# Project Overview

This is a SEC filings data processing system for S&P 500 companies. The project downloads and processes SEC filings data using the datamule library. Data is organized chronologically by year/month and archived annually.

## Core Architecture

### Main Components
- **SEC Filings CLI** (`scripts/sec_filings_cli.py`): Main Python script for downloading and processing SEC filings
- **Batch Wrapper** (`scripts/run_sec_filings_full.bat`): Windows batch script that runs the CLI with proper environment setup
- **Archive Management**: Annual TAR compression organized by year

### Data Flow
1. Download SEC filings for S&P 500 companies using datamule API
2. Process HTML/HTM files and organize data by year/month folders (e.g., `sp500/2016_09/`)
3. Create annual archives in `archive/` directory (e.g., `archive/2024.tar`)

### Key Directories
- `sp500/`: Main output directory containing year_month folders
- `scripts/`: All Python scripts and batch files
- `work/`: Development documentation and configuration history
- `archive/`: Annual compressed archives (e.g., `2024.tar`, `2025.tar`)

## Running the Main Script

### Primary Command
The main entry point is the batch wrapper script:

```cmd
scripts\run_sec_filings_full.bat [options]
```

### Required Setup
1. Install dependencies:
```cmd
pip install -r requirements.txt
```

2. Set up API key (one of these methods):
   - Create `.env` file with `DATAMULE_API_KEY=your_key`
   - Pass via command line: `--api-key YOUR_KEY`

### Common Usage Examples

#### Full Processing from Beginning
```cmd
scripts\run_sec_filings_full.bat --api-key YOUR_KEY --start-year 2016 --start-month 9
```

#### Resume from Specific Date
```cmd
scripts\run_sec_filings_full.bat --start-year 2024 --start-month 10 --batch-size 20 --max-workers 8
```

#### Sample Mode for Testing
```cmd
scripts\run_sec_filings_full.bat --sample --sample-size 6 --batch-size 3 --start-year 2024
```

### Command Line Parameters
- `--api-key KEY`: Datamule API key (required if not in .env)
- `--start-year YYYY`: Starting year (default: 2016)
- `--start-month MM`: Starting month (default: 9)
- `--end-year YYYY`: Ending year (optional)
- `--end-month MM`: Ending month (optional)
- `--batch-size N`: Companies processed per batch (default: 20)
- `--max-workers N`: Parallel processing threads (default: 6)
- `--sample`: Enable sample mode for testing
- `--sample-size N`: Number of companies in sample mode (default: 10)
- `--output-dir DIR`: Output directory (default: sp500)
- `--archive-format FORMAT`: Archive format (tar/zip, default: auto)
- `--verbose`: Enable verbose logging

### Environment Setup
The batch script automatically handles Python environment setup:
1. Tries `conda activate $env`
2. Falls back to `conda activate finapp`
3. Falls back to `conda activate base`
4. Falls back to system Python
5. Falls back to `py` launcher

## Configuration

### Environment Variables (.env file)
```
DATAMULE_API_KEY=your_api_key
OUTPUT_DIR=sp500
MAX_WORKERS=6
```

### Parameter Files
Each run creates a JSON parameter file in the output directory containing the arguments used for that execution. These files are preserved during cleanup operations.

## Data Organization

### Output Structure
```
sp500/
├── 2016_09/        # Year_Month folders
├── 2016_10/
├── params_*.json   # Parameter files for each run
└── ...

archive/
├── 2024.tar        # Annual archives
├── 2025.tar
└── ...
```

### Processing Flow
1. **Download**: Fetch SEC filings from datamule API
2. **Process**: Extract and process HTML/HTM files only
3. **Archive**: Create annual TAR archives in chronological order

## Error Handling

### Common Issues
1. **"EnvironmentNameNotFound: Could not find conda environment: finapp"**
   - Solution: The script will automatically try other environments or use system Python

2. **File lock errors** (e.g., "being used by another process")
   - These have been addressed in recent updates

3. **API Rate Limiting**
   - Reduce `--max-workers` or increase `--batch-size`

### Debugging Commands
```cmd
# Test with sample data
python scripts\sec_filings_cli.py full --sample --sample-size 6 --batch-size 3 --start-year 2024

# Check dependencies
python scripts\check_dependencies.py
```

## Development Notes

### Recent Changes (from work/ directory)
- Monitor functionality has been removed
- TAR file cleanup has been disabled
- Focus on chronological processing and annual archiving
- Enhanced environment detection and fallback mechanisms
- Parameter logging for audit trail

### Testing
Always test with sample mode first:
```cmd
scripts\run_sec_filings_full.bat --sample --sample-size 5 --batch-size 3
```

## Dependencies

- **Core**: datamule, pandas, requests, python-dotenv, lxml, html5lib
- **Environment**: Python 3.9+, conda (optional)
