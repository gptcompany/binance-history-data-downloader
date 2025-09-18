# Binance Futures Data Downloader & Verifier

This project provides a unified, intelligent tool to discover, download, verify, and extract historical market data for Binance USDⓈ-M Futures. It supports various data types including klines, trades, funding rates, and more.

## Key Features

- **🔍 Smart Discovery**: Uses binary search algorithm to efficiently find earliest available data dates
- **⚡ Asynchronous Processing**: High-performance concurrent downloads using `asyncio` and `aiohttp`
- **🔐 Data Integrity**: SHA256 checksum verification for all downloaded files
- **🔄 Robust Retry Logic**: Exponential backoff for network failures and configurable retry attempts  
- **📊 Comprehensive Reporting**: Detailed logs and CSV reports of missing/invalid files
- **💾 Intelligent Caching**: Saves discovery results to avoid redundant API calls

## Project Structure

-   `unified_downloader.py` - Main script with integrated discovery, download, verification, and reporting
-   `binance_downloader.py` - Legacy downloader (superseded by unified version)
-   `discover_data_ranges.py` - Standalone discovery utility (functionality integrated into unified script)
-   `data_availability.json` - Cached discovery results for earliest available dates per symbol/data type
-   `requirements.txt` - Python dependencies

## Directory Structure

-   `downloads/` - Contains downloaded raw `.zip` archive files, organized by symbol/type/interval. (Can be cleaned up automatically by the script).
-   `data/` - Contains the extracted `.csv` data files, organized by symbol/type/interval.
-   `logs/` - Contains timestamped application log files.
-   `reports/` - Contains `.csv` reports detailing any missing or invalid files found during the process.

## Supported Data Types

- **Daily Data**: klines, trades, aggTrades, bookDepth, metrics, indexPriceKlines, markPriceKlines, premiumIndexKlines
- **Monthly Data**: fundingRate
- **Configurable Intervals**: Default 1-minute klines (configurable in script constants)

## Technical Architecture

- **Binary Search Discovery**: Efficiently locates earliest available data without checking every date
- **Concurrent Processing**: Configurable semaphore limits (default: 5 concurrent downloads)
- **SSL Configuration**: Uses permissive SSL settings for API compatibility ⚠️ *Note: Certificate validation disabled*
- **Memory Efficient**: Streams large files in chunks to minimize memory usage
- **Automatic Cleanup**: Optionally removes ZIP files after successful extraction

## Getting Started

1.  **Setup Environment:** Create and activate a Python virtual environment (Python 3.8+ recommended).
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Run the Downloader:** Execute the `unified_downloader.py` script. See "How to Use" for options.

## How to Use

Run the script from your terminal. You can specify symbols and a start date.

**Basic Usage (Defaults: BTCUSDT, BTCUSDC starting from 2020-01-01):**
```bash
python unified_downloader.py
```

**Specify Symbols:**
```bash
python unified_downloader.py --symbols BTCUSDT,ETHUSDT,BNBBTC
```

**Specify Start Date (YYYY-MM-DD):**
```bash
python unified_downloader.py --start-date 2022-01-01
```

**Specify Symbols and Start Date:**
```bash
python unified_downloader.py --symbols ETHUSDT --start-date 2021-06-15
```

**Enable Verbose Logging:**
```bash
python unified_downloader.py --verbose
```

The script will:
1.  Load existing data availability from `data_availability.json` or run discovery if needed.
2.  Iterate through the specified date range for each symbol and data type.
3.  Check if the final `.csv` file exists and is valid.
4.  If not, check for the `.zip` file, verify its checksum, and download/re-download if necessary.
5.  Extract the `.zip` file and verify the resulting `.csv`.
6.  Log progress and errors.
7.  Generate a report in the `reports/` directory if any files are missing or invalid after processing.

## Configuration & Constants

Key configurable parameters in `unified_downloader.py`:

```python
# Download Settings
MAX_CONCURRENT_DOWNLOADS = 5        # Concurrent download limit
MAX_DOWNLOAD_RETRIES = 2            # Retry attempts after initial failure
CHECKSUM_VERIFICATION = True        # Enable/disable checksum verification
CLEAN_UP_ZIPS = True               # Auto-delete ZIPs after extraction
VERIFY_EXTRACTED_FILES = True      # Validate extracted CSV files

# Discovery Settings  
DISCOVERY_RETRY_COUNT = 3          # API request retries for discovery
DISCOVERY_RATE_LIMIT_DELAY = 0.2   # Delay between discovery requests
```

## Security Note

⚠️ **SSL Certificate Validation**: The script uses permissive SSL settings (`ssl.CERT_NONE`) to handle certificate issues with Binance's API. This is a potential security risk in untrusted environments.

## Logs and Reports

-   **Log files**: `logs/unified_downloader_YYYYMMDD_HHMMSS.log` (timestamped)
-   **Missing file reports**: `reports/missing_files_report_YYYYMMDD_HHMMSS.csv`
-   **Discovery cache**: `data_availability.json` (auto-generated and updated)

## Performance Considerations

- Discovery phase uses binary search: O(log n) complexity vs O(n) linear search
- Asynchronous downloads can saturate bandwidth - adjust `MAX_CONCURRENT_DOWNLOADS` as needed
- Large files are streamed in 8KB chunks to minimize memory usage
- Progress bars available if `tqdm` is installed (optional dependency)