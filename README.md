# Binance Futures Data Downloader & Verifier

![CI](https://github.com/gptcompany/binance-history-data-downloader/actions/workflows/ci.yml/badge.svg?branch=main)
![Python](https://img.shields.io/badge/python-3.10%2B-blue?style=flat-square&logo=python)
![License](https://img.shields.io/github/license/gptcompany/binance-history-data-downloader?style=flat-square)
![Last Commit](https://img.shields.io/github/last-commit/gptcompany/binance-history-data-downloader?style=flat-square)
![Issues](https://img.shields.io/github/issues/gptcompany/binance-history-data-downloader?style=flat-square)

This project provides a unified, intelligent tool to discover, download, verify, and extract historical market data for Binance USDⓈ-M Futures. It supports various data types including klines, trades, funding rates, and more.

## Key Features

- **🔍 Smart Discovery**: Uses binary search algorithm to efficiently find earliest available data dates
- **⚡ Asynchronous Processing**: High-performance concurrent downloads using `asyncio` and `aiohttp`
- **🔐 Data Integrity**: SHA256 checksum verification for all downloaded files
- **🔄 Enhanced Error Handling**: Smart retry logic with circuit breaker pattern and exponential backoff
- **✅ Intelligent Validation**: Input validation with smart suggestions for typos and common mistakes
- **📊 Comprehensive Reporting**: Detailed logs, CSV reports, and temporal gap analysis
- **💾 Intelligent Caching**: Saves discovery results to avoid redundant API calls
- **📖 Enhanced Help System**: Detailed documentation with usage examples and data type descriptions
- **🧪 Edge Case Testing**: Comprehensive test suite with 50+ edge cases and safety timeouts

## Project Structure

```
binance-history-data-downloader/
├── src/                              # Core source code
│   ├── unified_downloader.py         # Main enhanced downloader script
│   ├── binance_downloader.py         # Legacy downloader (maintained for compatibility)
│   ├── discover_data_ranges.py       # Data discovery utility
│   ├── error_handling.py             # Enhanced error handling system
│   └── temporal_gap_detector.py      # Gap analysis and temporal validation
├── tests/                            # Comprehensive test suite
│   ├── run_edge_case_tests.sh        # Complete edge case test suite (50+ tests)
│   ├── quick_edge_test.sh            # Quick testing utility by category
│   ├── test_enhanced_features.py     # Enhanced features unit tests
│   ├── test_real_download.py         # Real-world download testing
│   ├── test_historical.py           # Historical data validation tests
│   ├── test_endpoints.py            # API endpoint testing
│   └── test_simple_downloader.py    # Basic downloader functionality tests
├── scripts/                          # Utility scripts and examples
│   └── example_usage.py              # Usage examples and demonstrations
├── docs/                             # Documentation and guides
│   ├── EDGE_CASE_TESTING.md          # Complete edge case testing guide
│   ├── ENHANCED_FEATURES.md          # Enhanced features documentation
│   ├── CRYPTO_MARKETS_CORRECTION.md  # 24/7 crypto market handling notes
│   ├── docs_clean_downloader.md      # Clean implementation documentation
│   └── migliorare_binance_downloader.md  # Improvement suggestions
├── config/                           # Configuration files
│   └── binance_config.py             # Binance API configuration
├── README.md                         # This file - comprehensive project documentation
├── requirements.txt                  # Python dependencies
├── .gitignore                        # Git ignore rules (excludes data/, downloads/, reports/)
├── data/                            # Downloaded CSV data files (git-ignored)
├── downloads/                       # Raw ZIP archives (git-ignored)
├── reports/                         # Analysis reports (git-ignored)
└── logs/                           # Application logs (git-ignored)
```

## Data Directories (Auto-created, Git-ignored)

The following directories are automatically created during execution and excluded from git:

-   `data/` - Extracted CSV data files organized by symbol/data_type/interval
-   `downloads/` - Raw ZIP archive files (can be auto-cleaned after extraction)
-   `logs/` - Timestamped application log files with detailed execution info
-   `reports/` - Analysis reports including gap analysis and missing file reports

## Supported Data Types

- **Daily Data**: klines, trades, aggTrades, bookDepth, metrics, indexPriceKlines, markPriceKlines, premiumIndexKlines
- **Monthly Data**: fundingRate
- **All Intervals**: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo

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

## Docker (Production)
```bash
# Optional: customize runtime parameters
cp .env.example .env

# Build image
docker compose build binance-sync

# Run one sync job
docker compose run --rm binance-sync
```

Data persistence is controlled by `BINANCE_DATA_ROOT` (volume mount in `docker-compose.yml`).
On this host, set `BINANCE_REPO_ROOT` and `BINANCE_DATA_ROOT` in `/etc/downloader-sync.env` and use the
`deploy/systemd/binance-sync-docker.service` unit, which reads those variables via `EnvironmentFile=`.

### Docker-First Execution (CI)
These services are intended to run **inside Docker** (CI actions launch Docker services, not systemd).
The compose service runs `scripts/run-sync-with-notify.sh`, which captures the original run summary and
sends it to Discord when `DISCORD_WEBHOOK_HISTORY` is set.

### Notifications
Healthchecks pings are emitted by `cron-wrapper.sh` (monitoring-stack).
Discord delivery is configured via environment (no hardcoded webhook). On this host, the webhook is read
from `/media/sam/1TB/.env` via `dotenvx` (use `DISCORD_WEBHOOK_HISTORY` for run results), and
`DISCORD_NOTIFY_ON_SUCCESS=1` enables per-run success alerts.
To (re)configure the Healthchecks Discord webhook on this host, run:
```bash
dotenvx run -f /media/sam/1TB/.env -- /media/sam/1TB/monitoring-stack/scripts/configure-healthchecks-discord.sh
```

## How to Use

### Enhanced Help System
View comprehensive help with detailed descriptions, examples, and data type documentation:
```bash
python src/unified_downloader.py --help
```

### Basic Usage Examples

**Default Download (BTCUSDT, BTCUSDC from 2020-01-01):**
```bash
python src/unified_downloader.py
```

**Specify Data Types and Intervals:**
```bash
python src/unified_downloader.py --data-types "klines,trades" --interval 5m --start-date 2020-01-01 --end-date 2020-01-07
```

**Download Order Book Data with Custom Date Range:**
```bash
python src/unified_downloader.py --data-types bookDepth --start-date 2023-01-01 --end-date 2023-01-31 --verbose
```

**Multiple Symbols with All Data Types:**
```bash
python src/unified_downloader.py --symbols "BTCUSDT,ETHUSDT,ADAUSDT" --interval 1h --start-date 2021-01-01
```

**Monthly Funding Rate Data:**
```bash
python src/unified_downloader.py --data-types fundingRate --start-date 2022-01-01 --end-date 2022-12-31
```

The script will:
1.  Load existing data availability from `data_availability.json` or run discovery if needed.
2.  Iterate through the specified date range for each symbol and data type.
3.  Check if the final `.csv` file exists and is valid.
4.  If not, check for the `.zip` file, verify its checksum, and download/re-download if necessary.
5.  Extract the `.zip` file and verify the resulting `.csv`.
6.  Log progress and errors.
7.  Generate a report in the `reports/` directory if any files are missing or invalid after processing.

## Input Validation & Smart Suggestions

The system provides intelligent validation with helpful error messages:

**Smart Typo Detection:**
```bash
# Input: python src/unified_downloader.py --data-types "kline,trade"
# Output: Did you mean 'klines' instead of 'kline'?
#         Did you mean 'trades' instead of 'trade'?
```

**Interval Format Assistance:**
```bash
# Input: python src/unified_downloader.py --interval "5min"
# Output: Did you mean '5m' instead of '5min'?
```

**Date Range Validation:**
```bash
# Prevents future dates, invalid formats, and impossible dates
# Automatically suggests corrections for common mistakes
```

## Edge Case Testing

Comprehensive test suite for system robustness:

**Run Complete Test Suite (50+ test cases):**
```bash
cd tests && ./run_edge_case_tests.sh
```

**Quick Category Tests:**
```bash
cd tests && ./quick_edge_test.sh validation   # Test input validation
cd tests && ./quick_edge_test.sh intervals    # Test interval formats
cd tests && ./quick_edge_test.sh dates        # Test date validation
cd tests && ./quick_edge_test.sh functional   # Test functional edge cases
```

The test suite includes timeout protection (60s per test) and comprehensive reporting with automatic result classification.

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


## CI/CD

This repository uses **GitHub Actions** for continuous integration and automated deployment.

### Testing
- **Automated Testing**: Comprehensive test suite runs on every push and pull request to `main`.
- **Platform Coverage**: Tests on Ubuntu and macOS.
- **Code Coverage**: Reports via `pytest-cov`, uploaded to Codecov (OIDC auth).

### Auto-Deploy Pipeline
On push to `main` (when `Dockerfile`, `docker-compose.yml`, `pyproject.toml`, `uv.lock`, `src/**`, `scripts/**` change):

1. **Trigger**: `trigger-progressive-deploy.yml` dispatches `binance-downloader-build` to `progressive-deploy`
2. **Build**: Progressive-deploy builds Docker image and pushes to `ghcr.io/gptcompany/binance-downloader`
3. **GitOps**: Image tag updated in `gitops/apps/binance-downloader/base/kustomization.yaml`
4. **Promotion**: Kargo promotes through dev → staging → prod

**Required secret**: `PROGRESSIVE_DEPLOY_PAT` (GitHub classic PAT with `repo` scope)

### Local Execution
The service runs nightly via systemd timer (`binance-sync-docker.timer` at 03:00):
```bash
docker compose run --rm binance-sync
```

The systemd service uses `dotenvx` to load secrets from `/media/sam/1TB/.env` (SSOT) and `cron-wrapper.sh` from `monitoring-stack` for notifications. Runtime env vars are sourced from `/etc/downloader-sync.env` (contains `BINANCE_REPO_ROOT`).

## Security Note

⚠️ **SSL Certificate Validation**: The script uses permissive SSL settings (`ssl.CERT_NONE`) to handle certificate issues with Binance's API. This is a potential security risk in untrusted environments.

## Logs and Reports

-   **Log files**: `logs/unified_downloader_YYYYMMDD_HHMMSS.log` (timestamped)
-   **Missing file reports**: `reports/missing_files_report_YYYYMMDD_HHMMSS.csv`
-   **Discovery cache**: `data_availability.json` (auto-generated and updated)

## Data Quality Analysis

### Gap Analysis Results (September 2025)

**Dataset Completeness:**
- **ETHUSDT & BTCUSDT**: 97-99% complete across all data types
- **Total files**: ~9,000 expected, 15 missing (0.17% gap rate)

**Critical Gap Assessment:**

| Data Type | Coverage | Gap Status | Production Ready |
|-----------|----------|------------|------------------|
| Trades | 99.95% | ✅ No timeline gaps | ✅ Excellent |
| Metrics | 99.93% | ✅ End-of-timeline only | ✅ Excellent |
| Funding Rate | 97.1% | ✅ Recent months only | ✅ Good |
| BookDepth | 99.6-99.7% | ⚠️ 5 historical gaps | ✅ Good with handling |

**Historical Gaps (404 on Binance servers):**
- ETHUSDT/BTCUSDT BookDepth: 2023-02-08, 2023-02-09
- BTCUSDT BookDepth: 2024-04-18

**Recommendation:** Dataset is production-ready for algorithmic trading. Use forward-fill for BookDepth gaps or exclude affected dates from backtesting.

## Performance Considerations

- Discovery phase uses binary search: O(log n) complexity vs O(n) linear search
- Asynchronous downloads can saturate bandwidth - adjust `MAX_CONCURRENT_DOWNLOADS` as needed
- Large files are streamed in 8KB chunks to minimize memory usage
- Progress bars available if `tqdm` is installed (optional dependency)
